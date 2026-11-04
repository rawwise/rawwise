package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.ByteArrays;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public final class ParquetRowGroupReader {

    private ParquetRowGroupReader() {}

    private static int compareBytes(byte[] left, byte[] right) {
        if (left == null || right == null) {
            return 0;
        }
        return ByteArrays.compareUnsigned(left, right);
    }

    public static ColumnChunkMetaData findKeyColumn(BlockMetaData block, String keyColumnName) {
        ColumnPath keyPath = ColumnPath.fromDotString(keyColumnName);
        for (ColumnChunkMetaData col : block.getColumns()) {
            if (col.getPath().equals(keyPath)) {
                return col;
            }
        }
        return null;
    }

    public static List<BlockMetaData> findMatchingBlocks(
            List<BlockMetaData> allBlocks, String keyColumnName, byte[] targetKey) {

        if (allBlocks.isEmpty()) {
            return Collections.emptyList();
        }

        int lo = 0, hi = allBlocks.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            BlockMetaData block = allBlocks.get(mid);
            ColumnChunkMetaData keyCol = findKeyColumn(block, keyColumnName);

            if (keyCol == null || !keyCol.getStatistics().hasNonNullValue()) {
                List<BlockMetaData> fallback = new ArrayList<>();
                fallback.add(block);
                if (mid > 0) fallback.add(allBlocks.get(mid - 1));
                if (mid < allBlocks.size() - 1) fallback.add(allBlocks.get(mid + 1));
                return fallback;
            }

            byte[] blockMin = keyCol.getStatistics().getMinBytes();
            byte[] blockMax = keyCol.getStatistics().getMaxBytes();

            if (blockMin == null || blockMax == null) {
                List<BlockMetaData> fallback = new ArrayList<>();
                fallback.add(block);
                if (mid > 0) fallback.add(allBlocks.get(mid - 1));
                if (mid < allBlocks.size() - 1) fallback.add(allBlocks.get(mid + 1));
                return fallback;
            }

            if (compareBytes(blockMax, targetKey) < 0) {
                lo = mid + 1;
            } else if (compareBytes(blockMin, targetKey) > 0) {
                hi = mid - 1;
            } else {
                List<BlockMetaData> result = new ArrayList<>();
                result.add(block);
                if (mid > 0) {
                    ColumnChunkMetaData prevCol =
                            findKeyColumn(allBlocks.get(mid - 1), keyColumnName);
                    if (prevCol != null && prevCol.getStatistics().hasNonNullValue()) {
                        byte[] prevMax = prevCol.getStatistics().getMaxBytes();
                        if (prevMax != null && compareBytes(prevMax, targetKey) >= 0) {
                            result.add(allBlocks.get(mid - 1));
                        }
                    }
                }
                if (mid < allBlocks.size() - 1) {
                    ColumnChunkMetaData nextCol =
                            findKeyColumn(allBlocks.get(mid + 1), keyColumnName);
                    if (nextCol != null && nextCol.getStatistics().hasNonNullValue()) {
                        byte[] nextMin = nextCol.getStatistics().getMinBytes();
                        if (nextMin != null && compareBytes(nextMin, targetKey) <= 0) {
                            result.add(allBlocks.get(mid + 1));
                        }
                    }
                }
                return result;
            }
        }

        return Collections.emptyList();
    }

    public static long findSkipRowsByColumnIndex(
            ParquetFileReader reader, BlockMetaData block, String keyColumnName, byte[] targetKey) {

        ColumnChunkMetaData keyCol = findKeyColumn(block, keyColumnName);
        if (keyCol == null) {
            return 0;
        }
        if (keyCol.getColumnIndexReference() == null) {
            return 0;
        }

        try {
            ColumnIndex columnIndex = reader.readColumnIndex(keyCol);
            OffsetIndex offsetIndex = reader.readOffsetIndex(keyCol);
            if (columnIndex == null) {
                return 0;
            }

            List<ByteBuffer> maxValues = columnIndex.getMaxValues();
            int pageCount = maxValues.size();
            long skipRows = 0;
            for (int i = 0; i < pageCount; i++) {
                ByteBuffer pageMaxBuf = maxValues.get(i);
                if (pageMaxBuf == null) {
                    skipRows += pageRowCount(offsetIndex, i, pageCount, block.getRowCount());
                    continue;
                }
                byte[] pageMaxBytes = new byte[pageMaxBuf.remaining()];
                pageMaxBuf.duplicate().get(pageMaxBytes);
                if (compareBytes(pageMaxBytes, targetKey) >= 0) {
                    return skipRows;
                }
                skipRows += pageRowCount(offsetIndex, i, pageCount, block.getRowCount());
            }
        } catch (Exception ignored) {
        }

        return 0;
    }

    private static long pageRowCount(
            OffsetIndex offsetIndex, int pageIdx, int totalPages, long totalRowCount) {
        if (offsetIndex == null) {
            return 0;
        }
        if (pageIdx + 1 < totalPages) {
            return offsetIndex.getFirstRowIndex(pageIdx + 1)
                    - offsetIndex.getFirstRowIndex(pageIdx);
        }
        return totalRowCount - offsetIndex.getFirstRowIndex(pageIdx);
    }

    public static RowGroupReadResult readSingleRowGroup(
            Configuration conf,
            Path path,
            int blockIndex,
            MessageType fileSchema,
            Map<String, String> extraMeta)
            throws IOException {

        return readSingleRowGroup(conf, path, blockIndex, fileSchema, extraMeta, null, null);
    }

    public static RowGroupReadResult readSingleRowGroup(
            Configuration conf,
            Path path,
            int blockIndex,
            MessageType fileSchema,
            Map<String, String> extraMeta,
            String keyColumnName,
            byte[] targetKey)
            throws IOException {

        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
        ParquetFileReader fileReader = ParquetFileReader.open(inputFile);

        BlockMetaData block = fileReader.getFooter().getBlocks().get(blockIndex);

        long columnIndexSkipRows = 0;
        if (keyColumnName != null && targetKey != null) {
            columnIndexSkipRows =
                    findSkipRowsByColumnIndex(fileReader, block, keyColumnName, targetKey);
        }

        GroupReadSupport readSupport = new GroupReadSupport();
        ReadSupport.ReadContext readContext = readSupport.init(conf, extraMeta, fileSchema);
        RecordMaterializer<Group> materializer =
                readSupport.prepareForRead(conf, extraMeta, fileSchema, readContext);

        ColumnIOFactory columnIOFactory = new ColumnIOFactory(true);
        MessageColumnIO columnIO =
                columnIOFactory.getColumnIO(readContext.getRequestedSchema(), fileSchema);

        PageReadStore pageStore = fileReader.readRowGroup(blockIndex);
        RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, materializer);

        long rowCount = block.getRowCount();

        return new RowGroupReadResult(recordReader, fileReader, rowCount, columnIndexSkipRows);
    }

    public static long computeRowGroupStartOffset(List<BlockMetaData> allBlocks, int targetIndex) {
        long offset = 0;
        for (int i = 0; i < targetIndex; i++) {
            offset += allBlocks.get(i).getRowCount();
        }
        return offset;
    }

    public record RowGroupReadResult(
            RecordReader<Group> recordReader,
            ParquetFileReader fileReader,
            long rowCount,
            long columnIndexSkipRows) {}
}
