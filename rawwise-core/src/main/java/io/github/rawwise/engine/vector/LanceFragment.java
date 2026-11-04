package io.github.rawwise.engine.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LanceFragment implements AutoCloseable {

    private static final int MAGIC = 0x4C414E43;
    private static final int FORMAT_VERSION = 2;
    private static final byte COLUMN_TYPE_STRING = 0x01;
    private static final byte COLUMN_TYPE_FIXED_SIZE_LIST_FLOAT = 0x02;
    private static final int FOOTER_SIZE = 16;
    private static final int HEADER_SIZE = 4 + 4 + 4 + 4;
    private static final long SEGMENT_SIZE = 256L * 1024L * 1024L;

    private final Path filePath;
    @Getter private final int dimension;
    private FileChannel readChannel;
    private List<MappedByteBuffer> segments;
    private long readPos;
    private long fileSize;
    @Getter private int numRows;
    private long vectorDataOffset;
    private List<String> entryKeys;
    private List<String> l0Abstracts;

    public LanceFragment(Path filePath, int dimension) {
        this.filePath = filePath;
        this.dimension = dimension;
    }

    public static LanceFragment write(Path filePath, int dimension, List<ColdVectorEntry> entries) {
        try (FileChannel channel =
                FileChannel.open(
                        filePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {

            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            header.putInt(MAGIC);
            header.putInt(FORMAT_VERSION);
            header.putInt(dimension);
            header.putInt(entries.size());
            header.flip();
            channel.write(header);

            writeStringColumn(channel, entries, ColdVectorEntry::entryKey);

            ByteBuffer vectorHeader = ByteBuffer.allocate(1 + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);
            vectorHeader.put(COLUMN_TYPE_FIXED_SIZE_LIST_FLOAT);
            vectorHeader.putInt(dimension);
            vectorHeader.putInt(entries.size());
            vectorHeader.flip();
            channel.write(vectorHeader);

            long vectorDataPos = channel.position();
            int batchSize = Math.max(1, 64 * 1024 / (dimension * 4));
            ByteBuffer vectorBuf =
                    ByteBuffer.allocate(
                                    Math.min(
                                            batchSize * dimension * 4,
                                            entries.size() * dimension * 4))
                            .order(ByteOrder.LITTLE_ENDIAN);
            int written = 0;
            while (written < entries.size()) {
                vectorBuf.clear();
                int end = Math.min(written + batchSize, entries.size());
                for (int idx = written; idx < end; idx++) {
                    float[] emb = entries.get(idx).embedding();
                    for (int i = 0; i < dimension; i++) {
                        vectorBuf.putFloat(i < emb.length ? emb[i] : 0f);
                    }
                }
                vectorBuf.flip();
                channel.write(vectorBuf);
                written = end;
            }

            writeStringColumn(channel, entries, e -> e.l0Abstract() != null ? e.l0Abstract() : "");

            ByteBuffer footer = ByteBuffer.allocate(FOOTER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
            footer.putLong(0L);
            footer.putInt(MAGIC);
            footer.putInt(FORMAT_VERSION);
            footer.flip();
            channel.write(footer);

            channel.force(true);

            log.debug(
                    "Wrote Lance fragment: {} entries, dimension={}, vectorDataOffset={}",
                    entries.size(),
                    dimension,
                    vectorDataPos);
        } catch (IOException e) {
            try {
                java.nio.file.Files.deleteIfExists(filePath);
            } catch (IOException ex) {
                log.debug("Failed to delete incomplete fragment: {}", filePath, ex);
            }
            throw new RuntimeException("Failed to write Lance fragment: " + filePath, e);
        }

        LanceFragment fragment = new LanceFragment(filePath, dimension);
        fragment.openForRead();
        return fragment;
    }

    private interface StringExtractor {
        String extract(ColdVectorEntry entry);
    }

    private static void writeStringColumn(
            FileChannel channel, List<ColdVectorEntry> entries, StringExtractor extractor)
            throws IOException {
        ByteBuffer colHeader = ByteBuffer.allocate(1 + 4).order(ByteOrder.LITTLE_ENDIAN);
        colHeader.put(COLUMN_TYPE_STRING);
        colHeader.putInt(entries.size());
        colHeader.flip();
        channel.write(colHeader);

        for (ColdVectorEntry entry : entries) {
            byte[] bytes = extractor.extract(entry).getBytes(StandardCharsets.UTF_8);
            ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
            lenBuf.putInt(bytes.length);
            lenBuf.flip();
            channel.write(lenBuf);
            channel.write(ByteBuffer.wrap(bytes));
        }
    }

    public void openForRead() {
        try {
            readChannel = FileChannel.open(filePath, StandardOpenOption.READ);
            fileSize = readChannel.size();
            segments = new ArrayList<>();
            long offset = 0;
            while (offset < fileSize) {
                long segSize = Math.min(SEGMENT_SIZE, fileSize - offset);
                MappedByteBuffer seg =
                        readChannel.map(FileChannel.MapMode.READ_ONLY, offset, segSize);
                seg.order(ByteOrder.LITTLE_ENDIAN);
                segments.add(seg);
                offset += segSize;
            }
            readPos = 0;

            int magic = readInt();
            if (magic != MAGIC) {
                throw new IOException("Invalid Lance magic: 0x" + Integer.toHexString(magic));
            }
            int version = readInt();
            if (version != FORMAT_VERSION) {
                throw new IOException("Unsupported Lance version: " + version);
            }
            int dim = readInt();
            if (dim != dimension) {
                throw new IOException("Dimension mismatch: expected " + dimension + ", got " + dim);
            }
            numRows = readInt();

            entryKeys = readStringColumn();

            byte vecColType = readByte();
            if (vecColType != COLUMN_TYPE_FIXED_SIZE_LIST_FLOAT) {
                throw new IOException(
                        "Expected FIXED_SIZE_LIST_FLOAT column, got: 0x"
                                + Integer.toHexString(vecColType & 0xFF));
            }
            int vecDim = readInt();
            if (vecDim != dimension) {
                throw new IOException(
                        "Vector dimension mismatch: expected " + dimension + ", got " + vecDim);
            }
            int numVectors = readInt();
            if (numVectors != numRows) {
                throw new IOException(
                        "Vector count mismatch: expected " + numRows + ", got " + numVectors);
            }
            vectorDataOffset = readPos;

            skipBytes((long) numVectors * dimension * 4L);

            l0Abstracts = readStringColumn();

            log.debug(
                    "Opened Lance fragment for read: {} rows, dimension={}, vectorDataOffset={}, fileSize={}",
                    numRows,
                    dimension,
                    vectorDataOffset,
                    fileSize);
        } catch (IOException e) {
            close();
            throw new RuntimeException("Failed to open Lance fragment for read: " + filePath, e);
        }
    }

    private ByteBuffer segmentAt(long position) {
        int segIdx = (int) (position / SEGMENT_SIZE);
        long segOffset = position % SEGMENT_SIZE;
        MappedByteBuffer seg = segments.get(segIdx);
        ByteBuffer dup = seg.duplicate();
        dup.position((int) segOffset);
        return dup.order(ByteOrder.LITTLE_ENDIAN);
    }

    private int readInt() {
        int val = segmentAt(readPos).getInt();
        readPos += 4;
        return val;
    }

    private byte readByte() {
        byte val = segmentAt(readPos).get();
        readPos += 1;
        return val;
    }

    private void readBytes(byte[] dst) {
        long remaining = dst.length;
        int offset = 0;
        while (remaining > 0) {
            ByteBuffer buf = segmentAt(readPos);
            int chunk = (int) Math.min(remaining, buf.remaining());
            buf.get(dst, offset, chunk);
            readPos += chunk;
            offset += chunk;
            remaining -= chunk;
        }
    }

    private void skipBytes(long n) {
        readPos += n;
    }

    private List<String> readStringColumn() {
        byte colType = readByte();
        if (colType != COLUMN_TYPE_STRING) {
            throw new RuntimeException(
                    "Expected STRING column, got: 0x" + Integer.toHexString(colType & 0xFF));
        }
        int count = readInt();
        List<String> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = readInt();
            byte[] bytes = new byte[len];
            readBytes(bytes);
            result.add(new String(bytes, StandardCharsets.UTF_8));
        }
        return result;
    }

    public float[] getVector(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= numRows) {
            throw new IndexOutOfBoundsException("Row index out of range: " + rowIndex);
        }
        long offset = vectorDataOffset + (long) rowIndex * dimension * 4L;
        float[] vec = new float[dimension];
        ByteBuffer buf = segmentAt(offset);
        for (int i = 0; i < dimension; i++) {
            vec[i] = buf.getFloat();
        }
        return vec;
    }

    public String getEntryKey(int rowIndex) {
        return entryKeys.get(rowIndex);
    }

    public String getL0Abstract(int rowIndex) {
        return l0Abstracts.get(rowIndex);
    }

    public Path getFilePath() {
        return filePath;
    }

    public long fileLength() {
        return fileSize;
    }

    public long vectorDataOffset() {
        return vectorDataOffset;
    }

    public FragmentMetadata metadata() {
        return new FragmentMetadata(filePath, dimension, numRows, vectorDataOffset, fileSize);
    }

    public record FragmentMetadata(
            Path filePath, int dimension, int numRows, long vectorDataOffset, long fileLength) {}

    @Override
    public synchronized void close() {
        segments = null;
        entryKeys = null;
        l0Abstracts = null;
        if (readChannel != null) {
            try {
                readChannel.close();
            } catch (IOException e) {
                log.debug("Failed to close Lance fragment channel: {}", filePath, e);
            }
            readChannel = null;
        }
    }
}
