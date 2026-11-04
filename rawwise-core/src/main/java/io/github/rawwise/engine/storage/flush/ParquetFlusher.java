package io.github.rawwise.engine.storage.flush;

import io.github.rawwise.engine.storage.writer.DefaultParquetWriterBuilder;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class ParquetFlusher<T> implements Flusher {
    private final Iterator<T> iterator;
    private final ParquetWriter<T> fileWriter;

    public ParquetFlusher(Iterator<T> iterator, String filePath, WriteSupport<T> writeSupport) {
        this.iterator = iterator;
        DefaultParquetWriterBuilder<T> builder =
                new DefaultParquetWriterBuilder<>(new Path(filePath), writeSupport);
        try {
            this.fileWriter =
                    builder.withBloomFilterEnabled(true)
                            .withDictionaryEncoding(true)
                            .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                            .withCompressionCodec(CompressionCodecName.ZSTD)
                            .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        iterator.forEachRemaining(
                element -> {
                    try {
                        fileWriter.write(element);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
