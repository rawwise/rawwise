package io.github.rawwise.engine.storage.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

public class DefaultParquetWriterBuilder<T>
        extends ParquetWriter.Builder<T, DefaultParquetWriterBuilder<T>> {
    private WriteSupport<T> writeSupport;

    public DefaultParquetWriterBuilder(Path path, WriteSupport<T> writeSupport) {
        super(path);
        this.writeSupport = writeSupport;
    }

    public DefaultParquetWriterBuilder(Path path) {
        super(path);
    }

    protected DefaultParquetWriterBuilder(OutputFile path) {
        super(path);
    }

    @Override
    protected DefaultParquetWriterBuilder<T> self() {
        return this;
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration conf) {
        return writeSupport;
    }
}
