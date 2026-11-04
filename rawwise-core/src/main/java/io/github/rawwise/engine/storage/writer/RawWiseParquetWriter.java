package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.model.record.Schema;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class RawWiseParquetWriter<T> implements FileWriter<T>, Initializable {
    private ParquetWriter<T> writer;
    private final String filePath;
    private final Schema schema;
    private WriteSupport<T> writeSupport;
    private Properties props;
    private final AtomicLong recordsWrittenCount = new AtomicLong(0);
    private final AtomicLong recordsFailedCount = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final ReentrantLock writeLock = new ReentrantLock();

    public RawWiseParquetWriter(String filePath, Schema schema) {
        this.filePath = filePath;
        this.schema = schema;
    }

    public RawWiseParquetWriter(String filePath, Schema schema, WriteSupport<T> writeSupport) {
        this.filePath = filePath;
        this.schema = schema;
        this.writeSupport = writeSupport;
    }

    @Override
    public Storage.StoragePath filePath() {
        String scheme = filePath.startsWith("hdfs://") ? "hdfs" : "file";
        return new Storage.StoragePath(scheme, filePath);
    }

    @Override
    public void init(Properties props) throws IOException {
        this.props = props;
        if (initialized.compareAndSet(false, true)) {
            initWriter();
        }
    }

    private void initWriter() throws IOException {
        if (writeSupport == null) {
            throw new IllegalStateException("WriteSupport not configured.");
        }
        DefaultParquetWriterBuilder<T> builder =
                new DefaultParquetWriterBuilder<>(new Path(filePath), writeSupport);
        this.writer =
                builder.withBloomFilterEnabled(true)
                        .withDictionaryEncoding(true)
                        .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE)
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .build();
    }

    private void ensureInitialized() throws IOException {
        if (!initialized.get()) {
            init(props);
        }
    }

    @Override
    public WriteResult write(T obj) throws IOException {
        writeLock.lock();
        try {
            if (closed.get()) {
                recordsFailedCount.incrementAndGet();
                throw new IOException("Writer is closed");
            }
            ensureInitialized();
            writer.write(obj);
            recordsWrittenCount.incrementAndGet();
            return WriteResult.SUCCESS;
        } catch (IOException e) {
            recordsFailedCount.incrementAndGet();
            throw e;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long recordsWritten() {
        return recordsWrittenCount.get();
    }

    @Override
    public long recordsFailed() {
        return recordsFailedCount.get();
    }

    @Override
    public void flush() throws IOException {
        writeLock.lock();
        try {
            throw new UnsupportedOperationException(
                    "ParquetWriter does not support flush; data is buffered until close()");
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public long length() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (closed.compareAndSet(false, true)) {
                if (writer != null) {
                    writer.close();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
}
