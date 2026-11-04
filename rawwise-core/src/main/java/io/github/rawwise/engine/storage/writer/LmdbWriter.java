package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.model.record.WriteEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

@Slf4j
public class LmdbWriter
        implements RecordWriter<WriteEntry>, BatchWriter<WriteEntry>, Initializable {

    public static final String MAPFULL_MESSAGE = "LMDB Map Full";
    public static final String READONLY_MESSAGE = "Instance is read-only";

    private final LmdbInstance instance;
    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> dbi;
    private final DynamicMmapStore store;
    private final KeyCodec keyCodec;

    private Properties config;
    private final AtomicLong writtenCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private List<WriteEntry> buffer = Collections.synchronizedList(new ArrayList<>());
    private int batchSize;
    private boolean useBuffer;

    private DeadLetterHandler deadLetterHandler;

    public LmdbWriter(LmdbInstance instance) {
        this(instance, null);
    }

    public LmdbWriter(LmdbInstance instance, DynamicMmapStore store) {
        this(instance, store, new DefaultKeyCodec());
    }

    public LmdbWriter(LmdbInstance instance, DynamicMmapStore store, KeyCodec keyCodec) {
        Objects.requireNonNull(instance, "LmdbInstance cannot be null");
        Objects.requireNonNull(keyCodec, "KeyCodec cannot be null");
        this.instance = instance;
        this.env = instance.getEnv();
        this.dbi = instance.getDbi();
        this.store = store;
        this.keyCodec = keyCodec;

        if (store == null) {
            log.warn(
                    "LmdbWriter created without DynamicMmapStore, "
                            + "ShardVersionTracker will be unavailable. "
                            + "Use LmdbWriter(instance, store) if transactions are needed.");
        }
    }

    public LmdbWriter withDeadLetterHandler(DeadLetterHandler handler) {
        this.deadLetterHandler = handler;
        return this;
    }

    @Override
    public void init(Properties config) throws IOException {
        if (this.instance == null) {
            throw new IllegalStateException("LmdbInstance not initialized");
        }
        this.config = config;
        int confBatchSize = (int) config.getOrDefault("writer_batch_size", 0);
        this.batchSize = confBatchSize > 0 ? confBatchSize : 100;
        this.useBuffer = (boolean) config.getOrDefault("writer_use_buffer", false);
        this.buffer = Collections.synchronizedList(new ArrayList<>(batchSize));

        if (this.deadLetterHandler == null) {
            String hdfsUri = config.getProperty("dead_letter_hdfs_uri");
            String hdfsBasePath =
                    config.getProperty("dead_letter_hdfs_base_path", "/lmdb/dead-letter");
            if (hdfsUri != null && !hdfsUri.isEmpty()) {
                this.deadLetterHandler = new HdfsDeadLetterHandler(hdfsUri, hdfsBasePath);
            } else {
                log.info(
                        "No dead letter handler configured (dead_letter_hdfs_uri not set). Dead letter handling disabled.");
            }
        }
    }

    @Override
    public WriteResult write(WriteEntry data) throws IOException {
        if (closed.get()) {
            throw new IOException("Writer is closed");
        }
        if (data == null) {
            failedCount.incrementAndGet();
            return WriteResult.FAILURE;
        }

        if (useBuffer) {
            try {
                buffer.add(data);

                if (buffer.size() >= batchSize) {
                    flush();
                }

                return WriteResult.SUCCESS;

            } catch (Exception e) {
                buffer.remove(data);
                failedCount.incrementAndGet();
                throw new IOException("Buffer write failed: " + e.getMessage(), e);
            }
        }

        return doWrite(data);
    }

    @Override
    public BatchWriteResult batchWrite(Collection<WriteEntry> objs) throws IOException {
        long success = 0;
        long fail = 0;
        long bytes = 0;

        for (WriteEntry obj : objs) {
            try {
                WriteResult res = doWrite(obj);
                if (res.isFullSuccess()) {
                    success++;
                    bytes += obj.key().length + obj.encodedValue().length;
                } else {
                    fail++;
                    log.warn("Batch write failed for one record");
                }
            } catch (IOException e) {
                fail++;
                log.warn("Batch write failed for one record: {}", e.getMessage());
            }
        }

        return new BatchWriteResult(success, fail, bytes);
    }

    @Override
    public void flush() throws IOException {
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        List<WriteEntry> toFlush;
        synchronized (buffer) {
            if (buffer.isEmpty()) return;
            toFlush = new ArrayList<>(buffer);
            buffer.clear();
        }

        try (Txn<ByteBuffer> txn = env.txnWrite()) {

            for (WriteEntry record : toFlush) {
                ByteBuffer keyBuf = ByteBuffer.allocateDirect(record.key().length);
                keyBuf.put(record.key()).flip();

                byte[] encodedValue =
                        record.valuePreEncoded()
                                ? record.encodedValue()
                                : NullableValueCodec.encode(record.encodedValue());
                ByteBuffer valBuf = ByteBuffer.allocateDirect(encodedValue.length);
                valBuf.put(encodedValue).flip();

                dbi.put(txn, keyBuf, valBuf);
                writtenCount.incrementAndGet();
            }

            txn.commit();

        } catch (Env.MapFullException e) {
            failedCount.addAndGet(toFlush.size());
            instance.markAsReadOnly();
            throw new IOException("LMDB Map Full, instance switched to read-only.", e);
        } catch (Exception e) {
            failedCount.addAndGet(toFlush.size());
            handleFlushFailure(toFlush, e);
            throw new IOException(
                    "Flush failed, transaction rolled back. Data moved to Dead Letter Queue.", e);
        }
    }

    @Override
    public long recordsWritten() {
        return writtenCount.get();
    }

    @Override
    public long recordsFailed() {
        return failedCount.get();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            flush();
        }
    }

    private WriteResult doWrite(WriteEntry data) throws IOException {
        try {
            byte[] key = data.key();
            byte[] encodedValue =
                    data.valuePreEncoded()
                            ? data.encodedValue()
                            : NullableValueCodec.encode(data.encodedValue());
            LmdbInstance.PutStatus status = instance.put(key, encodedValue);

            if (status == LmdbInstance.PutStatus.SUCCESS) {
                writtenCount.incrementAndGet();
                return WriteResult.SUCCESS;
            } else {
                failedCount.incrementAndGet();
                String errorMsg =
                        switch (status) {
                            case MAP_FULL -> MAPFULL_MESSAGE;
                            case READ_ONLY -> READONLY_MESSAGE;
                            default -> "Unknown LMDB status: " + status;
                        };
                throw new IOException(errorMsg);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            failedCount.incrementAndGet();
            throw new IOException("Write failed: " + e.getMessage(), e);
        }
    }

    private void handleFlushFailure(List<WriteEntry> failedBuffer, Exception e) {
        if (deadLetterHandler != null) {
            try {
                deadLetterHandler.save(failedBuffer, e);
            } catch (Exception ex) {
                log.error(
                        "FATAL: Failed to save to Dead Letter Queue. Data lost. Size: {}",
                        failedBuffer.size(),
                        ex);
            }
        }
    }
}
