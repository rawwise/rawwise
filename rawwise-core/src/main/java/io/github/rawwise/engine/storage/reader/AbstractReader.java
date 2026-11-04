package io.github.rawwise.engine.storage.reader;

import io.github.rawwise.engine.storage.metrics.NoOpReaderMetrics;
import io.github.rawwise.engine.storage.metrics.ReaderMetrics;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractReader implements Reader {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ReaderMetrics metrics;

    protected AbstractReader() {
        this(NoOpReaderMetrics.INSTANCE);
    }

    protected AbstractReader(ReaderMetrics metrics) {
        this.metrics = metrics != null ? metrics : NoOpReaderMetrics.INSTANCE;
    }

    protected ReaderMetrics metrics() {
        return metrics;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }

    protected void doClose() throws IOException {}

    public void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Reader is closed");
        }
    }
}
