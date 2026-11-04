package io.github.rawwise.engine.storage.reader;

import io.github.rawwise.engine.storage.ComponentContext;
import io.github.rawwise.engine.storage.metrics.NoOpReaderMetrics;
import io.github.rawwise.engine.storage.metrics.ReaderMetrics;
import io.github.rawwise.engine.storage.spi.Storage;
import org.apache.hadoop.conf.Configuration;

public final class ReaderContext extends ComponentContext {

    public static ReaderContext empty() {
        return new ReaderContext();
    }

    public static ReaderContext ofHadoopConfig(Configuration conf) {
        ReaderContext ctx = new ReaderContext();
        ctx.put("hadoopConfiguration", conf);
        return ctx;
    }

    public static ReaderContext ofStorage(Storage storage) {
        ReaderContext ctx = new ReaderContext();
        ctx.put("storage", storage);
        return ctx;
    }

    @Override
    public ReaderContext put(String key, Object value) {
        super.put(key, value);
        return this;
    }

    public <T> T getRequired(String key, Class<T> type) {
        T value = get(key, type);
        if (value == null) {
            throw new IllegalStateException(key + " is required in ReaderContext");
        }
        return value;
    }

    public ReaderMetrics metrics() {
        ReaderMetrics m = get("readerMetrics", ReaderMetrics.class);
        return m != null ? m : NoOpReaderMetrics.INSTANCE;
    }

    public ReaderContext withMetrics(ReaderMetrics metrics) {
        put("readerMetrics", metrics);
        return this;
    }
}
