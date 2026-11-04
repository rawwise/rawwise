package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.engine.storage.ComponentContext;

public final class WriterContext extends ComponentContext {

    public static WriterContext empty() {
        return new WriterContext();
    }

    @Override
    public WriterContext put(String key, Object value) {
        super.put(key, value);
        return this;
    }

    public <T> T getRequired(String key, Class<T> type) {
        T value = get(key, type);
        if (value == null) {
            throw new IllegalStateException(key + " is required in WriterContext");
        }
        return value;
    }
}
