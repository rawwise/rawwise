package io.github.rawwise.engine.storage.writer;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class WriterFactoryRegistry {
    private static final WriterFactoryRegistry INSTANCE = new WriterFactoryRegistry();

    private final Map<String, WriterFactory<?>> factories = new ConcurrentHashMap<>();

    public static WriterFactoryRegistry getInstance() {
        return INSTANCE;
    }

    public void register(WriterFactory<?> factory) {
        Objects.requireNonNull(factory, "factory must not be null");
        WriterFactory<?> existing = factories.putIfAbsent(factory.writerType(), factory);
        if (existing != null) {
            throw new IllegalStateException(
                    "WriterFactory already registered for type: " + factory.writerType());
        }
    }

    @SuppressWarnings("unchecked")
    public <W extends Writer> W create(
            String type, Class<W> writerClass, WriterConfig config, WriterContext context)
            throws IOException {
        WriterFactory<?> factory = factories.get(type);
        if (factory == null) {
            throw new IllegalArgumentException("No WriterFactory registered for type: " + type);
        }
        if (!writerClass.isAssignableFrom(factory.writerClass())) {
            throw new ClassCastException(
                    "Factory creates "
                            + factory.writerClass().getName()
                            + ", expected "
                            + writerClass.getName());
        }
        return writerClass.cast(factory.create(config, context));
    }

    public WriterFactory<?> getFactory(String type) {
        return factories.get(type);
    }

    public boolean hasFactory(String type) {
        return factories.containsKey(type);
    }
}
