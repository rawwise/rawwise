package io.github.rawwise.engine.storage.reader;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ReaderFactoryRegistry {
    private static final ReaderFactoryRegistry INSTANCE = new ReaderFactoryRegistry();

    private final Map<ReaderType, ReaderFactory<?>> factories = new ConcurrentHashMap<>();

    public static ReaderFactoryRegistry getInstance() {
        return INSTANCE;
    }

    public ReaderFactoryRegistry() {}

    public <R extends Reader> void register(ReaderFactory<R> factory) {
        ReaderFactory<?> existing = factories.putIfAbsent(factory.readerType(), factory);
        if (existing != null) {
            throw new IllegalStateException(
                    "ReaderFactory already registered for type: " + factory.readerType());
        }
    }

    public <R extends Reader> boolean registerIfAbsent(ReaderFactory<R> factory) {
        return factories.putIfAbsent(factory.readerType(), factory) == null;
    }

    public void unregister(ReaderType type) {
        factories.remove(type);
    }

    @SuppressWarnings("unchecked")
    public <R extends Reader> R create(
            ReaderType type, Class<R> readerClass, ReaderConfig config, ReaderContext context)
            throws IOException {
        ReaderFactory<?> factory = factories.get(type);
        if (factory == null) {
            throw new IllegalArgumentException("No ReaderFactory registered for type: " + type);
        }
        if (!readerClass.isAssignableFrom(factory.readerClass())) {
            throw new ClassCastException(
                    "Factory for '"
                            + type
                            + "' creates "
                            + factory.readerClass().getName()
                            + ", not "
                            + readerClass.getName());
        }
        return readerClass.cast(factory.create(config, context));
    }

    public boolean hasFactory(ReaderType type) {
        return factories.containsKey(type);
    }

    public Set<ReaderType> registeredTypes() {
        return Collections.unmodifiableSet(factories.keySet());
    }

    public void clear() {
        factories.clear();
    }
}
