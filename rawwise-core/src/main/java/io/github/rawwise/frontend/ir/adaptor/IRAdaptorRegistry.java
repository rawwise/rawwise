package io.github.rawwise.frontend.ir.adaptor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class IRAdaptorRegistry {

    private static final Map<String, IRAdaptorFactory> factories = new ConcurrentHashMap<>();

    static {
        register("spark", deps -> new SparkIRAdaptor());
        register("trino", deps -> new TrinoIRAdaptor());
        register("duckdb", deps -> new DuckDBIRAdaptor());
    }

    private IRAdaptorRegistry() {}

    @FunctionalInterface
    public interface IRAdaptorFactory {
        IRAdaptor<?> create(Object... deps);
    }

    public static void register(String engine, IRAdaptorFactory factory) {
        factories.put(engine.toLowerCase(), factory);
    }

    @SuppressWarnings("unchecked")
    public static <E> IRAdaptor<E> create(String engine, Object... deps) {
        IRAdaptorFactory factory = factories.get(engine.toLowerCase());
        if (factory == null) {
            throw new IllegalArgumentException("No IR adaptor registered for engine: " + engine);
        }
        return (IRAdaptor<E>) factory.create(deps);
    }
}
