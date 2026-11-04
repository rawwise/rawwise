package io.github.rawwise.engine.storage;

import java.util.HashMap;
import java.util.Map;

public class ComponentContext {
    private final Map<String, Object> resources;

    public ComponentContext() {
        this.resources = new HashMap<>();
    }

    public ComponentContext put(String key, Object value) {
        resources.put(key, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key, Class<T> type) {
        Object value = resources.get(key);
        if (value == null) return null;
        return type.cast(value);
    }

    public static ComponentContext empty() {
        return new ComponentContext();
    }
}
