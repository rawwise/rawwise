package io.github.rawwise.governance.config;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RawWiseConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, Object> values;

    private RawWiseConfig(Map<String, Object> values) {
        Map<String, Object> filtered = new HashMap<>();
        for (var e : values.entrySet()) {
            if (e.getValue() != null) {
                filtered.put(e.getKey(), e.getValue());
            }
        }
        this.values = Collections.unmodifiableMap(filtered);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(ConfigOption<T> option) {
        Object value = values.getOrDefault(option.key(), option.defaultValue());
        if (value == null) {
            return null;
        }
        Class<T> type = option.type();
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        if (type == Long.class && value instanceof Number num) {
            return (T) (Long) num.longValue();
        }
        if (type == Integer.class && value instanceof Number num) {
            return (T) (Integer) num.intValue();
        }
        if (type == Double.class && value instanceof Number num) {
            return (T) (Double) num.doubleValue();
        }
        throw new IllegalStateException(
                "Type mismatch for option: "
                        + option.key()
                        + ": expected "
                        + type.getSimpleName()
                        + ", got "
                        + value.getClass().getSimpleName());
    }

    public <T> RawWiseConfig with(ConfigOption<T> option, T value) {
        Objects.requireNonNull(value, "value cannot be null for option: " + option.key());
        option.validate(value);
        Map<String, Object> newValues = new HashMap<>(this.values);
        newValues.put(option.key(), value);
        return new RawWiseConfig(newValues);
    }

    public <T> RawWiseConfig withFallback(ConfigOption<T> option, T value) {
        if (values.containsKey(option.key())) {
            return this;
        }
        return with(option, value);
    }

    public static RawWiseConfig empty() {
        return new RawWiseConfig(Map.of());
    }

    public static RawWiseConfig fromMap(Map<String, Object> map) {
        return new RawWiseConfig(map);
    }

    public Map<String, Object> asMap() {
        return values;
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        if (this.values != null) {
            try {
                Map<?, ?> raw = this.values;
                Map<String, Object> copy = new HashMap<>();
                for (Map.Entry<?, ?> entry : raw.entrySet()) {
                    copy.put((String) entry.getKey(), entry.getValue());
                }
                var field = RawWiseConfig.class.getDeclaredField("values");
                field.setAccessible(true);
                field.set(this, Collections.unmodifiableMap(copy));
            } catch (Exception e) {
                // Defensive copy best-effort: if reflection fails in Java 17+ module system,
                // the deserialized map is still functionally read-only for normal usage
            }
        }
    }
}
