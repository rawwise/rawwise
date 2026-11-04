package io.github.rawwise.governance.config;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigOption<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(ConfigOption.class);

    public enum Unit {
        PLAIN,
        BYTES,
        MIB,
        KIB,
        MS,
        SECONDS,
        MINUTES
    }

    private final String key;
    private final T defaultValue;
    private final Class<T> type;
    private transient Unit unit;
    private transient List<Predicate<T>> validators;

    private ConfigOption(
            String key, T defaultValue, Class<T> type, Unit unit, List<Predicate<T>> validators) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.type = type;
        this.unit = unit;
        this.validators = validators;
    }

    public String key() {
        return key;
    }

    public T defaultValue() {
        return defaultValue;
    }

    public Class<T> type() {
        return type;
    }

    public Unit unit() {
        if (unit != null) {
            return unit;
        }
        ConfigOption<?> registered = RawWiseOptions.findOption(this.key);
        if (registered != null && registered.unit != null) {
            return registered.unit;
        }
        return Unit.PLAIN;
    }

    @SuppressWarnings("unchecked")
    public void validate(T value) {
        List<Predicate<T>> activeValidators = this.validators;
        if (activeValidators == null || activeValidators.isEmpty()) {
            ConfigOption<?> original = RawWiseOptions.findOption(this.key);
            if (original != null && original.validators != null && !original.validators.isEmpty()) {
                activeValidators = (List<Predicate<T>>) (List<?>) original.validators;
            } else {
                log.warn(
                        "No validators found for option '{}' after deserialization; validation skipped",
                        key);
                return;
            }
        }
        for (Predicate<T> validator : activeValidators) {
            if (!validator.test(value)) {
                throw new IllegalArgumentException(
                        "Validation failed for option '" + key + "' with value: " + value);
            }
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.unit = null;
        this.validators = List.of();
    }

    public static <T> ConfigOptionBuilder<T> builder(String key, T defaultValue, Class<T> type) {
        return new ConfigOptionBuilder<>(key, defaultValue, type);
    }

    public static ConfigOption<String> string(String key, String defaultValue) {
        return builder(key, defaultValue, String.class).build();
    }

    public static ConfigOption<Integer> int_(String key, int defaultValue) {
        return builder(key, defaultValue, Integer.class).build();
    }

    public static ConfigOption<Long> long_(String key, long defaultValue) {
        return builder(key, defaultValue, Long.class).build();
    }

    public static ConfigOption<Boolean> bool(String key, boolean defaultValue) {
        return builder(key, defaultValue, Boolean.class).build();
    }

    public static ConfigOption<Double> double_(String key, double defaultValue) {
        return builder(key, defaultValue, Double.class).build();
    }

    public static ConfigOption<Duration> duration(String key, Duration defaultValue) {
        return builder(key, defaultValue, Duration.class).build();
    }

    public static class ConfigOptionBuilder<T> {
        private final String key;
        private final T defaultValue;
        private final Class<T> type;
        private Unit unit = Unit.PLAIN;
        private final java.util.ArrayList<Predicate<T>> validators = new java.util.ArrayList<>();

        private ConfigOptionBuilder(String key, T defaultValue, Class<T> type) {
            this.key = key;
            this.defaultValue = defaultValue;
            this.type = type;
        }

        public ConfigOptionBuilder<T> unit(Unit unit) {
            this.unit = unit;
            return this;
        }

        public ConfigOptionBuilder<T> validate(Predicate<T> validator) {
            validators.add(validator);
            return this;
        }

        public ConfigOptionBuilder<T> validateNonNull() {
            validators.add(Objects::nonNull);
            return this;
        }

        public ConfigOptionBuilder<T> validatePositive() {
            validators.add(v -> v instanceof Number && ((Number) v).doubleValue() > 0);
            return this;
        }

        public ConfigOptionBuilder<T> validateRange(int min, int max) {
            validators.add(
                    v -> {
                        if (!(v instanceof Integer)) {
                            return false;
                        }
                        int val = (Integer) v;
                        return val >= min && val <= max;
                    });
            return this;
        }

        public ConfigOption<T> build() {
            return new ConfigOption<>(key, defaultValue, type, unit, List.copyOf(validators));
        }
    }
}
