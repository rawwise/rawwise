package io.github.rawwise.governance.config;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class ConfigFactory {

    private static final Map<String, String> KEY_ALIASES =
            Map.ofEntries(
                    Map.entry("rawwise.routing.shardCount", "rawwise.routing.shard-count"),
                    Map.entry(
                            "rawwise.routing.register.maxRetries",
                            "rawwise.routing.register.max-retries"),
                    Map.entry(
                            "rawwise.routing.register.backoffMs",
                            "rawwise.routing.register.backoff-ms"),
                    Map.entry(
                            "rawwise.routing.register.failOpen",
                            "rawwise.routing.register.fail-open"),
                    Map.entry(
                            "rawwise.routing.forward.maxRetries",
                            "rawwise.routing.forward.max-retries"),
                    Map.entry(
                            "rawwise.routing.forward.backoffMs",
                            "rawwise.routing.forward.backoff-ms"),
                    Map.entry(
                            "rawwise.routing.write.assumePartitionIsShard",
                            "rawwise.routing.write.assume-partition-is-shard"),
                    Map.entry(
                            "rawwise.routing.write.batchRows", "rawwise.routing.write.batch-rows"),
                    Map.entry("frontend.port", "rawwise.frontend.port"),
                    Map.entry("frontend.deployment.mode", "rawwise.frontend.deployment.mode"),
                    Map.entry("grpc.backend.host", "rawwise.frontend.grpc.backend.host"),
                    Map.entry("grpc.backend.port", "rawwise.frontend.grpc.backend.port"),
                    Map.entry("query.timeout.seconds", "rawwise.frontend.query.timeout-seconds"),
                    Map.entry("rawwise.lmdb.lease.ttl.ms", "rawwise.storage.lmdb.lease.ttl-ms"),
                    Map.entry("rawwise.storage.wal.enabled", "rawwise.wal.enabled"),
                    Map.entry("rawwise.storage.wal.buffer-size", "rawwise.wal.buffer-size"),
                    Map.entry(
                            "rawwise.storage.wal.sync-interval-ms", "rawwise.wal.sync-interval-ms"),
                    Map.entry("rawwise.storage.wal.max-size-bytes", "rawwise.wal.max-size-bytes"),
                    Map.entry("rawwise.storage.wal.hdfs-path", "rawwise.wal.hdfs-path"),
                    Map.entry(
                            "rawwise.storage.wal.compression-codec",
                            "rawwise.wal.compression-codec"),
                    Map.entry("rawwise.storage.wal.file-format", "rawwise.wal.file-format"),
                    Map.entry(
                            "rawwise.storage.wal.append.timeout-ms",
                            "rawwise.wal.append.timeout-ms"),
                    Map.entry(
                            "rawwise.storage.compaction.max-concurrent",
                            "rawwise.compaction.max-concurrent"),
                    Map.entry("rawwise.shard.count", "rawwise.routing.shard-count"),
                    Map.entry(
                            "rawwise.config.admin.cidr-whitelist",
                            "rawwise.config.admin.allowed-cidrs"),
                    Map.entry("rawwise.config.admin.token", "rawwise.config.admin.bearer-token"));

    private static final Set<String> TRUE_VALUES = Set.of("true", "1", "yes", "on");
    private static final Set<String> FALSE_VALUES = Set.of("false", "0", "no", "off");

    public static String resolveAlias(String key) {
        return KEY_ALIASES.getOrDefault(key, key);
    }

    public static Map<String, String> getKeyAliases() {
        return KEY_ALIASES;
    }

    public static RawWiseConfig fromSparkConf(SparkConf sparkConf) {
        Map<String, String> rawValues = new HashMap<>();
        Tuple2<String, String>[] sparkPrefixEntries = sparkConf.getAllWithPrefix("spark.rawwise.");
        for (Tuple2<String, String> entry : sparkPrefixEntries) {
            String resolvedKey = resolveAlias("rawwise." + entry._1());
            rawValues.put(resolvedKey, entry._2());
        }
        Tuple2<String, String>[] directPrefixEntries = sparkConf.getAllWithPrefix("rawwise.");
        for (Tuple2<String, String> entry : directPrefixEntries) {
            String resolvedKey = resolveAlias("rawwise." + entry._1());
            rawValues.put(resolvedKey, entry._2());
        }
        return coerceAndBuild(rawValues);
    }

    public static RawWiseConfig fromMap(Map<String, String> map) {
        Map<String, Object> resolved = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String resolvedKey = resolveAlias(entry.getKey());
            resolved.put(resolvedKey, entry.getValue());
        }
        return buildValidated(resolved, false, true);
    }

    public static RawWiseConfig fromSystemProperties() {
        Map<String, String> rawValues = new HashMap<>();
        System.getProperties().stringPropertyNames().stream()
                .filter(k -> k.startsWith("rawwise."))
                .forEach(
                        k -> {
                            String resolvedKey = resolveAlias(k);
                            rawValues.put(resolvedKey, System.getProperty(k));
                        });
        return coerceAndBuild(rawValues);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static RawWiseConfig createDefault() {
        return builder()
                .set(RawWiseOptions.CLUSTER_ID, "default")
                .set(RawWiseOptions.ZOOKEEPER_CONNECT_STRING, "127.0.0.1:2181")
                .set(RawWiseOptions.ZOOKEEPER_NAMESPACE, "rawwise")
                .set(RawWiseOptions.METASTORE_TYPE, "inmemory")
                .set(RawWiseOptions.ROUTING_SHARD_COUNT, 1024)
                .set(RawWiseOptions.STORAGE_MMAP_PATH, "/tmp/rawwise/mmap")
                .set(RawWiseOptions.STORAGE_MMAP_SIZE, 1024L * 1024L * 1024L)
                .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                .build();
    }

    private static final Set<String> SYSTEM_OVERRIDE_ALLOWED =
            Set.of(
                    "rawwise.storage.native-memory-limit-mb",
                    "rawwise.container-memory.warning-threshold-ratio",
                    "rawwise.container-memory.critical-threshold-ratio",
                    "rawwise.container-memory.min-warning-bytes",
                    "rawwise.container-memory.min-critical-bytes",
                    "rawwise.container-memory.safety-margin-ratio",
                    "rawwise.container-memory.min-safety-margin-bytes",
                    "rawwise.fast-path.row-cache.enabled",
                    "rawwise.fast-path.row-cache.max-size",
                    "rawwise.fast-path.row-cache.ttl-minutes");

    public static RawWiseConfig createDefaultWithSystemOverrides() {
        Builder builder =
                builder()
                        .set(RawWiseOptions.CLUSTER_ID, "default")
                        .set(RawWiseOptions.ZOOKEEPER_CONNECT_STRING, "127.0.0.1:2181")
                        .set(RawWiseOptions.ZOOKEEPER_NAMESPACE, "rawwise")
                        .set(RawWiseOptions.METASTORE_TYPE, "inmemory")
                        .set(RawWiseOptions.ROUTING_SHARD_COUNT, 1024)
                        .set(RawWiseOptions.STORAGE_MMAP_PATH, "/tmp/rawwise/mmap")
                        .set(RawWiseOptions.STORAGE_MMAP_SIZE, 1024L * 1024L * 1024L)
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS");
        System.getProperties().stringPropertyNames().stream()
                .filter(k -> k.startsWith("rawwise."))
                .forEach(
                        k -> {
                            String resolvedKey = resolveAlias(k);
                            if (!SYSTEM_OVERRIDE_ALLOWED.contains(resolvedKey)) {
                                return;
                            }
                            ConfigOption<?> option = RawWiseOptions.findOption(resolvedKey);
                            if (option != null) {
                                Object coerced =
                                        coerceValue(
                                                resolvedKey, System.getProperty(k), option.type());
                                builder.set((ConfigOption) option, coerced);
                            }
                        });
        return builder.build();
    }

    static Object coerceValue(String key, String stringValue, Class<?> targetType) {
        try {
            if (targetType == String.class) {
                return stringValue;
            } else if (targetType == Integer.class) {
                return coerceInteger(key, stringValue);
            } else if (targetType == Long.class) {
                return coerceLong(key, stringValue);
            } else if (targetType == Double.class) {
                return Double.parseDouble(stringValue);
            } else if (targetType == Boolean.class) {
                return coerceBoolean(key, stringValue);
            } else if (targetType == Duration.class) {
                return coerceDuration(key, stringValue);
            } else if (targetType == List.class) {
                return Arrays.asList(stringValue.split(","));
            } else {
                return stringValue;
            }
        } catch (NumberFormatException e) {
            throw new ConfigException.TypeCoercionError(
                    "Cannot parse value '"
                            + stringValue
                            + "' as "
                            + targetType.getSimpleName()
                            + " for key '"
                            + key
                            + "'",
                    e);
        }
    }

    static Object coerceToType(Object value, Class<?> targetType, String key) {
        if (value instanceof String stringValue) {
            return coerceValue(key, stringValue, targetType);
        }
        if (targetType.isInstance(value)) {
            return value;
        }
        if (targetType == Long.class && value instanceof Number num) {
            return num.longValue();
        }
        if (targetType == Integer.class && value instanceof Number num) {
            return num.intValue();
        }
        if (targetType == Double.class && value instanceof Number num) {
            return num.doubleValue();
        }
        if (targetType == String.class) {
            return value.toString();
        }
        throw new ConfigException.TypeCoercionError(
                "Cannot coerce value of type "
                        + value.getClass().getName()
                        + " to "
                        + targetType.getName()
                        + " for key '"
                        + key
                        + "'");
    }

    private static boolean coerceBoolean(String key, String stringValue) {
        String lower = stringValue.toLowerCase().trim();
        if (TRUE_VALUES.contains(lower)) {
            return true;
        }
        if (FALSE_VALUES.contains(lower)) {
            return false;
        }
        throw new ConfigException.TypeCoercionError(
                "Cannot parse value '"
                        + stringValue
                        + "' as Boolean for key '"
                        + key
                        + "'. Accepted values: true/false, 1/0, yes/no, on/off (case-insensitive)");
    }

    private static int coerceInteger(String key, String stringValue) {
        ConfigOption<?> option = RawWiseOptions.findOption(key);
        ConfigOption.Unit unit = option != null ? option.unit() : ConfigOption.Unit.PLAIN;
        return switch (unit) {
            case MIB, KIB, MS, SECONDS, MINUTES -> {
                long longVal = Long.parseLong(stringValue);
                if (longVal < Integer.MIN_VALUE || longVal > Integer.MAX_VALUE) {
                    throw new ConfigException.TypeCoercionError(
                            "Value '"
                                    + stringValue
                                    + "' for key '"
                                    + key
                                    + "' overflows Integer range");
                }
                yield (int) longVal;
            }
            case BYTES, PLAIN -> {
                Long memVal = HoconParsers.parseMemorySize(stringValue);
                long longVal = memVal != null ? memVal : Long.parseLong(stringValue);
                if (longVal < Integer.MIN_VALUE || longVal > Integer.MAX_VALUE) {
                    throw new ConfigException.TypeCoercionError(
                            "Memory size value '"
                                    + stringValue
                                    + "' for key '"
                                    + key
                                    + "' overflows Integer range");
                }
                yield (int) longVal;
            }
        };
    }

    private static long coerceLong(String key, String stringValue) {
        ConfigOption<?> option = RawWiseOptions.findOption(key);
        ConfigOption.Unit unit = option != null ? option.unit() : ConfigOption.Unit.PLAIN;
        return switch (unit) {
            case MIB, KIB, MS, SECONDS, MINUTES -> Long.parseLong(stringValue);
            case BYTES, PLAIN -> {
                Long memVal = HoconParsers.parseMemorySize(stringValue);
                if (memVal != null) yield memVal;
                yield Long.parseLong(stringValue);
            }
        };
    }

    private static Duration coerceDuration(String key, String stringValue) {
        Duration hoconDur = HoconParsers.parseDuration(stringValue);
        if (hoconDur != null) {
            return hoconDur;
        }
        try {
            return Duration.parse(stringValue);
        } catch (Exception e) {
            try {
                return Duration.ofMillis(Long.parseLong(stringValue));
            } catch (NumberFormatException e2) {
                throw new ConfigException.TypeCoercionError(
                        "Cannot parse Duration value '" + stringValue + "' for key '" + key + "'",
                        e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    static <T> void validateOption(ConfigOption<T> option, Object value) {
        option.validate((T) value);
    }

    private static void applyAliases(Map<String, Object> resolved) {
        Map<String, String> aliases = getKeyAliases();
        Set<String> keysToRename =
                resolved.keySet().stream().filter(aliases::containsKey).collect(Collectors.toSet());
        for (String oldKey : keysToRename) {
            String newKey = aliases.get(oldKey);
            Object value = resolved.remove(oldKey);
            if (!resolved.containsKey(newKey)) {
                resolved.put(newKey, value);
            }
        }
    }

    static RawWiseConfig buildValidated(
            Map<String, Object> rawValues, boolean applyAliases, boolean fillDefaults) {
        Map<String, Object> resolved = new HashMap<>(rawValues);

        if (applyAliases) {
            applyAliases(resolved);
        }

        Map<String, Object> coerced = new HashMap<>();
        for (Map.Entry<String, Object> entry : resolved.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            ConfigOption<?> option = RawWiseOptions.findOption(key);
            if (option != null) {
                if (value != null) {
                    Object coercedValue = coerceToType(value, option.type(), key);
                    validateOption(option, coercedValue);
                    coerced.put(key, coercedValue);
                } else {
                    coerced.put(key, null);
                }
            } else {
                coerced.put(key, value);
            }
        }

        if (fillDefaults) {
            for (String key : RawWiseOptions.allKeys()) {
                if (!coerced.containsKey(key) || coerced.get(key) == null) {
                    ConfigOption<?> option = RawWiseOptions.findOption(key);
                    if (option != null) {
                        coerced.put(key, option.defaultValue());
                    }
                }
            }
        }

        RawWiseConfig config = RawWiseConfig.fromMap(coerced);

        for (ConfigConstraint constraint : ConfigConstraints.ALL) {
            if (!constraint.validate(config)) {
                throw new ConfigException.ValidationError(
                        "Config constraint violated: " + constraint.errorMessage(config));
            }
        }

        return config;
    }

    private static RawWiseConfig coerceAndBuild(Map<String, String> rawValues) {
        Map<String, Object> values = new HashMap<>();
        for (Map.Entry<String, String> entry : rawValues.entrySet()) {
            values.put(entry.getKey(), entry.getValue());
        }
        return buildValidated(values, false, true);
    }

    public static class Builder {
        private final Map<String, Object> values = new HashMap<>();

        public <T> Builder set(ConfigOption<T> option, T value) {
            values.put(option.key(), value);
            return this;
        }

        public <T> Builder setIfAbsent(ConfigOption<T> option, T value) {
            if (!values.containsKey(option.key())) {
                values.put(option.key(), value);
            }
            return this;
        }

        public RawWiseConfig build() {
            return buildValidated(values, false, true);
        }
    }
}
