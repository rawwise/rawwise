package io.github.rawwise.governance.config;

import java.util.ArrayList;
import java.util.List;

public final class ConfigConstraints {

    public static final ConfigConstraint WAL_HDFS_DEPENDENCY =
            new ConfigConstraint() {
                @Override
                public String description() {
                    return "WAL-HDFS dependency: durability-level=NODE requires non-empty wal.hdfs-path";
                }

                @Override
                public boolean validate(RawWiseConfig config) {
                    String durabilityLevel = config.get(RawWiseOptions.STORAGE_DURABILITY_LEVEL);
                    String hdfsPath = config.get(RawWiseOptions.WAL_HDFS_PATH);
                    if ("NODE".equalsIgnoreCase(durabilityLevel)) {
                        return hdfsPath != null && !hdfsPath.isEmpty();
                    }
                    return true;
                }

                @Override
                public String errorMessage(RawWiseConfig config) {
                    return "durability-level=NODE requires non-empty wal.hdfs-path, but wal.hdfs-path is empty";
                }
            };

    public static final ConfigConstraint LMDB_SIZE_ORDERING =
            new ConfigConstraint() {
                @Override
                public String description() {
                    return "LMDB size ordering: initial-map-size-mb must be less than max-map-size-mb";
                }

                @Override
                public boolean validate(RawWiseConfig config) {
                    long initial = config.get(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB);
                    long max = config.get(RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB);
                    return initial < max;
                }

                @Override
                public String errorMessage(RawWiseConfig config) {
                    long initial = config.get(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB);
                    long max = config.get(RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB);
                    return "storage.lmdb.initial-map-size-mb ("
                            + initial
                            + ") must be less than max-map-size-mb ("
                            + max
                            + ")";
                }
            };

    public static final ConfigConstraint BACKPRESSURE_THRESHOLD_ORDERING =
            new ConfigConstraint() {
                @Override
                public String description() {
                    return "Backpressure threshold ordering: warning < throttle < reject";
                }

                @Override
                public boolean validate(RawWiseConfig config) {
                    double warning = config.get(RawWiseOptions.BACKPRESSURE_WARNING_THRESHOLD);
                    double throttle = config.get(RawWiseOptions.BACKPRESSURE_THROTTLE_THRESHOLD);
                    double reject = config.get(RawWiseOptions.BACKPRESSURE_REJECT_THRESHOLD);
                    return warning < throttle && throttle < reject;
                }

                @Override
                public String errorMessage(RawWiseConfig config) {
                    double warning = config.get(RawWiseOptions.BACKPRESSURE_WARNING_THRESHOLD);
                    double throttle = config.get(RawWiseOptions.BACKPRESSURE_THROTTLE_THRESHOLD);
                    double reject = config.get(RawWiseOptions.BACKPRESSURE_REJECT_THRESHOLD);
                    return "backpressure thresholds must satisfy warning ("
                            + warning
                            + ") < throttle ("
                            + throttle
                            + ") < reject ("
                            + reject
                            + ")";
                }
            };

    public static final ConfigConstraint COMPACTION_LEVEL_BOUNDS =
            new ConfigConstraint() {
                @Override
                public String description() {
                    return "Compaction level bounds: max-level must be >= 1";
                }

                @Override
                public boolean validate(RawWiseConfig config) {
                    int maxLevel = config.get(RawWiseOptions.COMPACTION_MAX_LEVEL);
                    return maxLevel >= 1;
                }

                @Override
                public String errorMessage(RawWiseConfig config) {
                    int maxLevel = config.get(RawWiseOptions.COMPACTION_MAX_LEVEL);
                    return "compaction.max-level must be >= 1, got " + maxLevel;
                }
            };

    public static final ConfigConstraint FLUSH_THRESHOLD_POSITIVE =
            new ConfigConstraint() {
                @Override
                public String description() {
                    return "Flush threshold must be positive";
                }

                @Override
                public boolean validate(RawWiseConfig config) {
                    long threshold = config.get(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES);
                    return threshold > 0;
                }

                @Override
                public String errorMessage(RawWiseConfig config) {
                    long threshold = config.get(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES);
                    return "storage.flush.threshold-bytes must be > 0, got " + threshold;
                }
            };

    public static final ConfigConstraint BACKPRESSURE_WEIGHT_SUM =
            new ConfigConstraint() {
                private static final double TOLERANCE = 0.01;

                @Override
                public String description() {
                    return "Backpressure weight sum must equal 1.0 (±0.01 tolerance)";
                }

                @Override
                public boolean validate(RawWiseConfig config) {
                    double queue = config.get(RawWiseOptions.BACKPRESSURE_QUEUE_WEIGHT);
                    double cpu = config.get(RawWiseOptions.BACKPRESSURE_CPU_WEIGHT);
                    double memory = config.get(RawWiseOptions.BACKPRESSURE_MEMORY_WEIGHT);
                    double disk = config.get(RawWiseOptions.BACKPRESSURE_DISK_WEIGHT);
                    double network = config.get(RawWiseOptions.BACKPRESSURE_NETWORK_WEIGHT);
                    double sum = queue + cpu + memory + disk + network;
                    return Math.abs(sum - 1.0) <= TOLERANCE;
                }

                @Override
                public String errorMessage(RawWiseConfig config) {
                    double queue = config.get(RawWiseOptions.BACKPRESSURE_QUEUE_WEIGHT);
                    double cpu = config.get(RawWiseOptions.BACKPRESSURE_CPU_WEIGHT);
                    double memory = config.get(RawWiseOptions.BACKPRESSURE_MEMORY_WEIGHT);
                    double disk = config.get(RawWiseOptions.BACKPRESSURE_DISK_WEIGHT);
                    double network = config.get(RawWiseOptions.BACKPRESSURE_NETWORK_WEIGHT);
                    double sum = queue + cpu + memory + disk + network;
                    return "backpressure weight sum must equal 1.0 (±0.01), got "
                            + sum
                            + " (queue="
                            + queue
                            + ", cpu="
                            + cpu
                            + ", memory="
                            + memory
                            + ", disk="
                            + disk
                            + ", network="
                            + network
                            + ")";
                }
            };

    public static final ConfigConstraint CHECKPOINT_LSN_BINDING_CONSISTENCY =
            new ConfigConstraint() {
                @Override
                public String description() {
                    return "When LSN binding is enabled, snapshot auto-generation on checkpoint must be enabled";
                }

                @Override
                public boolean validate(RawWiseConfig config) {
                    boolean lsnEnabled = config.get(RawWiseOptions.CHECKPOINT_LSN_BINDING_ENABLED);
                    if (!lsnEnabled) return true;
                    return config.get(RawWiseOptions.SNAPSHOT_AUTO_GENERATE_ON_CHECKPOINT);
                }

                @Override
                public String errorMessage(RawWiseConfig config) {
                    return "LSN binding is enabled but snapshot.auto-generate-on-checkpoint is disabled. "
                            + "Set rawwise.snapshot.auto-generate-on-checkpoint=true when using LSN binding.";
                }
            };

    private static final int MIN_BEARER_TOKEN_LENGTH = 16;

    public static final ConfigConstraint ADMIN_SECURITY_CONSTRAINT =
            new ConfigConstraint() {
                @Override
                public String description() {
                    return "Admin security: when admin is enabled, bearer token must meet minimum length and CIDRs must be valid";
                }

                @Override
                public boolean validate(RawWiseConfig config) {
                    return collectViolations(config).isEmpty();
                }

                @Override
                public String errorMessage(RawWiseConfig config) {
                    List<String> violations = collectViolations(config);
                    return violations.isEmpty() ? "" : String.join("; ", violations);
                }

                private List<String> collectViolations(RawWiseConfig config) {
                    List<String> violations = new ArrayList<>();
                    boolean adminEnabled = config.get(RawWiseOptions.CONFIG_ADMIN_ENABLED);
                    if (!adminEnabled) return violations;

                    String token = config.get(RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN);
                    if (token == null || token.isBlank()) {
                        violations.add("bearer token is empty");
                    } else if (token.length() < MIN_BEARER_TOKEN_LENGTH) {
                        violations.add(
                                "bearer token too short (min "
                                        + MIN_BEARER_TOKEN_LENGTH
                                        + " chars, got "
                                        + token.length()
                                        + ")");
                    }

                    List<String> cidrs = config.get(RawWiseOptions.CONFIG_ADMIN_ALLOWED_CIDRS);
                    if (cidrs != null && !cidrs.isEmpty()) {
                        for (String cidr : cidrs) {
                            if (!isValidCidr(cidr)) {
                                violations.add("invalid CIDR format: " + cidr);
                            }
                        }
                    }

                    return violations;
                }

                private boolean isValidCidr(String cidr) {
                    try {
                        String[] parts = cidr.split("/");
                        if (parts.length != 2) return false;
                        java.net.InetAddress addr = java.net.InetAddress.getByName(parts[0]);
                        int prefix = Integer.parseInt(parts[1]);
                        int maxPrefix = addr instanceof java.net.Inet6Address ? 128 : 32;
                        return prefix >= 0 && prefix <= maxPrefix;
                    } catch (Exception e) {
                        return false;
                    }
                }
            };

    public static final List<ConfigConstraint> ALL =
            List.of(
                    WAL_HDFS_DEPENDENCY,
                    LMDB_SIZE_ORDERING,
                    BACKPRESSURE_THRESHOLD_ORDERING,
                    COMPACTION_LEVEL_BOUNDS,
                    FLUSH_THRESHOLD_POSITIVE,
                    BACKPRESSURE_WEIGHT_SUM,
                    CHECKPOINT_LSN_BINDING_CONSISTENCY,
                    ADMIN_SECURITY_CONSTRAINT);

    private ConfigConstraints() {}
}
