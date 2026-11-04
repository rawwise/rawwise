package io.github.rawwise.governance.config;

import static org.assertj.core.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

@Execution(ExecutionMode.SAME_THREAD)
@DisplayName("Unified Config System - E2E Business Scenario Tests")
class UnifiedConfigTest {

    @TempDir Path tempDir;

    private final Set<String> systemPropsToClean = new java.util.HashSet<>();

    @AfterEach
    void cleanupSystemProperties() {
        for (String key : systemPropsToClean) {
            System.clearProperty(key);
        }
        systemPropsToClean.clear();
    }

    // ========================================================================
    // E2E-1: HOCON File Loading
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-1: HOCON file loading - all types parsed, Duration/MemorySize syntax, nested objects flattened")
    void testHoconFileLoadingCompleteFlow() throws Exception {
        String hoconContent =
                """
            rawwise {
              cluster.id = "hocon-cluster"

              storage {
                mmap.path = "/data/rawwise/mmap"
                mmap.size = 2GB
                durability-level = "PROCESS"
              }

              storage.lmdb {
                initial-map-size-mb = 256
                max-map-size-mb = 4096
                usage-threshold = 0.75
              }

              wal {
                enabled = true
                sync-interval-ms = 5000
                hdfs-path = ""
              }

              routing {
                shard-count = 2048
              }

              backpressure {
                warning-threshold = 0.7
                throttle-threshold = 0.9
                reject-threshold = 0.95
              }

              compaction {
                max-level = 3
              }

              storage.flush {
                threshold-bytes = 512MB
              }

              zookeeper.connection-timeout = 10s
              zookeeper.session-timeout = 5m

              config.admin.allowed-cidrs = ["10.0.0.0/8", "172.16.0.0/12"]
            }
            """;

        Path configFile = tempDir.resolve("rawwise.conf");
        Files.writeString(configFile, hoconContent);

        RawWiseConfig config = ConfigLoader.builder().configFile(configFile).load();

        assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("hocon-cluster");
        assertThat(config.get(RawWiseOptions.STORAGE_MMAP_PATH)).isEqualTo("/data/rawwise/mmap");
        assertThat(config.get(RawWiseOptions.ROUTING_SHARD_COUNT)).isEqualTo(2048);
        assertThat(config.get(RawWiseOptions.COMPACTION_MAX_LEVEL)).isEqualTo(3);
        assertThat(config.get(RawWiseOptions.STORAGE_MMAP_SIZE))
                .isEqualTo(2L * 1024L * 1024L * 1024L);
        assertThat(config.get(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB)).isEqualTo(256L);
        assertThat(config.get(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES))
                .isEqualTo(512L * 1024L * 1024L);
        assertThat(config.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.75);
        assertThat(config.get(RawWiseOptions.WAL_ENABLED)).isTrue();
        assertThat(config.get(RawWiseOptions.ZOOKEEPER_CONNECTION_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(10));
        assertThat(config.get(RawWiseOptions.ZOOKEEPER_SESSION_TIMEOUT))
                .isEqualTo(Duration.ofMinutes(5));
        assertThat(config.get(RawWiseOptions.WAL_SYNC_INTERVAL_MS)).isEqualTo(5000L);
        assertThat(config.get(RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB)).isEqualTo(4096L);

        @SuppressWarnings("unchecked")
        List<String> cidrs = (List<String>) config.get(RawWiseOptions.CONFIG_ADMIN_ALLOWED_CIDRS);
        assertThat(cidrs).containsExactly("10.0.0.0/8", "172.16.0.0/12");
    }

    // ========================================================================
    // E2E-2: Priority Merging (with env layer)
    // ========================================================================

    @Test
    @DisplayName("E2E-2: Priority merging - CLI > env > file > SparkConf > system props > defaults")
    void testPriorityMergingCompleteFlow() throws Exception {
        String hoconContent =
                """
            rawwise.cluster.id = "from-file"
            rawwise.storage.mmap.path = "/file-path"
            rawwise.routing.shard-count = 512
            """;
        Path configFile = tempDir.resolve("priority.conf");
        Files.writeString(configFile, hoconContent);

        setSystemProperty("rawwise.cluster.id", "from-sysprop");
        setSystemProperty("rawwise.storage.mmap.path", "/sysprop-path");

        SparkConf sparkConf =
                new SparkConf()
                        .set("rawwise.cluster.id", "from-spark")
                        .set("rawwise.storage.store-id", "spark-store");

        Map<String, String> cliOverrides = Map.of("rawwise.cluster.id", "from-cli");

        boolean envVarSet = false;
        try {
            setEnvVar("RAWWISE_CLUSTER_ID", "from-env");
            setEnvVar("RAWWISE_STORAGE_MMAP_PATH", "/env-path");
            envVarSet = true;
        } catch (Exception e) {
            // env var reflection unavailable; env priority verified in E2E-3
        }

        try {
            RawWiseConfig config =
                    ConfigLoader.builder()
                            .configFile(configFile)
                            .sparkConf(sparkConf)
                            .cliOverrides(cliOverrides)
                            .load();

            // CLI > env > file > SparkConf > system props
            assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("from-cli");

            if (envVarSet) {
                // env > file: mmap.path from env overrides file
                assertThat(config.get(RawWiseOptions.STORAGE_MMAP_PATH)).isEqualTo("/env-path");
            } else {
                // file > sysprop (no env override)
                assertThat(config.get(RawWiseOptions.STORAGE_MMAP_PATH)).isEqualTo("/file-path");
            }

            assertThat(config.get(RawWiseOptions.STORAGE_STORE_ID)).isEqualTo("spark-store");
            assertThat(config.get(RawWiseOptions.ROUTING_SHARD_COUNT)).isEqualTo(512);
            assertThat(config.get(RawWiseOptions.WAL_BUFFER_SIZE)).isEqualTo(8192);
        } finally {
            if (envVarSet) {
                removeEnvVar("RAWWISE_CLUSTER_ID");
                removeEnvVar("RAWWISE_STORAGE_MMAP_PATH");
            }
        }
    }

    // ========================================================================
    // E2E-3: Environment Variable Mapping
    // ========================================================================

    @Test
    @DisplayName("E2E-3: Environment variable mapping - RAWWISE_* -> rawwise.* with __->- and _->.")
    void testEnvVarMappingCompleteFlow() throws Exception {
        setEnvVar("RAWWISE_CLUSTER_ID", "env-cluster");
        setEnvVar("RAWWISE_WAL_ENABLED", "true");
        setEnvVar("RAWWISE_STORAGE_LMDB_INITIAL__MAP__SIZE__MB", "512");

        try {
            Path configFile = writeMinimalConfig();

            RawWiseConfig config = ConfigLoader.builder().configFile(configFile).load();

            assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("env-cluster");
            assertThat(config.get(RawWiseOptions.WAL_ENABLED)).isTrue();
            assertThat(config.get(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB)).isEqualTo(512L);
        } finally {
            removeEnvVar("RAWWISE_CLUSTER_ID");
            removeEnvVar("RAWWISE_WAL_ENABLED");
            removeEnvVar("RAWWISE_STORAGE_LMDB_INITIAL__MAP__SIZE__MB");
        }
    }

    // ========================================================================
    // E2E-4: Type Coercion
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-4: Type coercion - String->Integer/Long/Double/Boolean/Duration/MemorySize via ConfigLoader")
    void testTypeCoercionCompleteFlow() throws Exception {
        SparkConf sparkConf =
                new SparkConf()
                        .set("rawwise.routing.shard-count", "2048")
                        .set("rawwise.storage.mmap.size", "1073741824")
                        .set("rawwise.wal.max-size-bytes", "200MB")
                        .set("rawwise.storage.lmdb.usage-threshold", "0.85")
                        .set("rawwise.wal.enabled", "true")
                        .set("rawwise.metrics.enable-jmx", "1")
                        .set("rawwise.clean.enabled", "yes")
                        .set("rawwise.metrics.enable-system", "on")
                        .set("rawwise.config.admin.enabled", "false")
                        .set("rawwise.zookeeper.connection-timeout", "30s")
                        .set("rawwise.zookeeper.session-timeout", "PT5M")
                        .set("rawwise.wal.sync-interval-ms", "300000")
                        .set("rawwise.storage.flush.threshold-bytes", "1GB");

        Path configFile = writeMinimalConfig();

        RawWiseConfig config =
                ConfigLoader.builder().configFile(configFile).sparkConf(sparkConf).load();

        assertThat(config.get(RawWiseOptions.ROUTING_SHARD_COUNT))
                .isInstanceOf(Integer.class)
                .isEqualTo(2048);
        assertThat(config.get(RawWiseOptions.STORAGE_MMAP_SIZE))
                .isInstanceOf(Long.class)
                .isEqualTo(1073741824L);
        assertThat(config.get(RawWiseOptions.WAL_MAX_SIZE_BYTES))
                .isInstanceOf(Long.class)
                .isEqualTo(200L * 1024L * 1024L);
        assertThat(config.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                .isInstanceOf(Double.class)
                .isEqualTo(0.85);
        assertThat(config.get(RawWiseOptions.WAL_ENABLED)).isInstanceOf(Boolean.class).isTrue();
        assertThat(config.get(RawWiseOptions.METRICS_ENABLE_JMX))
                .isInstanceOf(Boolean.class)
                .isTrue();
        assertThat(config.get(RawWiseOptions.CLEAN_ENABLED)).isInstanceOf(Boolean.class).isTrue();
        assertThat(config.get(RawWiseOptions.METRICS_ENABLE_SYSTEM))
                .isInstanceOf(Boolean.class)
                .isTrue();
        assertThat(config.get(RawWiseOptions.CONFIG_ADMIN_ENABLED))
                .isInstanceOf(Boolean.class)
                .isFalse();
        assertThat(config.get(RawWiseOptions.ZOOKEEPER_CONNECTION_TIMEOUT))
                .isEqualTo(Duration.ofSeconds(30));
        assertThat(config.get(RawWiseOptions.ZOOKEEPER_SESSION_TIMEOUT))
                .isEqualTo(Duration.ofMinutes(5));
        assertThat(config.get(RawWiseOptions.WAL_SYNC_INTERVAL_MS))
                .isInstanceOf(Long.class)
                .isEqualTo(300000L);
        assertThat(config.get(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES))
                .isInstanceOf(Long.class)
                .isEqualTo(1L * 1024L * 1024L * 1024L);
    }

    // ========================================================================
    // E2E-5: Cross-Key Constraint Validation
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-5: Cross-key constraint validation - WAL-HDFS, LMDB size, backpressure, compaction, flush, weights")
    void testConstraintValidationCompleteFlow() throws Exception {
        // --- Case 1: durability-level=NODE without wal.hdfs-path ---
        String hoconNodeNoHdfs =
                """
            rawwise.storage.durability-level = "NODE"
            rawwise.wal.hdfs-path = ""
            """;
        Path case1File = tempDir.resolve("case1.conf");
        Files.writeString(case1File, hoconNodeNoHdfs);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case1File).load())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("durability-level=NODE requires non-empty wal.hdfs-path");

        // --- Case 2: initial-map-size-mb > max-map-size-mb ---
        String hoconLmdbSizeInverted =
                """
            rawwise.storage.lmdb.initial-map-size-mb = 8192
            rawwise.storage.lmdb.max-map-size-mb = 4096
            """;
        Path case2File = tempDir.resolve("case2.conf");
        Files.writeString(case2File, hoconLmdbSizeInverted);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case2File).load())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("initial-map-size-mb")
                .hasMessageContaining("must be less than max-map-size-mb");

        // --- Case 3: backpressure thresholds not in order ---
        String hoconBackpressureInverted =
                """
            rawwise.backpressure.warning-threshold = 0.9
            rawwise.backpressure.throttle-threshold = 0.7
            rawwise.backpressure.reject-threshold = 0.95
            """;
        Path case3File = tempDir.resolve("case3.conf");
        Files.writeString(case3File, hoconBackpressureInverted);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case3File).load())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("warning")
                .hasMessageContaining("throttle")
                .hasMessageContaining("reject");

        // --- Case 4: COMPACTION_LEVEL_BOUNDS - max-level=0 ---
        String hoconCompactionZero =
                """
            rawwise.compaction.max-level = 0
            """;
        Path case4File = tempDir.resolve("case4.conf");
        Files.writeString(case4File, hoconCompactionZero);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case4File).load())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("compaction.max-level must be >= 1");

        // --- Case 5: FLUSH_THRESHOLD_POSITIVE - threshold=0 ---
        String hoconFlushZero =
                """
            rawwise.storage.flush.threshold-bytes = 0
            """;
        Path case5File = tempDir.resolve("case5.conf");
        Files.writeString(case5File, hoconFlushZero);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case5File).load())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("storage.flush.threshold-bytes must be > 0");

        // --- Case 6: BACKPRESSURE_WEIGHT_SUM - weights don't sum to 1.0 ---
        String hoconBadWeights =
                """
            rawwise.backpressure.queue-weight = 0.5
            rawwise.backpressure.cpu-weight = 0.5
            rawwise.backpressure.memory-weight = 0.5
            rawwise.backpressure.disk-weight = 0.5
            rawwise.backpressure.network-weight = 0.5
            """;
        Path case6File = tempDir.resolve("case6.conf");
        Files.writeString(case6File, hoconBadWeights);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case6File).load())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("backpressure weight sum must equal 1.0");

        // --- Case 7: Valid config passes all constraints ---
        String hoconValid =
                """
            rawwise.storage.durability-level = "NODE"
            rawwise.wal.hdfs-path = "hdfs://namenode:8020/rawwise/wal"
            rawwise.storage.lmdb.initial-map-size-mb = 128
            rawwise.storage.lmdb.max-map-size-mb = 4096
            rawwise.backpressure.warning-threshold = 0.7
            rawwise.backpressure.throttle-threshold = 0.9
            rawwise.backpressure.reject-threshold = 0.95
            """;
        Path case7File = tempDir.resolve("case7.conf");
        Files.writeString(case7File, hoconValid);

        RawWiseConfig validConfig = ConfigLoader.builder().configFile(case7File).load();
        assertThat(validConfig.get(RawWiseOptions.STORAGE_DURABILITY_LEVEL)).isEqualTo("NODE");
        assertThat(validConfig.get(RawWiseOptions.WAL_HDFS_PATH))
                .isEqualTo("hdfs://namenode:8020/rawwise/wal");
    }

    // ========================================================================
    // E2E-6: Unknown Key Detection
    // ========================================================================

    @Test
    @DisplayName("E2E-6: Unknown key detection - strict mode throws, non-strict mode continues")
    void testUnknownKeyDetectionCompleteFlow() throws Exception {
        String hoconWithUnknown =
                """
            rawwise.cluster.id = "test-cluster"
            rawwise.unknown.key = "should-be-detected"
            rawwise.typo.shard-count = 100
            """;
        Path configFile = tempDir.resolve("unknown-keys.conf");
        Files.writeString(configFile, hoconWithUnknown);

        setOwnerOnlyPermissions(configFile);

        assertThatThrownBy(
                        () -> ConfigLoader.builder().configFile(configFile).strictMode(true).load())
                .isInstanceOf(ConfigException.UnknownKeyError.class)
                .hasMessageContaining("rawwise.unknown.key")
                .hasMessageContaining("rawwise.typo.shard-count");

        RawWiseConfig config =
                ConfigLoader.builder().configFile(configFile).strictMode(false).load();

        assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("test-cluster");
        assertThat(config.get(RawWiseOptions.ROUTING_SHARD_COUNT))
                .isEqualTo(RawWiseOptions.ROUTING_SHARD_COUNT.defaultValue());
    }

    // ========================================================================
    // E2E-7: Alias Resolution (Step 6 removed - isolated static method call)
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-7: Alias resolution - deprecated spark.rawwise.* keys resolve to rawwise.* keys")
    void testAliasResolutionCompleteFlow() throws Exception {
        SparkConf sparkConf =
                new SparkConf()
                        .set("spark.rawwise.routing.shardCount", "4096")
                        .set("spark.rawwise.routing.register.maxRetries", "5")
                        .set("spark.rawwise.routing.register.backoffMs", "500");

        Path configFile = writeMinimalConfig();

        RawWiseConfig config =
                ConfigLoader.builder().configFile(configFile).sparkConf(sparkConf).load();

        assertThat(config.get(RawWiseOptions.ROUTING_SHARD_COUNT)).isEqualTo(4096);
        assertThat(config.get(RawWiseOptions.ROUTING_REGISTER_MAX_RETRIES)).isEqualTo(5);
        assertThat(config.get(RawWiseOptions.ROUTING_REGISTER_BACKOFF_MS)).isEqualTo(500L);
    }

    // ========================================================================
    // E2E-8: Spark Broadcast - validators transient verification
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-8: Spark broadcast - serialize on Driver, deserialize on Executor, values preserved, validators transient")
    void testSparkBroadcastCompleteFlow() throws Exception {
        // === Driver side ===

        RawWiseConfig driverConfig =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CLUSTER_ID, "prod-cluster-01")
                        .set(RawWiseOptions.STORAGE_MMAP_PATH, "/data/rawwise/prod")
                        .set(RawWiseOptions.STORAGE_MMAP_SIZE, 4L * 1024L * 1024L * 1024L)
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "NODE")
                        .set(RawWiseOptions.WAL_HDFS_PATH, "hdfs://nn:8020/wal")
                        .set(RawWiseOptions.ROUTING_SHARD_COUNT, 2048)
                        .set(RawWiseOptions.WAL_SYNC_INTERVAL_MS, 2000L)
                        .set(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD, 0.80)
                        .set(RawWiseOptions.BACKPRESSURE_WARNING_THRESHOLD, 0.7)
                        .set(RawWiseOptions.BACKPRESSURE_THROTTLE_THRESHOLD, 0.9)
                        .set(RawWiseOptions.BACKPRESSURE_REJECT_THRESHOLD, 0.95)
                        .build();

        assertThat(driverConfig.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("prod-cluster-01");
        assertThat(driverConfig.get(RawWiseOptions.STORAGE_MMAP_SIZE))
                .isEqualTo(4L * 1024L * 1024L * 1024L);

        byte[] serialized = serialize(driverConfig);

        // === Executor side ===

        RawWiseConfig executorConfig = deserialize(serialized);

        assertThat(executorConfig.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("prod-cluster-01");
        assertThat(executorConfig.get(RawWiseOptions.STORAGE_MMAP_PATH))
                .isEqualTo("/data/rawwise/prod");
        assertThat(executorConfig.get(RawWiseOptions.STORAGE_MMAP_SIZE))
                .isEqualTo(4L * 1024L * 1024L * 1024L);
        assertThat(executorConfig.get(RawWiseOptions.STORAGE_DURABILITY_LEVEL)).isEqualTo("NODE");
        assertThat(executorConfig.get(RawWiseOptions.WAL_HDFS_PATH))
                .isEqualTo("hdfs://nn:8020/wal");
        assertThat(executorConfig.get(RawWiseOptions.ROUTING_SHARD_COUNT)).isEqualTo(2048);
        assertThat(executorConfig.get(RawWiseOptions.WAL_SYNC_INTERVAL_MS)).isEqualTo(2000L);
        assertThat(executorConfig.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.80);

        // --- Verify validators are transient: deserialized ConfigOption skips validation ---
        // Create a custom ConfigOption with validators (not in RawWiseOptions)
        // so validators cannot be recovered from RawWiseOptions.findOption()
        ConfigOption<Integer> customValidatedOption =
                ConfigOption.<Integer>builder("test.transient-validators-option", 10, Integer.class)
                        .validateRange(1, 100)
                        .build();

        // On Driver: validation works - invalid value throws
        assertThatThrownBy(() -> driverConfig.with(customValidatedOption, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Validation failed");

        // Serialize the custom ConfigOption (simulating broadcast of option metadata)
        byte[] optionBytes = serializeObject(customValidatedOption);
        @SuppressWarnings("unchecked")
        ConfigOption<Integer> deserializedOption =
                (ConfigOption<Integer>) deserializeObject(optionBytes);

        // On Executor: deserialized option has empty validators - invalid value does NOT throw
        RawWiseConfig executorModified = executorConfig.with(deserializedOption, 0);
        assertThat(executorModified.get(deserializedOption)).isEqualTo(0);

        // Static RawWiseOptions fields still have validators on Executor
        assertThatThrownBy(() -> executorConfig.with(RawWiseOptions.ROUTING_SHARD_COUNT, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Validation failed");

        // get() returns defaults for unset keys
        assertThat(executorConfig.get(RawWiseOptions.WAL_BUFFER_SIZE))
                .isEqualTo(RawWiseOptions.WAL_BUFFER_SIZE.defaultValue());

        // with() returns new instance (immutability)
        RawWiseConfig modified = executorConfig.with(RawWiseOptions.CLUSTER_ID, "modified-cluster");
        assertThat(modified.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("modified-cluster");
        assertThat(executorConfig.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("prod-cluster-01");
    }

    // ========================================================================
    // E2E-9: ConfigMetadata Registry - registration integrity
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-9: ConfigMetadata registry - registration completeness, hot-reloadable/sensitive sets consistent")
    void testConfigMetadataRegistryCompleteFlow() {
        // --- Registration completeness: most RawWiseOptions keys are registered ---
        Set<String> allKeys = RawWiseOptions.allKeys();
        Set<String> registeredKeys = ConfigMetadata.allRegisteredKeys();
        assertThat(registeredKeys.size()).isGreaterThan(allKeys.size() * 9 / 10);
        assertThat(registeredKeys)
                .contains(
                        RawWiseOptions.CLUSTER_ID.key(),
                        RawWiseOptions.ROUTING_SHARD_COUNT.key(),
                        RawWiseOptions.WAL_SYNC_INTERVAL_MS.key());

        // --- Hot-reloadable keys set is non-empty and consistent ---
        Set<String> hotKeys = ConfigMetadata.allHotReloadableKeys();
        assertThat(hotKeys).isNotEmpty();
        // Verify known hot-reloadable keys are present
        assertThat(hotKeys)
                .contains(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(),
                        RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES.key(),
                        RawWiseOptions.WAL_SYNC_INTERVAL_MS.key(),
                        RawWiseOptions.BACKPRESSURE_QUEUE_WEIGHT.key());
        // Verify non-hot-reloadable keys are absent
        assertThat(hotKeys)
                .doesNotContain(
                        RawWiseOptions.CLUSTER_ID.key(),
                        RawWiseOptions.STORAGE_MMAP_PATH.key(),
                        RawWiseOptions.ROUTING_SHARD_COUNT.key());

        // --- Sensitive keys set is non-empty and consistent ---
        Set<String> sensitiveKeys =
                ConfigMetadata.allRegisteredKeys().stream()
                        .filter(ConfigMetadata::isSensitive)
                        .collect(java.util.stream.Collectors.toSet());
        assertThat(sensitiveKeys).isNotEmpty();
        assertThat(sensitiveKeys)
                .contains(
                        RawWiseOptions.ZOOKEEPER_CONNECT_STRING.key(),
                        RawWiseOptions.WAL_HDFS_PATH.key(),
                        RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN.key());
        assertThat(sensitiveKeys)
                .doesNotContain(
                        RawWiseOptions.CLUSTER_ID.key(), RawWiseOptions.ROUTING_SHARD_COUNT.key());

        // --- TransitionPolicy consistency (Phase 1 registration verification) ---
        // Hot-reloadable keys with NEXT_CYCLE policy
        assertThat(ConfigMetadata.getTransitionPolicy(RawWiseOptions.WAL_SYNC_INTERVAL_MS.key()))
                .isEqualTo(TransitionPolicy.NEXT_CYCLE);
        assertThat(
                        ConfigMetadata.getTransitionPolicy(
                                RawWiseOptions.WAL_RECOVERY_CHECK_INTERVAL_MS.key()))
                .isEqualTo(TransitionPolicy.NEXT_CYCLE);
        // Hot-reloadable keys with DATA_VERSION_SAFE policy
        assertThat(
                        ConfigMetadata.getTransitionPolicy(
                                RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES.key()))
                .isEqualTo(TransitionPolicy.DATA_VERSION_SAFE);
        assertThat(
                        ConfigMetadata.getTransitionPolicy(
                                RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key()))
                .isEqualTo(TransitionPolicy.DATA_VERSION_SAFE);
        // Non-hot-reloadable keys default to IMMEDIATE
        assertThat(ConfigMetadata.getTransitionPolicy(RawWiseOptions.CLUSTER_ID.key()))
                .isEqualTo(TransitionPolicy.IMMEDIATE);
    }

    // ========================================================================
    // E2E-10: ConfigFactory Builder
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-10: ConfigFactory builder - set/setIfAbsent/build with constraint validation and type-safe get")
    void testConfigFactoryBuilderCompleteFlow() {
        RawWiseConfig config =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CLUSTER_ID, "builder-cluster")
                        .set(RawWiseOptions.STORAGE_MMAP_PATH, "/builder/mmap")
                        .set(RawWiseOptions.STORAGE_MMAP_SIZE, 2L * 1024L * 1024L * 1024L)
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "NODE")
                        .set(RawWiseOptions.WAL_HDFS_PATH, "hdfs://nn:8020/wal")
                        .set(RawWiseOptions.ROUTING_SHARD_COUNT, 4096)
                        .set(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB, 256L)
                        .set(RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB, 8192L)
                        .set(RawWiseOptions.BACKPRESSURE_WARNING_THRESHOLD, 0.7)
                        .set(RawWiseOptions.BACKPRESSURE_THROTTLE_THRESHOLD, 0.9)
                        .set(RawWiseOptions.BACKPRESSURE_REJECT_THRESHOLD, 0.95)
                        .build();

        assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("builder-cluster");
        assertThat(config.get(RawWiseOptions.STORAGE_MMAP_PATH)).isEqualTo("/builder/mmap");
        assertThat(config.get(RawWiseOptions.STORAGE_MMAP_SIZE))
                .isEqualTo(2L * 1024L * 1024L * 1024L);
        assertThat(config.get(RawWiseOptions.ROUTING_SHARD_COUNT)).isEqualTo(4096);

        RawWiseConfig configWithFallback =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CLUSTER_ID, "original")
                        .setIfAbsent(RawWiseOptions.CLUSTER_ID, "fallback")
                        .setIfAbsent(RawWiseOptions.STORAGE_STORE_ID, "fallback-store")
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                        .set(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB, 128L)
                        .set(RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB, 4096L)
                        .set(RawWiseOptions.BACKPRESSURE_WARNING_THRESHOLD, 0.7)
                        .set(RawWiseOptions.BACKPRESSURE_THROTTLE_THRESHOLD, 0.9)
                        .set(RawWiseOptions.BACKPRESSURE_REJECT_THRESHOLD, 0.95)
                        .build();

        assertThat(configWithFallback.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("original");
        assertThat(configWithFallback.get(RawWiseOptions.STORAGE_STORE_ID))
                .isEqualTo("fallback-store");

        assertThatThrownBy(
                        () ->
                                ConfigFactory.builder()
                                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "NODE")
                                        .build())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("durability-level=NODE requires non-empty wal.hdfs-path");

        assertThatThrownBy(
                        () ->
                                ConfigFactory.builder()
                                        .set(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB, 9999L)
                                        .set(RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB, 100L)
                                        .build())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("initial-map-size-mb");

        assertThatThrownBy(
                        () ->
                                ConfigFactory.builder()
                                        .set(RawWiseOptions.ROUTING_SHARD_COUNT, 0)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Validation failed");

        assertThatThrownBy(
                        () ->
                                ConfigFactory.builder()
                                        .set(RawWiseOptions.ROUTING_SHARD_COUNT, 70000)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Validation failed");

        RawWiseConfig defaultConfig = ConfigFactory.createDefault();
        assertThat(defaultConfig.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("default");
        assertThat(defaultConfig.get(RawWiseOptions.METASTORE_TYPE)).isEqualTo("inmemory");
        assertThat(defaultConfig.get(RawWiseOptions.ROUTING_SHARD_COUNT)).isEqualTo(1024);
    }

    // ========================================================================
    // E2E-11: HOCON ParseError fault injection
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-11: HOCON ParseError fault injection - malformed HOCON triggers ConfigException.ParseError")
    void testHoconParseErrorFaultInjection() throws Exception {
        // --- Case 1: Unclosed bracket ---
        String unclosedBracket =
                """
            rawwise {
              cluster.id = "test"
              storage {
                mmap.path = "/data"
            """;
        Path case1File = tempDir.resolve("unclosed-bracket.conf");
        Files.writeString(case1File, unclosedBracket);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case1File).load())
                .isInstanceOf(ConfigException.ParseError.class);

        // --- Case 2: Unresolved variable substitution ---
        String unresolvedVar =
                """
            rawwise {
              cluster.id = ${missing.variable}
            }
            """;
        Path case2File = tempDir.resolve("unresolved-var.conf");
        Files.writeString(case2File, unresolvedVar);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case2File).load())
                .isInstanceOf(ConfigException.ParseError.class)
                .hasMessageContaining("Unresolved");

        // --- Case 3: Invalid HOCON syntax ---
        String invalidSyntax = """
            rawwise.cluster.id = "test
            """;
        Path case3File = tempDir.resolve("invalid-syntax.conf");
        Files.writeString(case3File, invalidSyntax);

        assertThatThrownBy(() -> ConfigLoader.builder().configFile(case3File).load())
                .isInstanceOf(ConfigException.ParseError.class);
    }

    // ========================================================================
    // E2E-12: TypeCoercionError fault injection
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-12: TypeCoercionError fault injection - invalid values via SparkConf trigger TypeCoercionError")
    void testTypeCoercionErrorFaultInjection() throws Exception {
        Path configFile = writeMinimalConfig();

        // --- Case 1: "abc" -> Integer ---
        SparkConf sparkConf1 = new SparkConf().set("rawwise.routing.shard-count", "abc");
        assertThatThrownBy(
                        () ->
                                ConfigLoader.builder()
                                        .configFile(configFile)
                                        .sparkConf(sparkConf1)
                                        .load())
                .isInstanceOf(ConfigException.TypeCoercionError.class)
                .hasMessageContaining("shard-count");

        // --- Case 2: "maybe" -> Boolean ---
        SparkConf sparkConf2 = new SparkConf().set("rawwise.wal.enabled", "maybe");
        assertThatThrownBy(
                        () ->
                                ConfigLoader.builder()
                                        .configFile(configFile)
                                        .sparkConf(sparkConf2)
                                        .load())
                .isInstanceOf(ConfigException.TypeCoercionError.class)
                .hasMessageContaining("Boolean");

        // --- Case 3: "not-a-duration" -> Duration ---
        SparkConf sparkConf3 =
                new SparkConf().set("rawwise.zookeeper.connection-timeout", "not-a-duration");
        assertThatThrownBy(
                        () ->
                                ConfigLoader.builder()
                                        .configFile(configFile)
                                        .sparkConf(sparkConf3)
                                        .load())
                .isInstanceOf(ConfigException.TypeCoercionError.class)
                .hasMessageContaining("Duration");

        // --- Case 4: Integer overflow ---
        SparkConf sparkConf4 = new SparkConf().set("rawwise.routing.shard-count", "9999999999");
        assertThatThrownBy(
                        () ->
                                ConfigLoader.builder()
                                        .configFile(configFile)
                                        .sparkConf(sparkConf4)
                                        .load())
                .isInstanceOf(ConfigException.TypeCoercionError.class)
                .hasMessageContaining("overflows Integer");
    }

    // ========================================================================
    // E2E-13: Config file not found / empty file
    // ========================================================================

    @Test
    @DisplayName("E2E-13: Config file not found / empty file - ConfigException and ParseError")
    void testConfigFileNotFoundAndEmptyFile() throws Exception {
        // --- Case 1: Non-existent explicit file path ---
        Path nonExistent = tempDir.resolve("does-not-exist.conf");
        assertThatThrownBy(() -> ConfigLoader.builder().configFile(nonExistent).load())
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("not found");

        // --- Case 2: Empty HOCON file ---
        Path emptyFile = tempDir.resolve("empty.conf");
        Files.writeString(emptyFile, "");
        assertThatThrownBy(() -> ConfigLoader.builder().configFile(emptyFile).load())
                .isInstanceOf(ConfigException.ParseError.class)
                .hasMessageContaining("empty");

        // --- Case 3: Whitespace-only file is also treated as empty ---
        Path whitespaceFile = tempDir.resolve("whitespace.conf");
        Files.writeString(whitespaceFile, "   \n\t  \n");
        assertThatThrownBy(() -> ConfigLoader.builder().configFile(whitespaceFile).load())
                .isInstanceOf(ConfigException.ParseError.class)
                .hasMessageContaining("empty");
    }

    // ========================================================================
    // E2E-14: K8s ConfigMap discovery (RAWWISE_CONFIG_FILE env)
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-14: K8s ConfigMap discovery - RAWWISE_CONFIG_FILE env var loads config without explicit configFile()")
    void testK8sConfigMapDiscovery() throws Exception {
        String hoconContent =
                """
            rawwise.cluster.id = "k8s-cluster"
            rawwise.storage.mmap.path = "/k8s/mmap"
            """;
        Path configFile = tempDir.resolve("k8s-rawwise.conf");
        Files.writeString(configFile, hoconContent);
        setOwnerOnlyPermissions(configFile);

        boolean envVarSet = false;
        try {
            setEnvVar("RAWWISE_CONFIG_FILE", configFile.toAbsolutePath().toString());
            envVarSet = true;
        } catch (Exception e) {
            Assumptions.assumeTrue(
                    false,
                    "Cannot modify env vars (add --add-opens java.base/java.util=ALL-UNNAMED): "
                            + e.getMessage());
        }

        try {
            // Load without explicit configFile() - should discover via RAWWISE_CONFIG_FILE env var
            RawWiseConfig config = ConfigLoader.builder().load();

            assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("k8s-cluster");
            assertThat(config.get(RawWiseOptions.STORAGE_MMAP_PATH)).isEqualTo("/k8s/mmap");
        } finally {
            if (envVarSet) {
                removeEnvVar("RAWWISE_CONFIG_FILE");
            }
        }
    }

    // ========================================================================
    // E2E-15: World-readable file + strict mode
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-15: World-readable file + strict mode - ValidationError with 'world-readable'")
    void testWorldReadableFileStrictMode() throws Exception {
        Assumptions.assumeTrue(
                java.io.File.separator.equals("/"),
                "POSIX file permissions not available on this OS");

        String hoconContent = """
            rawwise.cluster.id = "strict-test"
            """;
        Path configFile = tempDir.resolve("world-readable.conf");
        Files.writeString(configFile, hoconContent);

        // Make file world-readable (chmod 644)
        Set<PosixFilePermission> worldReadable =
                Set.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.GROUP_READ,
                        PosixFilePermission.OTHERS_READ);
        Files.setPosixFilePermissions(configFile, worldReadable);

        assertThatThrownBy(
                        () -> ConfigLoader.builder().configFile(configFile).strictMode(true).load())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("world-readable");

        // Non-strict mode should succeed (just logs a warning)
        RawWiseConfig config =
                ConfigLoader.builder().configFile(configFile).strictMode(false).load();
        assertThat(config.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("strict-test");
    }

    // ========================================================================
    // E2E-16: Classpath config + strict mode rejection
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-16: Classpath config + strict mode rejection - strict mode forbids classpath config source")
    void testClasspathConfigStrictModeRejection() throws Exception {
        // When no explicit file, no RAWWISE_CONFIG_FILE, no RAWWISE_HOME, no cwd rawwise.conf,
        // ConfigLoader falls back to classpath. Strict mode should reject classpath config.
        // This test only runs if a classpath rawwise.conf resource actually exists.
        try {
            ConfigLoader.builder().strictMode(true).load();
            // No exception means no classpath config was found - skip
            Assumptions.assumeTrue(
                    false,
                    "No classpath config resource found; skipping classpath strict mode test");
        } catch (ConfigException.ValidationError e) {
            assertThat(e.getMessage()).contains("classpath");
        }
    }

    // ========================================================================
    // E2E-17: Concurrent config access after deserialization
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-17: Concurrent config access after deserialization - 16 threads x 1000 reads, no exceptions")
    void testConcurrentConfigAccessAfterDeserialization() throws Exception {
        RawWiseConfig original =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CLUSTER_ID, "concurrent-test")
                        .set(RawWiseOptions.STORAGE_MMAP_PATH, "/data/concurrent")
                        .set(RawWiseOptions.ROUTING_SHARD_COUNT, 4096)
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                        .build();

        byte[] serialized = serialize(original);
        RawWiseConfig deserialized = deserialize(serialized);

        int threadCount = 16;
        int iterations = 1000;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threadCount; t++) {
            futures.add(
                    executor.submit(
                            () -> {
                                try {
                                    barrier.await();
                                    for (int i = 0; i < iterations; i++) {
                                        assertThat(deserialized.get(RawWiseOptions.CLUSTER_ID))
                                                .isEqualTo("concurrent-test");
                                        assertThat(
                                                        deserialized.get(
                                                                RawWiseOptions.STORAGE_MMAP_PATH))
                                                .isEqualTo("/data/concurrent");
                                        assertThat(
                                                        deserialized.get(
                                                                RawWiseOptions.ROUTING_SHARD_COUNT))
                                                .isEqualTo(4096);
                                    }
                                } catch (AssertionError | Exception e) {
                                    errorCount.incrementAndGet();
                                }
                            }));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();

        assertThat(errorCount.get()).isEqualTo(0);
    }

    // ========================================================================
    // E2E-18: withFallback() E2E
    // ========================================================================

    @Test
    @DisplayName("E2E-18: withFallback() - existing value preserved, empty config accepts fallback")
    void testWithFallbackCompleteFlow() {
        // --- Case 1: Config with existing value -> withFallback same key -> original preserved ---
        RawWiseConfig existing =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CLUSTER_ID, "original-value")
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                        .build();

        RawWiseConfig withFallback =
                existing.withFallback(RawWiseOptions.CLUSTER_ID, "fallback-value");
        assertThat(withFallback.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("original-value");
        // withFallback returns same instance when key already exists
        assertThat(withFallback).isSameAs(existing);

        // --- Case 2: Empty config -> withFallback -> fallback value set ---
        RawWiseConfig empty = RawWiseConfig.empty();
        RawWiseConfig withFallbackSet =
                empty.withFallback(RawWiseOptions.CLUSTER_ID, "fallback-value");
        assertThat(withFallbackSet.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("fallback-value");

        // --- Case 3: ConfigFactory.builder().setIfAbsent() mirrors withFallback semantics ---
        RawWiseConfig builderFallback =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CLUSTER_ID, "first")
                        .setIfAbsent(RawWiseOptions.CLUSTER_ID, "second")
                        .setIfAbsent(RawWiseOptions.STORAGE_STORE_ID, "fallback-store")
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                        .build();

        assertThat(builderFallback.get(RawWiseOptions.CLUSTER_ID)).isEqualTo("first");
        assertThat(builderFallback.get(RawWiseOptions.STORAGE_STORE_ID))
                .isEqualTo("fallback-store");
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    private Path writeMinimalConfig() throws IOException {
        Path configFile = tempDir.resolve("minimal.conf");
        Files.writeString(configFile, "rawwise.cluster.id = \"minimal\"\n");
        return configFile;
    }

    private static void setOwnerOnlyPermissions(Path path) throws IOException {
        try {
            Set<PosixFilePermission> ownerOnly =
                    Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(path, ownerOnly);
        } catch (UnsupportedOperationException e) {
            // Non-POSIX systems ignore
        }
    }

    private void setSystemProperty(String key, String value) {
        System.setProperty(key, value);
        systemPropsToClean.add(key);
    }

    @SuppressWarnings("unchecked")
    private static void setEnvVar(String key, String value) {
        try {
            Map<String, String> env = System.getenv();
            Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> mutableEnv = (Map<String, String>) field.get(env);
            mutableEnv.put(key, value);
        } catch (NoSuchFieldException e) {
            trySetEnvVarAlternative(key, value);
        } catch (IllegalAccessException e) {
            Assumptions.assumeTrue(
                    false,
                    "Cannot modify env vars (add --add-opens java.base/java.util=ALL-UNNAMED): "
                            + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static void trySetEnvVarAlternative(String key, String value) {
        try {
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            Field theCaseInsensitiveEnvironment =
                    processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            theCaseInsensitiveEnvironment.setAccessible(true);
            Map<String, String> env = (Map<String, String>) theCaseInsensitiveEnvironment.get(null);
            env.put(key, value);
        } catch (Exception e2) {
            Assumptions.assumeTrue(false, "Cannot modify env vars on this JVM: " + e2.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static void removeEnvVar(String key) {
        try {
            Map<String, String> env = System.getenv();
            Field field = env.getClass().getDeclaredField("m");
            field.setAccessible(true);
            Map<String, String> mutableEnv = (Map<String, String>) field.get(env);
            mutableEnv.remove(key);
        } catch (Exception e) {
            try {
                Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
                Field theCaseInsensitiveEnvironment =
                        processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
                theCaseInsensitiveEnvironment.setAccessible(true);
                Map<String, String> env =
                        (Map<String, String>) theCaseInsensitiveEnvironment.get(null);
                env.remove(key);
            } catch (Exception e2) {
                // Best effort
            }
        }
    }

    private static byte[] serialize(Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            return bos.toByteArray();
        }
    }

    private static RawWiseConfig deserialize(byte[] data)
            throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (RawWiseConfig) ois.readObject();
        }
    }

    private static byte[] serializeObject(Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            return bos.toByteArray();
        }
    }

    private static Object deserializeObject(byte[] data)
            throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        }
    }
}
