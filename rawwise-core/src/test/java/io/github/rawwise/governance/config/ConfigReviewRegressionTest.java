package io.github.rawwise.governance.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Config Review Report Regression Tests")
class ConfigReviewRegressionTest {

    @TempDir Path tempDir;

    private DynamicConfigRegistry registry;
    private Path configFile;

    @BeforeEach
    void setUp() throws IOException {
        configFile = tempDir.resolve("rawwise.conf");
        writeConfigFile(0.70);
        RawWiseConfig initialConfig = ConfigLoader.builder().configFile(configFile).load();
        registry =
                DynamicConfigRegistry.create(
                        initialConfig,
                        20,
                        HoconConfigReader::read,
                        ConfigFileWatcher::computeFileHash,
                        () -> new ConfigFileWatcher(100, 50));
    }

    @AfterEach
    void tearDown() {
        if (registry != null && registry.isRunning()) {
            registry.stop();
        }
    }

    @Test
    @DisplayName("BUG 4.1: propagation callback exception does not cause NPE in doReload")
    void propagationCallbackExceptionNoNPE() throws IOException {
        AtomicBoolean abortCalled = new AtomicBoolean(false);

        registry.setPropagationCallback(
                new DynamicConfigRegistry.PropagationCallback() {
                    @Override
                    public DynamicConfigRegistry.PropagationResult propagate(
                            ConfigUpdateMessage message) {
                        throw new RuntimeException("Simulated network failure");
                    }

                    @Override
                    public void sendAbort(
                            ConfigUpdateMessage message, Set<String> targetExecutorIds) {
                        abortCalled.set(true);
                    }
                });

        registry.start(configFile);

        try {
            writeConfigFile(0.85);
            assertThatCode(() -> registry.reload()).doesNotThrowAnyException();
        } finally {
            registry.stop();
        }
    }

    @Test
    @DisplayName("BUG 4.1: propagation callback returns null does not cause NPE in doReload")
    void propagationCallbackReturnsNullNoNPE() throws IOException {
        AtomicBoolean abortCalled = new AtomicBoolean(false);

        registry.setPropagationCallback(
                new DynamicConfigRegistry.PropagationCallback() {
                    @Override
                    public DynamicConfigRegistry.PropagationResult propagate(
                            ConfigUpdateMessage message) {
                        return null;
                    }

                    @Override
                    public void sendAbort(
                            ConfigUpdateMessage message, Set<String> targetExecutorIds) {
                        abortCalled.set(true);
                    }
                });

        registry.start(configFile);

        try {
            writeConfigFile(0.85);
            boolean result = registry.reload();
            assertThat(result).isFalse();
            assertThat(abortCalled.get()).isTrue();
        } finally {
            registry.stop();
        }
    }

    @Test
    @DisplayName("Secret redaction: registered sensitive keys are redacted")
    void registeredSensitiveKeysRedacted() {
        assertThat(ConfigDiff.redactIfNeeded("rawwise.zookeeper.connect-string", "zk://secret"))
                .isEqualTo("***");
    }

    @Test
    @DisplayName("Secret redaction: heuristic keywords trigger redaction for unregistered keys")
    void heuristicSensitiveKeywordsRedacted() {
        assertThat(ConfigDiff.redactIfNeeded("rawwise.custom.password", "my-secret"))
                .isEqualTo("***");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.custom.secret-key", "abc123"))
                .isEqualTo("***");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.custom.token", "tok-123")).isEqualTo("***");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.custom.credential", "cred-123"))
                .isEqualTo("***");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.custom.api-key", "ak-123")).isEqualTo("***");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.custom.access-key", "ak-123"))
                .isEqualTo("***");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.custom.private-key", "pk-123"))
                .isEqualTo("***");
    }

    @Test
    @DisplayName("Secret redaction: non-sensitive keys are not redacted")
    void nonSensitiveKeysNotRedacted() {
        assertThat(ConfigDiff.redactIfNeeded("rawwise.cluster.id", "my-cluster"))
                .isEqualTo("my-cluster");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.routing.shard-count", "1024"))
                .isEqualTo("1024");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.storage.mmap.path", "/data/mmap"))
                .isEqualTo("/data/mmap");
    }

    @Test
    @DisplayName("Secret redaction: 'key' alone does not cause false positive redaction")
    void bareKeyWordDoesNotCauseFalsePositive() {
        assertThat(ConfigDiff.redactIfNeeded("rawwise.shard-key", "value")).isEqualTo("value");
        assertThat(ConfigDiff.redactIfNeeded("rawwise.routing-key", "value")).isEqualTo("value");
    }

    @Test
    @DisplayName("Alias priority: canonicalize before merge preserves high-priority source value")
    void aliasCanonicalizeBeforeMergePreservesPriority() throws Exception {
        String aliasKey = "rawwise.shard.count";
        String canonicalKey = "rawwise.routing.shard-count";

        Map<String, String> aliases = ConfigFactory.getKeyAliases();
        assertThat(aliases).containsKey(aliasKey);
        assertThat(aliases.get(aliasKey)).isEqualTo(canonicalKey);

        String hoconContent =
                String.format(
                        """
            rawwise {
              cluster.id = "alias-test"
              shard.count = 512
            }
            """);
        Path file = tempDir.resolve("alias-test.conf");
        Files.writeString(file, hoconContent);

        RawWiseConfig config = ConfigLoader.builder().configFile(file).load();

        assertThat(config.get(RawWiseOptions.ROUTING_SHARD_COUNT)).isEqualTo(512);
    }

    @Test
    @DisplayName("Unit amplification: MIB key rejects memory-size literal '1g'")
    void mibKeyRejectsMemorySizeLiteral() {
        assertThatThrownBy(
                        () ->
                                ConfigFactory.coerceValue(
                                        "rawwise.storage.lmdb.initial-map-size-mb",
                                        "1g",
                                        Long.class))
                .isInstanceOf(ConfigException.TypeCoercionError.class);
    }

    @Test
    @DisplayName("Unit amplification: MIB key accepts plain number and interprets as MiB")
    void mibKeyAcceptsPlainNumber() {
        Object result =
                ConfigFactory.coerceValue(
                        "rawwise.storage.lmdb.initial-map-size-mb", "1024", Long.class);
        assertThat(result).isEqualTo(1024L);
    }

    @Test
    @DisplayName(
            "Unit amplification: BYTES key accepts memory-size literal '1g' and converts to bytes")
    void bytesKeyAcceptsMemorySizeLiteral() {
        Object result =
                ConfigFactory.coerceValue(
                        "rawwise.storage.flush.threshold-bytes", "1g", Long.class);
        assertThat(result).isEqualTo(1073741824L);
    }

    @Test
    @DisplayName("Registration consistency: KEY_MAP is unmodifiable and allKeys matches")
    void keyMapUnmodifiableAndConsistent() {
        Set<String> allKeys = RawWiseOptions.allKeys();
        assertThat(allKeys).isNotEmpty();
        assertThat(allKeys).contains(RawWiseOptions.ROUTING_SHARD_COUNT.key());
        assertThat(allKeys).contains(RawWiseOptions.CLUSTER_ID.key());
        assertThat(allKeys).doesNotContain("rawwise.nonexistent.key");

        assertThatThrownBy(() -> allKeys.add("rawwise.injected.key"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName(
            "fromSparkConf alias resolution: spark.rawwise.* prefix stripped before alias lookup")
    void fromSparkConfAliasResolution() {
        org.apache.spark.SparkConf sparkConf =
                new org.apache.spark.SparkConf().set("spark.rawwise.routing.shardCount", "4096");

        RawWiseConfig config = ConfigFactory.fromSparkConf(sparkConf);
        assertThat(config.get(RawWiseOptions.ROUTING_SHARD_COUNT)).isEqualTo(4096);
    }

    @Test
    @DisplayName("Sensitive key filtering in RPC propagation")
    void sensitiveKeyFilteredInPropagation() throws IOException {
        AtomicReference<ConfigUpdateMessage> capturedMessage = new AtomicReference<>();

        registry.setPropagationCallback(
                new DynamicConfigRegistry.PropagationCallback() {
                    @Override
                    public DynamicConfigRegistry.PropagationResult propagate(
                            ConfigUpdateMessage message) {
                        capturedMessage.set(message);
                        return new DynamicConfigRegistry.PropagationResult(
                                1, 1, 0, 0, true, Set.of("exec-0"));
                    }

                    @Override
                    public void sendCommit(
                            ConfigUpdateMessage message, Set<String> targetExecutorIds) {}
                });

        registry.start(configFile);

        try {
            writeConfigFile(0.85);
            registry.reload();

            ConfigUpdateMessage msg = capturedMessage.get();
            if (msg != null) {
                assertThat(msg.configValues())
                        .doesNotContainKey(RawWiseOptions.ZOOKEEPER_CONNECT_STRING.key());
                assertThat(msg.configValues())
                        .doesNotContainKey(RawWiseOptions.WAL_HDFS_PATH.key());
            }
        } finally {
            registry.stop();
        }
    }

    @Test
    @DisplayName("CIDR validation: IPv4 prefix > 32 rejected, IPv6 prefix > 128 rejected")
    void cidrValidationRejectsOversizedPrefix() {
        assertThatThrownBy(
                        () ->
                                ConfigFactory.builder()
                                        .set(RawWiseOptions.CONFIG_ADMIN_ENABLED, true)
                                        .set(
                                                RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN,
                                                "a-valid-long-token-here")
                                        .set(
                                                RawWiseOptions.CONFIG_ADMIN_ALLOWED_CIDRS,
                                                java.util.List.of("10.0.0.0/33"))
                                        .build())
                .isInstanceOf(ConfigException.ValidationError.class)
                .hasMessageContaining("invalid CIDR format");
    }

    @Test
    @DisplayName("CIDR validation: IPv4 prefix <= 32 accepted")
    void cidrValidationAcceptsValidIPv4Prefix() {
        RawWiseConfig config =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CONFIG_ADMIN_ENABLED, true)
                        .set(RawWiseOptions.CONFIG_ADMIN_BEARER_TOKEN, "a-valid-long-token-here")
                        .set(
                                RawWiseOptions.CONFIG_ADMIN_ALLOWED_CIDRS,
                                java.util.List.of("10.0.0.0/8", "192.168.1.0/32"))
                        .build();
        assertThat(config.get(RawWiseOptions.CONFIG_ADMIN_ENABLED)).isTrue();
    }

    private void writeConfigFile(double usageThreshold) throws IOException {
        String content =
                String.format(
                        """
            rawwise {
              cluster.id = "test-cluster"
              storage.mmap.path = "/data/rawwise/mmap"
              storage.durability-level = "PROCESS"
              storage.lmdb.usage-threshold = %.2f
              storage.flush.threshold-bytes = %d
              wal.sync-interval-ms = %d
            }
            """,
                        usageThreshold, 512L * 1024L * 1024L, 1000L);
        Files.writeString(configFile, content);
    }
}
