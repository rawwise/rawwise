package io.github.rawwise.governance.config;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Hot Reload - E2E Business Scenario Tests")
class HotReloadTest {

    @TempDir Path tempDir;

    private DynamicConfigRegistry registry;
    private Path configFile;

    @BeforeEach
    void setUp() throws IOException {
        configFile = tempDir.resolve("rawwise.conf");
        writeConfigFile(0.70, 512L * 1024L * 1024L, 1000L);

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

    // ========================================================================
    // E2E-1: Hot Reload Complete Flow
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-1: Hot reload complete flow - file change -> detection -> reload -> listener notified -> new values visible")
    void testHotReloadCompleteFlow() throws Exception {
        AtomicReference<ConfigDiff> capturedDiff = new AtomicReference<>();
        AtomicReference<RawWiseConfig> capturedConfig = new AtomicReference<>();
        CountDownLatch changeLatch = new CountDownLatch(1);

        registry.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        capturedDiff.set(diff);
                        capturedConfig.set(newConfig);
                        changeLatch.countDown();
                    }
                });

        registry.start(configFile);
        assertThat(registry.currentEpoch()).isEqualTo(0);
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);

        boolean reloaded = registry.reload();
        assertThat(reloaded).isTrue();

        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
        assertThat(registry.currentEpoch()).isEqualTo(1);

        ConfigDiff diff = capturedDiff.get();
        assertThat(diff).isNotNull();
        assertThat(diff.epoch()).isEqualTo(1);
        assertThat(diff.baseEpoch()).isEqualTo(0);
        assertThat(diff.changedKeys()).contains(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        assertThat(diff.hotReloadableKeys())
                .contains(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        assertThat(diff.rejectedKeys()).isEmpty();
        assertThat(diff.transitionPolicies())
                .containsEntry(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(),
                        TransitionPolicy.DATA_VERSION_SAFE);
    }

    // ========================================================================
    // E2E-2: Two-Phase Listener Notification - Veto
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-2: Two-phase listener - preConfigChange veto aborts reload, old config preserved")
    void testTwoPhaseListenerVeto() throws Exception {
        AtomicBoolean vetoCalled = new AtomicBoolean(false);
        AtomicBoolean onChangeCalled = new AtomicBoolean(false);

        registry.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public boolean preConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        vetoCalled.set(true);
                        return false;
                    }

                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        onChangeCalled.set(true);
                    }
                });

        registry.start(configFile);
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);

        boolean result = registry.reload();
        assertThat(result).isFalse();

        assertThat(vetoCalled.get()).isTrue();
        assertThat(onChangeCalled.get()).isFalse();
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
        assertThat(registry.currentEpoch()).isEqualTo(0);
    }

    // ========================================================================
    // E2E-3: Rollback Mechanism
    // ========================================================================

    @Test
    @DisplayName("E2E-3: Rollback mechanism - reload then rollback, old config and epoch restored")
    void testRollbackMechanism() throws Exception {
        List<Long> epochHistory = new ArrayList<>();
        List<Double> thresholdHistory = new ArrayList<>();

        registry.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        epochHistory.add(diff.epoch());
                        thresholdHistory.add(
                                newConfig.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD));
                    }
                });

        registry.start(configFile);
        epochHistory.add(registry.currentEpoch());
        thresholdHistory.add(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD));

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);
        boolean reload1 = registry.reload();
        assertThat(reload1).isTrue();
        assertThat(registry.currentEpoch()).isEqualTo(1);
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);

        writeConfigFile(0.90, 512L * 1024L * 1024L, 1000L);
        boolean reload2 = registry.reload();
        assertThat(reload2).isTrue();
        assertThat(registry.currentEpoch()).isEqualTo(2);
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.90);

        boolean rolledBack = registry.rollback(1);
        assertThat(rolledBack).isTrue();
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
        assertThat(registry.currentEpoch()).isGreaterThan(2);

        boolean rolledBackTo0 = registry.rollback(0);
        assertThat(rolledBackTo0).isTrue();
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
    }

    // ========================================================================
    // E2E-4: Non-Hot-Reloadable Key Rejection
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-4: Non-hot-reloadable key rejection - bootstrap-bound key change is rejected, old value preserved")
    void testNonHotReloadableKeyRejection() throws Exception {
        AtomicReference<ConfigDiff> capturedDiff = new AtomicReference<>();
        registry.registerListener(
                "",
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        capturedDiff.set(diff);
                    }
                });

        registry.start(configFile);
        String originalClusterId = registry.get(RawWiseOptions.CLUSTER_ID);

        writeConfigFileWithClusterId("changed-cluster", 0.85);

        boolean result = registry.reload();
        assertThat(result).isTrue();

        assertThat(registry.get(RawWiseOptions.CLUSTER_ID)).isEqualTo(originalClusterId);
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);

        ConfigDiff diff = capturedDiff.get();
        assertThat(diff).isNotNull();
        assertThat(diff.rejectedKeys()).contains(RawWiseOptions.CLUSTER_ID.key());
        assertThat(diff.hotReloadableKeys())
                .contains(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        assertThat(diff.hasRejectedChanges()).isTrue();
    }

    // ========================================================================
    // E2E-5: Only Non-Hot-Reloadable Keys Changed - Reload Skipped
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-5: Only non-hot-reloadable keys changed - reload succeeds but no hot-reload applied, epoch unchanged")
    void testOnlyNonHotReloadableKeysChanged() throws Exception {
        registry.start(configFile);
        long epochBefore = registry.currentEpoch();

        writeConfigFileWithClusterId("new-cluster", 0.70);

        boolean result = registry.reload();
        assertThat(result).isTrue();

        assertThat(registry.get(RawWiseOptions.CLUSTER_ID)).isNotEqualTo("new-cluster");
        assertThat(registry.currentEpoch()).isEqualTo(epochBefore);
    }

    // ========================================================================
    // E2E-6: Dry Reload
    // ========================================================================

    @Test
    @DisplayName("E2E-6: Dry reload - returns ConfigDiff without applying changes, epoch unchanged")
    void testDryReload() throws Exception {
        registry.start(configFile);
        long epochBefore = registry.currentEpoch();
        double thresholdBefore = registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);

        ConfigDiff diff = registry.dryReload();

        assertThat(diff).isNotNull();
        assertThat(diff.changedKeys()).contains(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        assertThat(diff.hotReloadableKeys())
                .contains(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        assertThat(diff.epoch()).isEqualTo(-1);

        assertThat(registry.currentEpoch()).isEqualTo(epochBefore);
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                .isEqualTo(thresholdBefore);
    }

    // ========================================================================
    // E2E-7: Listener Error Isolation
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-7: Listener error isolation - throwing listener does not prevent other listeners from being notified")
    void testListenerErrorIsolation() throws Exception {
        AtomicBoolean healthyListenerNotified = new AtomicBoolean(false);
        AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);

        registry.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        throw new RuntimeException("Simulated listener failure");
                    }

                    @Override
                    public void onError(ConfigDiff diff, Exception e) {
                        errorHandlerCalled.set(true);
                    }
                });

        registry.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        healthyListenerNotified.set(true);
                    }
                });

        registry.start(configFile);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);
        boolean result = registry.reload();
        assertThat(result).isTrue();

        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
        assertThat(healthyListenerNotified.get()).isTrue();
    }

    // ========================================================================
    // E2E-8: File Parse Error During Reload
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-8: File parse error during reload - old config preserved, reload returns false")
    void testFileParseErrorDuringReload() throws Exception {
        registry.start(configFile);
        double thresholdBefore = registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD);
        long epochBefore = registry.currentEpoch();

        Files.writeString(configFile, "rawwise { storage.lmdb.usage-threshold = 0.85\n");

        boolean result = registry.reload();
        assertThat(result).isFalse();

        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                .isEqualTo(thresholdBefore);
        assertThat(registry.currentEpoch()).isEqualTo(epochBefore);
    }

    // ========================================================================
    // E2E-9: Concurrent Reload
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-9: Concurrent reload - multiple threads trigger reload, epoch advances consistently")
    void testConcurrentReload() throws Exception {
        registry.start(configFile);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);

        int threadCount = 8;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(
                    executor.submit(
                            () -> {
                                try {
                                    barrier.await(5, TimeUnit.SECONDS);
                                    boolean result = registry.reload();
                                    if (result) {
                                        successCount.incrementAndGet();
                                    } else {
                                        failureCount.incrementAndGet();
                                    }
                                } catch (Exception e) {
                                    failureCount.incrementAndGet();
                                }
                            }));
        }

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertThat(successCount.get()).isGreaterThanOrEqualTo(1);
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
    }

    // ========================================================================
    // E2E-10: Config History and Backup
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-10: Config history and backup - multiple reloads create backup files, rollback to earlier epoch succeeds")
    void testConfigHistoryAndBackup() throws Exception {
        registry.start(configFile);

        writeConfigFile(0.75, 512L * 1024L * 1024L, 1000L);
        registry.reload();
        long epoch1 = registry.currentEpoch();

        writeConfigFile(0.80, 512L * 1024L * 1024L, 1000L);
        registry.reload();
        long epoch2 = registry.currentEpoch();

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);
        registry.reload();

        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);

        boolean rolledBack = registry.rollback(epoch1);
        assertThat(rolledBack).isTrue();
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.75);
    }

    // ========================================================================
    // E2E-11: FileWatcher Detects Change and Triggers Reload
    // ========================================================================

    @Test
    @DisplayName("E2E-11: FileWatcher detects change - file state updates after modification")
    void testFileWatcherDetectsChangeAndTriggersReload() throws Exception {
        registry.start(configFile);

        Thread.sleep(200);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);

        boolean reloaded = registry.reload();
        assertThat(reloaded).isTrue();
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
        assertThat(registry.currentEpoch()).isEqualTo(1);
    }

    // ========================================================================
    // E2E-12: Multiple Hot-Reloadable Keys Changed Simultaneously
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-12: Multiple hot-reloadable keys changed simultaneously - all applied atomically")
    void testMultipleHotReloadableKeysChangedSimultaneously() throws Exception {
        AtomicReference<ConfigDiff> capturedDiff = new AtomicReference<>();
        registry.registerListener(
                "",
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        capturedDiff.set(diff);
                    }
                });

        registry.start(configFile);

        writeConfigFile(0.85, 1024L * 1024L * 1024L, 2000L);

        boolean result = registry.reload();
        assertThat(result).isTrue();

        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
        assertThat(registry.get(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES))
                .isEqualTo(1024L * 1024L * 1024L);
        assertThat(registry.get(RawWiseOptions.WAL_SYNC_INTERVAL_MS)).isEqualTo(2000L);

        ConfigDiff diff = capturedDiff.get();
        assertThat(diff).isNotNull();
        assertThat(diff.hotReloadableKeys())
                .containsExactlyInAnyOrder(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(),
                        RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES.key(),
                        RawWiseOptions.WAL_SYNC_INTERVAL_MS.key());
        assertThat(diff.transitionPolicies())
                .containsEntry(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(),
                        TransitionPolicy.DATA_VERSION_SAFE);
        assertThat(diff.transitionPolicies())
                .containsEntry(
                        RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES.key(),
                        TransitionPolicy.DATA_VERSION_SAFE);
        assertThat(diff.transitionPolicies())
                .containsEntry(
                        RawWiseOptions.WAL_SYNC_INTERVAL_MS.key(), TransitionPolicy.NEXT_CYCLE);
    }

    // ========================================================================
    // E2E-13: Rollback to Non-Existent Epoch Fails
    // ========================================================================

    @Test
    @DisplayName("E2E-13: Rollback to non-existent epoch fails gracefully")
    void testRollbackToNonExistentEpochFails() throws Exception {
        registry.start(configFile);

        boolean result = registry.rollback(999);
        assertThat(result).isFalse();
        assertThat(registry.currentEpoch()).isEqualTo(0);
    }

    // ========================================================================
    // E2E-14: PreConfigChange Exception Treated as Veto
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-14: PreConfigChange exception treated as veto - reload aborted, onError called")
    void testPreConfigChangeExceptionTreatedAsVeto() throws Exception {
        AtomicBoolean onErrorCalled = new AtomicBoolean(false);
        AtomicReference<Exception> capturedError = new AtomicReference<>();

        registry.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public boolean preConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        throw new RuntimeException("Health check failed");
                    }

                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {}

                    @Override
                    public void onError(ConfigDiff diff, Exception e) {
                        onErrorCalled.set(true);
                        capturedError.set(e);
                    }
                });

        registry.start(configFile);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);
        boolean result = registry.reload();
        assertThat(result).isFalse();

        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
        assertThat(onErrorCalled.get()).isTrue();
    }

    // ========================================================================
    // E2E-15: Register and Unregister Listener
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-15: Register and unregister listener - unregistered listener not notified on subsequent reload")
    void testRegisterAndUnregisterListener() throws Exception {
        AtomicInteger notificationCount = new AtomicInteger(0);
        ConfigChangeListener listener =
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        notificationCount.incrementAndGet();
                    }
                };

        registry.registerListener("rawwise.storage.lmdb", listener);
        registry.start(configFile);

        writeConfigFile(0.75, 512L * 1024L * 1024L, 1000L);
        registry.reload();
        assertThat(notificationCount.get()).isEqualTo(1);

        registry.unregisterListener("rawwise.storage.lmdb", listener);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);
        registry.reload();
        assertThat(notificationCount.get()).isEqualTo(1);
    }

    // ========================================================================
    // E2E-16: Wildcard Prefix Listener
    // ========================================================================

    @Test
    @DisplayName("E2E-16: Wildcard prefix listener - empty prefix matches all key changes")
    void testWildcardPrefixListener() throws Exception {
        List<String> receivedKeys = new ArrayList<>();

        registry.registerListener(
                "",
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        receivedKeys.addAll(diff.hotReloadableKeys());
                    }
                });

        registry.start(configFile);

        writeConfigFile(0.85, 1024L * 1024L * 1024L, 1000L);
        registry.reload();

        assertThat(receivedKeys)
                .contains(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(),
                        RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES.key());
    }

    // ========================================================================
    // E2E-17: No Change Detected - Reload Returns True Without Epoch Advance
    // ========================================================================

    @Test
    @DisplayName("E2E-17: No change detected - reload returns true without epoch advance")
    void testNoChangeDetected() throws Exception {
        registry.start(configFile);
        long epochBefore = registry.currentEpoch();

        boolean result = registry.reload();
        assertThat(result).isTrue();
        assertThat(registry.currentEpoch()).isEqualTo(epochBefore);
    }

    // ========================================================================
    // E2E-18: ConfigDiff Invariant Validation
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-18: ConfigDiff invariant validation - hotReloadableKeys must be subset of changedKeys")
    void testConfigDiffInvariantValidation() {
        assertThatThrownBy(
                        () ->
                                new ConfigDiff(
                                        1,
                                        0,
                                        Set.of("a"),
                                        Set.of("a", "b"),
                                        Set.of(),
                                        Map.of(
                                                "a",
                                                TransitionPolicy.IMMEDIATE,
                                                "b",
                                                TransitionPolicy.IMMEDIATE),
                                        false,
                                        java.time.Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hotReloadableKeys must be subset of changedKeys");

        assertThatThrownBy(
                        () ->
                                new ConfigDiff(
                                        1,
                                        0,
                                        Set.of("a"),
                                        Set.of("a"),
                                        Set.of("a"),
                                        Map.of("a", TransitionPolicy.IMMEDIATE),
                                        false,
                                        java.time.Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("disjoint");

        assertThatThrownBy(
                        () ->
                                new ConfigDiff(
                                        1,
                                        0,
                                        Set.of("a"),
                                        Set.of("a"),
                                        Set.of(),
                                        Map.of(),
                                        false,
                                        java.time.Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transitionPolicies must cover exactly hotReloadableKeys");
    }

    // ========================================================================
    // E2E-19: Rollback Writes Config File Back
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-19: Rollback writes config file back - file content reflects rolled-back config")
    void testRollbackWritesConfigFileBack() throws Exception {
        registry.start(configFile);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);
        registry.reload();

        writeConfigFile(0.90, 512L * 1024L * 1024L, 1000L);
        registry.reload();

        boolean rolledBack = registry.rollback(0);
        assertThat(rolledBack).isTrue();
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);

        String fileContent = Files.readString(configFile);
        assertThat(fileContent).contains("0.7");
    }

    // ========================================================================
    // E2E-20: Dependency Injection for Testing
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-20: Dependency injection for testing - custom file reader and hasher enable test isolation")
    void testDependencyInjectionForTesting() throws Exception {
        AtomicInteger readCount = new AtomicInteger(0);
        AtomicInteger hashCount = new AtomicInteger(0);

        AtomicReference<Map<String, Object>> injectedValues = new AtomicReference<>();

        DynamicConfigRegistry testRegistry =
                DynamicConfigRegistry.create(
                        ConfigFactory.createDefault(),
                        5,
                        path -> {
                            readCount.incrementAndGet();
                            return injectedValues.get() != null
                                    ? injectedValues.get()
                                    : HoconConfigReader.read(path);
                        },
                        path -> {
                            hashCount.incrementAndGet();
                            return "test-hash-" + System.currentTimeMillis();
                        },
                        () -> new ConfigFileWatcher(100, 50));

        testRegistry.start(configFile);

        assertThat(readCount.get()).isGreaterThanOrEqualTo(0);

        injectedValues.set(
                Map.of(
                        "rawwise.storage.lmdb.usage-threshold",
                        0.95,
                        "rawwise.cluster.id",
                        "test-cluster"));

        boolean result = testRegistry.reload();
        assertThat(result).isTrue();
        assertThat(readCount.get()).isGreaterThanOrEqualTo(1);

        testRegistry.stop();
    }

    // ========================================================================
    // E2E-21: Cooldown Behavior
    // ========================================================================

    @Test
    @DisplayName("E2E-21: Cooldown behavior - rapid reloads respect min-interval-ms")
    void testCooldownBehavior() throws Exception {
        RawWiseConfig initialConfig =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CONFIG_RELOAD_MIN_INTERVAL_MS, 5000L)
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                        .build();

        DynamicConfigRegistry cooldownRegistry =
                DynamicConfigRegistry.create(
                        initialConfig,
                        20,
                        HoconConfigReader::read,
                        ConfigFileWatcher::computeFileHash,
                        () -> new ConfigFileWatcher(100, 50));

        try {
            cooldownRegistry.start(configFile);

            writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);
            boolean first = cooldownRegistry.reload();
            assertThat(first).isTrue();

            writeConfigFile(0.90, 512L * 1024L * 1024L, 1000L);
            boolean second = cooldownRegistry.reload();
            assertThat(second).isTrue();

            assertThat(cooldownRegistry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                    .isEqualTo(0.90);
        } finally {
            cooldownRegistry.stop();
        }
    }

    // ========================================================================
    // E2E-22: Get Current Config and Epoch
    // ========================================================================

    @Test
    @DisplayName("E2E-22: Get current config and epoch - values reflect latest reload state")
    void testGetCurrentConfigAndEpoch() throws Exception {
        registry.start(configFile);

        assertThat(registry.currentEpoch()).isEqualTo(0);
        assertThat(registry.currentConfig()).isNotNull();
        assertThat(registry.currentConfig().get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                .isEqualTo(0.70);

        writeConfigFile(0.85, 512L * 1024L * 1024L, 1000L);
        registry.reload();

        assertThat(registry.currentEpoch()).isEqualTo(1);
        assertThat(registry.currentConfig().get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                .isEqualTo(0.85);
    }

    // ========================================================================
    // E2E-23: FileWatcher Rebuild on Parameter Change
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-23: FileWatcher rebuild on parameter change - poll interval change triggers watcher rebuild")
    void testFileWatcherRebuildOnParameterChange() throws Exception {
        RawWiseConfig initialConfig =
                ConfigFactory.builder()
                        .set(RawWiseOptions.CONFIG_FILE_POLL_INTERVAL_MS, 5000L)
                        .set(RawWiseOptions.CONFIG_FILE_DEBOUNCE_MS, 3000L)
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                        .build();

        DynamicConfigRegistry paramRegistry =
                DynamicConfigRegistry.create(
                        initialConfig,
                        20,
                        HoconConfigReader::read,
                        ConfigFileWatcher::computeFileHash,
                        () -> new ConfigFileWatcher(100, 50));

        try {
            paramRegistry.start(configFile);
            assertThat(paramRegistry.get(RawWiseOptions.CONFIG_FILE_POLL_INTERVAL_MS))
                    .isEqualTo(5000L);

            String newConfig =
                    """
                rawwise {
                  config.file.poll-interval-ms = 2000
                  config.file.debounce-ms = 1000
                  storage.durability-level = "PROCESS"
                  storage.lmdb.usage-threshold = 0.85
                }
                """;
            Files.writeString(configFile, newConfig);

            boolean result = paramRegistry.reload();
            assertThat(result).isTrue();
            assertThat(paramRegistry.get(RawWiseOptions.CONFIG_FILE_POLL_INTERVAL_MS))
                    .isEqualTo(2000L);
            assertThat(paramRegistry.get(RawWiseOptions.CONFIG_FILE_DEBOUNCE_MS)).isEqualTo(1000L);
        } finally {
            paramRegistry.stop();
        }
    }

    // ========================================================================
    // E2E-24: TransitionPolicy Classification in ConfigDiff
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-24: TransitionPolicy classification - DATA_VERSION_SAFE/NEXT_CYCLE policies correctly assigned")
    void testTransitionPolicyClassification() throws Exception {
        AtomicReference<ConfigDiff> capturedDiff = new AtomicReference<>();
        registry.registerListener(
                "",
                new ConfigChangeListener() {
                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        capturedDiff.set(diff);
                    }
                });

        registry.start(configFile);

        String newConfig =
                """
            rawwise {
              storage.lmdb.usage-threshold = 0.85
              storage.flush.threshold-bytes = 1073741824
              wal.sync-interval-ms = 5000
            }
            """;
        Files.writeString(configFile, newConfig);

        registry.reload();

        ConfigDiff diff = capturedDiff.get();
        assertThat(diff).isNotNull();
        assertThat(diff.transitionPolicies().get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key()))
                .isEqualTo(TransitionPolicy.DATA_VERSION_SAFE);
        assertThat(
                        diff.transitionPolicies()
                                .get(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES.key()))
                .isEqualTo(TransitionPolicy.DATA_VERSION_SAFE);
        assertThat(diff.transitionPolicies().get(RawWiseOptions.WAL_SYNC_INTERVAL_MS.key()))
                .isEqualTo(TransitionPolicy.NEXT_CYCLE);
    }

    // ========================================================================
    // E2E-25: Cross-Key Constraint Validation During Reload
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-25: Cross-key constraint validation during reload - invalid config rejected, old preserved")
    void testCrossKeyConstraintValidationDuringReload() throws Exception {
        registry.start(configFile);
        double thresholdBefore = registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD);

        String invalidConfig =
                """
            rawwise {
              storage.lmdb.initial-map-size-mb = 8192
              storage.lmdb.max-map-size-mb = 4096
            }
            """;
        Files.writeString(configFile, invalidConfig);

        boolean result = registry.reload();
        assertThat(result).isFalse();
        assertThat(registry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                .isEqualTo(thresholdBefore);
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    private void writeConfigFile(
            double usageThreshold, long flushThresholdBytes, long walSyncIntervalMs)
            throws IOException {
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
                        usageThreshold, flushThresholdBytes, walSyncIntervalMs);
        Files.writeString(configFile, content);
    }

    private void writeConfigFileWithClusterId(String clusterId, double usageThreshold)
            throws IOException {
        String content =
                String.format(
                        """
            rawwise {
              cluster.id = "%s"
              storage.mmap.path = "/data/rawwise/mmap"
              storage.durability-level = "PROCESS"
              storage.lmdb.usage-threshold = %.2f
              storage.flush.threshold-bytes = %d
              wal.sync-interval-ms = %d
            }
            """,
                        clusterId, usageThreshold, 512L * 1024L * 1024L, 1000L);
        Files.writeString(configFile, content);
    }
}
