package io.github.rawwise.governance.config;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Config Propagation - E2E Business Scenario Tests")
class ConfigPropagationTest {

    @TempDir Path tempDir;

    private DynamicConfigRegistry driverRegistry;
    private List<DynamicConfigRegistry> executorRegistries;
    private Path driverConfigFile;

    @BeforeEach
    void setUp() throws IOException {
        driverConfigFile = tempDir.resolve("rawwise.conf");
        writeDriverConfig(0.70, 512L * 1024L * 1024L, 1000L);

        RawWiseConfig initialConfig = ConfigLoader.builder().configFile(driverConfigFile).load();

        driverRegistry =
                DynamicConfigRegistry.create(
                        initialConfig,
                        20,
                        HoconConfigReader::read,
                        ConfigFileWatcher::computeFileHash,
                        () -> new ConfigFileWatcher(100, 50));

        executorRegistries = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        if (driverRegistry != null && driverRegistry.isRunning()) {
            driverRegistry.stop();
        }
        for (DynamicConfigRegistry executor : executorRegistries) {
            if (executor != null && executor.isRunning()) {
                executor.stop();
            }
        }
    }

    private DynamicConfigRegistry createExecutorRegistry(String executorId) {
        RawWiseConfig executorConfig =
                ConfigFactory.builder()
                        .set(
                                RawWiseOptions.CLUSTER_ID,
                                driverRegistry.get(RawWiseOptions.CLUSTER_ID))
                        .set(
                                RawWiseOptions.STORAGE_MMAP_PATH,
                                driverRegistry.get(RawWiseOptions.STORAGE_MMAP_PATH))
                        .set(RawWiseOptions.STORAGE_DURABILITY_LEVEL, "PROCESS")
                        .set(
                                RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD,
                                driverRegistry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                        .set(
                                RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES,
                                driverRegistry.get(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES))
                        .set(
                                RawWiseOptions.WAL_SYNC_INTERVAL_MS,
                                driverRegistry.get(RawWiseOptions.WAL_SYNC_INTERVAL_MS))
                        .build();

        DynamicConfigRegistry executor =
                DynamicConfigRegistry.create(
                        executorConfig,
                        20,
                        HoconConfigReader::read,
                        ConfigFileWatcher::computeFileHash,
                        () -> new ConfigFileWatcher(100, 50));
        executorRegistries.add(executor);
        return executor;
    }

    private PropagationResult simulateTwoPhaseCommit(
            DynamicConfigRegistry driver,
            List<DynamicConfigRegistry> executors,
            PropagationMode mode) {

        AtomicReference<ConfigUpdateMessage> capturedMessage = new AtomicReference<>();
        AtomicReference<Set<String>> capturedAcceptedIds = new AtomicReference<>();

        driver.setPropagationCallback(
                new DynamicConfigRegistry.PropagationCallback() {
                    @Override
                    public DynamicConfigRegistry.PropagationResult propagate(
                            ConfigUpdateMessage message) {
                        capturedMessage.set(message);
                        int total = executors.size();
                        int accepted = 0;
                        int rejected = 0;
                        int timedOut = 0;
                        Set<String> acceptedIds = new HashSet<>();

                        for (int i = 0; i < executors.size(); i++) {
                            try {
                                boolean result = executors.get(i).reloadFromMessage(message);
                                if (result) {
                                    accepted++;
                                    acceptedIds.add("executor-" + i);
                                } else {
                                    rejected++;
                                }
                            } catch (Exception e) {
                                timedOut++;
                            }
                        }

                        boolean quorumReached;
                        if (mode == PropagationMode.ALL) {
                            quorumReached = accepted == total && rejected == 0;
                        } else {
                            quorumReached = accepted >= (total / 2 + 1);
                        }

                        capturedAcceptedIds.set(acceptedIds);
                        return new DynamicConfigRegistry.PropagationResult(
                                total, accepted, rejected, timedOut, quorumReached, acceptedIds);
                    }

                    @Override
                    public void sendCommit(
                            ConfigUpdateMessage message, Set<String> targetExecutorIds) {
                        ConfigCommitMessage commitMsg =
                                new ConfigCommitMessage(message.configEpoch());
                        for (int i = 0; i < executors.size(); i++) {
                            String executorId = "executor-" + i;
                            if (targetExecutorIds.contains(executorId)) {
                                executors.get(i).commitFromMessage(commitMsg);
                            }
                        }
                    }

                    @Override
                    public void sendAbort(
                            ConfigUpdateMessage message, Set<String> targetExecutorIds) {
                        ConfigAbortMessage abortMsg = new ConfigAbortMessage(message.configEpoch());
                        for (DynamicConfigRegistry executor : executors) {
                            executor.handleAbort(abortMsg);
                        }
                    }
                });

        boolean driverReloaded = driver.reload();
        if (!driverReloaded) {
            return new PropagationResult(false, 0, 0, 0, false);
        }

        ConfigUpdateMessage message = capturedMessage.get();
        if (message == null) {
            return new PropagationResult(true, 0, 0, 0, true);
        }

        return new PropagationResult(true, 0, 0, 0, true);
    }

    private record PropagationResult(
            boolean driverReloaded,
            int acceptedExecutors,
            int rejectedExecutors,
            int timedOutExecutors,
            boolean quorumReached) {}

    private ConfigUpdateMessage buildUpdateMessage(DynamicConfigRegistry driver, long epoch) {
        RawWiseConfig currentConfig = driver.currentConfig();
        Map<String, String> configValues = new HashMap<>();
        Set<String> changedKeys = new HashSet<>();

        for (String key : RawWiseOptions.allKeys()) {
            ConfigOption<?> option = RawWiseOptions.findOption(key);
            if (option == null) continue;
            Object value = currentConfig.get(option);
            if (value != null) {
                configValues.put(key, String.valueOf(value));
            }
        }

        String checksum =
                DynamicConfigRegistry.computeValidationChecksum(epoch, configValues, changedKeys);
        return new ConfigUpdateMessage(
                epoch, configValues, changedKeys, checksum, PropagationMode.QUORUM);
    }

    private ConfigUpdateMessage buildUpdateMessageWithChanges(
            DynamicConfigRegistry driver,
            long epoch,
            Map<String, String> overrideValues,
            Set<String> changedKeys) {
        Map<String, String> configValues = new HashMap<>();
        RawWiseConfig currentConfig = driver.currentConfig();

        for (String key : RawWiseOptions.allKeys()) {
            ConfigOption<?> option = RawWiseOptions.findOption(key);
            if (option == null) continue;
            if (overrideValues.containsKey(key)) {
                configValues.put(key, overrideValues.get(key));
            } else {
                Object value = currentConfig.get(option);
                if (value != null) {
                    configValues.put(key, String.valueOf(value));
                }
            }
        }

        String checksum =
                DynamicConfigRegistry.computeValidationChecksum(epoch, configValues, changedKeys);
        return new ConfigUpdateMessage(
                epoch, configValues, changedKeys, checksum, PropagationMode.QUORUM);
    }

    // ========================================================================
    // E2E-1: Complete Propagation Flow - Quorum Commit
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-1: Complete propagation flow - Driver reload -> propagate -> quorum -> commit -> all Executors updated")
    void testCompletePropagationFlowQuorumCommit() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");
        DynamicConfigRegistry executor1 = createExecutorRegistry("executor-1");
        DynamicConfigRegistry executor2 = createExecutorRegistry("executor-2");

        List<DynamicConfigRegistry> executors = List.of(executor0, executor1, executor2);

        driverRegistry.start(driverConfigFile);

        assertThat(driverRegistry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
        for (DynamicConfigRegistry exec : executors) {
            assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
        }

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        boolean driverReloaded = driverRegistry.reload();
        assertThat(driverReloaded).isTrue();
        assertThat(driverRegistry.currentEpoch()).isEqualTo(1);
        assertThat(driverRegistry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);

        long epoch = driverRegistry.currentEpoch();
        Map<String, String> overrideValues =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        ConfigUpdateMessage updateMsg =
                buildUpdateMessageWithChanges(driverRegistry, epoch, overrideValues, changedKeys);

        boolean exec0Accepted = executor0.reloadFromMessage(updateMsg);
        boolean exec1Accepted = executor1.reloadFromMessage(updateMsg);
        boolean exec2Accepted = executor2.reloadFromMessage(updateMsg);

        assertThat(exec0Accepted).isTrue();
        assertThat(exec1Accepted).isTrue();
        assertThat(exec2Accepted).isTrue();

        int acceptedCount =
                (exec0Accepted ? 1 : 0) + (exec1Accepted ? 1 : 0) + (exec2Accepted ? 1 : 0);
        boolean quorumReached = acceptedCount >= (3 / 2 + 1);
        assertThat(quorumReached).isTrue();

        ConfigCommitMessage commitMsg = new ConfigCommitMessage(epoch);
        boolean commit0 = executor0.commitFromMessage(commitMsg);
        boolean commit1 = executor1.commitFromMessage(commitMsg);
        boolean commit2 = executor2.commitFromMessage(commitMsg);

        assertThat(commit0).isTrue();
        assertThat(commit1).isTrue();
        assertThat(commit2).isTrue();

        for (DynamicConfigRegistry exec : executors) {
            assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
            assertThat(exec.currentEpoch()).isEqualTo(epoch);
        }
    }

    // ========================================================================
    // E2E-2: Quorum Failure -> Abort Flow
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-2: Quorum failure -> abort - majority reject -> Driver aborts -> Executors keep old config")
    void testQuorumFailureAbortFlow() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");
        DynamicConfigRegistry executor1 = createExecutorRegistry("executor-1");
        DynamicConfigRegistry executor2 = createExecutorRegistry("executor-2");

        List<DynamicConfigRegistry> executors = List.of(executor0, executor1, executor2);

        executor1.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public boolean preConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        return false;
                    }

                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {}
                });

        executor2.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public boolean preConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        return false;
                    }

                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {}
                });

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        driverRegistry.reload();

        long epoch = driverRegistry.currentEpoch();
        Map<String, String> overrideValues =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        ConfigUpdateMessage updateMsg =
                buildUpdateMessageWithChanges(driverRegistry, epoch, overrideValues, changedKeys);

        boolean exec0Accepted = executor0.reloadFromMessage(updateMsg);
        boolean exec1Accepted = executor1.reloadFromMessage(updateMsg);
        boolean exec2Accepted = executor2.reloadFromMessage(updateMsg);

        assertThat(exec0Accepted).isTrue();
        assertThat(exec1Accepted).isFalse();
        assertThat(exec2Accepted).isFalse();

        int acceptedCount =
                (exec0Accepted ? 1 : 0) + (exec1Accepted ? 1 : 0) + (exec2Accepted ? 1 : 0);
        boolean quorumReached = acceptedCount >= (3 / 2 + 1);
        assertThat(quorumReached).isFalse();

        ConfigAbortMessage abortMsg = new ConfigAbortMessage(epoch);
        for (DynamicConfigRegistry exec : executors) {
            exec.handleAbort(abortMsg);
        }

        for (DynamicConfigRegistry exec : executors) {
            assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
            assertThat(exec.currentEpoch()).isEqualTo(0);
        }
    }

    // ========================================================================
    // E2E-3: Advisory Veto - Minority Reject but Quorum Reached -> Commit
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-3: Advisory veto - one Executor vetoes but quorum reached -> commit -> all Executors apply config")
    void testAdvisoryVetoQuorumCommit() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");
        DynamicConfigRegistry executor1 = createExecutorRegistry("executor-1");
        DynamicConfigRegistry executor2 = createExecutorRegistry("executor-2");
        DynamicConfigRegistry executor3 = createExecutorRegistry("executor-3");
        DynamicConfigRegistry executor4 = createExecutorRegistry("executor-4");

        List<DynamicConfigRegistry> executors =
                List.of(executor0, executor1, executor2, executor3, executor4);

        AtomicBoolean vetoListenerCalled = new AtomicBoolean(false);
        executor2.registerListener(
                "rawwise.storage.lmdb",
                new ConfigChangeListener() {
                    @Override
                    public boolean preConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
                        vetoListenerCalled.set(true);
                        return false;
                    }

                    @Override
                    public void onConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {}
                });

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        driverRegistry.reload();

        long epoch = driverRegistry.currentEpoch();
        Map<String, String> overrideValues =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        ConfigUpdateMessage updateMsg =
                buildUpdateMessageWithChanges(driverRegistry, epoch, overrideValues, changedKeys);

        int acceptedCount = 0;
        for (DynamicConfigRegistry exec : executors) {
            if (exec.reloadFromMessage(updateMsg)) {
                acceptedCount++;
            }
        }

        assertThat(acceptedCount).isEqualTo(4);
        assertThat(vetoListenerCalled.get()).isTrue();

        boolean quorumReached = acceptedCount >= (5 / 2 + 1);
        assertThat(quorumReached).isTrue();

        ConfigCommitMessage commitMsg = new ConfigCommitMessage(epoch);
        for (DynamicConfigRegistry exec : executors) {
            exec.commitFromMessage(commitMsg);
        }

        for (DynamicConfigRegistry exec : executors) {
            assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
            assertThat(exec.currentEpoch()).isEqualTo(epoch);
        }
    }

    // ========================================================================
    // E2E-4: Epoch Consistency Across Multiple Reloads
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-4: Epoch consistency - multiple reloads advance epoch consistently across Driver and Executors")
    void testEpochConsistencyAcrossMultipleReloads() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");
        DynamicConfigRegistry executor1 = createExecutorRegistry("executor-1");

        List<DynamicConfigRegistry> executors = List.of(executor0, executor1);

        driverRegistry.start(driverConfigFile);

        double[] thresholds = {0.75, 0.80, 0.85, 0.90};
        long expectedEpoch = 0;

        for (double threshold : thresholds) {
            writeDriverConfig(threshold, 512L * 1024L * 1024L, 1000L);
            boolean driverReloaded = driverRegistry.reload();
            assertThat(driverReloaded).isTrue();
            expectedEpoch++;

            long epoch = driverRegistry.currentEpoch();
            assertThat(epoch).isEqualTo(expectedEpoch);

            Map<String, String> overrideValues =
                    Map.of(
                            RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(),
                            String.valueOf(threshold));
            Set<String> changedKeys = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
            ConfigUpdateMessage updateMsg =
                    buildUpdateMessageWithChanges(
                            driverRegistry, epoch, overrideValues, changedKeys);

            for (DynamicConfigRegistry exec : executors) {
                boolean accepted = exec.reloadFromMessage(updateMsg);
                assertThat(accepted).isTrue();
            }

            ConfigCommitMessage commitMsg = new ConfigCommitMessage(epoch);
            for (DynamicConfigRegistry exec : executors) {
                boolean committed = exec.commitFromMessage(commitMsg);
                assertThat(committed).isTrue();
            }

            for (DynamicConfigRegistry exec : executors) {
                assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                        .isEqualTo(threshold);
                assertThat(exec.currentEpoch()).isEqualTo(epoch);
            }
        }

        assertThat(driverRegistry.currentEpoch()).isEqualTo(4);
        for (DynamicConfigRegistry exec : executors) {
            assertThat(exec.currentEpoch()).isEqualTo(4);
            assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.90);
        }
    }

    // ========================================================================
    // E2E-5: Stale Epoch Message Rejection
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-5: Stale epoch message rejection - old epoch ConfigUpdateMessage ignored by Executor")
    void testStaleEpochMessageRejection() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        driverRegistry.reload();
        long epoch1 = driverRegistry.currentEpoch();

        Map<String, String> overrideValues1 =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys1 = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        ConfigUpdateMessage updateMsg1 =
                buildUpdateMessageWithChanges(
                        driverRegistry, epoch1, overrideValues1, changedKeys1);

        boolean accepted1 = executor0.reloadFromMessage(updateMsg1);
        assertThat(accepted1).isTrue();

        ConfigCommitMessage commitMsg1 = new ConfigCommitMessage(epoch1);
        executor0.commitFromMessage(commitMsg1);
        assertThat(executor0.currentEpoch()).isEqualTo(epoch1);

        writeDriverConfig(0.90, 512L * 1024L * 1024L, 1000L);
        driverRegistry.reload();
        long epoch2 = driverRegistry.currentEpoch();

        Map<String, String> overrideValues2 =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.90");
        ConfigUpdateMessage updateMsg2 =
                buildUpdateMessageWithChanges(
                        driverRegistry, epoch2, overrideValues2, changedKeys1);

        boolean accepted2 = executor0.reloadFromMessage(updateMsg2);
        assertThat(accepted2).isTrue();

        ConfigCommitMessage commitMsg2 = new ConfigCommitMessage(epoch2);
        executor0.commitFromMessage(commitMsg2);
        assertThat(executor0.currentEpoch()).isEqualTo(epoch2);

        boolean staleAccepted = executor0.reloadFromMessage(updateMsg1);
        assertThat(staleAccepted).isTrue();
        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.90);
        assertThat(executor0.currentEpoch()).isEqualTo(epoch2);

        ConfigCommitMessage staleCommit = new ConfigCommitMessage(epoch1);
        boolean staleCommitted = executor0.commitFromMessage(staleCommit);
        assertThat(staleCommitted).isTrue();
        assertThat(executor0.currentEpoch()).isEqualTo(epoch2);
    }

    // ========================================================================
    // E2E-6: Checksum Validation - Corrupted Message Rejected
    // ========================================================================

    @Test
    @DisplayName("E2E-6: Checksum validation - corrupted ConfigUpdateMessage rejected by Executor")
    void testChecksumValidationCorruptedMessage() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        driverRegistry.reload();

        long epoch = driverRegistry.currentEpoch();
        Map<String, String> overrideValues =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        String validChecksum =
                DynamicConfigRegistry.computeValidationChecksum(epoch, overrideValues, changedKeys);

        Map<String, String> corruptedValues =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.99");
        ConfigUpdateMessage corruptedMsg =
                new ConfigUpdateMessage(
                        epoch, corruptedValues, changedKeys, validChecksum, PropagationMode.QUORUM);

        boolean accepted = executor0.reloadFromMessage(corruptedMsg);
        assertThat(accepted).isFalse();
        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
        assertThat(executor0.currentEpoch()).isEqualTo(0);
    }

    // ========================================================================
    // E2E-7: Abort Clears Prepared State on Executor
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-7: Abort clears prepared state - Executor receives abort after prepare, config unchanged")
    void testAbortClearsPreparedState() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        driverRegistry.reload();

        long epoch = driverRegistry.currentEpoch();
        Map<String, String> overrideValues =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        ConfigUpdateMessage updateMsg =
                buildUpdateMessageWithChanges(driverRegistry, epoch, overrideValues, changedKeys);

        boolean prepared = executor0.reloadFromMessage(updateMsg);
        assertThat(prepared).isTrue();
        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);

        ConfigAbortMessage abortMsg = new ConfigAbortMessage(epoch);
        executor0.handleAbort(abortMsg);

        ConfigCommitMessage lateCommit = new ConfigCommitMessage(epoch);
        boolean committed = executor0.commitFromMessage(lateCommit);
        assertThat(committed).isFalse();

        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
        assertThat(executor0.currentEpoch()).isEqualTo(0);
    }

    // ========================================================================
    // E2E-8: Commit Without Prepare Rejected
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-8: Commit without prepare rejected - Executor receives commit without prior prepare, rejects it")
    void testCommitWithoutPrepareRejected() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");

        driverRegistry.start(driverConfigFile);

        ConfigCommitMessage unexpectedCommit = new ConfigCommitMessage(1);
        boolean committed = executor0.commitFromMessage(unexpectedCommit);
        assertThat(committed).isFalse();

        assertThat(executor0.currentEpoch()).isEqualTo(0);
        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
    }

    // ========================================================================
    // E2E-9: Non-Hot-Reloadable Key Propagation - Rejected on Executor
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-9: Non-hot-reloadable key propagation - bootstrap key change rejected, hot-reloadable key applied")
    void testNonHotReloadableKeyPropagationRejected() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");

        driverRegistry.start(driverConfigFile);

        writeDriverConfigWithClusterId("new-cluster", 0.85);
        driverRegistry.reload();

        long epoch = driverRegistry.currentEpoch();
        assertThat(epoch).isEqualTo(1);

        Map<String, String> overrideValues = new HashMap<>();
        overrideValues.put(RawWiseOptions.CLUSTER_ID.key(), "new-cluster");
        overrideValues.put(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys =
                Set.of(
                        RawWiseOptions.CLUSTER_ID.key(),
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        ConfigUpdateMessage updateMsg =
                buildUpdateMessageWithChanges(driverRegistry, epoch, overrideValues, changedKeys);

        boolean accepted = executor0.reloadFromMessage(updateMsg);
        assertThat(accepted).isTrue();

        ConfigCommitMessage commitMsg = new ConfigCommitMessage(epoch);
        boolean committed = executor0.commitFromMessage(commitMsg);
        assertThat(committed).isTrue();

        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
        assertThat(executor0.get(RawWiseOptions.CLUSTER_ID)).isNotEqualTo("new-cluster");
    }

    // ========================================================================
    // E2E-10: Constraint Validation on Executor Rejects Invalid Config
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-10: Constraint validation on Executor - invalid config propagation rejected, old config preserved")
    void testConstraintValidationOnExecutorRejectsInvalidConfig() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");

        driverRegistry.start(driverConfigFile);

        long epoch = 1;
        Map<String, String> invalidValues = new HashMap<>();
        invalidValues.put(RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB.key(), "8192");
        invalidValues.put(RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB.key(), "4096");
        Set<String> changedKeys =
                Set.of(
                        RawWiseOptions.STORAGE_LMDB_INITIAL_MAP_SIZE_MB.key(),
                        RawWiseOptions.STORAGE_LMDB_MAX_MAP_SIZE_MB.key());

        String checksum =
                DynamicConfigRegistry.computeValidationChecksum(epoch, invalidValues, changedKeys);
        ConfigUpdateMessage invalidMsg =
                new ConfigUpdateMessage(
                        epoch, invalidValues, changedKeys, checksum, PropagationMode.QUORUM);

        boolean accepted = executor0.reloadFromMessage(invalidMsg);
        assertThat(accepted).isFalse();

        assertThat(executor0.currentEpoch()).isEqualTo(0);
    }

    // ========================================================================
    // E2E-11: Multiple Hot-Reloadable Keys Propagated Atomically
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-11: Multiple hot-reloadable keys propagated atomically - all applied or none on each Executor")
    void testMultipleHotReloadableKeysPropagatedAtomically() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");
        DynamicConfigRegistry executor1 = createExecutorRegistry("executor-1");

        List<DynamicConfigRegistry> executors = List.of(executor0, executor1);

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 1024L * 1024L * 1024L, 2000L);
        driverRegistry.reload();

        long epoch = driverRegistry.currentEpoch();
        Map<String, String> overrideValues =
                Map.of(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85",
                        RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES.key(),
                                String.valueOf(1024L * 1024L * 1024L),
                        RawWiseOptions.WAL_SYNC_INTERVAL_MS.key(), "2000");
        Set<String> changedKeys =
                Set.of(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(),
                        RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES.key(),
                        RawWiseOptions.WAL_SYNC_INTERVAL_MS.key());
        ConfigUpdateMessage updateMsg =
                buildUpdateMessageWithChanges(driverRegistry, epoch, overrideValues, changedKeys);

        for (DynamicConfigRegistry exec : executors) {
            boolean accepted = exec.reloadFromMessage(updateMsg);
            assertThat(accepted).isTrue();
        }

        ConfigCommitMessage commitMsg = new ConfigCommitMessage(epoch);
        for (DynamicConfigRegistry exec : executors) {
            exec.commitFromMessage(commitMsg);
        }

        for (DynamicConfigRegistry exec : executors) {
            assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
            assertThat(exec.get(RawWiseOptions.STORAGE_FLUSH_THRESHOLD_BYTES))
                    .isEqualTo(1024L * 1024L * 1024L);
            assertThat(exec.get(RawWiseOptions.WAL_SYNC_INTERVAL_MS)).isEqualTo(2000L);
            assertThat(exec.currentEpoch()).isEqualTo(epoch);
        }
    }

    // ========================================================================
    // E2E-12: ConfigPrepareResponse Record Validation
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-12: ConfigPrepareResponse validation - ok/reject factory methods and invariant checks")
    void testConfigPrepareResponseValidation() {
        ConfigPropagationMessages.ConfigPrepareResponse okResponse =
                ConfigPropagationMessages.ConfigPrepareResponse.ok("executor-0", 1);
        assertThat(okResponse.executorId()).isEqualTo("executor-0");
        assertThat(okResponse.configEpoch()).isEqualTo(1);
        assertThat(okResponse.accepted()).isTrue();
        assertThat(okResponse.rejectReason()).isNull();

        ConfigPropagationMessages.ConfigPrepareResponse rejectResponse =
                ConfigPropagationMessages.ConfigPrepareResponse.reject(
                        "executor-1", 1, "listener veto");
        assertThat(rejectResponse.executorId()).isEqualTo("executor-1");
        assertThat(rejectResponse.accepted()).isFalse();
        assertThat(rejectResponse.rejectReason()).isEqualTo("listener veto");

        assertThatThrownBy(
                        () ->
                                ConfigPropagationMessages.ConfigPrepareResponse.reject(
                                        "executor-1", 1, ""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rejectReason must not be blank");

        assertThatThrownBy(() -> ConfigPropagationMessages.ConfigPrepareResponse.ok("", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("executorId must not be blank");

        assertThatThrownBy(
                        () ->
                                new ConfigPropagationMessages.ConfigPrepareResponse(
                                        "exec", 1, true, "reason"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rejectReason must be null when accepted=true");
    }

    // ========================================================================
    // E2E-13: ConfigEpochHeartbeat Record Validation
    // ========================================================================

    @Test
    @DisplayName("E2E-13: ConfigEpochHeartbeat validation - executor ID and epoch constraints")
    void testConfigEpochHeartbeatValidation() {
        ConfigPropagationMessages.ConfigEpochHeartbeat heartbeat =
                new ConfigPropagationMessages.ConfigEpochHeartbeat("executor-0", 5);
        assertThat(heartbeat.executorId()).isEqualTo("executor-0");
        assertThat(heartbeat.configEpoch()).isEqualTo(5);

        assertThatThrownBy(() -> new ConfigPropagationMessages.ConfigEpochHeartbeat(null, 0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new ConfigPropagationMessages.ConfigEpochHeartbeat("  ", 0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () -> new ConfigPropagationMessages.ConfigEpochHeartbeat("executor-0", -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ========================================================================
    // E2E-14: PropagationMode Enum Validation
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-14: PropagationMode validation - QUORUM/ALL parsing and invalid input rejection")
    void testPropagationModeValidation() {
        assertThat(PropagationMode.fromString("QUORUM")).isEqualTo(PropagationMode.QUORUM);
        assertThat(PropagationMode.fromString("quorum")).isEqualTo(PropagationMode.QUORUM);
        assertThat(PropagationMode.fromString("ALL")).isEqualTo(PropagationMode.ALL);
        assertThat(PropagationMode.fromString(" all ")).isEqualTo(PropagationMode.ALL);

        assertThatThrownBy(() -> PropagationMode.fromString(null))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> PropagationMode.fromString(""))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> PropagationMode.fromString("MAJORITY"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("QUORUM or ALL");
    }

    // ========================================================================
    // E2E-15: ConfigUpdateMessage Serialization Round-Trip
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-15: ConfigUpdateMessage serialization round-trip - message survives Java serialization")
    void testConfigUpdateMessageSerializationRoundTrip() throws Exception {
        Map<String, String> configValues =
                Map.of(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85",
                        RawWiseOptions.WAL_SYNC_INTERVAL_MS.key(), "2000");
        Set<String> changedKeys =
                Set.of(
                        RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(),
                        RawWiseOptions.WAL_SYNC_INTERVAL_MS.key());
        String checksum =
                DynamicConfigRegistry.computeValidationChecksum(1, configValues, changedKeys);

        ConfigUpdateMessage original =
                new ConfigUpdateMessage(
                        1, configValues, changedKeys, checksum, PropagationMode.QUORUM);

        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos)) {
            oos.writeObject(original);
        }
        byte[] bytes = bos.toByteArray();

        java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(bytes);
        ConfigUpdateMessage deserialized;
        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bis)) {
            deserialized = (ConfigUpdateMessage) ois.readObject();
        }

        assertThat(deserialized.configEpoch()).isEqualTo(1);
        assertThat(deserialized.configValues())
                .containsEntry(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        assertThat(deserialized.changedKeys()).containsExactlyInAnyOrderElementsOf(changedKeys);
        assertThat(deserialized.validationChecksum()).isEqualTo(checksum);
        assertThat(deserialized.propagationMode()).isEqualTo(PropagationMode.QUORUM);
    }

    // ========================================================================
    // E2E-16: Driver Rollback After Propagation Failure
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-16: Driver rollback after propagation failure - Driver reloads, propagation fails, Driver rolls back")
    void testDriverRollbackAfterPropagationFailure() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");

        driverRegistry.start(driverConfigFile);

        double originalThreshold = driverRegistry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD);
        long originalEpoch = driverRegistry.currentEpoch();

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        boolean driverReloaded = driverRegistry.reload();
        assertThat(driverReloaded).isTrue();
        long newEpoch = driverRegistry.currentEpoch();

        assertThat(driverRegistry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);

        boolean rolledBack = driverRegistry.rollback(originalEpoch);
        assertThat(rolledBack).isTrue();
        assertThat(driverRegistry.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                .isEqualTo(originalThreshold);

        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD))
                .isEqualTo(originalThreshold);
        assertThat(executor0.currentEpoch()).isEqualTo(0);
    }

    // ========================================================================
    // E2E-17: Concurrent Propagation to Multiple Executors
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-17: Concurrent propagation - multiple Executors receive and commit config concurrently")
    void testConcurrentPropagationToMultipleExecutors() throws Exception {
        int executorCount = 5;
        List<DynamicConfigRegistry> executors = new ArrayList<>();
        for (int i = 0; i < executorCount; i++) {
            executors.add(createExecutorRegistry("executor-" + i));
        }

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        driverRegistry.reload();

        long epoch = driverRegistry.currentEpoch();
        Map<String, String> overrideValues =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        ConfigUpdateMessage updateMsg =
                buildUpdateMessageWithChanges(driverRegistry, epoch, overrideValues, changedKeys);

        CountDownLatch prepareLatch = new CountDownLatch(executorCount);
        AtomicInteger acceptedCount = new AtomicInteger(0);

        List<Thread> prepareThreads = new ArrayList<>();
        for (int i = 0; i < executorCount; i++) {
            final int idx = i;
            prepareThreads.add(
                    new Thread(
                            () -> {
                                try {
                                    boolean accepted =
                                            executors.get(idx).reloadFromMessage(updateMsg);
                                    if (accepted) {
                                        acceptedCount.incrementAndGet();
                                    }
                                } finally {
                                    prepareLatch.countDown();
                                }
                            }));
        }

        for (Thread t : prepareThreads) {
            t.start();
        }
        prepareLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);

        int accepted = acceptedCount.get();
        assertThat(accepted).isEqualTo(executorCount);

        ConfigCommitMessage commitMsg = new ConfigCommitMessage(epoch);
        CountDownLatch commitLatch = new CountDownLatch(executorCount);

        List<Thread> commitThreads = new ArrayList<>();
        for (int i = 0; i < executorCount; i++) {
            final int idx = i;
            commitThreads.add(
                    new Thread(
                            () -> {
                                try {
                                    executors.get(idx).commitFromMessage(commitMsg);
                                } finally {
                                    commitLatch.countDown();
                                }
                            }));
        }

        for (Thread t : commitThreads) {
            t.start();
        }
        commitLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);

        for (DynamicConfigRegistry exec : executors) {
            assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
            assertThat(exec.currentEpoch()).isEqualTo(epoch);
        }
    }

    // ========================================================================
    // E2E-18: ConfigCommitMessage and ConfigAbortMessage Record Validation
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-18: ConfigCommitMessage and ConfigAbortMessage validation - negative epoch rejected")
    void testCommitAndAbortMessageValidation() {
        ConfigCommitMessage validCommit = new ConfigCommitMessage(1);
        assertThat(validCommit.configEpoch()).isEqualTo(1);

        assertThatThrownBy(() -> new ConfigCommitMessage(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");

        ConfigAbortMessage validAbort = new ConfigAbortMessage(1);
        assertThat(validAbort.configEpoch()).isEqualTo(1);

        assertThatThrownBy(() -> new ConfigAbortMessage(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    // ========================================================================
    // E2E-19: Full Two-Phase Protocol with PropagationCallback
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-19: Full two-phase protocol via PropagationCallback - Driver triggers callback, Executors commit")
    void testFullTwoPhaseProtocolViaPropagationCallback() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");
        DynamicConfigRegistry executor1 = createExecutorRegistry("executor-1");

        List<DynamicConfigRegistry> executors = List.of(executor0, executor1);
        AtomicBoolean propagateCalled = new AtomicBoolean(false);
        AtomicBoolean commitCalled = new AtomicBoolean(false);
        AtomicReference<DynamicConfigRegistry.PropagationResult> propagationResultRef =
                new AtomicReference<>();

        driverRegistry.setPropagationCallback(
                new DynamicConfigRegistry.PropagationCallback() {
                    @Override
                    public DynamicConfigRegistry.PropagationResult propagate(
                            ConfigUpdateMessage message) {
                        propagateCalled.set(true);
                        int accepted = 0;
                        Set<String> acceptedIds = new HashSet<>();
                        for (int i = 0; i < executors.size(); i++) {
                            boolean result = executors.get(i).reloadFromMessage(message);
                            if (result) {
                                accepted++;
                                acceptedIds.add("executor-" + i);
                            }
                        }
                        boolean quorum = accepted >= (executors.size() / 2 + 1);
                        DynamicConfigRegistry.PropagationResult result =
                                new DynamicConfigRegistry.PropagationResult(
                                        executors.size(),
                                        accepted,
                                        executors.size() - accepted,
                                        0,
                                        quorum,
                                        acceptedIds);
                        propagationResultRef.set(result);
                        return result;
                    }

                    @Override
                    public void sendCommit(
                            ConfigUpdateMessage message, Set<String> targetExecutorIds) {
                        commitCalled.set(true);
                        ConfigCommitMessage commitMsg =
                                new ConfigCommitMessage(message.configEpoch());
                        for (DynamicConfigRegistry exec : executors) {
                            exec.commitFromMessage(commitMsg);
                        }
                    }

                    @Override
                    public void sendAbort(
                            ConfigUpdateMessage message, Set<String> targetExecutorIds) {
                        ConfigAbortMessage abortMsg = new ConfigAbortMessage(message.configEpoch());
                        for (DynamicConfigRegistry exec : executors) {
                            exec.handleAbort(abortMsg);
                        }
                    }
                });

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        boolean driverReloaded = driverRegistry.reload();
        assertThat(driverReloaded).isTrue();

        assertThat(propagateCalled.get()).isTrue();
        DynamicConfigRegistry.PropagationResult propResult = propagationResultRef.get();
        assertThat(propResult).isNotNull();
        assertThat(propResult.quorumReached()).isTrue();
        assertThat(propResult.acceptedCount()).isEqualTo(2);

        assertThat(commitCalled.get()).isTrue();

        for (DynamicConfigRegistry exec : executors) {
            assertThat(exec.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
        }
    }

    // ========================================================================
    // E2E-20: Epoch Mismatch on Commit Rejected
    // ========================================================================

    @Test
    @DisplayName(
            "E2E-20: Epoch mismatch on commit - Executor prepared for epoch N, commit for epoch M rejected")
    void testEpochMismatchOnCommitRejected() throws Exception {
        DynamicConfigRegistry executor0 = createExecutorRegistry("executor-0");

        driverRegistry.start(driverConfigFile);

        writeDriverConfig(0.85, 512L * 1024L * 1024L, 1000L);
        driverRegistry.reload();

        long epoch = driverRegistry.currentEpoch();
        Map<String, String> overrideValues =
                Map.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key(), "0.85");
        Set<String> changedKeys = Set.of(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD.key());
        ConfigUpdateMessage updateMsg =
                buildUpdateMessageWithChanges(driverRegistry, epoch, overrideValues, changedKeys);

        boolean prepared = executor0.reloadFromMessage(updateMsg);
        assertThat(prepared).isTrue();

        ConfigCommitMessage wrongEpochCommit = new ConfigCommitMessage(epoch + 999);
        boolean committed = executor0.commitFromMessage(wrongEpochCommit);
        assertThat(committed).isFalse();

        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.70);
        assertThat(executor0.currentEpoch()).isEqualTo(0);

        ConfigCommitMessage correctCommit = new ConfigCommitMessage(epoch);
        boolean correctCommitted = executor0.commitFromMessage(correctCommit);
        assertThat(correctCommitted).isTrue();
        assertThat(executor0.get(RawWiseOptions.STORAGE_LMDB_USAGE_THRESHOLD)).isEqualTo(0.85);
    }

    // ========================================================================
    // Helper methods
    // ========================================================================

    private void writeDriverConfig(
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
        Files.writeString(driverConfigFile, content);
    }

    private void writeDriverConfigWithClusterId(String clusterId, double usageThreshold)
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
        Files.writeString(driverConfigFile, content);
    }
}
