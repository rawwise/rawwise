package io.github.rawwise.governance.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamicConfigRegistry {

    @FunctionalInterface
    public interface ConfigFileReader {
        Map<String, Object> read(Path path) throws Exception;
    }

    @FunctionalInterface
    public interface ConfigFileHasher {
        String computeHash(Path path) throws IOException;
    }

    @FunctionalInterface
    public interface PropagationCallback {
        PropagationResult propagate(ConfigUpdateMessage message);

        default void sendCommit(ConfigUpdateMessage message, Set<String> targetExecutorIds) {}

        default void sendAbort(ConfigUpdateMessage message, Set<String> targetExecutorIds) {}
    }

    public record PropagationResult(
            int totalExecutors,
            int acceptedCount,
            int rejectedCount,
            int timedOutCount,
            boolean quorumReached,
            Set<String> acceptedExecutorIds) {
        public PropagationResult {
            if (totalExecutors < 0) {
                throw new IllegalArgumentException("totalExecutors must be non-negative");
            }
            if (acceptedCount < 0) {
                throw new IllegalArgumentException("acceptedCount must be non-negative");
            }
            if (rejectedCount < 0) {
                throw new IllegalArgumentException("rejectedCount must be non-negative");
            }
            if (timedOutCount < 0) {
                throw new IllegalArgumentException("timedOutCount must be non-negative");
            }
            if (acceptedExecutorIds == null) {
                acceptedExecutorIds = Set.of();
            }
        }
    }

    private record ProposedChange(
            RawWiseConfig oldConfig,
            RawWiseConfig effectiveNewConfig,
            ConfigDiff diff,
            Set<String> changedKeys,
            String fileHash,
            long proposedEpoch,
            boolean failed) {}

    private final AtomicReference<RawWiseConfig> currentConfig;
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<ConfigChangeListener>> listeners;
    private final AtomicLong configEpoch;
    private final ArrayBlockingQueue<ConfigSnapshot> history;
    private final int maxHistoryVersions;
    private final boolean strictMode;

    private volatile ConfigFileWatcher fileWatcher;
    private volatile Path configFilePath;
    private volatile boolean started;
    private final AtomicLong lastReloadTimeMs = new AtomicLong(0);
    private final AtomicLong reloadFailureCount = new AtomicLong(0);
    private Thread shutdownHook;

    private final ReentrantLock reloadLock = new ReentrantLock();

    private final ConfigFileReader fileReader;
    private final ConfigFileHasher fileHasher;
    private Supplier<ConfigFileWatcher> watcherFactory;

    private volatile PropagationCallback propagationCallback;
    private volatile RawWiseConfig preparedConfig;
    private volatile ConfigDiff preparedDiff;
    private volatile long preparedEpoch = -1;
    private volatile boolean preparedRejected;

    private volatile ScheduledExecutorService preparedTimeoutScheduler;
    private volatile ScheduledFuture<?> preparedTimeoutFuture;

    private DynamicConfigRegistry(
            RawWiseConfig initialConfig,
            int maxHistoryVersions,
            ConfigFileReader fileReader,
            ConfigFileHasher fileHasher,
            Supplier<ConfigFileWatcher> watcherFactory,
            boolean strictMode) {
        this.currentConfig =
                new AtomicReference<>(
                        Objects.requireNonNull(initialConfig, "initialConfig must not be null"));
        this.listeners = new ConcurrentHashMap<>();
        this.configEpoch = new AtomicLong(0);
        this.maxHistoryVersions = maxHistoryVersions;
        this.history = new ArrayBlockingQueue<>(maxHistoryVersions);
        this.fileReader = Objects.requireNonNull(fileReader, "fileReader must not be null");
        this.fileHasher = Objects.requireNonNull(fileHasher, "fileHasher must not be null");
        this.watcherFactory = watcherFactory;
        this.strictMode = strictMode;
        addSnapshot(0, initialConfig, "");
    }

    public static DynamicConfigRegistry create(RawWiseConfig initialConfig) {
        DynamicConfigRegistry registry =
                new DynamicConfigRegistry(
                        initialConfig,
                        20,
                        HoconConfigReader::read,
                        ConfigFileWatcher::computeFileHash,
                        null,
                        false);
        registry.watcherFactory =
                () ->
                        new ConfigFileWatcher(
                                registry.currentConfig
                                        .get()
                                        .get(RawWiseOptions.CONFIG_FILE_POLL_INTERVAL_MS),
                                registry.currentConfig
                                        .get()
                                        .get(RawWiseOptions.CONFIG_FILE_DEBOUNCE_MS));
        return registry;
    }

    public static DynamicConfigRegistry create(
            RawWiseConfig initialConfig, int maxHistoryVersions) {
        DynamicConfigRegistry registry =
                new DynamicConfigRegistry(
                        initialConfig,
                        maxHistoryVersions,
                        HoconConfigReader::read,
                        ConfigFileWatcher::computeFileHash,
                        null,
                        false);
        registry.watcherFactory =
                () ->
                        new ConfigFileWatcher(
                                registry.currentConfig
                                        .get()
                                        .get(RawWiseOptions.CONFIG_FILE_POLL_INTERVAL_MS),
                                registry.currentConfig
                                        .get()
                                        .get(RawWiseOptions.CONFIG_FILE_DEBOUNCE_MS));
        return registry;
    }

    public static DynamicConfigRegistry create(
            RawWiseConfig initialConfig,
            int maxHistoryVersions,
            ConfigFileReader fileReader,
            ConfigFileHasher fileHasher,
            Supplier<ConfigFileWatcher> watcherFactory) {
        return new DynamicConfigRegistry(
                initialConfig, maxHistoryVersions, fileReader, fileHasher, watcherFactory, false);
    }

    public static DynamicConfigRegistry create(
            RawWiseConfig initialConfig,
            int maxHistoryVersions,
            ConfigFileReader fileReader,
            ConfigFileHasher fileHasher,
            Supplier<ConfigFileWatcher> watcherFactory,
            boolean strictMode) {
        return new DynamicConfigRegistry(
                initialConfig,
                maxHistoryVersions,
                fileReader,
                fileHasher,
                watcherFactory,
                strictMode);
    }

    public synchronized void start(Path configFilePath) {
        if (started) {
            log.warn("DynamicConfigRegistry already started");
            return;
        }
        this.configFilePath =
                Objects.requireNonNull(configFilePath, "configFilePath must not be null");

        if (watcherFactory == null) {
            throw new IllegalStateException("watcherFactory not initialized");
        }
        this.fileWatcher = watcherFactory.get();
        this.fileWatcher.start(configFilePath, this::onFileChange);
        this.started = true;

        this.shutdownHook = new Thread(this::stop, "config-registry-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        log.info(
                "DynamicConfigRegistry started: file={}, epoch={}",
                configFilePath,
                configEpoch.get());
    }

    public synchronized void stop() {
        if (!started) {
            return;
        }
        if (fileWatcher != null) {
            fileWatcher.stop();
            fileWatcher = null;
        }
        cancelPreparedTimeout();
        if (preparedTimeoutScheduler != null) {
            preparedTimeoutScheduler.shutdownNow();
            try {
                if (!preparedTimeoutScheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                    log.warn("Prepared timeout scheduler did not terminate in time");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            preparedTimeoutScheduler = null;
        }
        started = false;

        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (Exception e) {
                log.debug("Failed to remove shutdown hook", e);
            }
            shutdownHook = null;
        }

        log.info("DynamicConfigRegistry stopped at epoch={}", configEpoch.get());
    }

    public <T> T get(ConfigOption<T> option) {
        return currentConfig.get().get(option);
    }

    public RawWiseConfig currentConfig() {
        return currentConfig.get();
    }

    public long currentEpoch() {
        return configEpoch.get();
    }

    public boolean isRunning() {
        return started;
    }

    public void registerListener(String keyPrefix, ConfigChangeListener listener) {
        Objects.requireNonNull(keyPrefix, "keyPrefix must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        listeners.computeIfAbsent(keyPrefix, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    public void unregisterListener(String keyPrefix, ConfigChangeListener listener) {
        Objects.requireNonNull(keyPrefix, "keyPrefix must not be null");
        Objects.requireNonNull(listener, "listener must not be null");
        listeners.computeIfPresent(
                keyPrefix,
                (k, list) -> {
                    list.remove(listener);
                    return list.isEmpty() ? null : list;
                });
    }

    public boolean reload() {
        return doReload(false, null);
    }

    public ConfigDiff dryReload() {
        return computeDiff();
    }

    public boolean rollback(long targetEpoch) {
        reloadLock.lock();
        try {
            return doRollback(targetEpoch);
        } finally {
            reloadLock.unlock();
        }
    }

    public void setPropagationCallback(PropagationCallback callback) {
        this.propagationCallback = callback;
    }

    public boolean reloadFromMessage(ConfigUpdateMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        reloadLock.lock();
        try {
            RawWiseConfig configSnapshot = currentConfig.get();

            String expectedChecksum =
                    computeValidationChecksum(
                            message.configEpoch(), message.configValues(), message.changedKeys());
            if (!expectedChecksum.equals(message.validationChecksum())) {
                log.error(
                        "ConfigUpdateMessage checksum mismatch for epoch {}. Expected={}, got={}. "
                                + "Changed keys may differ between Driver and Executor.",
                        message.configEpoch(),
                        expectedChecksum,
                        message.validationChecksum());
                return false;
            }

            if (message.configEpoch() <= configEpoch.get()) {
                log.info(
                        "Ignoring stale ConfigUpdateMessage: messageEpoch={}, currentEpoch={}",
                        message.configEpoch(),
                        configEpoch.get());
                return true;
            }

            Map<String, Object> coercedValues = new HashMap<>();
            Set<String> allKeys = RawWiseOptions.allKeys();
            for (String key : allKeys) {
                ConfigOption<?> option = RawWiseOptions.findOption(key);
                if (option == null) continue;
                if (message.configValues().containsKey(key)) {
                    String stringValue = message.configValues().get(key);
                    Object coerced = ConfigFactory.coerceValue(key, stringValue, option.type());
                    ConfigFactory.validateOption(option, coerced);
                    coercedValues.put(key, coerced);
                } else {
                    coercedValues.put(key, configSnapshot.get(option));
                }
            }

            RawWiseConfig newConfig;
            try {
                newConfig = ConfigFactory.buildValidated(coercedValues, false, false);
            } catch (Exception e) {
                log.error("Config validation failed on executor reload: {}", e.getMessage());
                return false;
            }

            long baseEpoch = configEpoch.get();

            Set<String> changedKeys = computeChangedKeys(configSnapshot, newConfig);
            Set<String> hotReloadableKeys = new HashSet<>();
            Set<String> rejectedKeys = new HashSet<>();
            Map<String, TransitionPolicy> transitionPolicies = new HashMap<>();

            for (String key : changedKeys) {
                if (ConfigMetadata.isHotReloadable(key)) {
                    hotReloadableKeys.add(key);
                    transitionPolicies.put(key, ConfigMetadata.getTransitionPolicy(key));
                } else {
                    rejectedKeys.add(key);
                }
            }

            if (hotReloadableKeys.isEmpty()) {
                log.info(
                        "No hot-reloadable keys in ConfigUpdateMessage, storing as prepared for commit");
                this.preparedConfig = configSnapshot;
                this.preparedDiff =
                        new ConfigDiff(
                                -1,
                                baseEpoch,
                                Set.of(),
                                Set.of(),
                                Set.of(),
                                Map.of(),
                                false,
                                Instant.now());
                this.preparedEpoch = message.configEpoch();
                this.preparedRejected = false;
                schedulePreparedTimeout(message.configEpoch());
                return true;
            }

            RawWiseConfig effectiveNewConfig =
                    applyHotReloadableOnly(configSnapshot, newConfig, hotReloadableKeys);

            ConfigDiff diff =
                    new ConfigDiff(
                            -1,
                            baseEpoch,
                            changedKeys,
                            hotReloadableKeys,
                            rejectedKeys,
                            transitionPolicies,
                            !rejectedKeys.isEmpty(),
                            Instant.now());

            boolean listenerAccepted = notifyListenersPreConfigChange(diff, effectiveNewConfig);

            this.preparedConfig = effectiveNewConfig;
            this.preparedDiff = diff;
            this.preparedEpoch = message.configEpoch();
            this.preparedRejected = !listenerAccepted;

            schedulePreparedTimeout(message.configEpoch());

            if (!listenerAccepted) {
                log.warn(
                        "Executor config reload vetoed by listener at baseEpoch {}, storing as rejected prepare",
                        baseEpoch);
            }

            return listenerAccepted;
        } finally {
            reloadLock.unlock();
        }
    }

    public boolean commitFromMessage(ConfigCommitMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        reloadLock.lock();
        try {
            if (message.configEpoch() <= configEpoch.get()) {
                log.info(
                        "Ignoring stale ConfigCommitMessage: messageEpoch={}, currentEpoch={}",
                        message.configEpoch(),
                        configEpoch.get());
                return true;
            }

            if (message.configEpoch() != preparedEpoch) {
                log.warn(
                        "Commit epoch mismatch: messageEpoch={}, preparedEpoch={}. Rejecting commit.",
                        message.configEpoch(),
                        preparedEpoch);
                return false;
            }

            RawWiseConfig prepared = this.preparedConfig;
            ConfigDiff preparedDiffSnapshot = this.preparedDiff;
            if (prepared == null || preparedDiffSnapshot == null) {
                log.warn("No prepared config to commit for epoch {}", message.configEpoch());
                return false;
            }

            RawWiseConfig oldConfig = currentConfig.get();
            boolean swapped = currentConfig.compareAndSet(oldConfig, prepared);
            if (!swapped) {
                log.warn(
                        "Commit CAS failed for epoch {}, concurrent modification occurred",
                        message.configEpoch());
                return false;
            }

            long newEpoch = preparedEpoch;
            configEpoch.set(newEpoch);

            ConfigDiff commitDiff =
                    new ConfigDiff(
                            newEpoch,
                            preparedDiffSnapshot.baseEpoch(),
                            preparedDiffSnapshot.changedKeys(),
                            preparedDiffSnapshot.hotReloadableKeys(),
                            preparedDiffSnapshot.rejectedKeys(),
                            preparedDiffSnapshot.transitionPolicies(),
                            preparedRejected,
                            Instant.now());

            addSnapshot(newEpoch, prepared, "");
            notifyListenersOnConfigChange(commitDiff, prepared);

            cancelPreparedTimeout();
            this.preparedConfig = null;
            this.preparedDiff = null;
            this.preparedEpoch = -1;
            this.preparedRejected = false;

            log.info(
                    "Executor config committed from Driver propagation: epoch={}, changedKeys={}, appliedDespiteRejection={}",
                    newEpoch,
                    commitDiff.changedKeys().size(),
                    preparedRejected);
            return true;
        } finally {
            reloadLock.unlock();
        }
    }

    public void handleAbort(ConfigAbortMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        reloadLock.lock();
        try {
            if (message.configEpoch() == preparedEpoch) {
                log.info("Aborting prepared config for epoch {}", message.configEpoch());
                cancelPreparedTimeout();
                this.preparedConfig = null;
                this.preparedDiff = null;
                this.preparedEpoch = -1;
                this.preparedRejected = false;
            } else {
                log.debug(
                        "Ignoring ConfigAbortMessage for epoch {}, current preparedEpoch={}",
                        message.configEpoch(),
                        preparedEpoch);
            }
        } finally {
            reloadLock.unlock();
        }
    }

    public static String computeValidationChecksum(
            long epoch, Map<String, String> configValues, Set<String> changedKeys) {
        TreeSet<String> sortedKeys = new TreeSet<>(changedKeys);
        StringBuilder sb = new StringBuilder();
        sb.append(epoch).append(":");
        String joined =
                sortedKeys.stream()
                        .map(k -> k + "=" + configValues.getOrDefault(k, ""))
                        .reduce((a, b) -> a + "|" + b)
                        .orElse("");
        sb.append(joined);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                hexString.append(String.format("%02x", b));
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    public Map<String, String> serializeConfigForPropagation(Set<String> changedKeys) {
        Objects.requireNonNull(changedKeys, "changedKeys must not be null");
        return serializeConfigForPropagation(currentConfig.get(), changedKeys);
    }

    public Map<String, String> serializeConfigForPropagation(
            RawWiseConfig config, Set<String> changedKeys) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(changedKeys, "changedKeys must not be null");
        Map<String, String> result = new TreeMap<>();
        for (String key : changedKeys) {
            if (ConfigMetadata.isSensitive(key)) {
                log.warn(
                        "Skipping sensitive key '{}' from RPC propagation; use secure channel for credential updates",
                        key);
                continue;
            }
            ConfigOption<?> option = RawWiseOptions.findOption(key);
            if (option == null) continue;
            Object value = config.get(option);
            if (value != null) {
                result.put(key, valueToString(value));
            }
        }
        return result;
    }

    private static String valueToString(Object value) {
        if (value instanceof Duration d) {
            return String.valueOf(d.toMillis());
        }
        return String.valueOf(value);
    }

    private boolean doRollback(long targetEpoch) {
        ConfigSnapshot targetSnapshot = findSnapshot(targetEpoch);
        if (targetSnapshot == null) {
            Path backupFile = resolveBackupFile(targetEpoch);
            if (backupFile != null && Files.exists(backupFile)) {
                log.info("Loading rollback target from backup file: {}", backupFile);
                try {
                    Map<String, Object> values = fileReader.read(backupFile);
                    RawWiseConfig rollbackConfig = buildValidatedConfig(values);
                    targetSnapshot = new ConfigSnapshot(targetEpoch, rollbackConfig, "");
                } catch (Exception e) {
                    log.error(
                            "Failed to load backup file for epoch {}: {}",
                            targetEpoch,
                            e.getMessage());
                    return false;
                }
            } else {
                log.error(
                        "Rollback target epoch {} not found in history or backup files. Available range: 0-{}",
                        targetEpoch,
                        configEpoch.get());
                return false;
            }
        }

        RawWiseConfig oldConfig = currentConfig.get();
        RawWiseConfig rollbackConfig = targetSnapshot.config();
        long baseEpoch = configEpoch.get();

        if (configFilePath != null) {
            try {
                writeConfigFile(rollbackConfig);
            } catch (Exception e) {
                log.warn(
                        "Rollback file write failed (read-only mount?), continuing in-memory rollback: {}",
                        e.getMessage());
            }
        }

        boolean swapped = currentConfig.compareAndSet(oldConfig, rollbackConfig);
        if (!swapped) {
            log.warn(
                    "Rollback CAS failed for epoch {}, concurrent modification occurred",
                    targetEpoch);
            return false;
        }

        long newEpoch = configEpoch.incrementAndGet();

        Set<String> changedKeys = computeChangedKeys(oldConfig, rollbackConfig);
        Set<String> hotKeys = new HashSet<>();
        Set<String> rejectedKeys = new HashSet<>();
        Map<String, TransitionPolicy> policies = new HashMap<>();
        for (String key : changedKeys) {
            if (ConfigMetadata.isHotReloadable(key)) {
                hotKeys.add(key);
                policies.put(key, ConfigMetadata.getTransitionPolicy(key));
            } else {
                rejectedKeys.add(key);
            }
        }

        ConfigDiff diff =
                new ConfigDiff(
                        newEpoch,
                        baseEpoch,
                        changedKeys,
                        hotKeys,
                        rejectedKeys,
                        policies,
                        !rejectedKeys.isEmpty(),
                        Instant.now());

        if (configFilePath != null && fileWatcher != null) {
            ConfigFileWatcher oldWatcher = this.fileWatcher;
            oldWatcher.stop();

            this.fileWatcher = watcherFactory.get();
            this.fileWatcher.start(configFilePath, this::onFileChange);
        }

        addSnapshot(newEpoch, rollbackConfig, "");
        notifyListenersOnConfigChange(diff, rollbackConfig);

        log.info("Rollback to epoch {} completed, new epoch={}", targetEpoch, newEpoch);
        return true;
    }

    private void onFileChange(String fileHash) {
        long minIntervalMs = currentConfig.get().get(RawWiseOptions.CONFIG_RELOAD_MIN_INTERVAL_MS);
        long now = System.currentTimeMillis();
        long last = lastReloadTimeMs.get();
        if (now - last < minIntervalMs) {
            log.debug(
                    "Reload cooldown: {}ms since last reload, min interval={}ms",
                    now - last,
                    minIntervalMs);
            return;
        }
        doReload(true, fileHash);
    }

    private boolean doReload(boolean fromFileWatcher, String providedFileHash) {
        ProposedChange proposed = computeProposedChange(fromFileWatcher, providedFileHash);
        if (proposed == null) {
            return true;
        }
        if (proposed.failed()) {
            return false;
        }

        PropagationResult result = null;
        ConfigUpdateMessage propagateMessage = null;
        if (propagationCallback != null) {
            try {
                Map<String, String> configValues =
                        serializeConfigForPropagation(
                                proposed.effectiveNewConfig, proposed.changedKeys);
                String checksum =
                        computeValidationChecksum(
                                proposed.proposedEpoch, configValues, proposed.changedKeys);
                PropagationMode propagationMode =
                        PropagationMode.fromString(
                                proposed.effectiveNewConfig.get(
                                        RawWiseOptions.CONFIG_PROPAGATION_MODE));
                propagateMessage =
                        new ConfigUpdateMessage(
                                proposed.proposedEpoch,
                                configValues,
                                proposed.changedKeys,
                                checksum,
                                propagationMode);
                result = propagationCallback.propagate(propagateMessage);
            } catch (Exception e) {
                log.warn("Config propagation callback failed: {}", e.getMessage());
            }
        }

        boolean quorumReached;
        if (propagationCallback == null) {
            quorumReached = true;
        } else if (result == null) {
            quorumReached = false;
        } else {
            quorumReached = result.quorumReached();
        }

        if (quorumReached) {
            boolean applied = applyCommittedChange(proposed, fromFileWatcher);
            if (propagationCallback != null && propagateMessage != null) {
                if (applied) {
                    propagationCallback.sendCommit(
                            propagateMessage,
                            result != null ? result.acceptedExecutorIds() : Set.of());
                } else {
                    propagationCallback.sendAbort(
                            propagateMessage,
                            result != null ? result.acceptedExecutorIds() : Set.of());
                }
            }
            return applied;
        } else {
            if (propagationCallback != null && propagateMessage != null) {
                propagationCallback.sendAbort(
                        propagateMessage, result != null ? result.acceptedExecutorIds() : Set.of());
            }
            abortPendingChange();
            if (result != null) {
                log.warn(
                        "Config propagation quorum NOT reached: total={}, accepted={}, rejected={}, timedOut={}. "
                                + "Aborting config change on Driver.",
                        result.totalExecutors(),
                        result.acceptedCount(),
                        result.rejectedCount(),
                        result.timedOutCount());
            } else {
                log.warn(
                        "Config propagation callback returned null (exception during propagation). "
                                + "Aborting config change on Driver.");
            }
            return false;
        }
    }

    private ProposedChange computeProposedChange(boolean fromFileWatcher, String providedFileHash) {
        reloadLock.lock();
        try {
            if (configFilePath == null) {
                log.warn("Cannot reload: no config file path set");
                return null;
            }

            RawWiseConfig oldConfig = currentConfig.get();
            long baseEpoch = configEpoch.get();

            RawWiseConfig newConfig;
            try {
                Map<String, Object> values = fileReader.read(configFilePath);
                newConfig = buildValidatedConfig(values);
            } catch (Exception e) {
                log.error("Config reload parse failure, keeping old config: {}", e.getMessage());
                reloadFailureCount.incrementAndGet();
                return new ProposedChange(
                        oldConfig, oldConfig, null, Set.of(), "", baseEpoch + 1, true);
            }

            Set<String> changedKeys = computeChangedKeys(oldConfig, newConfig);
            if (changedKeys.isEmpty()) {
                log.debug("No config changes detected");
                return null;
            }

            Set<String> hotReloadableKeys = new HashSet<>();
            Set<String> rejectedKeys = new HashSet<>();
            Map<String, TransitionPolicy> transitionPolicies = new HashMap<>();

            for (String key : changedKeys) {
                if (ConfigMetadata.isHotReloadable(key)) {
                    hotReloadableKeys.add(key);
                    transitionPolicies.put(key, ConfigMetadata.getTransitionPolicy(key));
                } else {
                    rejectedKeys.add(key);
                }
            }

            if (!rejectedKeys.isEmpty()) {
                log.warn("Non-hot-reloadable config keys changed (NOT applied): {}", rejectedKeys);
            }

            if (hotReloadableKeys.isEmpty()) {
                log.info("Only non-hot-reloadable keys changed, reload skipped");
                return null;
            }

            RawWiseConfig effectiveNewConfig =
                    applyHotReloadableOnly(oldConfig, newConfig, hotReloadableKeys);

            ConfigDiff diff =
                    new ConfigDiff(
                            -1,
                            baseEpoch,
                            changedKeys,
                            hotReloadableKeys,
                            rejectedKeys,
                            transitionPolicies,
                            false,
                            Instant.now());

            if (!notifyListenersPreConfigChange(diff, effectiveNewConfig)) {
                log.warn("Reload aborted by listener veto at baseEpoch {}", baseEpoch);
                return new ProposedChange(
                        oldConfig, effectiveNewConfig, diff, changedKeys, "", baseEpoch + 1, true);
            }

            String fileHash = providedFileHash != null ? providedFileHash : "";
            if (fileHash.isEmpty()) {
                try {
                    fileHash = fileHasher.computeHash(configFilePath);
                } catch (IOException e) {
                    log.debug("Failed to compute file hash after reload", e);
                }
            }

            return new ProposedChange(
                    oldConfig,
                    effectiveNewConfig,
                    diff,
                    changedKeys,
                    fileHash,
                    baseEpoch + 1,
                    false);
        } finally {
            reloadLock.unlock();
        }
    }

    private boolean applyCommittedChange(ProposedChange proposed, boolean fromFileWatcher) {
        reloadLock.lock();
        try {
            boolean swapped =
                    currentConfig.compareAndSet(
                            proposed.oldConfig(), proposed.effectiveNewConfig());
            if (!swapped) {
                log.warn("Reload CAS failed, concurrent modification occurred");
                return false;
            }

            long baseEpoch = proposed.diff().baseEpoch();
            long proposedEpoch = proposed.proposedEpoch();
            boolean epochCas = configEpoch.compareAndSet(baseEpoch, proposedEpoch);
            if (!epochCas) {
                currentConfig.compareAndSet(proposed.effectiveNewConfig(), proposed.oldConfig());
                log.warn(
                        "Reload epoch CAS failed: expected={}, proposed={}. Rolling back config swap.",
                        baseEpoch,
                        proposedEpoch);
                return false;
            }

            ConfigDiff diff =
                    new ConfigDiff(
                            proposedEpoch,
                            baseEpoch,
                            proposed.diff().changedKeys(),
                            proposed.diff().hotReloadableKeys(),
                            proposed.diff().rejectedKeys(),
                            proposed.diff().transitionPolicies(),
                            false,
                            Instant.now());

            lastReloadTimeMs.set(System.currentTimeMillis());

            addSnapshot(proposedEpoch, proposed.effectiveNewConfig(), proposed.fileHash());
            backupConfigFile(proposed.oldConfig(), baseEpoch);

            maybeRebuildFileWatcher(proposed.oldConfig(), proposed.effectiveNewConfig());

            notifyListenersOnConfigChange(diff, proposed.effectiveNewConfig());

            logReload(diff, proposed.effectiveNewConfig(), fromFileWatcher);
            return true;
        } finally {
            reloadLock.unlock();
        }
    }

    private void abortPendingChange() {
        reloadLock.lock();
        try {
            log.info("Aborting pending config change due to propagation quorum failure");
        } finally {
            reloadLock.unlock();
        }
    }

    private void schedulePreparedTimeout(long epoch) {
        if (preparedTimeoutScheduler == null) {
            preparedTimeoutScheduler =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "config-prepared-timeout");
                                t.setDaemon(true);
                                return t;
                            });
        }
        ScheduledFuture<?> oldFuture = preparedTimeoutFuture;
        if (oldFuture != null && !oldFuture.isDone()) {
            oldFuture.cancel(false);
        }
        preparedTimeoutFuture =
                preparedTimeoutScheduler.schedule(
                        () -> {
                            reloadLock.lock();
                            try {
                                if (preparedEpoch == epoch) {
                                    log.warn(
                                            "Prepared config for epoch {} timed out, auto-clearing",
                                            epoch);
                                    this.preparedConfig = null;
                                    this.preparedDiff = null;
                                    this.preparedEpoch = -1;
                                    this.preparedRejected = false;
                                }
                            } finally {
                                reloadLock.unlock();
                            }
                        },
                        60,
                        TimeUnit.SECONDS);
    }

    private void cancelPreparedTimeout() {
        ScheduledFuture<?> future = preparedTimeoutFuture;
        if (future != null && !future.isDone()) {
            future.cancel(false);
        }
        preparedTimeoutFuture = null;
    }

    private void maybeRebuildFileWatcher(RawWiseConfig oldConfig, RawWiseConfig newConfig) {
        if (fileWatcher == null || configFilePath == null) {
            return;
        }
        long oldPoll = oldConfig.get(RawWiseOptions.CONFIG_FILE_POLL_INTERVAL_MS);
        long newPoll = newConfig.get(RawWiseOptions.CONFIG_FILE_POLL_INTERVAL_MS);
        long oldDebounce = oldConfig.get(RawWiseOptions.CONFIG_FILE_DEBOUNCE_MS);
        long newDebounce = newConfig.get(RawWiseOptions.CONFIG_FILE_DEBOUNCE_MS);

        if (oldPoll != newPoll || oldDebounce != newDebounce) {
            log.info(
                    "FileWatcher parameters changed (poll: {}→{}, debounce: {}→{}), rebuilding",
                    oldPoll,
                    newPoll,
                    oldDebounce,
                    newDebounce);
            fileWatcher.stop();
            this.fileWatcher = watcherFactory.get();
            this.fileWatcher.start(configFilePath, this::onFileChange);
        }
    }

    private ConfigDiff computeDiff() {
        if (configFilePath == null) {
            return new ConfigDiff(
                    -1,
                    configEpoch.get(),
                    Set.of(),
                    Set.of(),
                    Set.of(),
                    Map.of(),
                    false,
                    Instant.now());
        }

        reloadLock.lock();
        try {
            RawWiseConfig oldConfig = currentConfig.get();
            long baseEpoch = configEpoch.get();

            RawWiseConfig newConfig;
            try {
                Map<String, Object> values = fileReader.read(configFilePath);
                newConfig = buildValidatedConfig(values);
            } catch (Exception e) {
                return new ConfigDiff(
                        -1,
                        baseEpoch,
                        Set.of(),
                        Set.of(),
                        Set.of(),
                        Map.of(),
                        false,
                        Instant.now());
            }

            Set<String> changedKeys = computeChangedKeys(oldConfig, newConfig);

            Set<String> hotReloadableKeys = new HashSet<>();
            Set<String> rejectedKeys = new HashSet<>();
            Map<String, TransitionPolicy> transitionPolicies = new HashMap<>();

            for (String key : changedKeys) {
                if (ConfigMetadata.isHotReloadable(key)) {
                    hotReloadableKeys.add(key);
                    transitionPolicies.put(key, ConfigMetadata.getTransitionPolicy(key));
                } else {
                    rejectedKeys.add(key);
                }
            }

            return new ConfigDiff(
                    -1,
                    baseEpoch,
                    changedKeys,
                    hotReloadableKeys,
                    rejectedKeys,
                    transitionPolicies,
                    false,
                    Instant.now());
        } finally {
            reloadLock.unlock();
        }
    }

    private RawWiseConfig buildValidatedConfig(Map<String, Object> values) {
        if (strictMode) {
            Set<String> knownKeys = RawWiseOptions.allKeys();
            for (String key : values.keySet()) {
                if (!knownKeys.contains(key)) {
                    throw new ConfigException.ValidationError(
                            "Unknown config key in strict mode: " + key);
                }
            }
        }
        return ConfigFactory.buildValidated(values, false, true);
    }

    private Set<String> computeChangedKeys(RawWiseConfig oldConfig, RawWiseConfig newConfig) {
        Set<String> allKeys = RawWiseOptions.allKeys();
        Set<String> changed = new HashSet<>();
        for (String key : allKeys) {
            ConfigOption<?> option = RawWiseOptions.findOption(key);
            if (option == null) continue;

            Object oldValue = oldConfig.get(option);
            Object newValue = newConfig.get(option);
            if (!Objects.equals(oldValue, newValue)) {
                changed.add(key);
            }
        }
        return changed;
    }

    private RawWiseConfig applyHotReloadableOnly(
            RawWiseConfig oldConfig, RawWiseConfig newConfig, Set<String> hotReloadableKeys) {
        Map<String, Object> merged = new HashMap<>();
        Set<String> allKeys = RawWiseOptions.allKeys();

        for (String key : allKeys) {
            ConfigOption<?> option = RawWiseOptions.findOption(key);
            if (option == null) continue;

            if (hotReloadableKeys.contains(key)) {
                merged.put(key, newConfig.get(option));
            } else {
                merged.put(key, oldConfig.get(option));
            }
        }

        return RawWiseConfig.fromMap(merged);
    }

    private boolean notifyListenersPreConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
        List<ListenerEntry> entries = collectListeners(diff.changedKeys());
        for (ListenerEntry entry : entries) {
            try {
                if (!entry.listener.preConfigChange(diff, newConfig)) {
                    log.warn(
                            "Listener {} (prefix={}) vetoed config change at epoch {}",
                            entry.listener.getClass().getSimpleName(),
                            entry.prefix,
                            diff.epoch());
                    return false;
                }
            } catch (Exception e) {
                log.error(
                        "Listener {} (prefix={}) threw exception in preConfigChange",
                        entry.listener.getClass().getSimpleName(),
                        entry.prefix,
                        e);
                entry.listener.onError(diff, e);
                return false;
            }
        }
        return true;
    }

    private void notifyListenersOnConfigChange(ConfigDiff diff, RawWiseConfig newConfig) {
        List<ListenerEntry> entries = collectListeners(diff.changedKeys());
        for (ListenerEntry entry : entries) {
            try {
                entry.listener.onConfigChange(diff, newConfig);
            } catch (Exception e) {
                log.error(
                        "Listener {} (prefix={}) threw exception in onConfigChange",
                        entry.listener.getClass().getSimpleName(),
                        entry.prefix,
                        e);
                try {
                    entry.listener.onError(diff, e);
                } catch (Exception e2) {
                    log.error(
                            "Listener {} (prefix={}) threw exception in onError",
                            entry.listener.getClass().getSimpleName(),
                            entry.prefix,
                            e2);
                }
            }
        }
    }

    private List<ListenerEntry> collectListeners(Set<String> changedKeys) {
        List<ListenerEntry> result = new ArrayList<>();
        for (Map.Entry<String, CopyOnWriteArrayList<ConfigChangeListener>> entry :
                listeners.entrySet()) {
            String prefix = entry.getKey();
            boolean matches = prefix.isEmpty();
            if (!matches) {
                for (String key : changedKeys) {
                    if (key.startsWith(prefix)) {
                        matches = true;
                        break;
                    }
                }
            }
            if (matches) {
                for (ConfigChangeListener listener : entry.getValue()) {
                    result.add(new ListenerEntry(prefix, listener));
                }
            }
        }
        return result;
    }

    private void addSnapshot(long epoch, RawWiseConfig config, String fileHash) {
        ConfigSnapshot snapshot = new ConfigSnapshot(epoch, config, fileHash);
        if (!history.offer(snapshot)) {
            history.poll();
            history.offer(snapshot);
        }
    }

    private ConfigSnapshot findSnapshot(long targetEpoch) {
        for (ConfigSnapshot snapshot : history) {
            if (snapshot.epoch() == targetEpoch) {
                return snapshot;
            }
        }
        return null;
    }

    private Path resolveBackupFile(long epoch) {
        if (configFilePath == null) {
            return null;
        }
        Path parent = configFilePath.getParent();
        String fileName = configFilePath.getFileName().toString();
        Path backupPath = parent.resolve(fileName + ".bak." + epoch);
        if (!isWritableDir(parent)) {
            Path fallbackDir =
                    Path.of(System.getProperty("java.io.tmpdir"), "rawwise", "config-backup");
            try {
                Files.createDirectories(fallbackDir);
            } catch (IOException e) {
                log.warn("Failed to create fallback backup directory: {}", fallbackDir, e);
                return backupPath;
            }
            return fallbackDir.resolve(fileName + ".bak." + epoch);
        }
        return backupPath;
    }

    private boolean isWritableDir(Path dir) {
        try {
            Path testFile = dir.resolve(".rawwise_write_test");
            Files.writeString(testFile, "test");
            Files.deleteIfExists(testFile);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void backupConfigFile(RawWiseConfig oldConfig, long epoch) {
        if (configFilePath == null || !Files.exists(configFilePath)) {
            return;
        }
        Path backupPath = resolveBackupFile(epoch);
        if (backupPath == null) return;
        try {
            Files.createDirectories(backupPath.getParent());
            Files.copy(configFilePath, backupPath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Backed up config to {}", backupPath);
            cleanupOldBackups(backupPath.getParent());
        } catch (IOException e) {
            log.warn("Failed to backup config file to {}: {}", backupPath, e.getMessage());
        }
    }

    private void cleanupOldBackups(Path backupDir) {
        try {
            List<Path> backups = new ArrayList<>();
            try (var stream = Files.list(backupDir)) {
                stream.filter(p -> p.getFileName().toString().contains(".bak."))
                        .forEach(backups::add);
            }
            if (backups.size() > maxHistoryVersions) {
                backups.sort(
                        (a, b) -> {
                            String nameA = a.getFileName().toString();
                            String nameB = b.getFileName().toString();
                            return nameA.compareTo(nameB);
                        });
                int toDelete = backups.size() - maxHistoryVersions;
                for (int i = 0; i < toDelete; i++) {
                    Files.deleteIfExists(backups.get(i));
                    log.debug("Cleaned up old backup: {}", backups.get(i));
                }
            }
        } catch (IOException e) {
            log.debug("Failed to cleanup old backups in {}", backupDir, e);
        }
    }

    private void writeConfigFile(RawWiseConfig config) throws IOException {
        if (configFilePath == null) return;
        StringBuilder sb = new StringBuilder();
        sb.append("# RawWise Configuration — auto-generated by rollback\n");
        sb.append("# Generated at: ").append(Instant.now()).append("\n");
        sb.append("rawwise {\n");

        Set<String> allKeys = RawWiseOptions.allKeys();
        Map<String, Object> values = new HashMap<>();
        for (String key : allKeys) {
            ConfigOption<?> option = RawWiseOptions.findOption(key);
            if (option != null) {
                values.put(key, config.get(option));
            }
        }

        values.forEach(
                (key, value) -> {
                    if (key.startsWith("rawwise.")) {
                        String path = key.substring("rawwise.".length());
                        if (ConfigMetadata.isSensitive(key)) {
                            sb.append("  # ").append(path).append(" = *** (sensitive)\n");
                        } else {
                            sb.append("  ").append(path).append(" = ");
                            if (value instanceof String s) {
                                sb.append("\"").append(s).append("\"");
                            } else {
                                sb.append(value);
                            }
                            sb.append("\n");
                        }
                    }
                });

        sb.append("}\n");
        Files.writeString(configFilePath, sb.toString());
    }

    private void logReload(ConfigDiff diff, RawWiseConfig newConfig, boolean fromFileWatcher) {
        String source = fromFileWatcher ? "file_watcher" : "manual";
        log.info(
                "Config reload: epoch={}, baseEpoch={}, source={}, changedKeys={}, hotReloadableKeys={}, rejectedKeys={}",
                diff.epoch(),
                diff.baseEpoch(),
                source,
                diff.changedKeys().size(),
                diff.hotReloadableKeys().size(),
                diff.rejectedKeys().size());

        for (String key : diff.hotReloadableKeys()) {
            ConfigOption<?> option = RawWiseOptions.findOption(key);
            Object value = option != null ? newConfig.get(option) : null;
            log.info(
                    "  {} = {} (policy={})",
                    key,
                    ConfigDiff.redactIfNeeded(key, value),
                    diff.transitionPolicies().get(key));
        }
    }

    private record ListenerEntry(String prefix, ConfigChangeListener listener) {}
}
