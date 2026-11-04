package io.github.rawwise.governance.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ConfigFileWatcher {

    private static final String K8S_DATA_SYMLINK = "..data";
    private static final String KUBERNETES_SERVICE_HOST_ENV = "KUBERNETES_SERVICE_HOST";
    private static final long MAX_CONFIG_FILE_SIZE = 10 * 1024 * 1024;
    private static final long FILE_MISSING_THRESHOLD_MS = 30_000;

    private record FileState(long lastModified, long lastSize, String lastHash) {}

    private final long pollIntervalMs;
    private final long debounceMs;

    private volatile boolean running;
    private volatile ScheduledExecutorService scheduler;
    private volatile Path watchedPath;

    private final AtomicReference<FileState> fileState =
            new AtomicReference<>(new FileState(0, 0, ""));
    private volatile String lastK8sSymlinkTarget;
    private volatile long fileMissingSinceMs = 0;

    private final AtomicLong lastChangeDetectedMs = new AtomicLong(0);
    private final AtomicLong lastNotifiedMs = new AtomicLong(0);

    ConfigFileWatcher(long pollIntervalMs, long debounceMs) {
        this.pollIntervalMs = pollIntervalMs;
        this.debounceMs = debounceMs;
    }

    synchronized void start(Path path, Consumer<String> onChangeCallback) {
        if (running) {
            log.warn("ConfigFileWatcher already running");
            return;
        }

        this.watchedPath = path;
        initializeState(path);
        detectK8sSubPath(path);

        running = true;
        scheduler =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "config-file-watcher");
                            t.setDaemon(true);
                            return t;
                        });

        scheduler.scheduleAtFixedRate(
                () -> poll(path, onChangeCallback),
                pollIntervalMs,
                pollIntervalMs,
                TimeUnit.MILLISECONDS);

        log.info(
                "ConfigFileWatcher started: path={}, pollInterval={}ms, debounce={}ms",
                path,
                pollIntervalMs,
                debounceMs);
    }

    synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        log.info("ConfigFileWatcher stopped");
    }

    boolean isRunning() {
        return running;
    }

    private void initializeState(Path path) {
        try {
            if (Files.exists(path)) {
                long mtime = Files.getLastModifiedTime(path).toMillis();
                long size = Files.size(path);
                String hash = computeFileHash(path);
                fileState.set(new FileState(mtime, size, hash));
            }
            Path k8sDataDir = resolveK8sDataDir(path);
            if (k8sDataDir != null && Files.exists(k8sDataDir)) {
                lastK8sSymlinkTarget = readSymlinkTarget(k8sDataDir);
            }
        } catch (IOException e) {
            log.warn("Failed to initialize file watcher state for {}", path, e);
        }
    }

    private void detectK8sSubPath(Path path) {
        if (System.getenv(KUBERNETES_SERVICE_HOST_ENV) == null) {
            return;
        }
        Path k8sDataDir = resolveK8sDataDir(path);
        if (k8sDataDir == null) {
            return;
        }
        if (!Files.exists(k8sDataDir)) {
            return;
        }
        try {
            Path resolvedFile = k8sDataDir.resolve(path.getFileName());
            if (Files.exists(resolvedFile) && !Files.isSymbolicLink(path)) {
                boolean strictMode =
                        Boolean.parseBoolean(
                                System.getProperty("rawwise.config.strict-mode", "false"));
                if (strictMode) {
                    throw new ConfigException.ValidationError(
                            "Config file '"
                                    + path
                                    + "' is a K8s subPath mount. "
                                    + "Hot reload will NOT work. Use directory-level mount instead.");
                }
                log.warn(
                        "Config file '{}' appears to be a K8s subPath mount. "
                                + "Config updates will NOT be detected. Use directory-level mount instead.",
                        path);
            }
        } catch (SecurityException e) {
            log.debug("K8s subPath detection failed for {}", path, e);
        }
    }

    private void poll(Path path, Consumer<String> onChangeCallback) {
        if (!running) {
            return;
        }
        try {
            if (!Files.exists(path)) {
                handleFileMissing(path);
                return;
            }

            fileMissingSinceMs = 0;

            String currentHash = null;
            boolean k8sChanged = checkK8sSymlinkChange(path);
            boolean fileChanged = false;

            FileState current = fileState.get();
            long currentModified = Files.getLastModifiedTime(path).toMillis();
            long currentSize = Files.size(path);

            if (currentModified != current.lastModified() || currentSize != current.lastSize()) {
                currentHash = computeFileHash(path);
                if (!currentHash.equals(current.lastHash())) {
                    fileChanged = true;
                } else {
                    fileState.set(new FileState(currentModified, currentSize, current.lastHash()));
                }
            }

            if (k8sChanged || fileChanged) {
                lastChangeDetectedMs.set(System.currentTimeMillis());
            }

            long now = System.currentTimeMillis();
            long lastChange = lastChangeDetectedMs.get();
            long lastNotified = lastNotifiedMs.get();

            if (lastChange > 0 && (now - lastChange) >= debounceMs && lastChange > lastNotified) {
                lastNotifiedMs.set(now);
                if (currentHash == null) {
                    currentHash = computeFileHash(path);
                }
                refreshState(path, currentHash);
                log.info("Config file change detected: {}", path);
                onChangeCallback.accept(currentHash);
            }
        } catch (Exception e) {
            log.error("Error during config file poll", e);
        }
    }

    private void handleFileMissing(Path path) {
        long now = System.currentTimeMillis();
        if (fileMissingSinceMs == 0) {
            fileMissingSinceMs = now;
            log.warn("Config file disappeared: {}", path);
        } else if (now - fileMissingSinceMs > FILE_MISSING_THRESHOLD_MS) {
            log.error("Config file missing for {}ms: {}", now - fileMissingSinceMs, path);
        }
    }

    private boolean checkK8sSymlinkChange(Path path) {
        Path k8sDataDir = resolveK8sDataDir(path);
        if (k8sDataDir == null || !Files.exists(k8sDataDir)) {
            return false;
        }
        String currentTarget = readSymlinkTarget(k8sDataDir);
        if (currentTarget != null && !currentTarget.equals(lastK8sSymlinkTarget)) {
            lastK8sSymlinkTarget = currentTarget;
            return true;
        }
        return false;
    }

    private void refreshState(Path path, String hash) throws IOException {
        long mtime = Files.getLastModifiedTime(path).toMillis();
        long size = Files.size(path);
        fileState.set(new FileState(mtime, size, hash));
    }

    private Path resolveK8sDataDir(Path configPath) {
        Path parent = configPath.getParent();
        if (parent == null) {
            return null;
        }
        Path dataDir = parent.resolve(K8S_DATA_SYMLINK);
        return Files.exists(dataDir) ? dataDir : null;
    }

    private String readSymlinkTarget(Path symlinkPath) {
        try {
            if (Files.isSymbolicLink(symlinkPath)) {
                return Files.readSymbolicLink(symlinkPath).toString();
            }
        } catch (IOException e) {
            log.debug("Failed to read symlink target for {}", symlinkPath, e);
        }
        return null;
    }

    static String computeFileHash(Path path) throws IOException {
        long fileSize = Files.size(path);
        if (fileSize > MAX_CONFIG_FILE_SIZE) {
            throw new IOException(
                    "Config file too large for hash computation: " + fileSize + " bytes");
        }
        try (InputStream is = Files.newInputStream(path)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int read;
            while ((read = is.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
