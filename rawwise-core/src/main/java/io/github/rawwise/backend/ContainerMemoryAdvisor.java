package io.github.rawwise.backend;

import io.github.rawwise.common.SingletonContainer;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContainerMemoryAdvisor {
    private static final ContainerMemoryAdvisor INSTANCE =
            SingletonContainer.getInstance(ContainerMemoryAdvisor.class);

    private static final long DEFAULT_CACHE_TTL_MS = 1000L;

    private static final long PAGE_SIZE = resolvePageSize();

    private final AtomicReference<CompletableFuture<CacheEntry>> cache =
            new AtomicReference<>(null);
    private final long cacheTtlMs;

    private static long resolvePageSize() {
        try {
            return sun.misc.Unsafe.getUnsafe().pageSize();
        } catch (Exception e) {
            return 4096L;
        }
    }

    private ContainerMemoryAdvisor() {
        this(DEFAULT_CACHE_TTL_MS);
    }

    ContainerMemoryAdvisor(long cacheTtlMs) {
        this.cacheTtlMs = cacheTtlMs;
    }

    public static ContainerMemoryAdvisor getInstance() {
        return INSTANCE;
    }

    public long readMemoryLimit() {
        try {
            return readMemoryLimitInternal();
        } catch (IOException e) {
            log.warn("Failed to read memory limit: {}", e.getMessage());
            return -1;
        }
    }

    public long readMemoryUsage() {
        try {
            return readMemoryUsageWithRetry(3);
        } catch (IOException e) {
            log.warn("Failed to read memory usage after retries: {}", e.getMessage());
            return -1;
        }
    }

    public MemorySnapshot getMemorySnapshot() {
        long now = System.currentTimeMillis();

        CompletableFuture<CacheEntry> current = cache.get();
        if (current != null) {
            try {
                CacheEntry entry = current.get(3, TimeUnit.SECONDS);
                if (entry != null && now - entry.timestamp < cacheTtlMs) {
                    return new MemorySnapshot(entry.limitBytes, entry.usageBytes);
                }
            } catch (TimeoutException e) {
                log.warn("Timed out waiting for cached memory snapshot, will refresh");
            } catch (ExecutionException e) {
                log.warn(
                        "Previous snapshot computation failed, will refresh: {}",
                        e.getCause().getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new MemorySnapshot(-1, -1);
            }
        }

        CompletableFuture<CacheEntry> newFuture = new CompletableFuture<>();
        while (true) {
            current = cache.get();
            if (current != null) {
                try {
                    CacheEntry entry = current.get(3, TimeUnit.SECONDS);
                    if (entry != null && now - entry.timestamp < cacheTtlMs) {
                        return new MemorySnapshot(entry.limitBytes, entry.usageBytes);
                    }
                } catch (TimeoutException | ExecutionException e) {
                    // expired or failed, try to replace
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return new MemorySnapshot(-1, -1);
                }
            }

            if (cache.compareAndSet(current, newFuture)) {
                break;
            }
        }

        try {
            MemorySnapshot snapshot = computeSnapshotWithRetry(3);
            CacheEntry entry = new CacheEntry(snapshot.limitBytes(), snapshot.usageBytes(), now);
            newFuture.complete(entry);
            return snapshot;
        } catch (IOException e) {
            log.warn("Failed to compute memory snapshot: {}", e.getMessage());
            newFuture.completeExceptionally(e);
            cache.compareAndSet(newFuture, null);
            return new MemorySnapshot(-1, -1);
        }
    }

    private long readMemoryUsageWithRetry(int maxRetries) throws IOException {
        IOException lastEx = null;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return readMemoryUsageRaw();
            } catch (IOException e) {
                lastEx = e;
                if (i < maxRetries - 1) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw lastEx;
                    }
                }
            }
        }
        throw lastEx;
    }

    private MemorySnapshot computeSnapshotWithRetry(int maxRetries) throws IOException {
        IOException lastEx = null;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return computeSnapshot();
            } catch (IOException e) {
                lastEx = e;
                if (i < maxRetries - 1) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw lastEx;
                    }
                }
            }
        }
        throw lastEx;
    }

    private MemorySnapshot computeSnapshot() throws IOException {
        long limit = readMemoryLimitInternal();
        long usage = readMemoryUsageRaw();
        return new MemorySnapshot(limit, usage);
    }

    private long readMemoryLimitInternal() throws IOException {
        Path[] v1Paths = {
            Paths.get("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            Paths.get("/sys/fs/cgroup/memory.limit_in_bytes")
        };

        for (Path p : v1Paths) {
            if (Files.exists(p)) {
                try (BufferedReader reader = Files.newBufferedReader(p)) {
                    String line = reader.readLine();
                    if (line != null) {
                        long value = Long.parseLong(line.trim());
                        if (value > Long.MAX_VALUE / 2) return Long.MAX_VALUE;
                        return value;
                    }
                } catch (NumberFormatException e) {
                    log.error("Invalid memory limit format: {}", p);
                }
            }
        }

        Path v2Path = Paths.get("/sys/fs/cgroup/memory.max");
        if (Files.exists(v2Path)) {
            try (BufferedReader reader = Files.newBufferedReader(v2Path)) {
                String line = reader.readLine();
                if (line != null) {
                    line = line.trim();
                    if ("max".equals(line)) return Long.MAX_VALUE;
                    return Long.parseLong(line);
                }
            } catch (NumberFormatException e) {
                log.error("Invalid memory limit format: {}", v2Path);
            }
        }

        return -1;
    }

    private long readMemoryUsageRaw() throws IOException {
        Path v2Current = Paths.get("/sys/fs/cgroup/memory.current");
        if (Files.exists(v2Current)) {
            try (BufferedReader reader = Files.newBufferedReader(v2Current)) {
                String line = reader.readLine();
                if (line != null) return Long.parseLong(line.trim());
            }
        }
        Path v1Usage = Paths.get("/sys/fs/cgroup/memory/memory.usage_in_bytes");
        if (Files.exists(v1Usage)) {
            try (BufferedReader reader = Files.newBufferedReader(v1Usage)) {
                String line = reader.readLine();
                if (line != null) return Long.parseLong(line.trim());
            }
        }
        return readStatmRss();
    }

    private long readStatmRss() throws IOException {
        long pid = ProcessHandle.current().pid();
        Path statmPath = Paths.get("/proc/" + pid + "/statm");
        if (!Files.exists(statmPath)) {
            throw new IOException("No memory usage source available");
        }
        try (BufferedReader reader = Files.newBufferedReader(statmPath)) {
            String line = reader.readLine();
            if (line == null) throw new IOException("Empty statm file");
            String[] fields = line.trim().split("\\s+");
            long rssPages = Long.parseLong(fields[1]);
            return rssPages * PAGE_SIZE;
        }
    }

    public record MemorySnapshot(long limitBytes, long usageBytes) {}

    private record CacheEntry(long limitBytes, long usageBytes, long timestamp) {}
}
