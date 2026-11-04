package io.github.rawwise.axiom.orchestrator;

import io.github.rawwise.governance.context.ContextJson;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoordGcService implements AutoCloseable {
    private final Metastore metastore;
    private final long gcIntervalMs;
    private final long messageTtlMs;
    private volatile boolean running = true;
    private final Thread gcThread;
    private final AtomicLong gcFailureCount = new AtomicLong();

    public CoordGcService(Metastore metastore, long gcIntervalMs, long messageTtlMs) {
        this.metastore = metastore;
        this.gcIntervalMs = gcIntervalMs;
        this.messageTtlMs = messageTtlMs;
        this.gcThread = Thread.ofPlatform().daemon(true).name("coord-gc").start(this::runGc);
    }

    private void runGc() {
        while (running) {
            try {
                Thread.sleep(gcIntervalMs);
                if (!running) break;
                doGc();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void doGc() {
        try {
            long now = System.currentTimeMillis();
            gcExpiredLocks(now);
            gcExpiredLeases(now);
            gcExpiredElections(now);
            gcTriggeredBarriers(now);
            gcExpiredMessages(now);
        } catch (Exception e) {
            log.warn("Coord GC cycle failed", e);
        }
    }

    private void gcExpiredLocks(long now) throws IOException {
        byte[] prefix = "coord::lock::".getBytes(StandardCharsets.UTF_8);
        List<byte[]> toCheck = new ArrayList<>();
        for (KeyValue kv : metastore.scanAll(prefix)) {
            try {
                LockHandle lock = ContextJson.readValue(kv.value(), LockHandle.class);
                if (lock.expiresAt() <= now) toCheck.add(kv.key());
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
        for (byte[] key : toCheck) {
            try {
                Optional<byte[]> current = metastore.get(key);
                if (current.isPresent()) {
                    LockHandle lock = ContextJson.readValue(current.get(), LockHandle.class);
                    if (lock.expiresAt() <= now) {
                        metastore.compareAndSwap(
                                List.of(new Precondition.ValueEquals(key, current.get())),
                                List.of(new Mutation.Delete(key)));
                    }
                }
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
    }

    private void gcExpiredLeases(long now) throws IOException {
        byte[] prefix = "coord::lease::".getBytes(StandardCharsets.UTF_8);
        List<byte[]> toCheck = new ArrayList<>();
        for (KeyValue kv : metastore.scanAll(prefix)) {
            try {
                LeaseHandle lease = ContextJson.readValue(kv.value(), LeaseHandle.class);
                if (lease.expiresAt() <= now) toCheck.add(kv.key());
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
        for (byte[] key : toCheck) {
            try {
                Optional<byte[]> current = metastore.get(key);
                if (current.isPresent()) {
                    LeaseHandle lease = ContextJson.readValue(current.get(), LeaseHandle.class);
                    if (lease.expiresAt() <= now) {
                        byte[] agentIdxKey =
                                ("coord::lease-idx::" + lease.agentId() + "::" + lease.leaseId())
                                        .getBytes(StandardCharsets.UTF_8);
                        metastore.compareAndSwap(
                                List.of(new Precondition.ValueEquals(key, current.get())),
                                List.of(
                                        new Mutation.Delete(key),
                                        new Mutation.Delete(agentIdxKey)));
                    }
                }
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
    }

    private void gcExpiredElections(long now) throws IOException {
        byte[] prefix = "coord::election::".getBytes(StandardCharsets.UTF_8);
        List<byte[]> toCheck = new ArrayList<>();
        for (KeyValue kv : metastore.scanAll(prefix)) {
            try {
                ElectionHandle election = ContextJson.readValue(kv.value(), ElectionHandle.class);
                if (election.expiresAt() > 0 && election.expiresAt() <= now) toCheck.add(kv.key());
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
        for (byte[] key : toCheck) {
            try {
                Optional<byte[]> current = metastore.get(key);
                if (current.isPresent()) {
                    ElectionHandle election =
                            ContextJson.readValue(current.get(), ElectionHandle.class);
                    if (election.expiresAt() > 0 && election.expiresAt() <= now) {
                        byte[] agentIdxKey =
                                ("coord::election-idx::"
                                                + election.leaderAgentId()
                                                + "::"
                                                + election.electionId())
                                        .getBytes(StandardCharsets.UTF_8);
                        metastore.compareAndSwap(
                                List.of(new Precondition.ValueEquals(key, current.get())),
                                List.of(
                                        new Mutation.Delete(key),
                                        new Mutation.Delete(agentIdxKey)));
                    }
                }
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
    }

    private void gcTriggeredBarriers(long now) throws IOException {
        byte[] prefix = "coord::barrier::".getBytes(StandardCharsets.UTF_8);
        List<byte[]> toCheck = new ArrayList<>();
        long oneHourAgo = now - 3_600_000;
        for (KeyValue kv : metastore.scanAll(prefix)) {
            try {
                BarrierHandle barrier = ContextJson.readValue(kv.value(), BarrierHandle.class);
                if ((barrier.triggered() && barrier.createdAt() < oneHourAgo)
                        || (barrier.expiresAt() > 0 && barrier.expiresAt() <= now)) {
                    toCheck.add(kv.key());
                }
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
        for (byte[] key : toCheck) {
            try {
                Optional<byte[]> current = metastore.get(key);
                if (current.isPresent()) {
                    BarrierHandle barrier =
                            ContextJson.readValue(current.get(), BarrierHandle.class);
                    boolean shouldDelete =
                            (barrier.triggered() && barrier.createdAt() < oneHourAgo)
                                    || (barrier.expiresAt() > 0 && barrier.expiresAt() <= now);
                    if (shouldDelete) {
                        metastore.compareAndSwap(
                                List.of(new Precondition.ValueEquals(key, current.get())),
                                List.of(new Mutation.Delete(key)));
                    }
                }
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
    }

    private void gcExpiredMessages(long now) throws IOException {
        byte[] prefix = "coord::pubsub-msg::".getBytes(StandardCharsets.UTF_8);
        long cutoff = now - messageTtlMs;
        List<byte[]> toCheck = new ArrayList<>();
        for (KeyValue kv : metastore.scanAll(prefix)) {
            try {
                String keyStr = new String(kv.key(), StandardCharsets.UTF_8);
                String[] parts = keyStr.split("::");
                if (parts.length >= 4) {
                    long timestamp = Long.parseLong(parts[3]);
                    if (timestamp < cutoff) toCheck.add(kv.key());
                }
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
        for (byte[] key : toCheck) {
            try {
                Optional<byte[]> current = metastore.get(key);
                if (current.isPresent()) {
                    String keyStr = new String(key, StandardCharsets.UTF_8);
                    String[] parts = keyStr.split("::");
                    if (parts.length >= 4) {
                        long timestamp = Long.parseLong(parts[3]);
                        if (timestamp < cutoff) {
                            metastore.compareAndSwap(
                                    List.of(new Precondition.ValueEquals(key, current.get())),
                                    List.of(new Mutation.Delete(key)));
                        }
                    }
                }
            } catch (Exception e) {
                long failures = gcFailureCount.incrementAndGet();
                if (failures <= 3 || failures % 10 == 0) {
                    log.warn("Coord GC item failed, totalFailures={}", failures, e);
                }
            }
        }
    }

    @Override
    public void close() {
        running = false;
        gcThread.interrupt();
    }
}
