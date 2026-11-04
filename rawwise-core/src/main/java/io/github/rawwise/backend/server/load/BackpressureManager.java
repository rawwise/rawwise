package io.github.rawwise.backend.server.load;

import com.google.common.util.concurrent.RateLimiter;
import io.github.rawwise.backend.server.job.JobCategory;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackpressureManager {
    private final LoadCollector loadCollector;

    private final double queueWeight;
    private final double cpuWeight;
    private final double memoryWeight;
    private final double diskWeight;
    private final double networkWeight;

    private final double warningThreshold;
    private final double throttleThreshold;
    private final double rejectThreshold;

    private final Map<JobCategory, Semaphore> admissionSemaphores;
    private final Map<JobCategory, Long> admissionTimeoutMs;
    private final double sparkActiveWeight;
    private final double sparkPendingWeight;
    private final double trainingDegradeThreshold;

    private final RateLimiter throttleRateLimiter;
    private final AtomicLong warningCount = new AtomicLong(0);
    private final AtomicLong throttleCount = new AtomicLong(0);
    private final AtomicLong rejectCount = new AtomicLong(0);

    public BackpressureManager(
            LoadCollector loadCollector,
            double warningThreshold,
            double throttleThreshold,
            double rejectThreshold) {
        this(
                loadCollector,
                warningThreshold,
                throttleThreshold,
                rejectThreshold,
                Map.of(
                        JobCategory.INTERACTIVE_QUERY, 10,
                        JobCategory.BATCH_INGESTION, 5,
                        JobCategory.TRAINING_SCAN, 20,
                        JobCategory.INFERENCE_REQUEST, 50),
                Map.of(
                        JobCategory.INTERACTIVE_QUERY, 5000L,
                        JobCategory.BATCH_INGESTION, 10000L,
                        JobCategory.TRAINING_SCAN, 30000L,
                        JobCategory.INFERENCE_REQUEST, 1000L),
                0.85,
                50);
    }

    public BackpressureManager(LoadCollector loadCollector) {
        this(loadCollector, 0.7, 0.9, 0.95);
    }

    public BackpressureManager(
            LoadCollector loadCollector,
            double warningThreshold,
            double throttleThreshold,
            double rejectThreshold,
            Map<JobCategory, Integer> permits,
            Map<JobCategory, Long> timeoutMs,
            double trainingDegradeThreshold) {
        this(
                loadCollector,
                warningThreshold,
                throttleThreshold,
                rejectThreshold,
                permits,
                timeoutMs,
                trainingDegradeThreshold,
                50);
    }

    public BackpressureManager(
            LoadCollector loadCollector,
            double warningThreshold,
            double throttleThreshold,
            double rejectThreshold,
            Map<JobCategory, Integer> permits,
            Map<JobCategory, Long> timeoutMs,
            double trainingDegradeThreshold,
            int throttlePermits) {
        this.loadCollector = loadCollector;

        this.queueWeight = 0.40;
        this.cpuWeight = 0.15;
        this.memoryWeight = 0.15;
        this.diskWeight = 0.1;
        this.networkWeight = 0.1;
        this.sparkActiveWeight = 0.05;
        this.sparkPendingWeight = 0.05;

        this.warningThreshold = warningThreshold;
        this.throttleThreshold = throttleThreshold;
        this.rejectThreshold = rejectThreshold;

        this.admissionSemaphores = new EnumMap<>(JobCategory.class);
        for (JobCategory category : JobCategory.values()) {
            int permitCount = permits.getOrDefault(category, 10);
            admissionSemaphores.put(category, new Semaphore(permitCount, true));
        }

        this.admissionTimeoutMs = new EnumMap<>(JobCategory.class);
        for (JobCategory category : JobCategory.values()) {
            admissionTimeoutMs.put(category, timeoutMs.getOrDefault(category, 5000L));
        }

        this.trainingDegradeThreshold = trainingDegradeThreshold;

        this.throttleRateLimiter = RateLimiter.create(throttlePermits);
    }

    public BackendLoadInfo getCurrentLoad() {
        return loadCollector.collect();
    }

    public boolean shouldReject() {
        BackendLoadInfo load = getCurrentLoad();
        double loadScore = calculateLoadScore(load);
        return loadScore >= rejectThreshold;
    }

    public boolean shouldThrottle() {
        BackendLoadInfo load = getCurrentLoad();
        double loadScore = calculateLoadScore(load);
        return loadScore >= throttleThreshold;
    }

    public boolean shouldWarn() {
        BackendLoadInfo load = getCurrentLoad();
        double loadScore = calculateLoadScore(load);
        return loadScore >= warningThreshold && loadScore < throttleThreshold;
    }

    public Permit tryAcquire(JobCategory category) {
        Semaphore semaphore = admissionSemaphores.get(category);
        Long timeout = admissionTimeoutMs.get(category);
        if (semaphore == null || timeout == null) {
            return null;
        }

        BackendLoadInfo currentLoad = getCurrentLoad();
        double loadScore = calculateLoadScore(currentLoad);
        BackpressureStatus status = determineStatus(loadScore);

        if (status == BackpressureStatus.REJECTING) {
            rejectCount.incrementAndGet();
            return null;
        }

        if (status == BackpressureStatus.WARNING) {
            warningCount.incrementAndGet();
        }

        if (status == BackpressureStatus.THROTTLING) {
            throttleCount.incrementAndGet();
            if (!throttleRateLimiter.tryAcquire()) {
                return null;
            }
        }

        if (category == JobCategory.TRAINING_SCAN) {
            if (loadScore >= trainingDegradeThreshold) {
                log.warn(
                        "Training workload under high load: loadScore={} >= threshold={}",
                        loadScore,
                        trainingDegradeThreshold);
            }
        }

        try {
            if (semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS)) {
                return new Permit(this, category);
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    public boolean tryAcquireBoolean(JobCategory category) {
        return tryAcquire(category) != null;
    }

    public void release(JobCategory category) {
        Semaphore semaphore = admissionSemaphores.get(category);
        if (semaphore != null) {
            semaphore.release();
        }
    }

    public record Permit(BackpressureManager manager, JobCategory category)
            implements AutoCloseable {
        @Override
        public void close() {
            manager.release(category);
        }
    }

    public BackpressureStatus getStatus() {
        BackendLoadInfo load = getCurrentLoad();
        double loadScore = calculateLoadScore(load);
        return determineStatus(loadScore);
    }

    private BackpressureStatus determineStatus(double loadScore) {
        boolean semaphoresExhausted =
                admissionSemaphores.values().stream().allMatch(s -> s.availablePermits() == 0);

        if (loadScore < warningThreshold && !semaphoresExhausted) {
            return BackpressureStatus.NORMAL;
        } else if (loadScore < warningThreshold && semaphoresExhausted) {
            return BackpressureStatus.THROTTLING;
        } else if (loadScore < throttleThreshold) {
            return BackpressureStatus.WARNING;
        } else if (loadScore < rejectThreshold) {
            return BackpressureStatus.THROTTLING;
        } else {
            return BackpressureStatus.REJECTING;
        }
    }

    public long getWarningCount() {
        return warningCount.get();
    }

    public long getThrottleCount() {
        return throttleCount.get();
    }

    public long getRejectCount() {
        return rejectCount.get();
    }

    private double calculateLoadScore(BackendLoadInfo load) {
        if (load == null) {
            return 0.0;
        }
        return load.calculateLoadScore(
                queueWeight,
                cpuWeight,
                memoryWeight,
                diskWeight,
                networkWeight,
                sparkActiveWeight,
                sparkPendingWeight);
    }

    public long retryAfterSeconds() {
        BackpressureManager.BackpressureStatus status = getStatus();
        return switch (status) {
            case REJECTING -> 30L;
            case THROTTLING -> 10L;
            case WARNING -> 5L;
            case NORMAL -> 0L;
        };
    }

    public enum BackpressureStatus {
        NORMAL,
        WARNING,
        THROTTLING,
        REJECTING
    }
}
