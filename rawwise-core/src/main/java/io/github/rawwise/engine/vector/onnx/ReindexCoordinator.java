package io.github.rawwise.engine.vector.onnx;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReindexCoordinator {

    private static final String KEY_PREFIX = "reindex:job:";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Metastore metastore;
    private final ConcurrentMap<String, ReindexJob> inMemoryIndex = new ConcurrentHashMap<>();
    private volatile ReindexExecutor defaultExecutor;

    public ReindexCoordinator(Metastore metastore) {
        this.metastore = metastore;
    }

    public void registerDefaultExecutor(ReindexExecutor executor) {
        this.defaultExecutor = executor;
        log.info(
                "ReindexCoordinator: default executor registered ({})",
                executor == null ? "<null>" : executor.getClass().getSimpleName());
    }

    public ReindexJob.Result executeReindex(String jobId) {
        if (defaultExecutor == null) {
            throw new IllegalStateException(
                    "No default executor registered for jobId="
                            + jobId
                            + " — call executeReindex(jobId, executor) or registerDefaultExecutor() first");
        }
        return executeReindex(jobId, defaultExecutor);
    }

    public String submitReindex(String srcTable, String destTable, int newDimension) {
        return submitReindex(srcTable, destTable, 0, newDimension);
    }

    public String submitReindex(
            String srcTable, String destTable, int srcDimension, int destDimension) {
        String jobId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        ReindexJob job =
                new ReindexJob(
                        jobId,
                        srcTable,
                        destTable,
                        srcDimension,
                        destDimension,
                        ReindexJob.Status.PENDING,
                        now,
                        0L);
        persist(job);
        inMemoryIndex.put(jobId, job);
        log.info(
                "ReindexCoordinator.submitReindex: jobId={} src={} dest={} dim {}->{}",
                jobId,
                srcTable,
                destTable,
                srcDimension,
                destDimension);
        return jobId;
    }

    public ReindexJob.Result executeReindex(String jobId, ReindexExecutor executor) {
        ReindexJob job = inMemoryIndex.get(jobId);
        if (job == null) {
            throw new IllegalArgumentException("Reindex job not found: " + jobId);
        }
        if (job.status() != ReindexJob.Status.PENDING) {
            throw new IllegalStateException(
                    "Reindex job already started: " + jobId + " status=" + job.status());
        }
        updateStatus(jobId, ReindexJob.Status.RUNNING);
        ReindexContext context = new ReindexContext(job, this::updateProgress);
        try {
            executor.execute(context);
            updateStatus(jobId, ReindexJob.Status.COMPLETED);
            log.info(
                    "ReindexCoordinator.executeReindex completed: jobId={} processedRows={}/{}",
                    jobId,
                    context.processedRows(),
                    context.totalRows());
            return new ReindexJob.Result(jobId, true, context.processedRows(), null);
        } catch (Exception e) {
            ReindexJob failed = inMemoryIndex.get(jobId);
            if (failed != null) {
                ReindexJob withError = failed.withError(e.getMessage());
                inMemoryIndex.put(jobId, withError);
                persist(withError);
            }
            updateStatus(jobId, ReindexJob.Status.FAILED);
            log.warn("ReindexCoordinator.executeReindex failed: jobId={}", jobId, e);
            return new ReindexJob.Result(jobId, false, context.processedRows(), e.getMessage());
        }
    }

    public Optional<ReindexJob> getJobStatus(String jobId) {
        ReindexJob cached = inMemoryIndex.get(jobId);
        if (cached != null) {
            return Optional.of(cached);
        }
        try {
            Optional<byte[]> raw = metastore.get(key(jobId));
            if (raw.isEmpty()) return Optional.empty();
            ReindexJob job = MAPPER.readValue(raw.get(), ReindexJob.class);
            inMemoryIndex.put(jobId, job);
            return Optional.of(job);
        } catch (IOException e) {
            log.warn("ReindexCoordinator.getJobStatus failed for jobId={}", jobId, e);
            return Optional.empty();
        }
    }

    public List<ReindexJob> listJobs() {
        List<ReindexJob> jobs = new ArrayList<>(inMemoryIndex.values());
        try {
            List<KeyValue> all = metastore.scanAll(KEY_PREFIX.getBytes(StandardCharsets.UTF_8));
            for (KeyValue kv : all) {
                String jobId =
                        new String(kv.key(), StandardCharsets.UTF_8).substring(KEY_PREFIX.length());
                if (!inMemoryIndex.containsKey(jobId)) {
                    try {
                        ReindexJob job = MAPPER.readValue(kv.value(), ReindexJob.class);
                        inMemoryIndex.put(jobId, job);
                        jobs.add(job);
                    } catch (IOException e) {
                        log.warn(
                                "ReindexCoordinator.listJobs: failed to deserialize jobId={}",
                                jobId,
                                e);
                    }
                }
            }
        } catch (IOException e) {
            log.warn("ReindexCoordinator.listJobs: failed to scan metastore", e);
        }
        return jobs;
    }

    public void updateStatus(String jobId, ReindexJob.Status status) {
        ReindexJob existing = inMemoryIndex.get(jobId);
        if (existing == null) {
            log.warn("ReindexCoordinator.updateStatus: jobId={} not found", jobId);
            return;
        }
        long completedAt =
                (status == ReindexJob.Status.COMPLETED || status == ReindexJob.Status.FAILED)
                        ? System.currentTimeMillis()
                        : existing.completedAtMs();
        ReindexJob updated = existing.withStatus(status, completedAt);
        inMemoryIndex.put(jobId, updated);
        persist(updated);
    }

    public void updateProgress(String jobId, long processedRows, long totalRows) {
        ReindexJob existing = inMemoryIndex.get(jobId);
        if (existing == null) {
            log.warn("ReindexCoordinator.updateProgress: jobId={} not found", jobId);
            return;
        }
        ReindexJob updated = existing.withProgress(processedRows, totalRows);
        inMemoryIndex.put(jobId, updated);
        persist(updated);
    }

    private void persist(ReindexJob job) {
        try {
            metastore.put(key(job.jobId()), MAPPER.writeValueAsBytes(job));
        } catch (IOException e) {
            log.warn("ReindexCoordinator.persist failed for jobId={}", job.jobId(), e);
        }
    }

    private static byte[] key(String jobId) {
        return (KEY_PREFIX + jobId).getBytes(StandardCharsets.UTF_8);
    }

    @FunctionalInterface
    public interface ReindexExecutor {
        void execute(ReindexContext context) throws Exception;
    }

    public static final class ReindexContext {
        private final ReindexJob job;
        private final ProgressReporter progressReporter;
        private long processedRows;
        private long totalRows;

        ReindexContext(ReindexJob job, ProgressReporter progressReporter) {
            this.job = job;
            this.progressReporter = progressReporter;
        }

        public String jobId() {
            return job.jobId();
        }

        public String srcTableId() {
            return job.srcTableId();
        }

        public String destTableId() {
            return job.destTableId();
        }

        public int srcDimension() {
            return job.srcDimension();
        }

        public int destDimension() {
            return job.destDimension();
        }

        public long processedRows() {
            return processedRows;
        }

        public long totalRows() {
            return totalRows;
        }

        public void setTotalRows(long totalRows) {
            this.totalRows = totalRows;
            reportProgress();
        }

        public void incrementProcessed(long delta) {
            this.processedRows += delta;
            reportProgress();
        }

        public void reportProgress() {
            progressReporter.report(job.jobId(), processedRows, totalRows);
        }
    }

    @FunctionalInterface
    interface ProgressReporter {
        void report(String jobId, long processedRows, long totalRows);
    }
}
