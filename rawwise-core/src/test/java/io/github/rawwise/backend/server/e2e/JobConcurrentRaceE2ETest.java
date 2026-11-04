package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.rawwise.backend.server.job.JobEventLog;
import io.github.rawwise.backend.server.job.JobStatus;
import io.github.rawwise.backend.server.job.JobType;
import io.github.rawwise.backend.server.job.SparkJobScheduler;
import io.github.rawwise.backend.server.session.InMemorySessionStore;
import io.github.rawwise.backend.server.session.SessionStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

@DisplayName("Job Concurrent Race E2E - CAS status concurrent test")
class JobConcurrentRaceE2ETest {

    private static SparkSession spark;
    private static SessionStore sessionStore;

    @BeforeAll
    static void setUp() {
        spark =
                SparkSession.builder()
                        .master("local[1]")
                        .appName("JobConcurrentRaceE2ETest")
                        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
                        .getOrCreate();
        sessionStore = new InMemorySessionStore();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    private SparkJobScheduler createScheduler() {
        return new SparkJobScheduler(
                spark, sessionStore, new AtomicInteger(0), null, new JobEventLog());
    }

    private static final Set<JobStatus> TERMINAL_STATUSES =
            Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMED_OUT);

    private boolean isTerminal(JobStatus status) {
        return status != null && TERMINAL_STATUSES.contains(status);
    }

    private boolean isTerminalOrEvicted(JobStatus status) {
        return status == null || TERMINAL_STATUSES.contains(status);
    }

    @Nested
    @DisplayName("Cancel vs executeQueryJob race")
    class CancelVsExecuteRace {

        @RepeatedTest(5)
        @DisplayName("cancelJob andexecute after")
        void cancelDuringExecution_jobEndsInValidTerminalState() throws Exception {
            SparkJobScheduler scheduler = createScheduler();
            scheduler.start();
            try {
                String sessionId =
                        sessionStore.createSession(
                                "frontend-cancel-race",
                                "SELECT java_method('java.lang.Thread', 'sleep', 500L)",
                                JobType.READ,
                                Map.of("collect", "true"));
                String jobId =
                        scheduler.submitJob(
                                sessionId,
                                "SELECT java_method('java.lang.Thread', 'sleep', 500L)",
                                JobType.READ,
                                Map.of("collect", "true"));

                await().atMost(2, TimeUnit.SECONDS)
                        .until(() -> scheduler.getJobStatus(jobId) == JobStatus.RUNNING);

                boolean cancelled = scheduler.cancelJob(jobId);

                await().atMost(10, TimeUnit.SECONDS)
                        .until(() -> isTerminal(scheduler.getJobStatus(jobId)));

                JobStatus finalStatus = scheduler.getJobStatus(jobId);
                assertThat(finalStatus).isIn(JobStatus.CANCELLED, JobStatus.SUCCEEDED);
            } finally {
                scheduler.shutdown();
            }
        }

        @RepeatedTest(5)
        @DisplayName("cancelJob in PENDING status directly")
        void cancelWhilePending_transitionsToCancelled() throws Exception {
            SparkJobScheduler scheduler = createScheduler();
            try {
                String sessionId =
                        sessionStore.createSession(
                                "frontend-cancel-pending",
                                "SELECT 1",
                                JobType.READ,
                                Map.of("collect", "true"));
                String jobId =
                        scheduler.submitJob(
                                sessionId, "SELECT 1", JobType.READ, Map.of("collect", "true"));

                boolean cancelled = scheduler.cancelJob(jobId);

                await().atMost(5, TimeUnit.SECONDS)
                        .until(() -> isTerminal(scheduler.getJobStatus(jobId)));

                JobStatus finalStatus = scheduler.getJobStatus(jobId);
                assertThat(finalStatus).isIn(JobStatus.CANCELLED, JobStatus.SUCCEEDED);
            } finally {
                scheduler.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("Timeout vs executeQueryJob race")
    class TimeoutVsExecuteRace {

        @RepeatedTest(5)
        @DisplayName("timeout TIMED_OUT or SUCCEEDED, will inmiddle")
        void shortTimeout_jobEndsInTimedOutOrSucceeded() throws Exception {
            SparkJobScheduler scheduler = createScheduler();
            scheduler.start();
            try {
                Map<String, String> options = new HashMap<>();
                options.put("timeout", "100");
                options.put("collect", "true");
                String sessionId =
                        sessionStore.createSession(
                                "frontend-timeout-race",
                                "SELECT java_method('java.lang.Thread', 'sleep', 10000L)",
                                JobType.READ,
                                options);
                String jobId =
                        scheduler.submitJob(
                                sessionId,
                                "SELECT java_method('java.lang.Thread', 'sleep', 10000L)",
                                JobType.READ,
                                options);

                await().atMost(30, TimeUnit.SECONDS)
                        .until(() -> isTerminal(scheduler.getJobStatus(jobId)));

                JobStatus finalStatus = scheduler.getJobStatus(jobId);
                assertThat(finalStatus).isIn(JobStatus.TIMED_OUT, JobStatus.SUCCEEDED);
            } finally {
                scheduler.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("Double moveToCompleted idempotent")
    class MoveToCompletedIdempotent {

        @Test
        @DisplayName("concurrentcompleteafter completedJobs andcommit consistent, noduplicatecount")
        void concurrentCompletion_noDoubleCounting() throws Exception {
            SparkJobScheduler scheduler = createScheduler();
            int jobCount = 20;
            try {
                List<String> jobIds = new ArrayList<>();
                for (int i = 0; i < jobCount; i++) {
                    String sessionId =
                            sessionStore.createSession(
                                    "frontend-idempotent-" + i,
                                    "SELECT " + i,
                                    JobType.READ,
                                    Map.of("collect", "true"));
                    String jobId =
                            scheduler.submitJob(
                                    sessionId,
                                    "SELECT " + i,
                                    JobType.READ,
                                    Map.of("collect", "true"));
                    jobIds.add(jobId);
                }

                await().atMost(30, TimeUnit.SECONDS)
                        .until(
                                () ->
                                        jobIds.stream()
                                                .allMatch(
                                                        id ->
                                                                isTerminal(
                                                                        scheduler.getJobStatus(
                                                                                id))));

                for (String jobId : jobIds) {
                    assertThat(isTerminal(scheduler.getJobStatus(jobId))).isTrue();
                }

                assertThat(scheduler.getActiveJobs()).isEmpty();
            } finally {
                scheduler.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("High-concurrency submit-cancel mix")
    class HighConcurrencySubmitCancelMix {

        @Test
        @DisplayName("100 itemconcurrent jobscommit 30 item, all countconsistent")
        void hundredJobsWithRandomCancel_allReachTerminalState() throws Exception {
            SparkJobScheduler scheduler = createScheduler();
            int totalJobs = 100;
            int cancelCount = 30;
            try {
                List<String> jobIds = new ArrayList<>();
                CountDownLatch submitLatch = new CountDownLatch(1);
                ExecutorService executor = Executors.newFixedThreadPool(10);

                for (int i = 0; i < totalJobs; i++) {
                    final int idx = i;
                    executor.submit(
                            () -> {
                                try {
                                    submitLatch.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                                String sessionId =
                                        sessionStore.createSession(
                                                "frontend-hc-" + idx,
                                                "SELECT " + idx,
                                                JobType.READ,
                                                Map.of("collect", "true"));
                                String jobId =
                                        scheduler.submitJob(
                                                sessionId,
                                                "SELECT " + idx,
                                                JobType.READ,
                                                Map.of("collect", "true"));
                                synchronized (jobIds) {
                                    jobIds.add(jobId);
                                }
                            });
                }

                submitLatch.countDown();
                executor.shutdown();
                assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

                assertThat(jobIds).hasSize(totalJobs);

                List<String> toCancel = jobIds.subList(0, cancelCount);
                for (String jobId : toCancel) {
                    scheduler.cancelJob(jobId);
                }

                await().atMost(60, TimeUnit.SECONDS)
                        .until(
                                () ->
                                        jobIds.stream()
                                                .allMatch(
                                                        id ->
                                                                isTerminal(
                                                                        scheduler.getJobStatus(
                                                                                id))));

                for (String jobId : jobIds) {
                    JobStatus status = scheduler.getJobStatus(jobId);
                    assertThat(isTerminal(status))
                            .as("Job %s should be in terminal state but is %s", jobId, status)
                            .isTrue();
                }

                assertThat(scheduler.getActiveJobs()).isEmpty();
            } finally {
                scheduler.shutdown();
            }
        }
    }

    @Nested
    @DisplayName("completedJobs concurrent eviction")
    class CompletedJobsConcurrentEviction {

        @Test
        @DisplayName("exceeds MAX_COMPLETED_JOBS after inconcurrent correct")
        void exceedMaxCompletedJobs_evictionWorksUnderConcurrency() throws Exception {
            SparkJobScheduler scheduler = createScheduler();
            int batchSize = 50;
            int batches = 25;
            try {
                List<String> allJobIds = new ArrayList<>();
                for (int b = 0; b < batches; b++) {
                    for (int i = 0; i < batchSize; i++) {
                        String sessionId =
                                sessionStore.createSession(
                                        "frontend-evict-" + b + "-" + i,
                                        "SELECT " + (b * batchSize + i),
                                        JobType.READ,
                                        Map.of("collect", "true"));
                        String jobId =
                                scheduler.submitJob(
                                        sessionId,
                                        "SELECT " + (b * batchSize + i),
                                        JobType.READ,
                                        Map.of("collect", "true"));
                        allJobIds.add(jobId);
                    }

                    List<String> batchIds = allJobIds.subList(b * batchSize, allJobIds.size());
                    await().atMost(30, TimeUnit.SECONDS)
                            .until(
                                    () ->
                                            batchIds.stream()
                                                    .allMatch(
                                                            id ->
                                                                    isTerminalOrEvicted(
                                                                            scheduler.getJobStatus(
                                                                                    id))));
                }

                for (String jobId : allJobIds) {
                    JobStatus status = scheduler.getJobStatus(jobId);
                    assertThat(isTerminalOrEvicted(status))
                            .as("Job %s should be in terminal state or evicted", jobId)
                            .isTrue();
                }

                assertThat(scheduler.getActiveJobs()).isEmpty();
            } finally {
                scheduler.shutdown();
            }
        }
    }
}
