package io.github.rawwise.backend.server.job;

import io.github.rawwise.frontend.ir.QueryIR;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface JobScheduler {

    void start();

    String submitJob(String sessionId, String sql, JobType type, Map<String, String> options);

    /**
     * Submit a QueryIR for execution. Deserialized IR goes through IRAdaptor-&gt;LogicalPlan and is
     * executed directly, bypassing SQL parsing entirely (TD7: LOGICAL_PLAN no longer falls back to
     * submitQuery).
     */
    String submitIR(QueryIR ir, long timeoutMs, String tenantId);

    Optional<ManagedJob> getJob(String jobId);

    JobStatus getJobStatus(String jobId);

    boolean cancelJob(String jobId);

    JobResultHandle getJobResult(String jobId);

    Set<String> getEverSubmittedJobIds();

    String getJobTenantId(String jobId);

    List<ManagedJob> getActiveJobs();

    int getQueueLength();

    JobEventLog getEventLog();

    void shutdown();
}
