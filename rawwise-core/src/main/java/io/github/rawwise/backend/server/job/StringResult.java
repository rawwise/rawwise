package io.github.rawwise.backend.server.job;

import lombok.Getter;

@Getter
public class StringResult extends ManagedJobResult {
    private final String result;

    public StringResult(ManagedJobResult jobResult, String result) {
        this(
                jobResult.jobId,
                jobResult.sql,
                jobResult.status,
                jobResult.startTime,
                jobResult.endTime,
                jobResult.error,
                result);
    }

    public StringResult(
            String jobId,
            String sql,
            JobStatus status,
            long startTime,
            long endTime,
            String error,
            String result) {
        super(jobId, sql, status, startTime, endTime, error);
        this.result = result;
    }
}
