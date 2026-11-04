package io.github.rawwise.backend.server.job;

import java.util.List;
import lombok.Getter;
import org.apache.spark.sql.Row;

@Getter
public final class ListResult extends ManagedJobResult {
    private final List<Row> rows;

    public ListResult(ManagedJobResult jobResult, List<Row> rows) {
        this(
                jobResult.jobId,
                jobResult.sql,
                jobResult.status,
                jobResult.startTime,
                jobResult.endTime,
                jobResult.error,
                rows);
    }

    public ListResult(
            String jobId,
            String sql,
            JobStatus status,
            long startTime,
            long endTime,
            String error,
            List<Row> rows) {
        super(jobId, sql, status, startTime, endTime, error);
        this.rows = rows;
    }
}
