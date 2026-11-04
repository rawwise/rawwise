package io.github.rawwise.backend.server.session;

import io.github.rawwise.backend.server.job.JobType;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

public class BackendSession {
    @Getter private final String sessionId;
    @Getter private final String frontendSessionId;
    @Getter private final String sql;
    @Getter private final JobType jobType;
    @Getter private final Map<String, String> options;
    @Setter @Getter private SessionState state;
    @Getter private final long createTime;
    @Setter @Getter private long startTime;
    @Setter @Getter private long endTime;
    @Setter @Getter private Object result;
    @Setter @Getter private String error;

    public BackendSession(
            String sessionId,
            String frontendSessionId,
            String sql,
            JobType jobType,
            Map<String, String> options) {
        this.sessionId = sessionId;
        this.frontendSessionId = frontendSessionId;
        this.sql = sql;
        this.jobType = jobType;
        this.options = options;
        this.state = SessionState.PENDING;
        this.createTime = System.currentTimeMillis();
    }

    public void markRunning() {
        this.state = SessionState.RUNNING;
        this.startTime = System.currentTimeMillis();
    }

    public void markCompleted(Object result) {
        this.state = SessionState.COMPLETED;
        this.result = result;
        this.endTime = System.currentTimeMillis();
    }

    public void markFailed(String error) {
        this.state = SessionState.FAILED;
        this.error = error;
        this.endTime = System.currentTimeMillis();
    }

    public void markCancelled() {
        this.state = SessionState.CANCELLED;
        this.endTime = System.currentTimeMillis();
    }

    public void markTimeout() {
        this.state = SessionState.TIMEOUT;
        this.endTime = System.currentTimeMillis();
    }

    public long getDuration() {
        if (startTime > 0 && endTime > 0) {
            return endTime - startTime;
        }
        return 0;
    }
}
