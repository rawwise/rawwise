package io.github.rawwise.backend.server.session;

import io.github.rawwise.backend.server.job.JobType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface SessionStore {

    String createSession(
            String frontendSessionId, String sql, JobType type, Map<String, String> options);

    Optional<BackendSession> getSession(String sessionId);

    void updateSession(BackendSession session);

    void removeSession(String sessionId);

    List<BackendSession> getActiveSessions();

    List<BackendSession> getSessionsByFrontendId(String frontendSessionId);

    int getActiveSessionCount();
}
