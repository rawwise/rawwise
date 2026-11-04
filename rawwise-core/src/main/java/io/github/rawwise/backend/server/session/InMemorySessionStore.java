package io.github.rawwise.backend.server.session;

import io.github.rawwise.backend.server.job.JobType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class InMemorySessionStore implements SessionStore {
    private final Map<String, BackendSession> sessions;
    private final Map<String, List<String>> frontendToSessions;
    private final AtomicLong sessionIdCounter;

    public InMemorySessionStore() {
        this.sessions = new ConcurrentHashMap<>();
        this.frontendToSessions = new ConcurrentHashMap<>();
        this.sessionIdCounter = new AtomicLong(0);
    }

    @Override
    public synchronized String createSession(
            String frontendSessionId, String sql, JobType type, Map<String, String> options) {
        String sessionId = generateSessionId();
        BackendSession session =
                new BackendSession(sessionId, frontendSessionId, sql, type, options);
        sessions.put(sessionId, session);

        frontendToSessions
                .computeIfAbsent(frontendSessionId, k -> new CopyOnWriteArrayList<>())
                .add(sessionId);

        return sessionId;
    }

    @Override
    public synchronized Optional<BackendSession> getSession(String sessionId) {
        return Optional.ofNullable(sessions.get(sessionId));
    }

    @Override
    public void updateSession(BackendSession session) {
        sessions.put(session.getSessionId(), session);
    }

    @Override
    public synchronized void removeSession(String sessionId) {
        BackendSession session = sessions.remove(sessionId);
        if (session != null && session.getFrontendSessionId() != null) {
            List<String> sessionIds = frontendToSessions.get(session.getFrontendSessionId());
            if (sessionIds != null) {
                sessionIds.remove(sessionId);
            }
        }
    }

    @Override
    public List<BackendSession> getActiveSessions() {
        return sessions.values().stream()
                .filter(
                        s ->
                                s.getState() == SessionState.PENDING
                                        || s.getState() == SessionState.RUNNING)
                .collect(Collectors.toList());
    }

    @Override
    public List<BackendSession> getSessionsByFrontendId(String frontendSessionId) {
        List<String> sessionIds = frontendToSessions.get(frontendSessionId);
        if (sessionIds == null) {
            return Collections.emptyList();
        }
        return sessionIds.stream()
                .map(sessions::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public int getActiveSessionCount() {
        return getActiveSessions().size();
    }

    private String generateSessionId() {
        return "sess_" + System.currentTimeMillis() + "_" + sessionIdCounter.incrementAndGet();
    }
}
