package io.github.rawwise.frontend.session;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FrontendSessionStore implements AutoCloseable {

    private static final long SESSION_TTL_MINUTES = 30;
    private static final long CLEANUP_INTERVAL_MINUTES = 10;
    private static final int MAX_JOB_OWNERS = 100_000;

    private final Map<String, FrontendSession> sessions = new ConcurrentHashMap<>();
    private final Map<String, String> jobOwners = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;

    public FrontendSessionStore() {
        this.cleanupExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "session-cleanup");
                            t.setDaemon(true);
                            return t;
                        });
        this.cleanupExecutor.scheduleAtFixedRate(
                this::cleanupExpiredSessions,
                CLEANUP_INTERVAL_MINUTES,
                CLEANUP_INTERVAL_MINUTES,
                TimeUnit.MINUTES);
    }

    public FrontendSession createSession(String sessionId) {
        String actualSessionId = sessionId != null ? sessionId : UUID.randomUUID().toString();
        return sessions.computeIfAbsent(actualSessionId, id -> new FrontendSession(id));
    }

    public FrontendSession getSession(String sessionId) {
        FrontendSession session = sessions.get(sessionId);
        if (session != null) {
            session.touch();
        }
        return session;
    }

    public void bindJobOwner(String jobId, String principalId) {
        if (jobOwners.size() >= MAX_JOB_OWNERS) {
            log.warn("jobOwners map reached max size {}, evicting oldest entries", MAX_JOB_OWNERS);
            jobOwners.keySet().stream().findFirst().ifPresent(jobOwners::remove);
        }
        jobOwners.put(jobId, principalId);
    }

    public String getJobOwner(String jobId) {
        return jobOwners.get(jobId);
    }

    public void removeSession(String sessionId) {
        sessions.remove(sessionId);
    }

    public Map<String, FrontendSession> getAllSessions() {
        return Map.copyOf(sessions);
    }

    private void cleanupExpiredSessions() {
        Instant threshold = Instant.now().minusSeconds(SESSION_TTL_MINUTES * 60);
        int removed = 0;
        for (Map.Entry<String, FrontendSession> entry : sessions.entrySet()) {
            if (entry.getValue().getLastAccessTime().isBefore(threshold)) {
                if (sessions.remove(entry.getKey(), entry.getValue())) {
                    removed++;
                }
            }
        }
        if (removed > 0) {
            log.info("Cleaned up {} expired sessions", removed);
        }
        jobOwners
                .keySet()
                .removeIf(
                        jobId -> {
                            boolean orphaned = true;
                            for (String sid : sessions.keySet()) {
                                if (jobId.startsWith(sid)) {
                                    orphaned = false;
                                    break;
                                }
                            }
                            return orphaned;
                        });
    }

    @Override
    public void close() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cleanupExecutor.shutdownNow();
        }
    }
}
