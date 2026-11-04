package io.github.rawwise.frontend.api.session;

import io.javalin.http.Context;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SessionManager implements AutoCloseable {

    private static final String TXN_SESSION_ATTR = "rawwise.txnSession";
    private static final long TXN_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(30);
    private static final long CLEANUP_INTERVAL_MINUTES = 5;

    private final Map<String, TxnSession> activeTxnSessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor;

    public SessionManager() {
        this.cleanupExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "txn-session-cleanup");
                            t.setDaemon(true);
                            return t;
                        });
        this.cleanupExecutor.scheduleAtFixedRate(
                this::cleanupExpiredTxnSessions,
                CLEANUP_INTERVAL_MINUTES,
                CLEANUP_INTERVAL_MINUTES,
                TimeUnit.MINUTES);
    }

    public TxnSession getTxnSession(Context ctx) {
        String sessionId = ctx.sessionAttribute("sessionId");
        if (sessionId == null) {
            sessionId = ctx.req().getSession(true).getId();
            ctx.sessionAttribute("sessionId", sessionId);
        }
        TxnSession txnSession = activeTxnSessions.get(sessionId);
        if (txnSession != null && txnSession.isActive()) {
            TxnSession updated = txnSession.withActivity();
            activeTxnSessions.put(sessionId, updated);
            return updated;
        }
        return new TxnSession(0, null, null, 0, 0);
    }

    public void setTxnSession(Context ctx, TxnSession txnSession) {
        String sessionId = ctx.sessionAttribute("sessionId");
        if (sessionId == null) {
            sessionId = ctx.req().getSession(true).getId();
            ctx.sessionAttribute("sessionId", sessionId);
        }
        if (txnSession.isActive()) {
            activeTxnSessions.put(sessionId, txnSession);
        } else {
            activeTxnSessions.remove(sessionId);
        }
    }

    public void clearTxnSession(Context ctx) {
        String sessionId = ctx.sessionAttribute("sessionId");
        if (sessionId != null) {
            activeTxnSessions.remove(sessionId);
        }
    }

    private void cleanupExpiredTxnSessions() {
        long now = System.currentTimeMillis();
        int removed = 0;
        for (Map.Entry<String, TxnSession> entry : activeTxnSessions.entrySet()) {
            TxnSession txn = entry.getValue();
            if (txn.isActive() && (now - txn.lastActivityMs()) > TXN_TIMEOUT_MS) {
                activeTxnSessions.remove(entry.getKey());
                removed++;
                log.info(
                        "Cleaned up expired txn session: sessionId={} txnId={}",
                        entry.getKey(),
                        txn.txnId());
            }
        }
        if (removed > 0) {
            log.info("Cleaned up {} expired txn sessions", removed);
        }
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
