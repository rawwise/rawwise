package io.github.rawwise.frontend.session;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class FrontendSession {

    private final String sessionId;
    private String backendSessionId;
    private final Instant createTime;
    private Instant lastAccessTime;
    private Map<String, Object> context;

    public FrontendSession() {
        this.sessionId = UUID.randomUUID().toString();
        this.createTime = Instant.now();
        this.lastAccessTime = Instant.now();
    }

    public FrontendSession(String sessionId) {
        this.sessionId = sessionId;
        this.createTime = Instant.now();
        this.lastAccessTime = Instant.now();
    }

    public void touch() {
        this.lastAccessTime = Instant.now();
    }
}
