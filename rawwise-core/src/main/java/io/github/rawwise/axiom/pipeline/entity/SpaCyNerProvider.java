package io.github.rawwise.axiom.pipeline.entity;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class SpaCyNerProvider implements EntityProvider {

    private static final int HEARTBEAT_TIMEOUT_MS = 200;
    private static final int MAX_RESTARTS = 3;

    private final int sidecarPort;
    private final Duration timeout;
    private final AtomicInteger restartCount = new AtomicInteger(0);
    private volatile boolean disabled = false;

    public SpaCyNerProvider(int sidecarPort, Duration timeout) {
        this.sidecarPort = sidecarPort;
        this.timeout = timeout;
    }

    @Override
    public String name() {
        return "spacy";
    }

    @Override
    public EntityView extract(String text, EntityContext context) throws ExtractionException {
        if (disabled) {
            throw new ExtractionException(
                    "spacy provider disabled after max restarts: " + MAX_RESTARTS);
        }
        if (!tryHeartbeat()) {
            int attempts = restartCount.incrementAndGet();
            if (attempts > MAX_RESTARTS) {
                disabled = true;
                throw new ExtractionException(
                        "spacy sidecar unavailable on port "
                                + sidecarPort
                                + " (exceeded max restarts "
                                + MAX_RESTARTS
                                + ")");
            }
            throw new ExtractionException("spacy sidecar unavailable on port " + sidecarPort);
        }
        return extractStub(text, context);
    }

    boolean tryHeartbeat() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", sidecarPort), HEARTBEAT_TIMEOUT_MS);
            return true;
        } catch (IOException e) {
            log.debug("spaCy sidecar heartbeat failed on port {}: {}", sidecarPort, e.getMessage());
            return false;
        }
    }

    private EntityView extractStub(String text, EntityContext context) {
        List<Entity> entities = new ArrayList<>();
        if (text != null && !text.isBlank()) {
            String[] tokens = text.split("\\s+");
            for (String token : tokens) {
                String cleaned = token.replaceAll("[^\\p{L}\\p{N}]", "");
                if (cleaned.isEmpty()) continue;
                if (Character.isUpperCase(cleaned.charAt(0)) && cleaned.length() > 1) {
                    entities.add(
                            Entity.deterministic(
                                    EntityType.PERSON,
                                    cleaned,
                                    context == null ? "" : context.sampleId(),
                                    token,
                                    0.75));
                }
            }
        }
        String diaId = context == null ? "" : context.tableId() + ":" + context.sampleId();
        String sampleId = context == null ? "" : context.sampleId();
        return new EntityView(
                diaId,
                sampleId,
                entities,
                List.of(),
                "spacy",
                Instant.now(),
                ExtractionStatus.SUCCESS,
                null);
    }

    public int restartCount() {
        return restartCount.get();
    }

    public boolean isDisabled() {
        return disabled;
    }
}
