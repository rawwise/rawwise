package io.github.rawwise.axiom.memory.semantic;

import io.github.rawwise.axiom.memory.SummarizationService;
import io.github.rawwise.governance.context.ContextEntry;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LlmKnowledgeExtractor implements KnowledgeExtractor {

    private static final int CONSECUTIVE_FAILURE_THRESHOLD = 3;
    private static final long CIRCUIT_RESET_MS = 60_000L;

    enum CircuitState {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    record CircuitData(CircuitState state, int failures, long lastFailureTime) {
        static final CircuitData CLOSED = new CircuitData(CircuitState.CLOSED, 0, 0);
    }

    private final GraphEngine graphEngine;
    private final SummarizationService summarizationService;

    private final AtomicReference<CircuitData> circuitData =
            new AtomicReference<>(CircuitData.CLOSED);

    public LlmKnowledgeExtractor(
            GraphEngine graphEngine, SummarizationService summarizationService) {
        this.graphEngine = graphEngine;
        this.summarizationService = summarizationService;
    }

    @Override
    public ExtractionResult extract(
            String agentId, String graphId, List<ContextEntry> entries, ExtractionPolicy policy)
            throws GraphException {
        long start = System.currentTimeMillis();

        List<ContextEntry> filtered =
                entries.stream().filter(e -> e.importance() >= policy.minImportance()).toList();

        if (filtered.isEmpty()) {
            return new ExtractionResult(
                    agentId, graphId, 0, 0, 0, null, System.currentTimeMillis() - start);
        }

        int totalNodes = 0;
        int totalEdges = 0;
        int processed = 0;
        String lastCursor = policy.lastExtractionCursor();

        int batchSize = policy.batchSize() > 0 ? policy.batchSize() : 50;
        for (int i = 0; i < filtered.size(); i += batchSize) {
            List<ContextEntry> batch =
                    filtered.subList(i, Math.min(i + batchSize, filtered.size()));

            String batchContent = buildBatchContent(batch);

            SummarizationService.SummarizationRequest request =
                    new SummarizationService.SummarizationRequest(
                            null,
                            100,
                            policy.maxTokensResponse(),
                            "text/plain",
                            policy.extractionPrompt() != null
                                    ? policy.extractionPrompt()
                                    : "Extract knowledge graph nodes and edges from the following content. "
                                            + "Return JSON with 'nodes' and 'edges' arrays.");

            ExtractionResponse response =
                    callWithRetry(
                            () -> {
                                SummarizationService.SummarizationResult result =
                                        summarizationService.summarize(batchContent, request);
                                return parseExtractionResponse(result.l1Overview());
                            },
                            policy.maxRetries(),
                            policy.retryDelayMs(),
                            graphId);

            if (response == null) {
                continue;
            }

            Map<String, String> extractionIdMapping = new HashMap<>();

            for (ExtractionResponse.NodeDef nodeDef : response.nodes()) {
                try {
                    String extractionId = nodeDef.id();
                    Map<String, String> props = new LinkedHashMap<>(nodeDef.properties());
                    props.put("extraction_id", extractionId);

                    List<GraphNode> existing =
                            graphEngine.queryNodes(
                                    graphId,
                                    nodeDef.type(),
                                    Map.of("extraction_id", extractionId),
                                    1);

                    if (!existing.isEmpty()) {
                        if (policy.overwriteExisting()) {
                            GraphNode existingNode = existing.get(0);
                            GraphNode updated =
                                    new GraphNode(
                                            existingNode.nodeId(),
                                            graphId,
                                            nodeDef.type(),
                                            props,
                                            existingNode.importance(),
                                            existingNode.decayFactor(),
                                            existingNode.embeddingRef(),
                                            existingNode.embeddingModel(),
                                            existingNode.embeddingText(),
                                            existingNode.version(),
                                            existingNode.createdAt(),
                                            System.currentTimeMillis());
                            graphEngine.updateNode(graphId, updated);
                            extractionIdMapping.put(extractionId, existingNode.nodeId());
                            totalNodes++;
                        }
                    } else {
                        GraphNode added = graphEngine.addNode(graphId, nodeDef.type(), props);
                        extractionIdMapping.put(extractionId, added.nodeId());
                        totalNodes++;
                    }
                } catch (GraphCasConflictException e) {
                    log.debug(
                            "CAS conflict adding node extraction_id={} in graph={}, skipping",
                            nodeDef.id(),
                            graphId,
                            e);
                } catch (GraphException e) {
                    log.warn(
                            "Failed to add node extraction_id={} in graph={}",
                            nodeDef.id(),
                            graphId,
                            e);
                }
            }

            for (ExtractionResponse.EdgeDef edgeDef : response.edges()) {
                try {
                    String srcNodeId =
                            extractionIdMapping.getOrDefault(edgeDef.src(), edgeDef.src());
                    String dstNodeId =
                            extractionIdMapping.getOrDefault(edgeDef.dst(), edgeDef.dst());

                    if (graphEngine.getNode(graphId, srcNodeId).isEmpty()
                            || graphEngine.getNode(graphId, dstNodeId).isEmpty()) {
                        log.debug(
                                "Skipping edge src={} label={} dst={}: endpoint not found",
                                edgeDef.src(),
                                edgeDef.label(),
                                edgeDef.dst());
                        continue;
                    }

                    graphEngine.addEdge(
                            graphId,
                            srcNodeId,
                            edgeDef.label(),
                            dstNodeId,
                            edgeDef.properties(),
                            1.0f);
                    totalEdges++;
                } catch (GraphCasConflictException e) {
                    log.debug("CAS conflict adding edge in graph={}, skipping", graphId, e);
                } catch (GraphException e) {
                    log.warn("Failed to add edge in graph={}", graphId, e);
                }
            }

            processed += batch.size();
            if (!batch.isEmpty()) {
                lastCursor = batch.get(batch.size() - 1).key();
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        log.info(
                "Extraction complete agentId={} graphId={} nodes={} edges={} entries={}/{} elapsedMs={}",
                agentId,
                graphId,
                totalNodes,
                totalEdges,
                processed,
                filtered.size(),
                elapsed);

        return new ExtractionResult(
                agentId, graphId, totalNodes, totalEdges, processed, lastCursor, elapsed);
    }

    @Override
    public boolean isAvailable() {
        CircuitData current = circuitData.get();
        if (current.state() == CircuitState.OPEN) {
            long elapsed = System.currentTimeMillis() - current.lastFailureTime();
            if (elapsed > CIRCUIT_RESET_MS) {
                CircuitData halfOpen =
                        new CircuitData(
                                CircuitState.HALF_OPEN,
                                current.failures(),
                                current.lastFailureTime());
                return circuitData.compareAndSet(current, halfOpen)
                        && summarizationService.isAvailable();
            }
            return false;
        }
        return summarizationService.isAvailable();
    }

    private <T> T callWithRetry(
            ThrowingSupplier<T> supplier, int maxRetries, long retryDelayMs, String graphId)
            throws GraphException {
        CircuitData current = circuitData.get();
        if (current.state() == CircuitState.OPEN) {
            long elapsed = System.currentTimeMillis() - current.lastFailureTime();
            if (elapsed > CIRCUIT_RESET_MS) {
                CircuitData halfOpen =
                        new CircuitData(
                                CircuitState.HALF_OPEN,
                                current.failures(),
                                current.lastFailureTime());
                if (!circuitData.compareAndSet(current, halfOpen)) {
                    throw new GraphException(graphId, "Circuit breaker is open");
                }
            } else {
                throw new GraphException(graphId, "Circuit breaker is open");
            }
        }

        Exception lastException = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                T result = supplier.get();
                circuitData.updateAndGet(cd -> CircuitData.CLOSED);
                return result;
            } catch (Exception e) {
                lastException = e;
                long now = System.currentTimeMillis();
                circuitData.updateAndGet(
                        cd -> {
                            int newFailures = cd.failures() + 1;
                            if (newFailures >= CONSECUTIVE_FAILURE_THRESHOLD) {
                                return new CircuitData(CircuitState.OPEN, newFailures, now);
                            }
                            return new CircuitData(cd.state(), newFailures, now);
                        });

                CircuitData afterFailure = circuitData.get();
                if (afterFailure.state() == CircuitState.OPEN) {
                    log.warn(
                            "Circuit breaker opened after {} consecutive failures",
                            afterFailure.failures());
                    break;
                }

                if (attempt < maxRetries) {
                    try {
                        long delay =
                                retryDelayMs * (1L << attempt)
                                        + (long) (Math.random() * retryDelayMs);
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new GraphException(graphId, "Interrupted during retry", ie);
                    }
                }
            }
        }

        log.warn("All retries exhausted for graphId={}", graphId);
        return null;
    }

    private ExtractionResponse parseExtractionResponse(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            String cleaned = json.trim();
            if (cleaned.startsWith("```")) {
                int startIdx = cleaned.indexOf('\n');
                int endIdx = cleaned.lastIndexOf("```");
                if (startIdx > 0 && endIdx > startIdx) {
                    cleaned = cleaned.substring(startIdx + 1, endIdx).trim();
                }
            }

            return io.github.rawwise.governance.context.ContextJson.MAPPER.readValue(
                    cleaned, ExtractionResponse.class);
        } catch (IOException e) {
            log.warn("Failed to parse extraction response as JSON: {}", e.getMessage());
            return null;
        }
    }

    private String buildBatchContent(List<ContextEntry> batch) {
        StringBuilder sb = new StringBuilder();
        for (ContextEntry entry : batch) {
            sb.append("--- Entry: ").append(entry.key()).append(" ---\n");
            String content = entry.l1Overview();
            if (content == null || content.isEmpty()) {
                content = entry.l0Abstract();
            }
            if (content == null || content.isEmpty()) {
                content = entry.value();
            }
            if (content != null) {
                sb.append(content).append("\n\n");
            }
        }
        return sb.toString();
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }
}
