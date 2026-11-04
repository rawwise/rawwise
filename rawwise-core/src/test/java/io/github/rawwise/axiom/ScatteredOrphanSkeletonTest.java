package io.github.rawwise.axiom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.axiom.memory.MemoryTier;
import io.github.rawwise.axiom.memory.NoOpSummarizationService;
import io.github.rawwise.axiom.memory.semantic.DefaultGraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphEdge;
import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphEngineConfig;
import io.github.rawwise.axiom.memory.semantic.GraphException;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.axiom.memory.semantic.GraphSchema;
import io.github.rawwise.axiom.memory.semantic.GraphStateAdapter;
import io.github.rawwise.axiom.memory.semantic.GraphTrustAdapter;
import io.github.rawwise.axiom.memory.semantic.NoOpKnowledgeExtractor;
import io.github.rawwise.axiom.pipeline.derivation.SentenceChunkingStrategy;
import io.github.rawwise.axiom.pipeline.entity.NoOpEntityDerivationStage;
import io.github.rawwise.axiom.pipeline.state.InMemoryStateEngine;
import io.github.rawwise.axiom.pipeline.state.StateEngine;
import io.github.rawwise.axiom.pipeline.state.StateSpace;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.InMemoryVectorIndex;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.context.DefaultContextManager;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.MapRecord;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Scattered orphan components integration: chunking -> state adapter -> trust adapter")
class ScatteredOrphanSkeletonTest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private DefaultContextManager contextManager;
    private GraphEngine engine;
    private GraphTrustAdapter trustAdapter;
    private GraphStateAdapter stateAdapter;
    private StateEngine stateEngine;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        contextManager = new DefaultContextManager(metastore, 1, 1);
        EmbeddingProvider embeddingProvider = text -> new float[] {0.1f, 0.2f};
        VectorIndex vectorIndex = new InMemoryVectorIndex(embeddingProvider);
        trustAdapter = new GraphTrustAdapter(metastore);
        GraphEngineConfig config =
                new GraphEngineConfig(
                        metastore,
                        knowledgeStore,
                        contextManager,
                        new NoOpSummarizationService(),
                        vectorIndex,
                        embeddingProvider,
                        new NoOpKnowledgeExtractor(),
                        trustAdapter);
        engine = new DefaultGraphEngine(config);
        stateEngine = new InMemoryStateEngine();
        stateAdapter = new GraphStateAdapter(engine, stateEngine, metastore);
    }

    @AfterEach
    void tearDown() throws Exception {
        contextManager.close();
        knowledgeStore.close();
        metastore.close();
    }

    @Test
    @DisplayName("SentenceChunkingStrategy splits multi-sentence text by punctuation boundaries")
    void sentenceChunkingSplitsByPunctuation() {
        SentenceChunkingStrategy strategy = new SentenceChunkingStrategy();
        String text =
                "First sentence here. Second one follows! Third sentence? " + "第四个句子。第五个句子！最后一句？";
        byte[] content = text.getBytes(StandardCharsets.UTF_8);

        List<byte[]> chunks = strategy.split(content, Map.of("maxSentences", 2));
        assertNotNull(chunks);
        assertTrue(
                chunks.size() >= 3, "6 sentences with maxSentences=2 should produce >= 3 chunks");

        String firstChunk = new String(chunks.get(0), StandardCharsets.UTF_8);
        assertTrue(
                firstChunk.contains("First sentence here."),
                "first chunk should contain the first sentence");
        assertTrue(
                firstChunk.contains("Second one follows!"),
                "first chunk should contain the second sentence (grouped by maxSentences=2)");
    }

    @Test
    @DisplayName("SentenceChunkingStrategy handles single-sentence and empty input")
    void sentenceChunkingEdgeCases() {
        SentenceChunkingStrategy strategy = new SentenceChunkingStrategy();

        List<byte[]> single =
                strategy.split("Only one sentence.".getBytes(StandardCharsets.UTF_8), Map.of());
        assertEquals(1, single.size(), "single sentence should produce exactly 1 chunk");

        List<byte[]> empty = strategy.split(new byte[0], Map.of());
        assertTrue(empty.size() <= 1, "empty input should produce 0 or 1 empty chunk");
    }

    @Test
    @DisplayName(
            "Chunking -> GraphStateAdapter -> GraphTrustAdapter chain: high-importance node maps to SEMANTIC tier with verified trust")
    void fullChainChunkingToStateToTrust() throws Exception {
        SentenceChunkingStrategy strategy = new SentenceChunkingStrategy();
        String text =
                "Alice works at Acme Corp. She leads the engineering team. "
                        + "Bob is a senior developer there. They collaborate on the data platform. "
                        + "Carol joined recently as a product manager.";
        List<byte[]> chunks =
                strategy.split(text.getBytes(StandardCharsets.UTF_8), Map.of("maxSentences", 2));
        assertTrue(
                chunks.size() >= 2, "5 sentences with maxSentences=2 should produce >= 2 chunks");

        GraphSchema schema =
                engine.createGraph(
                        "agent-chain",
                        "chain-graph",
                        "chain test",
                        List.of("person", "concept"),
                        List.of("works_at", "collaborates"),
                        Map.of());
        String graphId = schema.graphId();

        StateSpace space =
                stateEngine.createStateSpace(
                        "agent-chain",
                        "entity-states",
                        "ACTIVE",
                        List.of("ACTIVE", "INACTIVE", "ARCHIVED"),
                        List.of(),
                        Map.of());

        GraphNode firstNode = null;
        for (int i = 0; i < chunks.size(); i++) {
            String chunkText = new String(chunks.get(i), StandardCharsets.UTF_8);
            GraphNode node =
                    engine.addNode(
                            graphId,
                            "person",
                            Map.of("name", "entity-" + i, "importance", "8", "chunk", chunkText));
            assertNotNull(node);
            if (firstNode == null) firstNode = node;
        }
        assertNotNull(firstNode);

        GraphStateAdapter.StateMapping binding =
                stateAdapter.bindNodeToStateSpace(
                        graphId, firstNode.nodeId(), space.spaceId(), "ACTIVE");
        assertEquals("ACTIVE", binding.stateName());
        assertEquals(1L, binding.version());

        GraphStateAdapter.StateMapping transitioned =
                stateAdapter.transitionNode(
                        graphId, firstNode.nodeId(), "INACTIVE", Map.of("reason", "context-ended"));
        assertEquals("INACTIVE", transitioned.stateName());
        assertEquals(2L, transitioned.version());

        GraphStateAdapter.StateMapping readBack =
                stateAdapter.getStateMapping(graphId, firstNode.nodeId());
        assertEquals("INACTIVE", readBack.stateName());
        assertEquals(2L, readBack.version());

        MemoryTier tier = stateAdapter.resolveMemoryTier(firstNode);
        assertEquals(MemoryTier.SEMANTIC, tier, "importance=8 should map to SEMANTIC tier");

        GraphStateAdapter.NodeStateTier stateTier =
                stateAdapter.extractNodeStateTier(graphId, firstNode.nodeId());
        assertEquals(firstNode.nodeId(), stateTier.nodeId());
        assertEquals("INACTIVE", stateTier.stateName());
        assertEquals(MemoryTier.SEMANTIC, stateTier.memoryTier());

        boolean integrityOk =
                trustAdapter.verifyNodeIntegrity(
                        graphId, firstNode.nodeId(), firstNode.version(), firstNode);
        assertTrue(
                integrityOk, "node integrity should be verified after addNode recorded trust hash");

        List<GraphEdge> edges = engine.getOutEdges(graphId, firstNode.nodeId(), null);
        double trustScore = trustAdapter.computeTrustScore(graphId, firstNode, edges);
        assertTrue(
                trustScore > 0.5,
                "trust score with verified integrity + importance=8 should be > 0.5, got "
                        + trustScore);
        assertTrue(trustScore <= 1.0, "trust score should not exceed 1.0, got " + trustScore);
    }

    @Test
    @DisplayName("GraphTrustAdapter: adding edges increases trust score via connectivity")
    void trustScoreIncreasesWithEdges() throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        "agent-trust",
                        "trust-graph",
                        "trust test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();

        GraphNode src = engine.addNode(graphId, "entity", Map.of("name", "src", "importance", "5"));
        GraphNode dst = engine.addNode(graphId, "entity", Map.of("name", "dst", "importance", "5"));

        List<GraphEdge> noEdges = engine.getOutEdges(graphId, src.nodeId(), null);
        double scoreBefore = trustAdapter.computeTrustScore(graphId, src, noEdges);

        for (int i = 0; i < 5; i++) {
            engine.addEdge(graphId, src.nodeId(), "rel", dst.nodeId(), Map.of(), 1.0f);
        }

        List<GraphEdge> fiveEdges = engine.getOutEdges(graphId, src.nodeId(), null);
        assertEquals(5, fiveEdges.size());
        double scoreAfter = trustAdapter.computeTrustScore(graphId, src, fiveEdges);

        assertTrue(
                scoreAfter > scoreBefore,
                "trust score with 5 edges ("
                        + scoreAfter
                        + ") should exceed score with 0 edges ("
                        + scoreBefore
                        + ")");
    }

    @Test
    @DisplayName("GraphTrustAdapter: tampered node fails integrity and reduces trust score")
    void tamperedNodeFailsIntegrity() throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        "agent-tamper",
                        "tamper-graph",
                        "tamper test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();

        GraphNode node =
                engine.addNode(graphId, "entity", Map.of("name", "original", "importance", "7"));

        boolean originalOk =
                trustAdapter.verifyNodeIntegrity(graphId, node.nodeId(), node.version(), node);
        assertTrue(originalOk, "original node should pass integrity check");

        GraphNode tampered =
                new GraphNode(
                        node.nodeId(),
                        node.graphId(),
                        node.type(),
                        Map.of("name", "hacked", "importance", "10"),
                        node.importance(),
                        node.decayFactor(),
                        node.embeddingRef(),
                        node.embeddingModel(),
                        node.embeddingText(),
                        node.version(),
                        node.createdAt(),
                        node.updatedAt());

        boolean tamperedOk =
                trustAdapter.verifyNodeIntegrity(
                        graphId, tampered.nodeId(), tampered.version(), tampered);
        assertFalse(tamperedOk, "tampered node should fail integrity check");

        double scoreOriginal = trustAdapter.computeTrustScore(graphId, node, List.of());
        double scoreTampered = trustAdapter.computeTrustScore(graphId, tampered, List.of());
        assertTrue(
                scoreOriginal > scoreTampered,
                "original trust score ("
                        + scoreOriginal
                        + ") should exceed tampered ("
                        + scoreTampered
                        + ")");
    }

    @Test
    @DisplayName("GraphStateAdapter: resolveMemoryTier maps importance levels correctly")
    void memoryTierMapping() {
        GraphNode highImportance =
                new GraphNode("n1", "g1", "entity", Map.of(), 9, 1.0f, null, null, null, 1, 0, 0);
        assertEquals(MemoryTier.SEMANTIC, stateAdapter.resolveMemoryTier(highImportance));

        GraphNode midImportance =
                new GraphNode("n2", "g1", "entity", Map.of(), 5, 1.0f, null, null, null, 1, 0, 0);
        assertEquals(MemoryTier.EPISODIC, stateAdapter.resolveMemoryTier(midImportance));

        GraphNode lowImportance =
                new GraphNode("n3", "g1", "entity", Map.of(), 2, 1.0f, null, null, null, 1, 0, 0);
        assertEquals(MemoryTier.WORKING, stateAdapter.resolveMemoryTier(lowImportance));

        GraphNode procedural =
                new GraphNode(
                        "n4", "g1", "procedure", Map.of(), 9, 1.0f, null, null, null, 1, 0, 0);
        assertEquals(MemoryTier.PROCEDURAL, stateAdapter.resolveMemoryTier(procedural));

        assertEquals(
                MemoryTier.WORKING,
                stateAdapter.resolveMemoryTier(null),
                "null node should default to WORKING tier");
    }

    @Test
    @DisplayName("GraphStateAdapter: binding non-existent node throws GraphNodeNotFoundException")
    void bindingNonExistentNodeThrows() {
        GraphSchema schema =
                engine.createGraph(
                        "agent-missing",
                        "missing-graph",
                        "missing test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();

        org.junit.jupiter.api.Assertions.assertThrows(
                GraphException.class,
                () ->
                        stateAdapter.bindNodeToStateSpace(
                                graphId, "non-existent-node", "space-1", "ACTIVE"));
    }

    @Test
    @DisplayName("GraphStateAdapter: double binding throws CAS conflict")
    void doubleBindingThrowsCasConflict() throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        "agent-cas",
                        "cas-graph",
                        "cas test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();
        GraphNode node = engine.addNode(graphId, "entity", Map.of("name", "n1"));

        StateSpace space =
                stateEngine.createStateSpace(
                        "agent-cas", "cas-space", "ACTIVE", List.of("ACTIVE"), List.of(), Map.of());

        stateAdapter.bindNodeToStateSpace(graphId, node.nodeId(), space.spaceId(), "ACTIVE");

        org.junit.jupiter.api.Assertions.assertThrows(
                GraphException.class,
                () ->
                        stateAdapter.bindNodeToStateSpace(
                                graphId, node.nodeId(), space.spaceId(), "ACTIVE"));
    }

    @Test
    @DisplayName("GraphTrustAdapter: edge integrity recording and verification")
    void edgeIntegrityRecording() throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        "agent-edge",
                        "edge-graph",
                        "edge trust test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();

        GraphNode src = engine.addNode(graphId, "entity", Map.of("name", "src"));
        GraphNode dst = engine.addNode(graphId, "entity", Map.of("name", "dst"));
        GraphEdge edge = engine.addEdge(graphId, src.nodeId(), "rel", dst.nodeId(), Map.of(), 0.8f);

        boolean edgeOk =
                trustAdapter.verifyEdgeIntegrity(graphId, edge.edgeId(), edge.version(), edge);
        assertTrue(edgeOk, "edge integrity should be verified after addEdge recorded trust hash");

        GraphEdge tamperedEdge =
                new GraphEdge(
                        edge.edgeId(),
                        edge.graphId(),
                        edge.srcNodeId(),
                        edge.label(),
                        edge.dstNodeId(),
                        Map.of("fake", "prop"),
                        edge.weight(),
                        edge.version(),
                        edge.createdAt(),
                        edge.updatedAt());
        boolean tamperedEdgeOk =
                trustAdapter.verifyEdgeIntegrity(
                        graphId, tamperedEdge.edgeId(), tamperedEdge.version(), tamperedEdge);
        assertFalse(tamperedEdgeOk, "tampered edge should fail integrity check");
    }

    @Test
    @DisplayName(
            "NoOpEntityDerivationStage: onWrite is pass-through with no KnowledgeStore side effects")
    void noOpEntityDerivationIsPassThrough() throws Exception {
        NoOpEntityDerivationStage noOp = new NoOpEntityDerivationStage();

        byte[] keyBytes = "dia-noop-1".getBytes(StandardCharsets.UTF_8);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("body", "Alice met Bob in Seattle");
        body.put("sample_id", "sample-noop");
        Record record = withKey(MapRecord.fromMap(body), Records.keyOf(keyBytes));

        int keysBefore = countKeysWithPrefix(knowledgeStore, "entity_view:");
        noOp.onWrite(record, "default.dev_dialogs", 1L);
        int keysAfter = countKeysWithPrefix(knowledgeStore, "entity_view:");

        assertEquals(
                keysBefore,
                keysAfter,
                "NoOpEntityDerivationStage must not write any entity_view:* keys (pass-through)");

        int graphKeysBefore = countKeysWithPrefix(knowledgeStore, "graph:node:");
        noOp.onWrite(record, "default.dev_dialogs", 2L);
        int graphKeysAfter = countKeysWithPrefix(knowledgeStore, "graph:node:");
        assertEquals(
                graphKeysBefore,
                graphKeysAfter,
                "NoOpEntityDerivationStage must not write any graph:node:* keys");

        noOp.shutdown();
    }

    @Test
    @DisplayName("NoOpEntityDerivationStage: onWrite with null record does not throw")
    void noOpEntityDerivationNullRecordSafe() {
        NoOpEntityDerivationStage noOp = new NoOpEntityDerivationStage();
        noOp.onWrite(null, "default.test", 0L);
        noOp.shutdown();
    }

    private static Record withKey(Record record, Record.Key key) throws Exception {
        Field f = MapRecord.class.getDeclaredField("key");
        f.setAccessible(true);
        f.set(record, key);
        return record;
    }

    private static int countKeysWithPrefix(KnowledgeStore ks, String prefix) throws Exception {
        int count = 0;
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        try (var it = ks.scan(prefixBytes)) {
            while (it.hasNext()) {
                it.next();
                count++;
            }
        }
        return count;
    }
}
