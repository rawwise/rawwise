package io.github.rawwise.axiom.memory.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.axiom.memory.NoOpSummarizationService;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.InMemoryVectorIndex;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.governance.context.DefaultContextManager;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * E2E test for P1.6 — DefaultGraphEngine node/edge counter persistence.
 *
 * <p>Validates that the counter-CAS path actually persists nodeCount/edgeCount across calls and
 * survives reload + concurrent writes. Prior to P1.6 the counter was read from a non-updated
 * GraphSchema record, causing CAS-conflict on every second addNode / addEdge call.
 */
class GraphEngineCountE2ETest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private ContextManager contextManager;
    private GraphEngine engine;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        contextManager = new DefaultContextManager(metastore, 1, 1);
        EmbeddingProvider embeddingProvider = text -> new float[1];
        VectorIndex vectorIndex = new InMemoryVectorIndex(embeddingProvider);
        GraphEngineConfig config =
                new GraphEngineConfig(
                        metastore,
                        knowledgeStore,
                        contextManager,
                        new NoOpSummarizationService(),
                        vectorIndex,
                        embeddingProvider,
                        new NoOpKnowledgeExtractor(),
                        null);
        engine = new DefaultGraphEngine(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (contextManager instanceof DefaultContextManager dcm) {
            dcm.close();
        }
        knowledgeStore.close();
        metastore.close();
    }

    @Test
    void addNodeHundredTimes_AllSucceed_AndQueryReturnsAll() throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        "agent-1",
                        "g-nodes",
                        "P1.6 add 100 nodes",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();

        for (int i = 0; i < 100; i++) {
            GraphNode node = engine.addNode(graphId, "entity", Map.of("idx", Integer.toString(i)));
            assertNotNull(node);
        }

        List<GraphNode> nodes = engine.queryNodes(graphId, "entity", null, 1000);
        assertEquals(100, nodes.size(), "all 100 nodes must be queryable");

        GraphSchema reloaded = engine.getGraph(graphId).orElseThrow();
        assertEquals(
                100,
                reloaded.nodeCount(),
                "reloaded schema.nodeCount must reflect persisted counter");
    }

    @Test
    void addEdgeTwoHundredTimes_AllSucceed_AndOutEdgesReturnsAll() throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        "agent-2",
                        "g-edges",
                        "P1.6 add 200 edges",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();

        GraphNode src = engine.addNode(graphId, "entity", Map.of("name", "src"));
        GraphNode dst = engine.addNode(graphId, "entity", Map.of("name", "dst"));

        for (int i = 0; i < 200; i++) {
            engine.addEdge(
                    graphId,
                    src.nodeId(),
                    "rel",
                    dst.nodeId(),
                    Map.of("idx", Integer.toString(i)),
                    1.0f);
        }

        List<GraphEdge> outEdges = engine.getOutEdges(graphId, src.nodeId(), "rel");
        assertEquals(200, outEdges.size(), "all 200 edges must be retrievable via getOutEdges");

        GraphSchema reloaded = engine.getGraph(graphId).orElseThrow();
        assertEquals(
                200,
                reloaded.edgeCount(),
                "reloaded schema.edgeCount must reflect persisted counter");
    }

    @Test
    void concurrentAddNode_TwoThreadsFiftyEach_ResultsInHundred() throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        "agent-3",
                        "g-concurrent",
                        "P1.6 concurrent addNode",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();

        int threads = 2;
        int perThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        List<Throwable> errors = new ArrayList<>();
        AtomicInteger ok = new AtomicInteger();

        for (int t = 0; t < threads; t++) {
            final int tid = t;
            pool.submit(
                    () -> {
                        try {
                            start.await();
                            for (int i = 0; i < perThread; i++) {
                                engine.addNode(
                                        graphId,
                                        "entity",
                                        Map.of(
                                                "tid",
                                                Integer.toString(tid),
                                                "idx",
                                                Integer.toString(i)));
                                ok.incrementAndGet();
                            }
                        } catch (Throwable e) {
                            synchronized (errors) {
                                errors.add(e);
                            }
                        } finally {
                            done.countDown();
                        }
                    });
        }
        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS), "concurrent addNode should finish within 60s");
        pool.shutdownNow();

        assertTrue(errors.isEmpty(), "no thread should have failed, got: " + errors);
        assertEquals(threads * perThread, ok.get(), "all addNode calls should succeed");

        GraphSchema reloaded = engine.getGraph(graphId).orElseThrow();
        assertEquals(
                threads * perThread,
                reloaded.nodeCount(),
                "persisted nodeCount must equal total successful adds");

        List<GraphNode> nodes = engine.queryNodes(graphId, "entity", null, 1000);
        assertEquals(
                threads * perThread,
                nodes.size(),
                "queryNodes must return all concurrently-added nodes");
    }

    @Test
    void reloadGraph_AfterAdds_ReflectsPersistedCounters() throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        "agent-4",
                        "g-reload",
                        "P1.6 reload",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        String graphId = schema.graphId();

        for (int i = 0; i < 7; i++) {
            engine.addNode(graphId, "entity", Map.of("idx", Integer.toString(i)));
        }
        GraphNode a = engine.addNode(graphId, "entity", Map.of("name", "a"));
        GraphNode b = engine.addNode(graphId, "entity", Map.of("name", "b"));
        for (int i = 0; i < 3; i++) {
            engine.addEdge(graphId, a.nodeId(), "rel", b.nodeId(), Map.of(), 1.0f);
        }

        GraphSchema reloaded = engine.getGraph(graphId).orElseThrow();
        assertEquals(9, reloaded.nodeCount(), "reloaded schema.nodeCount must persist");
        assertEquals(3, reloaded.edgeCount(), "reloaded schema.edgeCount must persist");
        assertEquals(
                schema.maxNodes(),
                reloaded.maxNodes(),
                "maxNodes must remain stable across reload");
        assertEquals(
                schema.maxEdges(),
                reloaded.maxEdges(),
                "maxEdges must remain stable across reload");
    }
}
