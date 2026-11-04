package io.github.rawwise.axiom.memory.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.vector.EmbeddingProvider;
import io.github.rawwise.engine.vector.InMemoryVectorIndex;
import io.github.rawwise.engine.vector.VectorIndex;
import io.github.rawwise.governance.context.ContextManager;
import io.github.rawwise.governance.context.DefaultContextManager;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * E2E test for P1.3 — GraphEngineFactory.
 *
 * <p>Validates that system-property driven factory creates a usable {@link GraphEngine} instance
 * for both {@code inmemory} (self-contained) and {@code default} (metastore-backed) types, and that
 * the resulting engine supports the full create/add/list cycle.
 */
class GraphEngineFactoryE2ETest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private ContextManager contextManager;
    private VectorIndex vectorIndex;
    private EmbeddingProvider embeddingProvider;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        contextManager = new DefaultContextManager(metastore, 1, 1);
        embeddingProvider = text -> new float[1];
        vectorIndex = new InMemoryVectorIndex(embeddingProvider);
        clearProperties();
    }

    @AfterEach
    void tearDown() throws Exception {
        clearProperties();
        if (contextManager instanceof DefaultContextManager dcm) {
            dcm.close();
        }
        knowledgeStore.close();
        metastore.close();
    }

    private static void clearProperties() {
        System.clearProperty(GraphEngineFactory.KEY_TYPE);
    }

    @Test
    void defaultType_IsInMemory_CreateAddNodeListEndToEnd() throws Exception {
        // Default (no property set) -> inmemory
        GraphEngine engine =
                GraphEngineFactory.createFromSystemProperties(
                        null, null, contextManager, embeddingProvider, vectorIndex);

        assertNotNull(engine);
        assertInstanceOf(
                DefaultGraphEngine.class,
                engine,
                "inmemory type uses DefaultGraphEngine over InMemoryMetastore+InMemoryKnowledgeStore");

        runCreateAddListCycle(engine, "agent-default");
    }

    @Test
    void typeInMemory_Explicit_IndependentOfSuppliedMetastore() throws Exception {
        System.setProperty(GraphEngineFactory.KEY_TYPE, "inmemory");

        GraphEngine engine =
                GraphEngineFactory.createFromSystemProperties(
                        metastore, knowledgeStore, contextManager, embeddingProvider, vectorIndex);

        assertNotNull(engine);
        assertInstanceOf(DefaultGraphEngine.class, engine);

        // Create a graph; verify it does NOT pollute the supplied metastore — i.e. inmemory is
        // self-contained even when metastore is supplied.
        GraphSchema schema =
                engine.createGraph(
                        "agent-iso",
                        "g-iso",
                        "isolation test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        assertNotNull(schema.graphId());

        // Supplied metastore must be empty (no graph::schema:: keys).
        byte[] schemaPrefix = "graph::schema::".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var leaked = metastore.scanAll(schemaPrefix);
        assertTrue(
                leaked.isEmpty(),
                "inmemory engine must not write to the supplied production metastore");
    }

    @Test
    void typeDefault_BackedByMetastore_PersistsAcrossEngineInstances() throws Exception {
        System.setProperty(GraphEngineFactory.KEY_TYPE, "default");

        GraphEngine engine1 =
                GraphEngineFactory.createFromSystemProperties(
                        metastore, knowledgeStore, contextManager, embeddingProvider, vectorIndex);
        assertNotNull(engine1);
        assertInstanceOf(DefaultGraphEngine.class, engine1);

        GraphSchema schema =
                engine1.createGraph(
                        "agent-default",
                        "g-persist",
                        "persist test",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        engine1.addNode(schema.graphId(), "entity", Map.of("name", "n1"));

        // Re-create the engine over the same metastore+knowledgeStore — graph must still be
        // visible.
        GraphEngine engine2 =
                GraphEngineFactory.createFromSystemProperties(
                        metastore, knowledgeStore, contextManager, embeddingProvider, vectorIndex);
        List<GraphSchema> graphs = engine2.listGraphsByAgent("agent-default");
        assertEquals(1, graphs.size(), "default-type engine must persist via supplied metastore");
        assertEquals(schema.graphId(), graphs.get(0).graphId());

        List<GraphNode> nodes = engine2.queryNodes(schema.graphId(), "entity", null, 10);
        assertEquals(1, nodes.size());
    }

    @Test
    void typeDefault_NullDeps_FallsBackToInMemory() throws Exception {
        System.setProperty(GraphEngineFactory.KEY_TYPE, "default");

        GraphEngine engine =
                GraphEngineFactory.createFromSystemProperties(
                        null, null, contextManager, embeddingProvider, vectorIndex);

        assertNotNull(engine);
        // Still a DefaultGraphEngine but backed by inmemory (degraded). Verify operations work.
        runCreateAddListCycle(engine, "agent-fallback");
    }

    @Test
    void invalidType_FallsBackToInMemory() throws Exception {
        System.setProperty(GraphEngineFactory.KEY_TYPE, "invalid-graph-engine");

        GraphEngine engine =
                GraphEngineFactory.createFromSystemProperties(
                        null, null, contextManager, embeddingProvider, vectorIndex);

        assertNotNull(engine);
        runCreateAddListCycle(engine, "agent-invalid");
    }

    private static void runCreateAddListCycle(GraphEngine engine, String agentId) throws Exception {
        GraphSchema schema =
                engine.createGraph(
                        agentId,
                        "g-cycle",
                        "create/add/list",
                        List.of("entity"),
                        List.of("rel"),
                        Map.of());
        assertNotNull(schema.graphId());

        GraphNode n1 = engine.addNode(schema.graphId(), "entity", Map.of("name", "n1"));
        GraphNode n2 = engine.addNode(schema.graphId(), "entity", Map.of("name", "n2"));
        assertNotNull(n1.nodeId());
        assertNotNull(n2.nodeId());

        List<GraphNode> nodes = engine.queryNodes(schema.graphId(), "entity", null, 10);
        assertEquals(2, nodes.size());

        List<GraphSchema> agentGraphs = engine.listGraphsByAgent(agentId);
        assertEquals(1, agentGraphs.size());
        assertEquals(schema.graphId(), agentGraphs.get(0).graphId());

        GraphSchema reloaded = engine.getGraph(schema.graphId()).orElseThrow();
        assertEquals(2, reloaded.nodeCount(), "node counter must persist after addNode");
    }
}
