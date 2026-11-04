package io.github.rawwise.e2e;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphException;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.axiom.pipeline.cognition.*;
import io.github.rawwise.axiom.pipeline.state.*;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.View;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConditionConstructionIntegrationE2ETest {

    private InMemoryStateEngine stateEngine;
    private DefaultStateContract stateContract;
    private GraphEngine graphEngine;
    private KnowledgeStore knowledgeStore;
    private CognitionEngine cognitionEngine;
    private ConditionConstructionPipeline pipeline;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

    @BeforeEach
    void setUp() {
        stateEngine = new InMemoryStateEngine();
        stateContract = new DefaultStateContract(new GuardExpressionEvaluator());

        graphEngine = mock(GraphEngine.class);
        knowledgeStore = mock(KnowledgeStore.class);
        cognitionEngine = mock(CognitionEngine.class);

        pipeline = new ConditionConstructionPipeline(graphEngine, knowledgeStore, cognitionEngine);

        when(cognitionEngine.deriveAction(anyList(), any(), anyMap()))
                .thenAnswer(
                        invocation -> {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> params = invocation.getArgument(2);
                            byte[] content = mapper.writeValueAsBytes(params);
                            return new Action(
                                    "act:" + java.util.UUID.randomUUID().toString().substring(0, 8),
                                    invocation.getArgument(0),
                                    invocation.getArgument(1),
                                    content,
                                    true);
                        });
    }

    @Test
    void fullPipeline_stateViolationProducesCorrectActionWithReferenceData() throws Exception {
        StateSpace space =
                stateEngine.createStateSpace(
                        "agent:video_gen:session_001",
                        "character_A_appearance",
                        "baseline",
                        List.of("baseline", "consistent", "minor_drift", "major_drift"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "baseline", "minor_drift", "similarity > 0.7", Map.of()),
                                new StateTransition(
                                        "baseline", "major_drift", "similarity <= 0.7", Map.of()),
                                new StateTransition(
                                        "minor_drift", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "major_drift", "consistent", "similarity > 0.9", Map.of())),
                        Map.of("entity_id", "entity:character_A", "graph_id", "graph:scene_1"));
        String spaceId = space.spaceId();

        stateEngine.write(
                spaceId,
                "character_A",
                "baseline_value",
                "baseline",
                Map.of("similarity", "1.0", "frame", "1"));

        StateEntry driftEntry =
                stateEngine.write(
                        spaceId,
                        "character_A",
                        "drift_value",
                        "baseline",
                        Map.of("similarity", "0.65", "frame", "37"));

        var violation = stateContract.validate(space, driftEntry, "consistent");

        assertTrue(violation.isPresent());
        assertTrue(violation.get().reason().contains("Guard failed"));

        var baselineNode =
                new GraphNode(
                        "entity:character_A",
                        "graph:scene_1",
                        "CHARACTER",
                        Map.of(
                                "clothing",
                                "red jacket",
                                "hair",
                                "black short hair",
                                "face",
                                "round"),
                        5,
                        0.01f,
                        "frame_1_view",
                        "clip-vit",
                        "character A",
                        1,
                        0L,
                        0L);

        when(graphEngine.getNode("graph:scene_1", "character_A"))
                .thenReturn(Optional.of(baselineNode));

        View mockView =
                new View(
                        "frame_1_view",
                        List.of(),
                        View.ViewType.EMBEDDING_INDEX,
                        1,
                        new byte[0],
                        Optional.of(new float[] {0.12f, -0.34f, 0.56f, 0.78f}),
                        Optional.empty(),
                        List.of());
        when(knowledgeStore.get("view:frame_1_view".getBytes()))
                .thenReturn(Optional.of(mapper.writeValueAsBytes(mockView)));

        var spec = ConditionConstructionSpec.defaults("graph:scene_1");
        Optional<Action> result = pipeline.constructCondition(violation.get(), spec);

        assertTrue(result.isPresent());
        assertEquals(Action.ActionType.CORRECT, result.get().actionType());
        assertTrue(result.get().executable());

        @SuppressWarnings("unchecked")
        Map<String, Object> params = mapper.readValue(result.get().content(), Map.class);
        assertTrue(params.containsKey("reference_embedding"));
        assertTrue(params.containsKey("text_condition"));
        assertTrue(params.toString().contains("red jacket"));
        assertTrue(params.toString().contains("black short hair"));
        assertTrue(params.containsKey("drift_description"));

        verify(cognitionEngine)
                .deriveAction(
                        anyList(),
                        eq(Action.ActionType.CORRECT),
                        argThat(
                                p ->
                                        p.containsKey("reference_embedding")
                                                && p.containsKey("text_condition")));
    }

    @Test
    void fullPipeline_textConditionCanBeEnhancedBeforeActionDerivation() throws Exception {
        pipeline =
                new ConditionConstructionPipeline(
                        graphEngine,
                        knowledgeStore,
                        cognitionEngine,
                        (condition, context) ->
                                new TextConditionEnhancement(
                                        condition,
                                        condition
                                                + " | learned_constraint=maintain stable lighting and smooth motion",
                                        List.of("maintain stable lighting and smooth motion"),
                                        List.of("memory:prompt_001")));

        StateSpace space =
                stateEngine.createStateSpace(
                        "agent:video_gen:session_enhanced",
                        "character_A_appearance",
                        "baseline",
                        List.of("baseline", "consistent", "major_drift"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "baseline", "major_drift", "similarity <= 0.7", Map.of())),
                        Map.of("entity_id", "entity:character_A", "graph_id", "graph:scene_1"));

        StateEntry driftEntry =
                stateEngine.write(
                        space.spaceId(),
                        "character_A",
                        "drift_value",
                        "baseline",
                        Map.of("similarity", "0.65", "frame", "37"));
        var violation = stateContract.validate(space, driftEntry, "consistent");
        assertTrue(violation.isPresent());

        var baselineNode =
                new GraphNode(
                        "entity:character_A",
                        "graph:scene_1",
                        "CHARACTER",
                        Map.of("clothing", "red jacket", "hair", "black short hair"),
                        5,
                        0.01f,
                        null,
                        "clip-vit",
                        "character A",
                        1,
                        0L,
                        0L);
        when(graphEngine.getNode("graph:scene_1", "character_A"))
                .thenReturn(Optional.of(baselineNode));

        Optional<Action> result =
                pipeline.constructCondition(
                        violation.get(), ConditionConstructionSpec.textOnly("graph:scene_1"));

        assertTrue(result.isPresent());
        @SuppressWarnings("unchecked")
        Map<String, Object> params = mapper.readValue(result.get().content(), Map.class);
        assertTrue(
                params.get("text_condition")
                        .toString()
                        .contains("learned_constraint=maintain stable lighting"));
        assertTrue(params.containsKey("condition_enhanced"));
        assertTrue(params.containsKey("original_text_condition"));
    }

    @Test
    void fullPipeline_enhancerFailureFallsBackToOriginalTextCondition() throws Exception {
        pipeline =
                new ConditionConstructionPipeline(
                        graphEngine,
                        knowledgeStore,
                        cognitionEngine,
                        (condition, context) -> {
                            throw new IllegalStateException("memory unavailable");
                        });

        StateSpace space =
                stateEngine.createStateSpace(
                        "agent:video_gen:session_enhancer_failure",
                        "character_A_appearance",
                        "baseline",
                        List.of("baseline", "consistent", "major_drift"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "baseline", "major_drift", "similarity <= 0.7", Map.of())),
                        Map.of("entity_id", "entity:character_A", "graph_id", "graph:scene_1"));

        StateEntry driftEntry =
                stateEngine.write(
                        space.spaceId(),
                        "character_A",
                        "drift_value",
                        "baseline",
                        Map.of("similarity", "0.65", "frame", "37"));
        var violation = stateContract.validate(space, driftEntry, "consistent");
        assertTrue(violation.isPresent());

        var baselineNode =
                new GraphNode(
                        "entity:character_A",
                        "graph:scene_1",
                        "CHARACTER",
                        Map.of("clothing", "red jacket", "hair", "black short hair"),
                        5,
                        0.01f,
                        null,
                        "clip-vit",
                        "character A",
                        1,
                        0L,
                        0L);
        when(graphEngine.getNode("graph:scene_1", "character_A"))
                .thenReturn(Optional.of(baselineNode));

        Optional<Action> result =
                assertDoesNotThrow(
                        () ->
                                pipeline.constructCondition(
                                        violation.get(),
                                        ConditionConstructionSpec.textOnly("graph:scene_1")));

        assertTrue(result.isPresent());
        @SuppressWarnings("unchecked")
        Map<String, Object> params = mapper.readValue(result.get().content(), Map.class);
        assertTrue(params.get("text_condition").toString().contains("red jacket"));
        assertEquals(true, params.get("condition_enhancement_failed"));
        assertTrue(
                params.get("condition_enhancement_error")
                        .toString()
                        .contains("memory unavailable"));
    }

    @Test
    void fullPipeline_embeddingStoreFailureFallsBackToTextCondition() throws Exception {
        StateSpace space =
                stateEngine.createStateSpace(
                        "agent:video_gen:session_embedding_failure",
                        "character_A_appearance",
                        "baseline",
                        List.of("baseline", "consistent", "major_drift"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "baseline", "major_drift", "similarity <= 0.7", Map.of())),
                        Map.of("entity_id", "entity:character_A", "graph_id", "graph:scene_1"));
        StateEntry driftEntry =
                stateEngine.write(
                        space.spaceId(),
                        "character_A",
                        "drift_value",
                        "baseline",
                        Map.of("similarity", "0.65"));
        var violation = stateContract.validate(space, driftEntry, "consistent");
        assertTrue(violation.isPresent());

        var baselineNode =
                new GraphNode(
                        "entity:character_A",
                        "graph:scene_1",
                        "CHARACTER",
                        Map.of("clothing", "red jacket"),
                        5,
                        0.01f,
                        "frame_1_view",
                        "clip-vit",
                        "character A",
                        1,
                        0L,
                        0L);
        when(graphEngine.getNode("graph:scene_1", "character_A"))
                .thenReturn(Optional.of(baselineNode));
        when(knowledgeStore.get("view:frame_1_view".getBytes()))
                .thenThrow(new IllegalStateException("store unavailable"));

        Optional<Action> result =
                assertDoesNotThrow(
                        () ->
                                pipeline.constructCondition(
                                        violation.get(),
                                        ConditionConstructionSpec.defaults("graph:scene_1")));

        assertTrue(result.isPresent());
        @SuppressWarnings("unchecked")
        Map<String, Object> params = mapper.readValue(result.get().content(), Map.class);
        assertTrue(params.containsKey("text_condition"));
        assertFalse(params.containsKey("reference_embedding"));
    }

    @Test
    void textConditionEnhancementEnhancedIsNullSafe() {
        var enhancement = new TextConditionEnhancement(null, "enhanced", null, null);

        assertDoesNotThrow(enhancement::enhanced);
        assertTrue(enhancement.enhanced());
        assertEquals(List.of(), enhancement.constraints());
        assertEquals(List.of(), enhancement.sourceIds());
    }

    @Test
    void textConditionEnhancementKeepsOriginalWhenEnhancedConditionIsNull() {
        var enhancement =
                new TextConditionEnhancement("original", null, List.of("constraint"), null);

        assertEquals("original", enhancement.enhancedCondition());
        assertTrue(enhancement.enhanced());
    }

    @Test
    void fullPipeline_noViolationProducesNoAction() throws Exception {
        StateSpace space =
                stateEngine.createStateSpace(
                        "agent:video_gen:session_002",
                        "character_B_appearance",
                        "baseline",
                        List.of("baseline", "consistent"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of())),
                        Map.of("entity_id", "entity:character_B", "graph_id", "graph:scene_2"));

        StateEntry consistentEntry =
                stateEngine.write(
                        space.spaceId(),
                        "character_B",
                        "value",
                        "baseline",
                        Map.of("similarity", "0.95", "frame", "5"));

        var violation = stateContract.validate(space, consistentEntry, "consistent");

        assertTrue(violation.isEmpty());
        verify(cognitionEngine, never()).deriveAction(anyList(), any(), anyMap());
    }

    @Test
    void fullPipeline_textOnlyConditionSkipsEmbedding() throws Exception {
        StateSpace space =
                stateEngine.createStateSpace(
                        "agent:text_gen:session_003",
                        "fact_consistency",
                        "baseline",
                        List.of("baseline", "consistent", "drift"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "accuracy > 0.95", Map.of()),
                                new StateTransition(
                                        "baseline", "drift", "accuracy <= 0.95", Map.of())),
                        Map.of("entity_id", "entity:fact_001", "graph_id", "graph:facts"));

        StateEntry driftEntry =
                stateEngine.write(
                        space.spaceId(),
                        "fact_001",
                        "drift",
                        "baseline",
                        Map.of("accuracy", "0.80"));

        var violation = stateContract.validate(space, driftEntry, "consistent");

        assertTrue(violation.isPresent());

        var baselineNode =
                new GraphNode(
                        "entity:fact_001",
                        "graph:facts",
                        "FACT",
                        Map.of("statement", "Earth orbits the Sun", "source", "NASA"),
                        3,
                        0.01f,
                        "fact_001_view",
                        "text-embed",
                        "Earth orbits the Sun",
                        1,
                        0L,
                        0L);

        when(graphEngine.getNode("graph:facts", "fact_001")).thenReturn(Optional.of(baselineNode));

        var spec = ConditionConstructionSpec.textOnly("graph:facts");
        Optional<Action> action = pipeline.constructCondition(violation.get(), spec);

        assertTrue(action.isPresent());
        verify(knowledgeStore, never()).get(any());
    }

    @Test
    void fullPipeline_concurrentViolationsProduceIndependentActions() throws Exception {
        StateSpace space =
                stateEngine.createStateSpace(
                        "agent:video_gen:session_004",
                        "multi_character_appearance",
                        "baseline",
                        List.of("baseline", "consistent", "major_drift"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "baseline", "major_drift", "similarity <= 0.7", Map.of())),
                        Map.of());
        String spaceId = space.spaceId();

        String[] entities = {"character_A", "character_B", "character_C"};
        var violations = new java.util.ArrayList<ContractViolation>();

        for (int i = 0; i < entities.length; i++) {
            StateEntry driftEntry =
                    stateEngine.write(
                            spaceId,
                            entities[i],
                            "drift",
                            "baseline",
                            Map.of("similarity", String.valueOf(0.60 + i * 0.05)));
            var v = stateContract.validate(space, driftEntry, "consistent");
            assertTrue(v.isPresent());
            violations.add(v.get());

            var node =
                    new GraphNode(
                            "entity:" + entities[i],
                            "graph:scene_1",
                            "CHARACTER",
                            Map.of("clothing", "outfit_" + i),
                            3,
                            0.01f,
                            null,
                            "clip-vit",
                            entities[i],
                            1,
                            0L,
                            0L);
            when(graphEngine.getNode("graph:scene_1", entities[i])).thenReturn(Optional.of(node));
        }

        var spec = ConditionConstructionSpec.textOnly("graph:scene_1");
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var futures =
                    violations.stream()
                            .map(v -> executor.submit(() -> pipeline.constructCondition(v, spec)))
                            .toList();

            for (var future : futures) {
                Optional<Action> result = future.get(5, TimeUnit.SECONDS);
                assertTrue(result.isPresent());
            }
        }

        verify(cognitionEngine, times(entities.length)).deriveAction(anyList(), any(), anyMap());
    }

    @Test
    void fullPipeline_graphEngineFailureReturnsEmptyOptional() throws Exception {
        StateSpace space =
                stateEngine.createStateSpace(
                        "agent:video_gen:session_005",
                        "character_C_appearance",
                        "baseline",
                        List.of("baseline", "consistent", "drift"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "baseline", "drift", "similarity <= 0.7", Map.of())),
                        Map.of("entity_id", "entity:character_C", "graph_id", "graph:scene_3"));
        String spaceId = space.spaceId();

        StateEntry driftEntry =
                stateEngine.write(
                        spaceId, "character_C", "drift", "baseline", Map.of("similarity", "0.55"));

        var violation = stateContract.validate(space, driftEntry, "consistent");
        assertTrue(violation.isPresent());

        when(graphEngine.getNode(anyString(), anyString()))
                .thenThrow(new GraphException("graph:scene_3", "connection refused"));

        var spec = ConditionConstructionSpec.defaults("graph:scene_3");
        Optional<Action> result = pipeline.constructCondition(violation.get(), spec);

        assertTrue(result.isEmpty());
        verify(cognitionEngine, never()).deriveAction(anyList(), any(), anyMap());
    }
}
