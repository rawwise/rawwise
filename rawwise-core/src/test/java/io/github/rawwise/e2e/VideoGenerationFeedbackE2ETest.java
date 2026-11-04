package io.github.rawwise.e2e;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.axiom.pipeline.cognition.*;
import io.github.rawwise.axiom.pipeline.state.*;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.View;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * simulate generate of E2E test.
 *
 * <p>scenario: item generate generate 50 frame , role "Alice" inNo. 1 frame , inNo. 37 frame
 * drift(). RawWise ofcondition construction detectto construct condition.
 *
 * <p>testdatasimulate of generate : 1. No. 1 frame: establish baseline(role + embedding) 2. No.
 * 2-36 frame: normal generation(similarity > 0.9) 3. No. 37 frame: drift detected(similarity =
 * 0.65) 4. condition construction: query baseline + embedding, generate CORRECT action
 */
class VideoGenerationFeedbackE2ETest {

    private InMemoryStateEngine stateEngine;
    private DefaultStateContract stateContract;
    private GraphEngine graphEngine;
    private KnowledgeStore knowledgeStore;
    private CognitionEngine cognitionEngine;
    private ConditionConstructionPipeline pipeline;
    private final ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

    // simulate generateof data
    private static final String VIDEO_SESSION = "agent:video_gen:session_2026_001";
    private static final String CHARACTER_ID = "Alice";
    private static final String GRAPH_ID = "graph:scene_city_street";
    private static final String BASELINE_VIEW_ID = "view:frame_001:alice";

    // Alice of baseline (No. 1 frame)
    private static final Map<String, String> ALICE_BASELINE_PROPERTIES =
            Map.of(
                    "clothing", "red jacket",
                    "hair", "black short hair",
                    "face_shape", "round",
                    "skin_tone", "fair",
                    "eye_color", "brown",
                    "accessory", "silver earrings");

    // simulateof CLIP embedding(512 ,)
    private static final float[] ALICE_BASELINE_EMBEDDING = {
        0.12f, -0.34f, 0.56f, 0.78f, -0.23f, 0.45f, -0.67f, 0.89f,
        0.11f, -0.22f, 0.33f, -0.44f, 0.55f, -0.66f, 0.77f, -0.88f,
        0.15f, -0.25f, 0.35f, -0.45f, 0.55f, -0.65f, 0.75f, -0.85f,
        0.21f, -0.31f, 0.41f, -0.51f, 0.61f, -0.71f, 0.81f, -0.91f
    };

    @BeforeEach
    void setUp() {
        stateEngine = new InMemoryStateEngine();
        stateContract = new DefaultStateContract(new GuardExpressionEvaluator());

        graphEngine = mock(GraphEngine.class);
        knowledgeStore = mock(KnowledgeStore.class);
        cognitionEngine = mock(CognitionEngine.class);

        pipeline = new ConditionConstructionPipeline(graphEngine, knowledgeStore, cognitionEngine);

        // CognitionEngine.deriveAction mock: package condition of Action
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
    void videoGeneration_37thFrameDrift_triggersConditionConstruction() throws Exception {
        // ========== 1. createstatus ==========
        StateSpace space =
                stateEngine.createStateSpace(
                        VIDEO_SESSION,
                        "alice_appearance_tracking",
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
                                        "consistent", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "consistent", "minor_drift", "similarity <= 0.9", Map.of()),
                                new StateTransition(
                                        "minor_drift", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "major_drift", "consistent", "similarity > 0.9", Map.of())),
                        Map.of(
                                "entity_id", CHARACTER_ID,
                                "graph_id", GRAPH_ID,
                                "video_session", VIDEO_SESSION,
                                "total_frames", "50",
                                "description", "Alice walking down a city street"));
        String spaceId = space.spaceId();

        // ========== 2. No. 1 frame: establish baseline ==========
        stateEngine.write(
                spaceId,
                CHARACTER_ID,
                "frame_1_data",
                "baseline",
                Map.of(
                        "similarity", "1.0",
                        "frame", "1",
                        "clothing", "red jacket",
                        "drift_type", "none"));

        // in GraphEngine inwrite Alice of baseline
        GraphNode aliceBaseline =
                new GraphNode(
                        CHARACTER_ID,
                        GRAPH_ID,
                        "CHARACTER",
                        ALICE_BASELINE_PROPERTIES,
                        10,
                        0.02f,
                        BASELINE_VIEW_ID,
                        "clip-vit-large",
                        "Alice - main character",
                        1,
                        0L,
                        0L);
        when(graphEngine.getNode(GRAPH_ID, CHARACTER_ID)).thenReturn(Optional.of(aliceBaseline));

        // in KnowledgeStore inwrite baseline View(embedding)
        View baselineView =
                new View(
                        BASELINE_VIEW_ID,
                        List.of("chunk:frame_001"),
                        View.ViewType.EMBEDDING_INDEX,
                        1,
                        "{\"frame\":1,\"description\":\"Alice in red jacket on city street\"}"
                                .getBytes(),
                        Optional.of(ALICE_BASELINE_EMBEDDING),
                        Optional.of(0.95),
                        List.of());
        when(knowledgeStore.get(("view:" + BASELINE_VIEW_ID).getBytes()))
                .thenReturn(Optional.of(mapper.writeValueAsBytes(baselineView)));

        // ========== 3. No. 2-36 frame: normal generation(similarity > 0.9)==========
        for (int frame = 2; frame <= 36; frame++) {
            double sim = 0.92 + (frame % 5) * 0.01; // 0.92-0.96 fluctuation
            stateEngine.write(
                    spaceId,
                    CHARACTER_ID,
                    "frame_" + frame,
                    "consistent",
                    Map.of(
                            "similarity", String.format("%.2f", sim),
                            "frame", String.valueOf(frame),
                            "drift_type", "none"));
        }

        // verifybefore 36 frame has violation
        StateEntry frame36Entry = stateEngine.read(spaceId, CHARACTER_ID);
        var frame36Violation = stateContract.validate(space, frame36Entry, "consistent");
        assertTrue(frame36Violation.isEmpty(), "Frame 36 should not have violation");

        // ========== 4. No. 37 frame: drift detected ==========
        // simulate: CLIP calculate frame_37 and frame_1 of = 0.65(from)
        StateEntry driftEntry =
                stateEngine.write(
                        spaceId,
                        CHARACTER_ID,
                        "frame_37_drift",
                        "consistent",
                        Map.of(
                                "similarity", "0.65",
                                "frame", "37",
                                "drift_type", "clothing_color_changed",
                                "expected_clothing", "red jacket",
                                "actual_clothing", "blue jacket",
                                "drift_description",
                                        "Alice's jacket color changed from red to blue at frame 37"));

        // StateContract detectto guard
        var violation = stateContract.validate(space, driftEntry, "consistent");

        assertTrue(violation.isPresent(), "Frame 37 should trigger guard violation");
        assertTrue(
                violation.get().reason().contains("Guard failed"),
                "Violation reason should mention guard");
        assertTrue(
                violation.get().reason().contains("similarity > 0.9"),
                "Violation should reference the guard expression");
        assertEquals(CHARACTER_ID, violation.get().entryKey(), "Violation should reference Alice");

        System.out.println("=== 偏差检测 ===");
        System.out.println("Frame: 37");
        System.out.println("Violation: " + violation.get().reason());

        // ========== 5. condition construction: query baseline + construct CORRECT action
        // ==========
        var spec = ConditionConstructionSpec.defaults(GRAPH_ID);
        Optional<Action> result = pipeline.constructCondition(violation.get(), spec);

        assertTrue(result.isPresent(), "CORRECT action should be constructed");
        assertEquals(Action.ActionType.CORRECT, result.get().actionType());
        assertTrue(result.get().executable());

        // parse Action content, verifycondition
        @SuppressWarnings("unchecked")
        Map<String, Object> conditionParams = mapper.readValue(result.get().content(), Map.class);

        System.out.println("\n=== 条件构造结果 ===");
        System.out.println("Action ID: " + result.get().actionId());
        System.out.println("Action Type: " + result.get().actionType());
        System.out.println("Condition Type: " + conditionParams.get("condition_type"));
        System.out.println("Entity ID: " + conditionParams.get("entity_id"));
        System.out.println("Drift Description: " + conditionParams.get("drift_description"));
        System.out.println("Text Condition: " + conditionParams.get("text_condition"));
        System.out.println("Reference Properties: " + conditionParams.get("reference_properties"));
        System.out.println(
                "Has Reference Embedding: " + conditionParams.containsKey("reference_embedding"));
        System.out.println("Embedding Model: " + conditionParams.get("embedding_model"));

        // verifycondition package correctof baseline data
        assertTrue(conditionParams.containsKey("text_condition"), "Should have text condition");
        assertTrue(
                conditionParams.containsKey("reference_embedding"),
                "Should have reference embedding");
        assertTrue(
                conditionParams.containsKey("reference_properties"),
                "Should have reference properties");

        String textCondition = (String) conditionParams.get("text_condition");
        assertTrue(
                textCondition.contains("red jacket"), "Text condition should mention red jacket");
        assertTrue(
                textCondition.contains("black short hair"),
                "Text condition should mention black short hair");

        @SuppressWarnings("unchecked")
        Map<String, String> refProps =
                (Map<String, String>) conditionParams.get("reference_properties");
        assertEquals(
                "red jacket", refProps.get("clothing"), "Reference clothing should be red jacket");
        assertEquals(
                "black short hair",
                refProps.get("hair"),
                "Reference hair should be black short hair");

        // verify embedding becorrectload(Jackson column aftertypecancan)
        Object embeddingObj = conditionParams.get("reference_embedding");
        assertNotNull(embeddingObj, "Reference embedding should not be null");
        System.out.println("Embedding type: " + embeddingObj.getClass().getSimpleName());

        System.out.println("\n=== 验证通过 ===");
        System.out.println("✅ 偏差检测：第37帧 similarity=0.65 < 0.9 阈值，触发 guard violation");
        System.out.println("✅ 条件构造：从 GraphEngine 查询到 Alice 的 baseline 属性");
        System.out.println("✅ 条件构造：从 KnowledgeStore 查询到 baseline embedding");
        System.out.println(
                "✅ CORRECT action 生成：包含 text_condition + reference_embedding + reference_properties");
        System.out.println("✅ 修正条件可传递给视频模型重生成第37帧");

        // verify CognitionEngine becorrectcall
        verify(cognitionEngine)
                .deriveAction(
                        anyList(),
                        eq(Action.ActionType.CORRECT),
                        argThat(
                                p ->
                                        p.containsKey("reference_embedding")
                                                && p.containsKey("text_condition")
                                                && p.containsKey("reference_properties")
                                                && p.containsKey("drift_description")
                                                && "hybrid".equals(p.get("condition_type"))));
    }

    @Test
    void videoGeneration_progressiveDrift_trackingAcrossFrames() throws Exception {
        StateSpace space =
                stateEngine.createStateSpace(
                        VIDEO_SESSION,
                        "alice_progressive_drift",
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
                                        "consistent", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "consistent", "minor_drift", "similarity <= 0.9", Map.of()),
                                new StateTransition(
                                        "minor_drift", "minor_drift", "similarity > 0.7", Map.of()),
                                new StateTransition(
                                        "minor_drift",
                                        "major_drift",
                                        "similarity <= 0.7",
                                        Map.of()),
                                new StateTransition(
                                        "minor_drift", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "major_drift", "consistent", "similarity > 0.9", Map.of())),
                        Map.of("description", "Progressive drift scenario"));
        String spaceId = space.spaceId();

        GraphNode aliceBaseline =
                new GraphNode(
                        CHARACTER_ID,
                        GRAPH_ID,
                        "CHARACTER",
                        ALICE_BASELINE_PROPERTIES,
                        10,
                        0.02f,
                        BASELINE_VIEW_ID,
                        "clip-vit-large",
                        "Alice",
                        1,
                        0L,
                        0L);
        when(graphEngine.getNode(GRAPH_ID, CHARACTER_ID)).thenReturn(Optional.of(aliceBaseline));

        View baselineView =
                new View(
                        BASELINE_VIEW_ID,
                        List.of(),
                        View.ViewType.EMBEDDING_INDEX,
                        1,
                        new byte[0],
                        Optional.of(ALICE_BASELINE_EMBEDDING),
                        Optional.of(0.95),
                        List.of());
        when(knowledgeStore.get(("view:" + BASELINE_VIEW_ID).getBytes()))
                .thenReturn(Optional.of(mapper.writeValueAsBytes(baselineView)));

        System.out.println("=== 渐进式漂移跟踪 ===");

        // Frame 1: baseline
        stateEngine.write(
                spaceId,
                CHARACTER_ID,
                "frame_1",
                "baseline",
                Map.of("similarity", "1.0", "frame", "1"));

        // Frame 10: slight drift (sim=0.88, minor_drift)
        StateEntry f10 =
                stateEngine.write(
                        spaceId,
                        CHARACTER_ID,
                        "frame_10",
                        "consistent",
                        Map.of("similarity", "0.88", "frame", "10"));
        var v10 = stateContract.validate(space, f10, "consistent");
        assertTrue(v10.isPresent(), "Frame 10: similarity=0.88 should trigger minor drift");
        System.out.println("Frame 10: similarity=0.88 → VIOLATION (minor drift)");

        // Frame 20: recovered (sim=0.93)
        StateEntry f20 =
                stateEngine.write(
                        spaceId,
                        CHARACTER_ID,
                        "frame_20",
                        "consistent",
                        Map.of("similarity", "0.93", "frame", "20"));
        var v20 = stateContract.validate(space, f20, "consistent");
        assertTrue(v20.isEmpty(), "Frame 20: similarity=0.93 should pass");
        System.out.println("Frame 20: similarity=0.93 → OK (recovered)");

        // Frame 37: major drift (sim=0.65)
        StateEntry f37 =
                stateEngine.write(
                        spaceId,
                        CHARACTER_ID,
                        "frame_37",
                        "consistent",
                        Map.of(
                                "similarity",
                                "0.65",
                                "frame",
                                "37",
                                "drift_type",
                                "clothing_color_changed"));
        var v37 = stateContract.validate(space, f37, "consistent");
        assertTrue(v37.isPresent(), "Frame 37: similarity=0.65 should trigger major drift");
        System.out.println("Frame 37: similarity=0.65 → VIOLATION (major drift)");

        // for frame 37 construct condition
        var spec = ConditionConstructionSpec.defaults(GRAPH_ID);
        Optional<Action> action = pipeline.constructCondition(v37.get(), spec);

        assertTrue(action.isPresent());
        System.out.println("Frame 37: CORRECT action constructed → " + action.get().actionId());

        // Frame 38: afterrecovery (sim=0.94)
        StateEntry f38 =
                stateEngine.write(
                        spaceId,
                        CHARACTER_ID,
                        "frame_38_corrected",
                        "consistent",
                        Map.of("similarity", "0.94", "frame", "38", "correction_applied", "true"));
        var v38 = stateContract.validate(space, f38, "consistent");
        assertTrue(v38.isEmpty(), "Frame 38: after correction similarity=0.94 should pass");
        System.out.println("Frame 38: similarity=0.94 → OK (correction applied)");

        System.out.println("\n=== 渐进式漂移跟踪验证通过 ===");
        System.out.println("✅ Frame 10: 检测到 minor drift");
        System.out.println("✅ Frame 20: 自然恢复");
        System.out.println("✅ Frame 37: 检测到 major drift + 构造修正条件");
        System.out.println("✅ Frame 38: 修正后恢复");
    }

    @Test
    void videoGeneration_textOnlyCondition_forTextBasedVideoModel() throws Exception {
        StateSpace space =
                stateEngine.createStateSpace(
                        VIDEO_SESSION,
                        "alice_text_condition",
                        "baseline",
                        List.of("baseline", "consistent", "drift"),
                        List.of(
                                new StateTransition(
                                        "baseline", "consistent", "similarity > 0.9", Map.of()),
                                new StateTransition(
                                        "baseline", "drift", "similarity <= 0.7", Map.of()),
                                new StateTransition(
                                        "drift", "consistent", "similarity > 0.9", Map.of())),
                        Map.of("model_type", "text-only"));
        String spaceId = space.spaceId();

        stateEngine.write(
                spaceId,
                CHARACTER_ID,
                "frame_1",
                "baseline",
                Map.of("similarity", "1.0", "frame", "1"));

        StateEntry drift =
                stateEngine.write(
                        spaceId,
                        CHARACTER_ID,
                        "frame_25",
                        "consistent",
                        Map.of(
                                "similarity",
                                "0.60",
                                "frame",
                                "25",
                                "drift_type",
                                "hair_color_changed"));

        var violation = stateContract.validate(space, drift, "consistent");
        assertTrue(violation.isPresent());

        GraphNode baseline =
                new GraphNode(
                        CHARACTER_ID,
                        GRAPH_ID,
                        "CHARACTER",
                        ALICE_BASELINE_PROPERTIES,
                        5,
                        0.01f,
                        BASELINE_VIEW_ID,
                        "clip-vit",
                        "Alice",
                        1,
                        0L,
                        0L);
        when(graphEngine.getNode(GRAPH_ID, CHARACTER_ID)).thenReturn(Optional.of(baseline));

        // text-only : query embedding
        var spec = ConditionConstructionSpec.textOnly(GRAPH_ID);
        Optional<Action> action = pipeline.constructCondition(violation.get(), spec);

        assertTrue(action.isPresent());

        @SuppressWarnings("unchecked")
        Map<String, Object> params = mapper.readValue(action.get().content(), Map.class);

        System.out.println("=== Text-Only 条件构造（文本条件视频模型）===");
        System.out.println("Text Condition: " + params.get("text_condition"));
        System.out.println("Has Embedding: " + params.containsKey("reference_embedding"));

        assertTrue(params.containsKey("text_condition"));
        assertFalse(
                params.containsKey("reference_embedding"),
                "Text-only mode should not include embedding");
        verify(knowledgeStore, never()).get(any());

        System.out.println("✅ Text-only 模式：只生成文本条件，不查询 embedding（适合纯文本 prompt 视频模型）");
    }
}
