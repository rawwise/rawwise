package io.github.rawwise.axiom.pipeline.cognition;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.pipeline.evaluation.DefaultEvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.ImmutabilitySource;
import io.github.rawwise.model.record.Raw;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Semantic;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.Types;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("CognitionEngine E2E - cognition full-chain business scenario test")
class CognitionEngineE2ETest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private EvaluationEngine evaluationEngine;
    private ActionExecutor actionExecutor;
    private CognitionEngine cognitionEngine;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        evaluationEngine = new DefaultEvaluationEngine(metastore, knowledgeStore);
        actionExecutor = new DefaultActionExecutor(metastore, knowledgeStore, null);
        cognitionEngine =
                new DefaultCognitionEngine(
                        metastore, knowledgeStore, evaluationEngine, actionExecutor);
    }

    private Raw createTestRaw(String keySuffix) {
        Schema schema =
                Schema.builder()
                        .field(new StructField("id", Types.int64()))
                        .field(new StructField("name", Types.string()))
                        .build();
        Record.Body body = new DefaultBody(schema, new Object[] {1L, "test-" + keySuffix});
        Record.Key key = Records.keyOf(("key-" + keySuffix).getBytes(StandardCharsets.UTF_8));
        Record.Header header =
                new Record.Header(
                        key,
                        1L,
                        0L,
                        0L,
                        System.currentTimeMillis(),
                        Record.Kind.INSERT,
                        true,
                        true,
                        ImmutabilitySource.CREATED);
        return Raw.of(key, header, body);
    }

    @Nested
    @DisplayName("S1-SemanticDerivation - derivation")
    class SemanticDerivation {

        @Test
        @DisplayName("from viewIds derivation Semantic, returns correctlytype")
        void deriveSemanticFromViewIds() {
            List<String> viewIds = List.of("view-1", "view-2");

            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            viewIds,
                            Semantic.SemanticType.CONCEPT,
                            Map.of("description", "test concept"));

            assertThat(semantic.semanticId()).startsWith("sem:");
            assertThat(semantic.semanticType()).isEqualTo(Semantic.SemanticType.CONCEPT);
            assertThat(semantic.sourceViewIds()).containsExactly("view-1", "view-2");
            assertThat(semantic.content()).isNotEmpty();
        }

        @Test
        @DisplayName("multiple type canderivation")
        void allSemanticTypesCanBeDerived() {
            List<String> viewIds = List.of("v1");

            for (Semantic.SemanticType type : Semantic.SemanticType.values()) {
                Semantic semantic = cognitionEngine.deriveSemantic(viewIds, type, Map.of());
                assertThat(semantic.semanticType()).isEqualTo(type);
                assertThat(semantic.semanticId()).startsWith("sem:");
            }
        }

        @Test
        @DisplayName("same view multiple timesderivation different semanticId")
        void repeatedDerivationProducesUniqueIds() {
            List<String> viewIds = List.of("view-unique");

            Semantic s1 =
                    cognitionEngine.deriveSemantic(
                            viewIds, Semantic.SemanticType.PATTERN, Map.of());
            Semantic s2 =
                    cognitionEngine.deriveSemantic(
                            viewIds, Semantic.SemanticType.PATTERN, Map.of());

            assertThat(s1.semanticId()).isNotEqualTo(s2.semanticId());
        }
    }

    @Nested
    @DisplayName("S2-ActionDerivation - actionderivation")
    class ActionDerivation {

        @Test
        @DisplayName("from semanticIds derivation Action, canexecuteaction")
        void deriveActionFromSemanticIds() {
            List<String> semanticIds = List.of("sem-1");

            Action action =
                    cognitionEngine.deriveAction(
                            semanticIds, Action.ActionType.RETRIEVE, Map.of("priority", "high"));

            assertThat(action.actionId()).startsWith("act:");
            assertThat(action.actionType()).isEqualTo(Action.ActionType.RETRIEVE);
            assertThat(action.sourceSemanticIds()).containsExactly("sem-1");
            assertThat(action.executable()).isTrue();
        }

        @Test
        @DisplayName("allactiontype canderivation")
        void allActionTypesCanBeDerived() {
            List<String> semanticIds = List.of("sem-a");

            for (Action.ActionType type : Action.ActionType.values()) {
                Action action = cognitionEngine.deriveAction(semanticIds, type, Map.of());
                assertThat(action.actionType()).isEqualTo(type);
            }
        }
    }

    @Nested
    @DisplayName("S3-ActionPlan - action")
    class ActionPlanTests {

        @Test
        @DisplayName("CHAIN contains sequential action list")
        void chainPlanContainsSequentialActions() {
            List<String> semanticIds = List.of("sem-1");

            ActionPlan plan =
                    cognitionEngine.deriveActionPlan(
                            semanticIds, CognitionEngine.PlanType.CHAIN, Map.of());

            assertThat(plan.planId()).startsWith("plan:");
            assertThat(plan.planType()).isEqualTo(CognitionEngine.PlanType.CHAIN);
            assertThat(plan.actions()).hasSize(2);
            assertThat(plan.actions().get(0).actionType()).isEqualTo(Action.ActionType.RETRIEVE);
            assertThat(plan.actions().get(1).actionType()).isEqualTo(Action.ActionType.TRANSFORM);
            assertThat(plan.dependencies()).isEmpty();
        }

        @Test
        @DisplayName("DAG package depend on")
        void dagPlanContainsDependencies() {
            List<String> semanticIds = List.of("sem-x", "sem-y", "sem-z");

            ActionPlan plan =
                    cognitionEngine.deriveActionPlan(
                            semanticIds, CognitionEngine.PlanType.DAG, Map.of());

            assertThat(plan.planType()).isEqualTo(CognitionEngine.PlanType.DAG);
            assertThat(plan.actions()).hasSize(2);
            assertThat(plan.dependencies()).isNotEmpty();
        }

        @Test
        @DisplayName("CHAIN and DAG generates different planId")
        void chainAndDagProduceDifferentPlanIds() {
            List<String> semanticIds = List.of("sem-p");

            ActionPlan chain =
                    cognitionEngine.deriveActionPlan(
                            semanticIds, CognitionEngine.PlanType.CHAIN, Map.of());
            ActionPlan dag =
                    cognitionEngine.deriveActionPlan(
                            semanticIds, CognitionEngine.PlanType.DAG, Map.of());

            assertThat(chain.planId()).isNotEqualTo(dag.planId());
        }
    }

    @Nested
    @DisplayName("S4-CognitiveAssetMetric - cognition")
    class CognitiveAssetMetricTests {

        private static final ObjectMapper METRIC_MAPPER = new ObjectMapper();

        private void createView(String viewId, Map<String, Object> content) throws Exception {
            byte[] key = ("view:" + viewId).getBytes(StandardCharsets.UTF_8);
            metastore.put(key, METRIC_MAPPER.writeValueAsBytes(content));
        }

        private void annotateView(String viewId, double qualityScore, double confidence)
                throws Exception {
            long now = System.currentTimeMillis();
            Map<String, Object> annotation = new LinkedHashMap<>();
            annotation.put("annotationId", "ann-" + viewId);
            annotation.put("viewId", viewId);
            annotation.put("annotatorId", "human");
            annotation.put("type", "QUALITY_SCORE");
            annotation.put("value", qualityScore);
            annotation.put("confidence", confidence);
            annotation.put("timestamp", now);
            byte[] key = ("view_annotations:" + viewId).getBytes(StandardCharsets.UTF_8);
            metastore.put(key, METRIC_MAPPER.writeValueAsBytes(List.of(annotation)));
        }

        @Test
        @DisplayName("Metastore value")
        void emptyMetastoreReturnsZeroAsset() {
            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();

            assertThat(metric.totalFeedbackRecords()).isEqualTo(0);
            assertThat(metric.feedbackCoverage()).isEqualTo(1.0);
            assertThat(metric.assetValue()).isEqualTo(0.0);
        }

        @Test
        @DisplayName("has View noannotation cover is 0")
        void viewsWithoutAnnotationsGiveZeroCoverage() throws Exception {
            createView("v-nofeedback", Map.of("x", 1));
            createView("v-nofeedback2", Map.of("y", 2));

            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();

            assertThat(metric.totalFeedbackRecords()).isEqualTo(0);
            assertThat(metric.feedbackCoverage()).isEqualTo(0.0);
            assertThat(metric.assetValue()).isEqualTo(0.0);
        }

        @Test
        @DisplayName("partial View hasannotation cover is")
        void partialAnnotationCoverage() throws Exception {
            createView("v-annotated", Map.of("a", 1));
            createView("v-annotated2", Map.of("b", 2));
            createView("v-bare", Map.of("c", 3));
            annotateView("v-annotated", 0.9, 0.8);
            annotateView("v-annotated2", 0.7, 0.6);

            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();

            assertThat(metric.totalFeedbackRecords()).isEqualTo(2);
            assertThat(metric.feedbackCoverage()).isEqualTo(2.0 / 3.0);
            assertThat(metric.assetValue()).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("annotationcover is 1.0")
        void fullCoverageEqualsOne() throws Exception {
            createView("v-full", Map.of("x", 1));
            annotateView("v-full", 1.0, 1.0);

            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();

            assertThat(metric.totalFeedbackRecords()).isEqualTo(1);
            assertThat(metric.feedbackCoverage()).isEqualTo(1.0);
            assertThat(metric.assetValue()).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("score value: >")
        void higherQualityScoreYieldsHigherAssetValue() throws Exception {
            createView("v-high", Map.of("x", 1));
            createView("v-low", Map.of("y", 2));
            annotateView("v-high", 1.0, 1.0);
            annotateView("v-low", 0.1, 0.1);

            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();

            assertThat(metric.totalFeedbackRecords()).isEqualTo(2);
            assertThat(metric.assetValue()).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("value: >")
        void higherConfidenceYieldsHigherAssetValue() throws Exception {
            createView("v-conf-high", Map.of("x", 1));
            createView("v-conf-low", Map.of("y", 2));
            annotateView("v-conf-high", 0.9, 0.9);
            annotateView("v-conf-low", 0.9, 0.1);

            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();

            assertThat(metric.totalFeedbackRecords()).isEqualTo(2);
            assertThat(metric.assetValue()).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("decay: annotationweight")
        void timeDecayReducesOlderAnnotationWeight() throws Exception {
            String viewId = "v-decay";
            createView(viewId, Map.of("x", 1));
            long oldTime = System.currentTimeMillis() - 3600_000;
            Map<String, Object> annotation = new LinkedHashMap<>();
            annotation.put("annotationId", "ann-old");
            annotation.put("viewId", viewId);
            annotation.put("annotatorId", "human");
            annotation.put("type", "QUALITY_SCORE");
            annotation.put("value", 0.9);
            annotation.put("confidence", 0.9);
            annotation.put("timestamp", oldTime);
            byte[] key = ("view_annotations:" + viewId).getBytes(StandardCharsets.UTF_8);
            metastore.put(key, METRIC_MAPPER.writeValueAsBytes(List.of(annotation)));

            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();

            assertThat(metric.assetValue()).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("QUALITY_SCORE ofannotationtypedoes not affect value")
        void nonQualityAnnotationsDontAffectAssetValue() throws Exception {
            String viewId = "v-nonquality";
            createView(viewId, Map.of("x", 1));
            Map<String, Object> annotation = new LinkedHashMap<>();
            annotation.put("annotationId", "ann-pref");
            annotation.put("viewId", viewId);
            annotation.put("annotatorId", "human");
            annotation.put("type", "PREFERENCE");
            annotation.put("value", "liked");
            annotation.put("confidence", 1.0);
            annotation.put("timestamp", System.currentTimeMillis());
            byte[] key = ("view_annotations:" + viewId).getBytes(StandardCharsets.UTF_8);
            metastore.put(key, METRIC_MAPPER.writeValueAsBytes(List.of(annotation)));

            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();

            assertThat(metric.totalFeedbackRecords()).isEqualTo(1);
            assertThat(metric.feedbackCoverage()).isEqualTo(1.0);
            assertThat(metric.assetValue()).isGreaterThan(0.0);
        }
    }

    @Nested
    @DisplayName("S5-ActionExecution - action execution")
    class ActionExecution {

        @Test
        @DisplayName("executederivationafterof RETRIEVE actionsuccess")
        void executeRetrieveActionSuccessfully() {
            Raw raw = createTestRaw("exec");
            List<String> viewIds = List.of("view-exec-1");
            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            viewIds, Semantic.SemanticType.CONCEPT, Map.of());
            Action action =
                    cognitionEngine.deriveAction(
                            List.of(semantic.semanticId()), Action.ActionType.RETRIEVE, Map.of());

            ExecutionContext ctx = DefaultExecutionContext.of(raw);
            CognitionEngine.ActionResult result = cognitionEngine.execute(action.actionId(), ctx);

            assertThat(result.success()).isTrue();
            assertThat(result.actionId()).isEqualTo(action.actionId());
            assertThat(result.output()).isNotEmpty();
        }

        @Test
        @DisplayName("CHAIN execute all actions")
        void chainPlanExecutesAllActions() {
            Raw raw = createTestRaw("chain");
            List<String> viewIds = List.of("view-chain-1");
            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            viewIds, Semantic.SemanticType.CONCEPT, Map.of());
            ActionPlan plan =
                    cognitionEngine.deriveActionPlan(
                            List.of(semantic.semanticId()),
                            CognitionEngine.PlanType.CHAIN,
                            Map.of());

            ExecutionContext ctx = DefaultExecutionContext.of(raw);
            CognitionEngine.PlanResult result = cognitionEngine.executePlan(plan.planId(), ctx);

            assertThat(result.success()).isTrue();
            assertThat(result.stepResults()).isNotEmpty();
            assertThat(result.summary()).contains(String.valueOf(result.stepResults().size()));
        }

        @Test
        @DisplayName("non-existent action execution returns failure")
        void nonexistentActionReturnsFailure() {
            Raw raw = createTestRaw("nonexistent");
            ExecutionContext ctx = DefaultExecutionContext.of(raw);

            CognitionEngine.ActionResult result = cognitionEngine.execute("nonexistent-id", ctx);

            assertThat(result.success()).isFalse();
            assertThat(result.output()).contains("not found");
        }
    }

    @Nested
    @DisplayName("S6-FullLoop - cognitionclosure")
    class FullLoop {

        @Test
        @DisplayName("View->Semantic->Action->Execute full chain")
        void fullCognitionLoop() {
            Raw raw = createTestRaw("full-loop");
            List<String> viewIds = List.of("view-loop-1", "view-loop-2");

            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            viewIds,
                            Semantic.SemanticType.CONCEPT,
                            Map.of("description", "derived from loop"));

            assertThat(semantic.sourceViewIds()).isEqualTo(viewIds);

            Action action =
                    cognitionEngine.deriveAction(
                            List.of(semantic.semanticId()),
                            Action.ActionType.TRANSFORM,
                            Map.of("mode", "batch"));

            assertThat(action.sourceSemanticIds()).containsExactly(semantic.semanticId());

            ActionPlan plan =
                    cognitionEngine.deriveActionPlan(
                            List.of(semantic.semanticId()),
                            CognitionEngine.PlanType.CHAIN,
                            Map.of());

            assertThat(plan.actions()).hasSize(2);

            ExecutionContext ctx = DefaultExecutionContext.of(raw);
            CognitionEngine.PlanResult result = cognitionEngine.executePlan(plan.planId(), ctx);

            assertThat(result.success()).isTrue();
            assertThat(result.summary()).contains("succeeded");
        }
    }

    @Nested
    @DisplayName("S8-SemanticContentGeneration - generate")
    class SemanticContentGeneration {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Test
        @DisplayName("CONCEPT semantics extract keywords from View content")
        void conceptSemanticExtractsKeywords() throws Exception {
            String viewId = "view-concept-1";
            byte[] viewContent =
                    MAPPER.writeValueAsBytes(
                            Map.of(
                                    "title", "machine learning algorithms",
                                    "body", "neural networks deep learning gradient descent"));
            metastore.put(("view:" + viewId).getBytes(StandardCharsets.UTF_8), viewContent);

            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            List.of(viewId), Semantic.SemanticType.CONCEPT, Map.of());

            Map<String, Object> parsed =
                    MAPPER.readValue(
                            semantic.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("concepts");
            assertThat(parsed).containsKey("conceptCount");
            assertThat(parsed.get("semanticType")).isEqualTo("CONCEPT");
        }

        @Test
        @DisplayName("RELATION calculate View")
        void relationSemanticComputesSimilarity() throws Exception {
            String v1 = "view-rel-1";
            String v2 = "view-rel-2";
            byte[] vc1 = MAPPER.writeValueAsBytes(Map.of("name", "alice", "age", 30));
            byte[] vc2 = MAPPER.writeValueAsBytes(Map.of("name", "bob", "age", 25));
            metastore.put(("view:" + v1).getBytes(StandardCharsets.UTF_8), vc1);
            metastore.put(("view:" + v2).getBytes(StandardCharsets.UTF_8), vc2);

            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            List.of(v1, v2), Semantic.SemanticType.RELATION, Map.of());

            Map<String, Object> parsed =
                    MAPPER.readValue(
                            semantic.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("relations");
            assertThat(parsed).containsKey("relationCount");
            assertThat(parsed.get("semanticType")).isEqualTo("RELATION");
        }

        @Test
        @DisplayName("PATTERN detect")
        void patternSemanticDetectsStructures() throws Exception {
            String v1 = "view-pat-1";
            String v2 = "view-pat-2";
            byte[] vc1 = MAPPER.writeValueAsBytes(Map.of("name", "x", "value", 1));
            byte[] vc2 = MAPPER.writeValueAsBytes(Map.of("name", "y", "value", 2));
            metastore.put(("view:" + v1).getBytes(StandardCharsets.UTF_8), vc1);
            metastore.put(("view:" + v2).getBytes(StandardCharsets.UTF_8), vc2);

            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            List.of(v1, v2), Semantic.SemanticType.PATTERN, Map.of());

            Map<String, Object> parsed =
                    MAPPER.readValue(
                            semantic.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("patterns");
            assertThat(parsed).containsKey("viewCount");
            assertThat((Integer) parsed.get("viewCount")).isEqualTo(2);
        }

        @Test
        @DisplayName("PREFERENCE field")
        void preferenceSemanticRanksFields() throws Exception {
            String viewId = "view-pref-1";
            byte[] viewContent =
                    MAPPER.writeValueAsBytes(
                            Map.of("important", "a long text value for scoring", "trivial", 1));
            metastore.put(("view:" + viewId).getBytes(StandardCharsets.UTF_8), viewContent);

            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            List.of(viewId), Semantic.SemanticType.PREFERENCE, Map.of());

            Map<String, Object> parsed =
                    MAPPER.readValue(
                            semantic.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("preferences");
        }

        @Test
        @DisplayName("View can still derive when not exists(degraded content)")
        void semanticWithMissingViewStillDerives() throws Exception {
            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            List.of("nonexistent-view"), Semantic.SemanticType.CONCEPT, Map.of());

            assertThat(semantic.semanticId()).startsWith("sem:");
            Map<String, Object> parsed =
                    MAPPER.readValue(
                            semantic.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed.get("semanticType")).isEqualTo("CONCEPT");
        }
    }

    @Nested
    @DisplayName("S9-ActionContentGeneration - action generate")
    class ActionContentGeneration {

        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Test
        @DisplayName("RETRIEVE actiongeneratequery")
        void retrieveActionGeneratesQuerySpec() throws Exception {
            String semId = "sem-retrieve-1";
            byte[] semContent =
                    MAPPER.writeValueAsBytes(
                            Map.of(
                                    "semanticType",
                                    "CONCEPT",
                                    "concepts",
                                    List.of(Map.of("term", "database", "frequency", 3))));
            metastore.put(("semantic:" + semId).getBytes(StandardCharsets.UTF_8), semContent);

            Action action =
                    cognitionEngine.deriveAction(
                            List.of(semId), Action.ActionType.RETRIEVE, Map.of("topK", 5));

            Map<String, Object> parsed =
                    MAPPER.readValue(action.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("retrieveSpec");
            Map<String, Object> spec = (Map<String, Object>) parsed.get("retrieveSpec");
            assertThat(spec).containsKey("queryTerms");
            assertThat(spec).containsKey("topK");
            assertThat(spec.get("topK")).isEqualTo(5);
        }

        @Test
        @DisplayName("TRANSFORM actiongenerate")
        void transformActionGeneratesTransformSpec() throws Exception {
            String semId = "sem-transform-1";
            byte[] semContent = MAPPER.writeValueAsBytes(Map.of("semanticType", "PATTERN"));
            metastore.put(("semantic:" + semId).getBytes(StandardCharsets.UTF_8), semContent);

            Action action =
                    cognitionEngine.deriveAction(
                            List.of(semId),
                            Action.ActionType.TRANSFORM,
                            Map.of("mode", "aggregate"));

            Map<String, Object> parsed =
                    MAPPER.readValue(action.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("transformSpec");
            Map<String, Object> spec = (Map<String, Object>) parsed.get("transformSpec");
            assertThat(spec.get("transformMode")).isEqualTo("aggregate");
            assertThat(spec.get("outputFormat")).isEqualTo("view");
        }

        @Test
        @DisplayName("NOTIFY actiongenerate")
        void notifyActionGeneratesNotifySpec() throws Exception {
            String semId = "sem-notify-1";
            byte[] semContent = MAPPER.writeValueAsBytes(Map.of("semanticType", "CONCEPT"));
            metastore.put(("semantic:" + semId).getBytes(StandardCharsets.UTF_8), semContent);

            Action action =
                    cognitionEngine.deriveAction(
                            List.of(semId),
                            Action.ActionType.NOTIFY,
                            Map.of("channels", List.of("email", "slack")));

            Map<String, Object> parsed =
                    MAPPER.readValue(action.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("notifySpec");
            Map<String, Object> spec = (Map<String, Object>) parsed.get("notifySpec");
            assertThat(spec).containsKey("channels");
            assertThat(spec).containsKey("messageTemplate");
        }

        @Test
        @DisplayName("CORRECT actiondetect semantic issue")
        void correctActionDetectsIssues() throws Exception {
            String semId = "sem-correct-1";
            byte[] semContent =
                    MAPPER.writeValueAsBytes(Map.of("semanticType", "CONCEPT", "conceptCount", 0));
            metastore.put(("semantic:" + semId).getBytes(StandardCharsets.UTF_8), semContent);

            Action action =
                    cognitionEngine.deriveAction(
                            List.of(semId), Action.ActionType.CORRECT, Map.of());

            Map<String, Object> parsed =
                    MAPPER.readValue(action.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("correctSpec");
            Map<String, Object> spec = (Map<String, Object>) parsed.get("correctSpec");
            assertThat(spec).containsKey("corrections");
            assertThat((Integer) spec.get("correctionCount")).isGreaterThan(0);
        }

        @Test
        @DisplayName("Semantic can still derive action when not exists(degraded content)")
        void actionWithMissingSemanticStillDerives() throws Exception {
            Action action =
                    cognitionEngine.deriveAction(
                            List.of("nonexistent-sem"), Action.ActionType.RETRIEVE, Map.of());

            assertThat(action.actionId()).startsWith("act:");
            Map<String, Object> parsed =
                    MAPPER.readValue(action.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed.get("actionType")).isEqualTo("RETRIEVE");
        }
    }

    @Nested
    @DisplayName("S10-DAGTopologicalSort - DAG topological sortexecute")
    class DagTopologicalSort {

        private final DefaultActionExecutor executor = new DefaultActionExecutor("test-dag-exec");

        @Test
        @DisplayName("topological sort: nodepend onof Action preserves original order")
        void topologicalSortNoDependencies() {
            Action a1 = new Action("a1", List.of(), Action.ActionType.RETRIEVE, new byte[0], true);
            Action a2 = new Action("a2", List.of(), Action.ActionType.NOTIFY, new byte[0], true);

            List<Action> sorted = executor.topologicalSort(List.of(a1, a2), Map.of());

            assertThat(sorted).hasSize(2);
            assertThat(sorted.get(0).actionId()).isEqualTo("a1");
            assertThat(sorted.get(1).actionId()).isEqualTo("a2");
        }

        @Test
        @DisplayName("topological sort: a2 depend on a1 a1 inbefore")
        void topologicalSortWithDependency() {
            Action a1 = new Action("a1", List.of(), Action.ActionType.RETRIEVE, new byte[0], true);
            Action a2 = new Action("a2", List.of(), Action.ActionType.TRANSFORM, new byte[0], true);

            List<Action> sorted =
                    executor.topologicalSort(List.of(a1, a2), Map.of("a2", Set.of("a1")));

            assertThat(sorted).hasSize(2);
            assertThat(sorted.get(0).actionId()).isEqualTo("a1");
            assertThat(sorted.get(1).actionId()).isEqualTo("a2");
        }

        @Test
        @DisplayName("topological sort: depend oncorrectly sorts")
        void topologicalSortDiamondDependency() {
            Action a1 = new Action("a1", List.of(), Action.ActionType.RETRIEVE, new byte[0], true);
            Action a2 = new Action("a2", List.of(), Action.ActionType.TRANSFORM, new byte[0], true);
            Action a3 = new Action("a3", List.of(), Action.ActionType.NOTIFY, new byte[0], true);
            Action a4 = new Action("a4", List.of(), Action.ActionType.CORRECT, new byte[0], true);

            Map<String, Set<String>> deps =
                    Map.of(
                            "a2", Set.of("a1"),
                            "a3", Set.of("a1"),
                            "a4", Set.of("a2", "a3"));

            List<Action> sorted = executor.topologicalSort(List.of(a1, a2, a3, a4), deps);

            assertThat(sorted).hasSize(4);
            int idx1 = indexOf(sorted, "a1");
            int idx2 = indexOf(sorted, "a2");
            int idx3 = indexOf(sorted, "a3");
            int idx4 = indexOf(sorted, "a4");
            assertThat(idx1).isLessThan(idx2);
            assertThat(idx1).isLessThan(idx3);
            assertThat(idx2).isLessThan(idx4);
            assertThat(idx3).isLessThan(idx4);
        }

        @Test
        @DisplayName("topological sort: depend on null")
        void topologicalSortCyclicReturnsNull() {
            Action a1 = new Action("a1", List.of(), Action.ActionType.RETRIEVE, new byte[0], true);
            Action a2 = new Action("a2", List.of(), Action.ActionType.TRANSFORM, new byte[0], true);

            List<Action> sorted =
                    executor.topologicalSort(
                            List.of(a1, a2), Map.of("a1", Set.of("a2"), "a2", Set.of("a1")));

            assertThat(sorted).isNull();
        }

        @Test
        @DisplayName("DAG execute: depend on allactionsuccess")
        void dagPlanExecutesInDependencyOrder() {
            Raw raw = createTestRaw("dag-order");
            List<String> viewIds = List.of("view-dag-1");
            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            viewIds, Semantic.SemanticType.CONCEPT, Map.of());
            ActionPlan plan =
                    cognitionEngine.deriveActionPlan(
                            List.of(semantic.semanticId()), CognitionEngine.PlanType.DAG, Map.of());

            ExecutionContext ctx = DefaultExecutionContext.of(raw);
            CognitionEngine.PlanResult result = cognitionEngine.executePlan(plan.planId(), ctx);

            assertThat(result.success()).isTrue();
            assertThat(result.stepResults()).hasSize(plan.actions().size());
        }

        @Test
        @DisplayName("DAG execute: depend onnot yet subsequentactionskip")
        void dagPlanSkipsActionsWithUnmetDependency() {
            Action a1 = new Action("a1", List.of(), Action.ActionType.RETRIEVE, new byte[0], true);
            Action a2 = new Action("a2", List.of(), Action.ActionType.TRANSFORM, new byte[0], true);

            Map<String, Set<String>> deps = Map.of("a2", Set.of("a1"));
            ActionPlan plan =
                    new ActionPlan(
                            "plan-dag-skip", List.of(a1, a2), CognitionEngine.PlanType.DAG, deps);

            Raw raw = createTestRaw("dag-skip");
            ExecutionContext ctx = DefaultExecutionContext.of(raw);
            CognitionEngine.PlanResult result = executor.executePlan(plan, ctx);

            assertThat(result.stepResults()).hasSize(2);
        }

        @Test
        @DisplayName("DAG depend on : TRANSFORM depend on RETRIEVE")
        void dagDependencyTransformDependsOnRetrieve() {
            List<String> semanticIds = List.of("sem-dep-1", "sem-dep-2");
            ActionPlan plan =
                    cognitionEngine.deriveActionPlan(
                            semanticIds, CognitionEngine.PlanType.DAG, Map.of());

            plan.dependencies().entrySet().stream()
                    .anyMatch(
                            e ->
                                    e.getValue().stream()
                                            .anyMatch(
                                                    depId ->
                                                            plan.actions().stream()
                                                                    .filter(
                                                                            a ->
                                                                                    a.actionId()
                                                                                            .equals(
                                                                                                    depId))
                                                                    .anyMatch(
                                                                            a ->
                                                                                    a.actionType()
                                                                                            == Action
                                                                                                    .ActionType
                                                                                                    .RETRIEVE)));

            assertThat(plan.dependencies()).isNotEmpty();
        }

        @Test
        @DisplayName("CHAIN stop on failure, DAG skip failed dependency")
        void chainStopsOnFailureDagSkips() {
            Action failAction =
                    new Action(
                            "fail-act",
                            List.of(),
                            Action.ActionType.RETRIEVE,
                            "fail-content".getBytes(StandardCharsets.UTF_8),
                            true);
            Action depAction =
                    new Action(
                            "dep-act",
                            List.of(),
                            Action.ActionType.TRANSFORM,
                            "dep-content".getBytes(StandardCharsets.UTF_8),
                            true);

            Raw raw = createTestRaw("chain-vs-dag");
            ExecutionContext ctx = DefaultExecutionContext.of(raw);

            List<CognitionEngine.ActionResult> chainResults = new ArrayList<>();
            chainResults.add(
                    new CognitionEngine.ActionResult(
                            "fail-act",
                            false,
                            "intentional failure",
                            Map.of(),
                            System.currentTimeMillis()));
            assertThat(chainResults).hasSize(1);
            assertThat(chainResults.get(0).success()).isFalse();

            DefaultActionExecutor dagExecutor =
                    new DefaultActionExecutor("dag-fail-exec") {
                        @Override
                        public CognitionEngine.ActionResult execute(
                                Action action, ExecutionContext context) {
                            if (action.actionId().equals("fail-act")) {
                                return new CognitionEngine.ActionResult(
                                        "fail-act",
                                        false,
                                        "intentional failure",
                                        Map.of(),
                                        System.currentTimeMillis());
                            }
                            return super.execute(action, context);
                        }
                    };

            ActionPlan dagPlan =
                    new ActionPlan(
                            "dag-fail",
                            List.of(failAction, depAction),
                            CognitionEngine.PlanType.DAG,
                            Map.of("dep-act", Set.of("fail-act")));
            CognitionEngine.PlanResult dagResult = dagExecutor.executePlan(dagPlan, ctx);
            assertThat(dagResult.stepResults()).hasSize(2);
            assertThat(dagResult.stepResults().get(0).success()).isFalse();
            assertThat(dagResult.stepResults().get(1).success()).isFalse();
            assertThat(dagResult.stepResults().get(1).output())
                    .contains("dependency not satisfied");
        }

        private int indexOf(List<Action> actions, String actionId) {
            for (int i = 0; i < actions.size(); i++) {
                if (actions.get(i).actionId().equals(actionId)) return i;
            }
            return -1;
        }
    }

    @Nested
    @DisplayName("S7-Concurrent - concurrentcognitionoperations")
    class Concurrent {

        @Test
        @DisplayName("multi-threaded concurrentderivation , all persisted")
        void concurrentSemanticDerivation() throws Exception {
            int threadCount = 4;
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger errorCount = new AtomicInteger(0);
            var executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                int threadId = t;
                executor.submit(
                        () -> {
                            try {
                                cognitionEngine.deriveSemantic(
                                        List.of("cv-" + threadId),
                                        Semantic.SemanticType.PREFERENCE,
                                        Map.of("thread", threadId));
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(15, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            assertThat(errorCount.get()).isEqualTo(0);

            CognitiveAssetMetric metric = cognitionEngine.cognitiveAssetMetric();
            assertThat(metric.totalFeedbackRecords()).isGreaterThanOrEqualTo(0);
        }
    }

    @Nested
    @DisplayName("S11-SimilarityService - calculate")
    class SimilarityServiceTests {

        private final SimilarityService service = new DefaultSimilarityService();
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static final ObjectMapper TEST_MAPPER = new ObjectMapper();

        @Test
        @DisplayName("same is 1.0")
        void identicalTextsReturnMaxSimilarity() {
            double sim = service.computeSimilarity("hello world", "hello world");
            assertThat(sim).isEqualTo(1.0);
        }

        @Test
        @DisplayName("different 0")
        void completelyDifferentTextsReturnLowSimilarity() {
            double sim =
                    service.computeSimilarity(
                            "machine learning algorithms", "breakfast lunch dinner");
            assertThat(sim).isLessThan(0.5);
        }

        @Test
        @DisplayName("")
        void semanticallyRelatedTextsReturnHigherSimilarity() {
            double sim1 =
                    service.computeSimilarity(
                            "deep learning neural networks", "machine learning deep networks");
            double sim2 =
                    service.computeSimilarity(
                            "deep learning neural networks", "cooking recipes food kitchen");
            assertThat(sim1).isGreaterThan(sim2);
        }

        @Test
        @DisplayName("is 0")
        void emptyTextReturnsZero() {
            assertThat(service.computeSimilarity("", "hello")).isEqualTo(0.0);
            assertThat(service.computeSimilarity("hello", "")).isEqualTo(0.0);
            assertThat(service.computeSimilarity("", "")).isEqualTo(0.0);
        }

        @Test
        @DisplayName("null 0")
        void nullInputReturnsZero() {
            assertThat(service.computeSimilarity(null, "hello")).isEqualTo(0.0);
            assertThat(service.computeSimilarity("hello", null)).isEqualTo(0.0);
        }

        @Test
        @DisplayName("same different of has (n-gram)")
        void ngramCapturesSubwordSimilarity() {
            double sim = service.computeSimilarity("running runner runs", "run running runner");
            assertThat(sim).isGreaterThan(0.0);
        }

        @Test
        @DisplayName("RELATION SimilarityService calculate")
        void relationSemanticUsesSimilarityService() throws Exception {
            String v1 = "view-sim-rel-1";
            String v2 = "view-sim-rel-2";
            byte[] vc1 =
                    MAPPER.writeValueAsBytes(
                            Map.of(
                                    "title", "machine learning basics",
                                    "description", "introduction to supervised learning"));
            byte[] vc2 =
                    MAPPER.writeValueAsBytes(
                            Map.of(
                                    "title", "deep learning advanced",
                                    "description", "neural networks and deep architectures"));
            metastore.put(("view:" + v1).getBytes(StandardCharsets.UTF_8), vc1);
            metastore.put(("view:" + v2).getBytes(StandardCharsets.UTF_8), vc2);

            Semantic semantic =
                    cognitionEngine.deriveSemantic(
                            List.of(v1, v2), Semantic.SemanticType.RELATION, Map.of());

            Map<String, Object> parsed =
                    MAPPER.readValue(
                            semantic.content(), new TypeReference<Map<String, Object>>() {});
            assertThat(parsed).containsKey("relations");
            assertThat(parsed.get("semanticType")).isEqualTo("RELATION");
        }

        @Test
        @DisplayName("can custom SimilarityService")
        void customSimilarityServiceCanBeInjected() {
            SimilarityService custom = (t1, t2) -> t1.contains(t2) ? 1.0 : 0.0;
            DefaultCognitionEngine engine =
                    new DefaultCognitionEngine(
                            metastore,
                            knowledgeStore,
                            evaluationEngine,
                            actionExecutor,
                            custom,
                            0.01);

            Semantic semantic =
                    engine.deriveSemantic(List.of("v-x"), Semantic.SemanticType.CONCEPT, Map.of());
            assertThat(semantic.semanticId()).startsWith("sem:");
        }
    }
}
