package io.github.rawwise.benchmark.vbench.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.axiom.pipeline.cognition.ActionPlan;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.CognitiveAssetMetric;
import io.github.rawwise.axiom.pipeline.cognition.ConditionConstructionPipeline;
import io.github.rawwise.axiom.pipeline.cognition.ConditionConstructionSpec;
import io.github.rawwise.axiom.pipeline.cognition.ConditionReasoningContext;
import io.github.rawwise.axiom.pipeline.cognition.ConditionReasoningResult;
import io.github.rawwise.axiom.pipeline.cognition.ConditionReasoningService;
import io.github.rawwise.axiom.pipeline.cognition.ExecutionContext;
import io.github.rawwise.axiom.pipeline.cognition.LlmConditionReasoningService;
import io.github.rawwise.axiom.pipeline.cognition.LlmStrategyLearningService;
import io.github.rawwise.axiom.pipeline.cognition.StrategyLearningContext;
import io.github.rawwise.axiom.pipeline.cognition.StrategyLearningService;
import io.github.rawwise.axiom.pipeline.cognition.TextConditionEnhancer;
import io.github.rawwise.axiom.pipeline.feedback.CollectedFeedback;
import io.github.rawwise.axiom.pipeline.state.ContractViolation;
import io.github.rawwise.axiom.pipeline.state.DefaultStateContract;
import io.github.rawwise.axiom.pipeline.state.GuardExpressionEvaluator;
import io.github.rawwise.axiom.pipeline.state.StateEngine;
import io.github.rawwise.axiom.pipeline.state.StateEntry;
import io.github.rawwise.axiom.pipeline.state.StateSpace;
import io.github.rawwise.axiom.pipeline.state.StateTransition;
import io.github.rawwise.benchmark.vbench.config.VBenchConfig;
import io.github.rawwise.benchmark.vbench.executors.HttpVideoGenerationClient;
import io.github.rawwise.benchmark.vbench.feedback.AxiomAnnotation;
import io.github.rawwise.benchmark.vbench.feedback.AxiomClient;
import io.github.rawwise.benchmark.vbench.feedback.AxiomIngest;
import io.github.rawwise.benchmark.vbench.feedback.HttpAxiomClient;
import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Raw;
import io.github.rawwise.model.record.Semantic;
import io.github.rawwise.model.record.StrategyAction;
import io.github.rawwise.model.record.View;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * Step 2: activate — letexperimentactuallygo through ConditionConstructionPipeline.
 *
 * <p>closed-loopflow(A → B → C):
 *
 * <p>Round A (baseline): original prompt → CogVideoX → video → VBench evaluation → ingest +
 * annotate
 *
 * <p>Round B (rawwise-v1): ContractViolation → ConditionConstructionPipeline (with
 * TextConditionEnhancer) → CORRECT action (text_condition enhanced by PromptEnhancementPlanner) →
 * VideoGenerationActionExecutor → CogVideoX → video → VBench evaluation → ingest + annotate
 *
 * <p>Round C (rawwise-v2): based on A/B of feedback memory → ConditionConstructionPipeline →
 * CORRECT action (text_condition enhanced based on A/B feedback) → VideoGenerationActionExecutor →
 * CogVideoX → video → VBench evaluation → ingest + annotate
 *
 * <p>Key difference vs before: Before: VideoGenerationActionExecutor directly calls
 * PromptEnhancementPlanner now: ContractViolation → ConditionConstructionPipeline → CORRECT action
 * → executor TextConditionEnhancer is called inside pipeline, violation info participates in
 * condition construction
 */
@Slf4j
public class ClosedLoopBenchmarkRunner {

    private final String rawwiseBaseUrl;
    private final String prompt;
    private final String promptId;
    private final HttpVideoGenerationClient videoClient;
    private final VBenchConfig vbenchConfig;
    private final SimpleStateEngine stateEngine;
    private final DefaultStateContract stateContract;
    private final InMemoryGraphEngine graphEngine;
    private final String graphId;
    private PromptResult lastResult;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // T function UpdateModels: strategy learning and retrieval (in-process, benchmark runs
    // independently)
    private final StrategyLearningService strategyLearningService;
    // static: batch mode shares strategy across prompts(fix2: consistent with metaPatterns)
    private static final Map<String, StrategyAction> strategyStore = new ConcurrentHashMap<>();
    private String lastUsedStrategyId;
    // optimization4: cross-prompt meta-learning patterns (static, shared across instances in batch
    // mode)
    private static final List<String> metaPatterns = new ArrayList<>();

    public ClosedLoopBenchmarkRunner(String rawwiseBaseUrl, String prompt, String promptId) {
        this.rawwiseBaseUrl = rawwiseBaseUrl;
        this.prompt = prompt;
        this.promptId = promptId;
        this.vbenchConfig = new VBenchConfig();
        this.videoClient = new HttpVideoGenerationClient(vbenchConfig);
        this.stateEngine = new SimpleStateEngine();
        this.stateContract = new DefaultStateContract(new GuardExpressionEvaluator());
        this.graphEngine = new InMemoryGraphEngine();
        this.graphId = initGraph();
        this.strategyLearningService = createStrategyLearningService();
    }

    /**
     * Create knowledge graph and write entity nodes. Dynamically extract character, clothing,
     * scene, action entities from prompt. ConditionConstructionPipeline will extract precise
     * constraints from them.
     */
    private String initGraph() {
        try {
            var schema =
                    graphEngine.createGraph(
                            "agent:video_gen",
                            "video_entities:" + promptId,
                            "Entity graph for video generation consistency",
                            List.of("character", "clothing", "scene", "action"),
                            List.of("WEARS", "IN", "DOING"),
                            Map.of("prompt_id", promptId));

            String[] entities = extractEntitiesFromPrompt(prompt);
            String characterDesc = entities[0];
            String clothingDesc = entities[1];
            String sceneDesc = entities[2];
            String actionDesc = entities[3];

            GraphNode character =
                    graphEngine.addNode(
                            schema.graphId(),
                            "character",
                            Map.of(
                                    "name",
                                    characterDesc.replaceAll("\\s+", "_"),
                                    "description",
                                    characterDesc,
                                    "embeddingText",
                                    characterDesc + " character"));
            GraphNode clothing =
                    graphEngine.addNode(
                            schema.graphId(),
                            "clothing",
                            Map.of(
                                    "name",
                                    clothingDesc.replaceAll("\\s+", "_"),
                                    "description",
                                    clothingDesc,
                                    "embeddingText",
                                    clothingDesc + " clothing"));
            GraphNode scene =
                    graphEngine.addNode(
                            schema.graphId(),
                            "scene",
                            Map.of(
                                    "name",
                                    sceneDesc.replaceAll("\\s+", "_"),
                                    "description",
                                    sceneDesc,
                                    "embeddingText",
                                    sceneDesc + " scene"));
            GraphNode action =
                    graphEngine.addNode(
                            schema.graphId(),
                            "action",
                            Map.of(
                                    "name",
                                    actionDesc.replaceAll("\\s+", "_"),
                                    "description",
                                    actionDesc,
                                    "embeddingText",
                                    actionDesc + " action"));

            graphEngine.addEdge(
                    schema.graphId(),
                    character.nodeId(),
                    "WEARS",
                    clothing.nodeId(),
                    Map.of(),
                    1.0f);
            graphEngine.addEdge(
                    schema.graphId(), character.nodeId(), "IN", scene.nodeId(), Map.of(), 1.0f);
            graphEngine.addEdge(
                    schema.graphId(), character.nodeId(), "DOING", action.nodeId(), Map.of(), 1.0f);

            System.out.println(
                    "Graph initialized: graphId="
                            + schema.graphId()
                            + ", character="
                            + characterDesc
                            + ", clothing="
                            + clothingDesc
                            + ", scene="
                            + sceneDesc
                            + ", action="
                            + actionDesc);
            return schema.graphId();
        } catch (Exception e) {
            throw new RuntimeException("Failed to init graph", e);
        }
    }

    /**
     * Simple entity extraction from prompt. Returns [character, clothing, scene, action] Uses rule
     * matching, no NLP library dependency.
     */
    private static String[] extractEntitiesFromPrompt(String prompt) {
        String lower = prompt.toLowerCase();
        String character = "the subject";
        String clothing = "their clothing";
        String scene = "the scene";
        String action = "the action";

        java.util.regex.Matcher m =
                java.util.regex.Pattern.compile(
                                "(?:a|an)\\s+((?:young\\s+|elderly\\s+|old\\s+|small\\s+|little\\s+|big\\s+)?(?:woman|man|child|boy|girl|person|chef|musician|scientist|cat|dog|horse|bird|ballerina))")
                        .matcher(lower);
        if (m.find()) {
            character = m.group(1);
        }

        m =
                java.util.regex.Pattern.compile(
                                "in\\s+(?:a\\s+|an\\s+)?((?:red\\s+|blue\\s+|green\\s+|white\\s+|black\\s+|yellow\\s+)?(?:jacket|coat|dress|uniform|shirt|hat|suit))")
                        .matcher(lower);
        if (m.find()) {
            clothing = m.group(1);
        }

        m =
                java.util.regex.Pattern.compile(
                                "(?:in|on|across|through|down)\\s+(?:a\\s+|an\\s+|the\\s+)?((?:busy\\s+|city\\s+|open\\s+|dark\\s+|sunny\\s+)?(?:street|park|kitchen|stage|lab|field|prairie|lake|market|library|sky|cafe|floor|cliffs|garden|room))")
                        .matcher(lower);
        if (m.find()) {
            scene = m.group(1);
        }

        m =
                java.util.regex.Pattern.compile(
                                "((?:walk|run|play|laugh|danc|spin|gallop|fly|fall|pour|bloom|stalk|pounce|crash|prepar|exam|play)\\w*)")
                        .matcher(lower);
        if (m.find()) {
            action = m.group(1);
        }

        return new String[] {character, clothing, scene, action};
    }

    public void runClosedLoop() {
        System.out.println("=== Closed-Loop Benchmark Start ===");
        System.out.println("Prompt: " + prompt);
        System.out.println("RawWise: " + rawwiseBaseUrl);
        System.out.println("Graph: graphId=" + graphId);

        RoundResult roundA = runRound("baseline", prompt, null);
        System.out.println("Round A (baseline): vbenchScore=" + roundA.vbenchScore());

        RoundResult roundB = runRound("rawwise-v1", prompt, roundA);
        System.out.println("Round B (rawwise-v1): vbenchScore=" + roundB.vbenchScore());

        // : Round B completeaftertriggerstrategylearn(A+B feedbacksufficient)
        System.out.println("--- Triggering strategy learning (A+B feedback) ---");
        triggerStrategyLearning(roundA, roundB);

        // : Round C attemptfromlearntoofstrategygenerategeneratecondition(optimization5: pass
        // indimensionmatch)
        String strategyIdForRoundC = null;
        Optional<StrategyAction> learnedStrategy = retrieveStrategy(roundB.dimensionScores());
        if (learnedStrategy.isPresent()) {
            strategyIdForRoundC = learnedStrategy.get().strategyId();
            System.out.println("Round C: using learned strategy " + strategyIdForRoundC);
        } else {
            System.out.println(
                    "Round C: no learned strategy, fallback to ConditionConstructionPipeline");
        }

        RoundResult roundC = runRound("rawwise-v2", prompt, roundB, learnedStrategy.orElse(null));
        System.out.println("Round C (rawwise-v2): vbenchScore=" + roundC.vbenchScore());

        // : Round C completeafterbackfillstrategyeffect
        if (strategyIdForRoundC != null) {
            System.out.println("--- Recording strategy effect ---");
            recordStrategyEffect(roundB, roundC, strategyIdForRoundC);
        }

        System.out.println("=== Closed-Loop Summary ===");
        System.out.println("A (baseline)   : " + roundA.vbenchScore());
        System.out.printf(
                "B (rawwise-v1) : %.4f (delta vs A: %+.4f)%n",
                roundB.vbenchScore(), roundB.vbenchScore() - roundA.vbenchScore());
        System.out.printf(
                "C (rawwise-v2) : %.4f (delta vs B: %+.4f, delta vs A: %+.4f)%n",
                roundC.vbenchScore(),
                roundC.vbenchScore() - roundB.vbenchScore(),
                roundC.vbenchScore() - roundA.vbenchScore());

        if (roundC.vbenchScore() > roundB.vbenchScore()
                && roundB.vbenchScore() > roundA.vbenchScore()) {
            System.out.println(
                    ">>> H2 VERIFIED: feedback loop brings additional gain (B->C positive) <<<");
        } else if (roundB.vbenchScore() > roundA.vbenchScore()) {
            System.out.println(
                    ">>> H1 VERIFIED: prompt enhancement effective (A->B positive), but H2 not yet (B->C not positive) <<<");
        } else {
            System.out.println(
                    ">>> No positive gain detected. Need to investigate feedback quality. <<<");
        }

        lastResult =
                new PromptResult(
                        prompt,
                        promptId,
                        roundA.vbenchScore(),
                        roundB.vbenchScore(),
                        roundC.vbenchScore());
    }

    public PromptResult getLastResult() {
        return lastResult;
    }

    public record PromptResult(
            String prompt, String promptId, double scoreA, double scoreB, double scoreC) {}

    private RoundResult runRound(String tag, String originalPrompt, RoundResult previousRound) {
        return runRound(tag, originalPrompt, previousRound, null);
    }

    /**
     * Execute one round of closed loop.
     *
     * @param learnedStrategy If non-null, generate condition from strategy first( accessinputpoint)
     */
    private RoundResult runRound(
            String tag,
            String originalPrompt,
            RoundResult previousRound,
            StrategyAction learnedStrategy) {
        System.out.println("--- Round " + tag + " ---");

        lastUsedStrategyId = null; // reset, set per round

        String generationPrompt = originalPrompt;
        String conditionSource = "original";

        // : If there is a learned strategy, generate condition from strategy first
        if (learnedStrategy != null) {
            Optional<String> strategyCondition =
                    generateConditionFromStrategy(learnedStrategy, previousRound);
            if (strategyCondition.isPresent()) {
                String promptHead =
                        originalPrompt.substring(0, Math.min(30, originalPrompt.length()));
                if (strategyCondition.get().contains(promptHead)) {
                    generationPrompt = strategyCondition.get();
                } else {
                    generationPrompt = originalPrompt + "\n\n" + strategyCondition.get();
                }
                conditionSource = "StrategyLearning";
                lastUsedStrategyId = learnedStrategy.strategyId();
                System.out.println("Round " + tag + ": prompt constructed from learned strategy");
                System.out.println(
                        "  strategy prompt: "
                                + generationPrompt.substring(
                                        0, Math.min(200, generationPrompt.length())));
            }
        } else if (previousRound != null) {
            // optimization3: High-score baseline lightweight enhancement (preserving strategy
            // learning signal)
            if (previousRound.vbenchScore() > 0.70 && !"rawwise-v2".equals(tag)) {
                String lightConstraint = extractLightweightConstraint(previousRound);
                if (lightConstraint != null && !lightConstraint.isBlank()) {
                    System.out.println(
                            "Round "
                                    + tag
                                    + ": baseline score="
                                    + previousRound.vbenchScore()
                                    + " > 0.70, using lightweight enhancement: "
                                    + lightConstraint.substring(
                                            0, Math.min(80, lightConstraint.length())));
                    generationPrompt = originalPrompt + "\n\n" + lightConstraint;
                    conditionSource = "lightweight_high_score";
                } else {
                    System.out.println(
                            "Round "
                                    + tag
                                    + ": baseline score="
                                    + previousRound.vbenchScore()
                                    + " > 0.70, lightweight constraint extraction failed, using original prompt");
                }
            } else {
                Optional<Action> correctAction = constructCorrectActionViaPipeline(previousRound);
                if (correctAction.isPresent()) {
                    String pipelinePrompt = extractPromptFromAction(correctAction.get());
                    String promptHead =
                            originalPrompt.substring(0, Math.min(30, originalPrompt.length()));
                    if (pipelinePrompt.contains(promptHead)) {
                        generationPrompt = pipelinePrompt;
                    } else {
                        generationPrompt = originalPrompt + "\n\n" + pipelinePrompt;
                    }
                    conditionSource = "ConditionConstructionPipeline";
                    System.out.println(
                            "Round "
                                    + tag
                                    + ": prompt constructed via ConditionConstructionPipeline");
                    System.out.println(
                            "  enhanced prompt: "
                                    + generationPrompt.substring(
                                            0, Math.min(200, generationPrompt.length())));
                } else {
                    System.out.println(
                            "Round "
                                    + tag
                                    + ": ConditionConstructionPipeline returned no action, using original prompt");
                }
            }
        }

        CognitionEngine.ActionResult genResult = generateVideo(generationPrompt, tag);

        Map<String, Double> dimensionScores = evaluateWithVbench(genResult, tag);
        double vbenchScore = dimensionScores.getOrDefault("vbench_subset_score", 0.0);

        ingestAndAnnotate(tag, originalPrompt, generationPrompt, vbenchScore, conditionSource);

        return new RoundResult(
                tag,
                originalPrompt,
                generationPrompt,
                vbenchScore,
                conditionSource,
                dimensionScores);
    }

    private Optional<Action> constructCorrectActionViaPipeline(RoundResult previousRound) {
        try {
            StateSpace space =
                    stateEngine.createStateSpace(
                            "agent:video_gen",
                            "appearance_consistency:" + promptId,
                            "baseline",
                            List.of("baseline", "consistent", "drift"),
                            List.of(
                                    new StateTransition(
                                            "baseline",
                                            "consistent",
                                            "vbench_score > 0.65",
                                            Map.of()),
                                    new StateTransition(
                                            "baseline", "drift", "vbench_score <= 0.65", Map.of())),
                            Map.of("prompt_id", promptId));

            String stateName = previousRound.vbenchScore() > 0.65 ? "consistent" : "drift";
            String entryKey = "video:" + previousRound.tag();
            StateEntry entry =
                    stateEngine.write(
                            space.spaceId(),
                            entryKey,
                            String.valueOf(previousRound.vbenchScore()),
                            stateName,
                            Map.of("vbench_score", String.valueOf(previousRound.vbenchScore())));

            Optional<ContractViolation> violation =
                    stateContract.validate(space, entry, "consistent");
            if (violation.isEmpty()) {
                System.out.println(
                        "No violation — previous round passed guard, no correction needed");
                return Optional.empty();
            }

            System.out.println("Violation detected: " + violation.get().reason());

            List<GraphNode> characterNodes = graphEngine.queryNodes(graphId, "character", null, 1);
            if (!characterNodes.isEmpty()) {
                violation =
                        Optional.of(
                                new ContractViolation(
                                        violation.get().spaceId(),
                                        characterNodes.get(0).nodeId(),
                                        violation.get().fromState(),
                                        violation.get().toState(),
                                        violation.get().guardExpression(),
                                        violation.get().reason()));
                System.out.println(
                        "Rewrote violation entryKey to character node: "
                                + characterNodes.get(0).nodeId());
            }

            PromptMemoryClient memoryClient = new RawWisePromptMemoryClient(rawwiseBaseUrl);
            PromptEnhancementPlanner planner = new PromptEnhancementPlanner(memoryClient);
            TextConditionEnhancer enhancer = new PromptPlannerTextConditionEnhancer(planner);

            // LLM condition reasoning service
            ConditionReasoningService reasoningService = createReasoningService();
            if (reasoningService.isAvailable()) {
                System.out.println("LLM condition reasoning service available");
            }

            CognitionEngine cognitionEngine = new SimpleCognitionEngine();

            ConditionConstructionPipeline pipeline =
                    new ConditionConstructionPipeline(
                            graphEngine, null, cognitionEngine, enhancer, reasoningService);

            ConditionConstructionSpec spec = ConditionConstructionSpec.textOnly(graphId);

            // Build LLM reasoning context
            ConditionReasoningContext reasoningContext =
                    buildReasoningContext(previousRound, memoryClient);

            Optional<Action> action =
                    pipeline.constructConditionWithReasoning(
                            violation.get(), spec, reasoningContext);

            if (action.isPresent()) {
                System.out.println(
                        "ConditionConstructionPipeline produced action with graphEngine (graphId="
                                + graphId
                                + ")");
            }
            return action;
        } catch (Exception e) {
            System.out.println("constructCorrectActionViaPipeline failed: " + e.getMessage());
            return Optional.empty();
        }
    }

    private CognitionEngine.ActionResult generateVideo(String prompt, String tag) {
        try {
            HttpVideoGenerationClient.VideoGenerationResult result =
                    videoClient.generate(prompt, tag, promptId, 0, Map.of());

            System.out.println(
                    "  video generated: "
                            + result.videoPath
                            + " ("
                            + result.fileSize
                            + " bytes, "
                            + result.elapsedMs
                            + "ms)");

            Map<String, Object> artifacts = new HashMap<>();
            artifacts.put("video_path", result.videoPath.toString());
            artifacts.put("file_size", result.fileSize);
            artifacts.put("model_version", result.modelVersion);
            artifacts.put("elapsed_ms", result.elapsedMs);

            return new CognitionEngine.ActionResult(
                    "action:" + tag + ":" + System.currentTimeMillis(),
                    true,
                    "Video generated: " + result.videoPath,
                    artifacts,
                    System.currentTimeMillis());
        } catch (Exception e) {
            System.out.println("  generateVideo failed for tag=" + tag + ": " + e.getMessage());
            return new CognitionEngine.ActionResult(
                    "action:" + tag, false, e.getMessage(), Map.of(), System.currentTimeMillis());
        }
    }

    private Map<String, Double> evaluateWithVbench(
            CognitionEngine.ActionResult genResult, String tag) {
        if (!genResult.success()) {
            System.out.println(
                    "  Round " + tag + ": video generation failed: " + genResult.output());
            return Map.of("vbench_subset_score", 0.0);
        }

        Object videoPathObj = genResult.artifacts().get("video_path");
        if (videoPathObj == null) {
            System.out.println("  Round " + tag + ": no video_path in artifacts");
            return Map.of("vbench_subset_score", 0.0);
        }

        String videoPath = videoPathObj.toString();
        System.out.println("  Round " + tag + ": evaluating with VBench-subset (MPS)...");

        try {
            ProcessBuilder pb =
                    new ProcessBuilder(
                            "rawwise-benchmark/benchmark/vbench/.venv/bin/python",
                            "rawwise-benchmark/benchmark/vbench/vbench_mps_eval.py",
                            "--video",
                            videoPath,
                            "--prompt",
                            prompt,
                            "--num-frames",
                            "8");
            pb.directory(Path.of(System.getProperty("user.dir")).toFile());
            pb.environment().put("HF_ENDPOINT", "https://hf-mirror.com");
            pb.redirectErrorStream(true);
            Process process = pb.start();
            String output = new String(process.getInputStream().readAllBytes());
            int exit = process.waitFor();
            if (exit != 0) {
                System.out.println("  Round " + tag + ": VBench-subset failed: " + output.strip());
                return Map.of("vbench_subset_score", 0.0);
            }

            String json = output.strip();
            int lastLine = json.lastIndexOf('\n');
            if (lastLine >= 0) {
                json = json.substring(lastLine + 1);
            }

            com.fasterxml.jackson.databind.JsonNode root = MAPPER.readTree(json);
            // Extract all 7 dimensions + total score
            Map<String, Double> dimensions = new LinkedHashMap<>();
            for (String dim :
                    new String[] {
                        "subject_consistency", "background_consistency", "temporal_flickering",
                        "motion_smoothness", "dynamic_degree", "spatial_quality",
                        "overall_consistency", "vbench_subset_score"
                    }) {
                if (root.has(dim)) {
                    dimensions.put(dim, root.path(dim).asDouble(0.0));
                }
            }

            double score = dimensions.getOrDefault("vbench_subset_score", 0.0);
            System.out.println("  Round " + tag + ": vbench_subset_score=" + score);
            // Print dimension details
            dimensions.forEach(
                    (k, v) -> {
                        if (!k.equals("vbench_subset_score")) {
                            System.out.printf("    %s: %.4f%n", k, v);
                        }
                    });

            return dimensions;
        } catch (Exception e) {
            System.out.println("  Round " + tag + ": evaluation failed: " + e.getMessage());
            return Map.of("vbench_subset_score", 0.0);
        }
    }

    private void ingestAndAnnotate(
            String tag,
            String originalPrompt,
            String enhancedPrompt,
            double vbenchScore,
            String conditionSource) {
        try {
            AxiomClient axiomClient = new HttpAxiomClient(rawwiseBaseUrl);

            String runKey = "video_run:" + promptId + ":" + tag + ":" + System.currentTimeMillis();
            String runBody =
                    String.format(
                            "prompt_id=%s, tag=%s, original_prompt=%s, enhanced_prompt=%s, vbench_score=%.4f, condition_source=%s",
                            promptId,
                            tag,
                            originalPrompt,
                            enhancedPrompt,
                            vbenchScore,
                            conditionSource);
            axiomClient.ingest(
                    new AxiomIngest(
                            "default.video_generation_runs",
                            runKey,
                            runBody,
                            false,
                            Map.of(
                                    "prompt_id", promptId,
                                    "tag", tag,
                                    "vbench_score", String.valueOf(vbenchScore),
                                    "condition_source", conditionSource,
                                    "text_content",
                                            runBody.substring(
                                                    0, Math.min(512, runBody.length())))));

            Thread.sleep(1000);

            String evalKey =
                    "video_eval:" + promptId + ":" + tag + ":" + System.currentTimeMillis();
            String evalBody =
                    String.format(
                            "prompt_id=%s, tag=%s, vbench_score=%.4f, condition_source=%s, prompt=%s",
                            promptId, tag, vbenchScore, conditionSource, originalPrompt);
            axiomClient.ingest(
                    new AxiomIngest(
                            "default.video_evaluations",
                            evalKey,
                            evalBody,
                            false,
                            Map.of(
                                    "prompt_id",
                                    promptId,
                                    "tag",
                                    tag,
                                    "vbench_score",
                                    String.valueOf(vbenchScore),
                                    "text_content",
                                    evalBody.substring(0, Math.min(512, evalBody.length())))));

            Thread.sleep(1000);

            String feedbackType = vbenchScore > 0.65 ? "POSITIVE" : "NEGATIVE";
            axiomClient.annotate(
                    new AxiomAnnotation(evalKey, feedbackType, "", "vbench-closed-loop", 0.85));
            log.info("Round {}: ingested run + eval + annotated as {}", tag, feedbackType);

        } catch (Exception e) {
            log.warn("Round {}: ingest/annotate failed: {}", tag, e.getMessage());
        }
    }

    private String extractPromptFromAction(Action action) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> params = MAPPER.readValue(action.content(), Map.class);
            Object textCondition = params.get("text_condition");
            return textCondition != null ? textCondition.toString() : prompt;
        } catch (Exception e) {
            log.warn("Failed to extract prompt from action: {}", e.getMessage());
            return prompt;
        }
    }

    /**
     * fromEnvironment variable/systempropertycreate LLM condition reasoning service. If endpoint
     * not configured, returns noop.
     */
    private ConditionReasoningService createReasoningService() {
        String endpoint = System.getenv("RAWWISE_CONDITION_REASONING_ENDPOINT");
        String apiKey = System.getenv("RAWWISE_CONDITION_REASONING_API_KEY");
        String model = System.getenv("RAWWISE_CONDITION_REASONING_MODEL");

        if (endpoint == null || endpoint.isEmpty()) {
            endpoint = System.getProperty("rawwise.condition.reasoning.endpoint", "");
            apiKey = System.getProperty("rawwise.condition.reasoning.api-key", "");
            model = System.getProperty("rawwise.condition.reasoning.model", "");
        }

        if (endpoint.isEmpty() || apiKey.isEmpty()) {
            return ConditionReasoningService.noop();
        }

        return new LlmConditionReasoningService(endpoint, apiKey, model);
    }

    /** +: Create strategy learning service. Reuses condition reasoning LLM endpoint config. */
    private StrategyLearningService createStrategyLearningService() {
        String endpoint = System.getenv("RAWWISE_STRATEGY_LLM_ENDPOINT");
        String apiKey = System.getenv("RAWWISE_STRATEGY_LLM_API_KEY");
        String model = System.getenv("RAWWISE_STRATEGY_LLM_MODEL");

        if (endpoint == null || endpoint.isEmpty()) {
            endpoint = System.getProperty("rawwise.strategy.llm.endpoint", "");
            apiKey = System.getProperty("rawwise.strategy.llm.api-key", "");
            model = System.getProperty("rawwise.strategy.llm.model", "gpt-4o-mini");
        }

        if (endpoint.isEmpty() || apiKey.isEmpty()) {
            log.info("Strategy learning disabled (no LLM endpoint configured)");
            return StrategyLearningService.noop();
        }

        log.info("Strategy learning enabled: endpoint={} model={}", endpoint, model);
        return new LlmStrategyLearningService(endpoint, apiKey, model);
    }

    /**
     * : triggerstrategylearn——frombeforetworoundsfeedbackinlearnstrategy. Called after Round B
     * completes.
     */
    private void triggerStrategyLearning(RoundResult roundA, RoundResult roundB) {
        if (!strategyLearningService.isAvailable()) {
            log.info("Strategy learning not available, skipping");
            return;
        }

        try {
            List<CollectedFeedback> feedbackHistory = new ArrayList<>();
            feedbackHistory.add(roundToFeedback(roundA, "SELF"));
            feedbackHistory.add(roundToFeedback(roundB, "SELF"));

            // Build dimension-level comparison analysis(optimization2)
            String dimensionAnalysis = buildDimensionAnalysis(roundA, roundB);

            // Add meta-learning experience (optimization4)
            String metaLearningContext =
                    metaPatterns.isEmpty()
                            ? "No prior patterns available."
                            : String.join("\n", metaPatterns);

            String contextStr =
                    "prompt="
                            + prompt
                            + "\nroundA_score="
                            + roundA.vbenchScore()
                            + "\nroundB_score="
                            + roundB.vbenchScore()
                            + "\n\n=== DIMENSION-LEVEL ANALYSIS ===\n"
                            + dimensionAnalysis
                            + "\n\n=== PRIOR META-PATTERNS ===\n"
                            + metaLearningContext;

            StrategyLearningContext context =
                    new StrategyLearningContext(feedbackHistory, contextStr, List.of(), "default");

            Optional<StrategyAction> learned = strategyLearningService.learn(context);
            if (learned.isPresent()) {
                StrategyAction strategy = learned.get();
                strategyStore.put(strategy.strategyId(), strategy);
                log.info(
                        "Strategy learned: id={} scene={} confidence={}",
                        strategy.strategyId(),
                        strategy.sceneCondition(),
                        strategy.confidence());
                System.out.println(
                        "Strategy learned: "
                                + strategy.strategyContent()
                                        .substring(
                                                0,
                                                Math.min(
                                                        200, strategy.strategyContent().length())));
            } else {
                log.info("Strategy learning returned empty");
            }
        } catch (Exception e) {
            log.warn("Strategy learning failed: {}", e.getMessage());
        }
    }

    /**
     * optimization3: Extract weakest dimension from dimension scores, generate lightweight
     * constraint.
     */
    private String extractLightweightConstraint(RoundResult round) {
        Map<String, Double> dims = round.dimensionScores();
        if (dims.isEmpty()) return null;

        // Find weakest dimension (excluding total score)
        String weakestDim = null;
        double weakestScore = 1.0;
        for (Map.Entry<String, Double> e : dims.entrySet()) {
            if (e.getKey().equals("vbench_subset_score")) continue;
            if (e.getValue() < weakestScore) {
                weakestScore = e.getValue();
                weakestDim = e.getKey();
            }
        }
        if (weakestDim == null) return null;

        // Generate lightweight constraint based on weak dimension
        return switch (weakestDim) {
            case "temporal_flickering" ->
                    "Ensure smooth, flicker-free motion with consistent frame-to-frame transitions.";
            case "motion_smoothness" ->
                    "Generate continuous, fluid motion without sudden jumps or stuttering.";
            case "subject_consistency" ->
                    "Keep the main subject's appearance identical in every frame.";
            case "background_consistency" -> "Maintain consistent background throughout the video.";
            case "overall_consistency" -> "Ensure overall visual consistency across all frames.";
            case "dynamic_degree" -> "Include natural, dynamic motion appropriate for the scene.";
            case "spatial_quality" -> "Maintain high spatial quality and sharp details throughout.";
            default -> null;
        };
    }

    /** Build dimension-level comparison analysis(optimization2). */
    private String buildDimensionAnalysis(RoundResult roundA, RoundResult roundB) {
        StringBuilder sb = new StringBuilder();
        Map<String, Double> aDims = roundA.dimensionScores();
        Map<String, Double> bDims = roundB.dimensionScores();

        if (aDims.isEmpty() || bDims.isEmpty()) {
            sb.append("Dimension scores not available.\n");
            return sb.toString();
        }

        sb.append("Round A scores:\n");
        aDims.forEach(
                (k, v) ->
                        sb.append("  ")
                                .append(k)
                                .append("=")
                                .append(String.format("%.4f", v))
                                .append("\n"));

        sb.append("Round B scores:\n");
        bDims.forEach(
                (k, v) ->
                        sb.append("  ")
                                .append(k)
                                .append("=")
                                .append(String.format("%.4f", v))
                                .append("\n"));

        sb.append("\nChanges (B - A):\n");
        Set<String> allDims = new LinkedHashSet<>(aDims.keySet());
        allDims.addAll(bDims.keySet());
        for (String dim : allDims) {
            double a = aDims.getOrDefault(dim, 0.0);
            double b = bDims.getOrDefault(dim, 0.0);
            double delta = b - a;
            String tag = delta > 0.01 ? " ← IMPROVED" : (delta < -0.01 ? " ← DEGRADED" : "");
            sb.append(String.format("  %s: %+.4f%s%n", dim, delta, tag));
        }

        return sb.toString();
    }

    /**
     * : strategyretrieval——fromalreadylearnofstrategyinfindtoandcurrentweakdimensionmostmatchof.
     * optimization5: addinputdimensionmatchlogic.
     */
    private Optional<StrategyAction> retrieveStrategy() {
        return retrieveStrategy(null);
    }

    /**
     * : strategyretrieval——based ondimensionmatchretrievalstrategy.
     *
     * @param currentDimensions Current prompt dimension scores (can be null)
     */
    private Optional<StrategyAction> retrieveStrategy(Map<String, Double> currentDimensions) {
        if (strategyStore.isEmpty()) {
            return Optional.empty();
        }

        if (currentDimensions == null || currentDimensions.isEmpty()) {
            // When no dimension info, take highest confidence
            return strategyStore.values().stream()
                    .max((a, b) -> Double.compare(a.confidence(), b.confidence()));
        }

        // Find current weakest dimension
        String weakestDim = null;
        double weakestScore = 1.0;
        for (Map.Entry<String, Double> e : currentDimensions.entrySet()) {
            if (e.getKey().equals("vbench_subset_score")) continue;
            if (e.getValue() < weakestScore) {
                weakestScore = e.getValue();
                weakestDim = e.getKey();
            }
        }

        if (weakestDim == null) {
            return strategyStore.values().stream()
                    .max((a, b) -> Double.compare(a.confidence(), b.confidence()));
        }

        // Prefer strategy matching weak dimension
        final String targetDim = weakestDim;
        return strategyStore.values().stream()
                .max(
                        (a, b) -> {
                            double scoreA = scoreStrategyForDimension(a, targetDim);
                            double scoreB = scoreStrategyForDimension(b, targetDim);
                            return Double.compare(scoreA, scoreB);
                        });
    }

    /** optimization5: Compute strategy-to-target-dimension match score. */
    private double scoreStrategyForDimension(StrategyAction strategy, String targetDim) {
        double baseScore = strategy.confidence();
        // strategyof conditionMetrics containtargetdimension → bonus
        if (strategy.conditionMetrics().containsKey(targetDim)) {
            baseScore += 0.3;
        }
        // strategyof sceneCondition or strategyContent mentiontargetdimension → bonus
        String dimSimple = targetDim.replace("_", " ");
        if (strategy.sceneCondition().toLowerCase().contains(dimSimple)
                || strategy.strategyContent().toLowerCase().contains(dimSimple)) {
            baseScore += 0.2;
        }
        // strategyhas effective=true ofhistoryrecord → bonus
        long effectiveCount =
                strategy.historicalEffects().stream().mapToLong(e -> e.effective() ? 1 : 0).sum();
        baseScore += effectiveCount * 0.1;
        return baseScore;
    }

    /** : Generate improved condition based on strategy. */
    private Optional<String> generateConditionFromStrategy(
            StrategyAction strategy, RoundResult previousRound) {
        ConditionReasoningService reasoningService = createReasoningService();
        if (!reasoningService.isAvailable()) {
            return Optional.of(strategy.strategyContent());
        }

        try {
            String strategySummary =
                    "Strategy: "
                            + strategy.strategyContent()
                            + "\nScene: "
                            + strategy.sceneCondition()
                            + "\nExpected: "
                            + strategy.expectedEffect()
                            + "\nConfidence: "
                            + strategy.confidence();

            // Pass all dimension scores(optimization1)
            Map<String, Double> evalScores = new HashMap<>(previousRound.dimensionScores());
            if (!evalScores.containsKey("vbench_subset_score")) {
                evalScores.put("vbench_subset_score", previousRound.vbenchScore());
            }

            ConditionReasoningContext reasoningContext =
                    new ConditionReasoningContext(
                            prompt,
                            List.of(strategySummary),
                            Map.of(),
                            Map.of(),
                            evalScores,
                            List.of());

            ConditionReasoningResult result = reasoningService.reason(reasoningContext);
            if (result != null
                    && result.improvedCondition() != null
                    && !result.improvedCondition().isBlank()) {
                return Optional.of(result.improvedCondition());
            }
        } catch (Exception e) {
            log.warn("Strategy-based condition generation failed: {}", e.getMessage());
        }

        return Optional.of(strategy.strategyContent());
    }

    /** : backfillstrategyeffect——record before/after metrics. Called after Round C completes. */
    private void recordStrategyEffect(
            RoundResult beforeRound, RoundResult afterRound, String strategyId) {
        if (strategyId == null) return;
        StrategyAction strategy = strategyStore.get(strategyId);
        if (strategy == null) {
            log.warn("Cannot record effect: strategy not found for id={}", strategyId);
            return;
        }

        // Record all dimensions(optimization1)
        Map<String, Double> beforeMetrics = new HashMap<>(beforeRound.dimensionScores());
        beforeMetrics.putIfAbsent("vbench_subset_score", beforeRound.vbenchScore());

        Map<String, Double> afterMetrics = new HashMap<>(afterRound.dimensionScores());
        afterMetrics.putIfAbsent("vbench_subset_score", afterRound.vbenchScore());

        double delta = afterRound.vbenchScore() - beforeRound.vbenchScore();
        double effectivenessMembership = delta > 0 ? 1.0 : 0.0;

        StrategyAction.EffectRecord effect =
                new StrategyAction.EffectRecord(
                        "attempt:" + System.currentTimeMillis(),
                        afterRound.tag(),
                        beforeMetrics,
                        afterMetrics,
                        delta,
                        effectivenessMembership,
                        "ENV");

        List<StrategyAction.EffectRecord> newEffects =
                new ArrayList<>(strategy.historicalEffects());
        if (newEffects.stream().noneMatch(e -> e.attemptId().equals(effect.attemptId()))) {
            newEffects.add(effect);
        }
        double newConfidence = StrategyAction.computeConfidence(newEffects);

        StrategyAction updated =
                new StrategyAction(
                        strategy.strategyId(),
                        strategy.sceneCondition(),
                        strategy.conditionMetrics(),
                        strategy.strategyContent(),
                        strategy.expectedEffect(),
                        newEffects,
                        newConfidence,
                        strategy.sourceViewIds(),
                        strategy.sourceFeedbackIds(),
                        strategy.createdAt(),
                        System.currentTimeMillis());
        strategyStore.put(strategyId, updated);

        // optimization4: Update meta-learning patterns (including strategy content summary)
        String strategySummary =
                strategy.strategyContent().length() > 100
                        ? strategy.strategyContent().substring(0, 100) + "..."
                        : strategy.strategyContent();
        updateMetaPatterns(
                beforeRound, afterRound, effectivenessMembership >= 0.5, strategySummary);

        log.info(
                "Effect recorded for strategy {}: delta={:+.4f} effective={} confidence={}",
                strategyId,
                delta,
                effectivenessMembership >= 0.5,
                newConfidence);
        System.out.printf(
                "Strategy effect: delta=%+.4f effective=%s confidence=%.2f%n",
                delta, effectivenessMembership >= 0.5, newConfidence);
    }

    /** optimization4: Extract cross-prompt meta-learning patterns from strategy effects. */
    private void updateMetaPatterns(
            RoundResult beforeRound,
            RoundResult afterRound,
            boolean effective,
            String strategySummary) {
        Map<String, Double> before = beforeRound.dimensionScores();
        Map<String, Double> after = afterRound.dimensionScores();
        if (before.isEmpty() || after.isEmpty()) return;

        Set<String> allDims = new LinkedHashSet<>(before.keySet());
        allDims.addAll(after.keySet());
        for (String dim : allDims) {
            double b = before.getOrDefault(dim, 0.0);
            double a = after.getOrDefault(dim, 0.0);
            double dimDelta = a - b;
            if (Math.abs(dimDelta) > 0.03) {
                String pattern =
                        String.format(
                                "Pattern: %s %s → strategy '%s' %s (dim_delta=%+.4f, overall %s)",
                                dim,
                                b < 0.5 ? "LOW" : (b > 0.8 ? "HIGH" : "MID"),
                                strategySummary,
                                effective ? "EFFECTIVE" : "INEFFECTIVE",
                                dimDelta,
                                effective ? "helped" : "hurt");
                if (!metaPatterns.contains(pattern)) {
                    metaPatterns.add(pattern);
                    System.out.println("  [META] " + pattern);
                }
            }
        }
    }

    /** Convert RoundResult to CollectedFeedback. */
    private CollectedFeedback roundToFeedback(RoundResult round, String source) {
        Map<String, Double> metrics = new HashMap<>(round.dimensionScores());
        if (!metrics.containsKey("vbench_subset_score")) {
            metrics.put("vbench_subset_score", round.vbenchScore());
        }

        return new CollectedFeedback(
                "view:" + round.tag(),
                "v1",
                metrics,
                round.vbenchScore(),
                "quality_score",
                round.vbenchScore(),
                source,
                System.currentTimeMillis());
    }

    /**
     * Build LLM reasoning context. Extract reasoning-required info from previous round results,
     * memory retrieval, graph entities, violation info.
     */
    private ConditionReasoningContext buildReasoningContext(
            RoundResult previousRound, PromptMemoryClient memoryClient) {
        // 1. Retrieve relevant factual memories
        List<String> facts = List.of();
        try {
            List<PromptMemoryRecord> records = memoryClient.recall(prompt, 5);
            facts =
                    records.stream()
                            .map(PromptMemoryRecord::summary)
                            .filter(s -> s != null && !s.isBlank())
                            .limit(5)
                            .toList();
        } catch (Exception e) {
            System.out.println("Memory search failed for reasoning context: " + e.getMessage());
        }

        // 2. Extract graph entity summary
        Map<String, String> entitySummary = new HashMap<>();
        try {
            for (String type : List.of("character", "clothing", "scene", "action")) {
                List<GraphNode> nodes = graphEngine.queryNodes(graphId, type, null, 1);
                if (!nodes.isEmpty()) {
                    String name = nodes.get(0).properties().get("name");
                    String desc = nodes.get(0).properties().get("description");
                    entitySummary.put(type, name != null ? name.replace('_', ' ') : desc);
                }
            }
        } catch (Exception e) {
            System.out.println("Graph query failed for reasoning context: " + e.getMessage());
        }

        // 3. Violation info
        Map<String, Double> violationMeta = new HashMap<>();
        if (previousRound.vbenchScore() <= 0.65) {
            violationMeta.put("overall_score", previousRound.vbenchScore());
        }

        // 4. Evaluation scores
        Map<String, Double> evalScores = new HashMap<>();
        evalScores.put("vbench_subset_score", previousRound.vbenchScore());

        // 5. Previous attempts
        List<ConditionReasoningContext.PreviousAttempt> attempts = new ArrayList<>();
        if (previousRound.tag() != null && !previousRound.tag().equals("baseline")) {
            attempts.add(
                    new ConditionReasoningContext.PreviousAttempt(
                            previousRound.enhancedPrompt(),
                            evalScores,
                            previousRound.vbenchScore() > 0.65 ? "improved" : "degraded"));
        }

        return new ConditionReasoningContext(
                prompt, facts, entitySummary, violationMeta, evalScores, attempts);
    }

    public static void main(String[] args) {
        // Configure SLF4J to output to stdout (log4j2 may not initialize correctly during
        // exec:java)
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.logStream", "out");

        String baseUrl = System.getProperty("rawwise.base.url", "http://localhost:8080");

        // Batch mode: when args[0] == "--batch" runs preset prompts (optional args[1] specifies
        // count)
        if (args.length > 0 && args[0].equals("--batch")) {
            String[] allPrompts = {
                "A young woman in a red jacket walking down a busy street",
                "An elderly man playing chess in a quiet park",
                "A child picking flowers in a sunny garden",
                "A chef preparing sushi in a modern kitchen",
                "A musician playing guitar on a small stage",
                "A scientist examining samples in a laboratory",
                "A crowded night market with colorful lanterns and food stalls",
                "A serene mountain lake reflecting snow-capped peaks at sunrise",
                "An ancient library with tall wooden bookshelves and warm candlelight",
                "A futuristic city skyline at dusk with flying vehicles",
                "A dog catching a frisbee in a grassy field",
                "A dancer performing ballet in a grand theater",
                "A fisherman on a boat at dawn casting nets into calm waters",
                "A painter working on a canvas in a sunlit studio",
                "A train traveling through a snowy mountain landscape"
            };

            // Supports specifying prompt count: --batch 3 runs only first 3
            int numPrompts = allPrompts.length;
            if (args.length > 1) {
                try {
                    numPrompts =
                            Math.max(1, Math.min(Integer.parseInt(args[1]), allPrompts.length));
                } catch (NumberFormatException ignored) {
                }
            }
            String[] prompts = java.util.Arrays.copyOf(allPrompts, numPrompts);

            System.out.println("[MAIN] Batch mode: " + prompts.length + " prompts");
            System.out.println("[MAIN] baseUrl=" + baseUrl);

            List<double[]> allResults = new ArrayList<>();
            List<String> completedPrompts = new ArrayList<>();
            List<String> failedPrompts = new ArrayList<>();

            for (int i = 0; i < prompts.length; i++) {
                String prompt = prompts[i];
                String promptId = "batch_p" + (i + 1);
                System.out.println(
                        "\n========== Batch Prompt "
                                + (i + 1)
                                + "/"
                                + prompts.length
                                + " ==========");
                System.out.println("[MAIN] prompt=" + prompt);
                try {
                    ClosedLoopBenchmarkRunner runner =
                            new ClosedLoopBenchmarkRunner(baseUrl, prompt, promptId);
                    runner.runClosedLoop();
                    // Get results from runner
                    double scoreA = runner.lastResult != null ? runner.lastResult.scoreA() : 0.0;
                    double scoreB = runner.lastResult != null ? runner.lastResult.scoreB() : 0.0;
                    double scoreC = runner.lastResult != null ? runner.lastResult.scoreC() : 0.0;
                    allResults.add(new double[] {scoreA, scoreB, scoreC});
                    completedPrompts.add(prompt);
                    System.out.println(
                            "[BATCH] Prompt "
                                    + (i + 1)
                                    + " completed: A="
                                    + scoreA
                                    + " B="
                                    + scoreB
                                    + " C="
                                    + scoreC);
                } catch (Exception e) {
                    System.err.println("[BATCH] Prompt " + (i + 1) + " FAILED: " + e.getMessage());
                    failedPrompts.add(prompt);
                }
            }

            // Output statistical results
            System.out.println("\n========== Batch Statistics ==========");
            System.out.println("Completed: " + completedPrompts.size() + "/" + prompts.length);
            if (!failedPrompts.isEmpty()) {
                System.out.println("Failed: " + failedPrompts.size());
            }

            if (!allResults.isEmpty()) {
                int n = allResults.size();
                double sumA = 0, sumB = 0, sumC = 0;
                double sumBA = 0, sumCB = 0, sumCA = 0;
                int winsBA = 0, winsCB = 0, winsCA = 0;
                int validBA = 0, validCB = 0, validCA = 0;

                for (double[] r : allResults) {
                    double a = r[0], b = r[1], c = r[2];
                    sumA += a;
                    sumB += b;
                    sumC += c;
                    if (a > 0 && b > 0) {
                        sumBA += (b - a);
                        validBA++;
                        if (b > a) winsBA++;
                    }
                    if (b > 0 && c > 0) {
                        sumCB += (c - b);
                        validCB++;
                        if (c > b) winsCB++;
                    }
                    if (a > 0 && c > 0) {
                        sumCA += (c - a);
                        validCA++;
                        if (c > a) winsCA++;
                    }
                }

                double meanA = sumA / n;
                double meanB = sumB / n;
                double meanC = sumC / n;
                double meanBA = validBA > 0 ? sumBA / validBA : 0;
                double meanCB = validCB > 0 ? sumCB / validCB : 0;
                double meanCA = validCA > 0 ? sumCA / validCA : 0;

                // Paired t-test
                double pBA = computePairedTTestPValue(allResults, 0, 1);
                double pCB = computePairedTTestPValue(allResults, 1, 2);
                double pCA = computePairedTTestPValue(allResults, 0, 2);

                System.out.println("\n--- Results per prompt ---");
                System.out.printf(
                        "%-4s %-50s %8s %8s %8s %8s %8s %8s%n",
                        "#", "Prompt", "A", "B", "C", "B-A", "C-B", "C-A");
                for (int i = 0; i < allResults.size(); i++) {
                    double[] r = allResults.get(i);
                    double a = r[0], b = r[1], c = r[2];
                    String shortPrompt =
                            completedPrompts
                                    .get(i)
                                    .substring(0, Math.min(50, completedPrompts.get(i).length()));
                    System.out.printf(
                            "%-4d %-50s %8.4f %8.4f %8.4f %+8.4f %+8.4f %+8.4f%n",
                            i + 1, shortPrompt, a, b, c, b - a, c - b, c - a);
                }

                System.out.println("\n--- Statistical Summary ---");
                System.out.printf("Mean A (baseline):    %.4f%n", meanA);
                System.out.printf("Mean B (rawwise-v1):  %.4f%n", meanB);
                System.out.printf("Mean C (rawwise-v2):  %.4f%n", meanC);
                System.out.printf(
                        "%nH1 (A→B): mean_delta=%+.4f, p=%.4f, wins=%d/%d %s%n",
                        meanBA, pBA, winsBA, validBA, pBA < 0.05 ? "*** SIGNIFICANT" : "");
                System.out.printf(
                        "H2 (B→C): mean_delta=%+.4f, p=%.4f, wins=%d/%d %s%n",
                        meanCB, pCB, winsCB, validCB, pCB < 0.05 ? "*** SIGNIFICANT" : "");
                System.out.printf(
                        "H3 (A→C): mean_delta=%+.4f, p=%.4f, wins=%d/%d %s%n",
                        meanCA, pCA, winsCA, validCA, pCA < 0.05 ? "*** SIGNIFICANT" : "");

                // Save results to JSON
                try {
                    var resultsObj = new java.util.LinkedHashMap<String, Object>();
                    resultsObj.put("total_prompts", prompts.length);
                    resultsObj.put("completed", completedPrompts.size());
                    resultsObj.put("failed", failedPrompts.size());
                    resultsObj.put("mean_A", meanA);
                    resultsObj.put("mean_B", meanB);
                    resultsObj.put("mean_C", meanC);
                    resultsObj.put("H1_AB_delta", meanBA);
                    resultsObj.put("H1_AB_pvalue", pBA);
                    resultsObj.put("H2_BC_delta", meanCB);
                    resultsObj.put("H2_BC_pvalue", pCB);
                    resultsObj.put("H3_AC_delta", meanCA);
                    resultsObj.put("H3_AC_pvalue", pCA);

                    var perPrompt = new java.util.ArrayList<Object>();
                    for (int i = 0; i < allResults.size(); i++) {
                        double[] r = allResults.get(i);
                        var entry = new java.util.LinkedHashMap<String, Object>();
                        entry.put("prompt_id", "batch_p" + (i + 1));
                        entry.put("prompt", completedPrompts.get(i));
                        entry.put("A", r[0]);
                        entry.put("B", r[1]);
                        entry.put("C", r[2]);
                        entry.put("B_minus_A", r[1] - r[0]);
                        entry.put("C_minus_B", r[2] - r[1]);
                        entry.put("C_minus_A", r[2] - r[0]);
                        perPrompt.add(entry);
                    }
                    resultsObj.put("per_prompt", perPrompt);

                    String jsonOutput =
                            MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(resultsObj);
                    String outputPath = "benchmark/vbench/eval_results/batch_strategy_results.json";
                    java.nio.file.Files.createDirectories(
                            java.nio.file.Path.of(outputPath).getParent());
                    java.nio.file.Files.writeString(java.nio.file.Path.of(outputPath), jsonOutput);
                    System.out.println("\nResults saved to " + outputPath);
                } catch (Exception e) {
                    System.err.println("Failed to save JSON: " + e.getMessage());
                }
            }
            return;
        }

        // Single prompt mode (original logic)
        String prompt =
                args.length > 0 ? args[0] : "A young woman in a red jacket walking down a street";
        String promptId = args.length > 1 ? args[1] : "prompt_closed_loop";

        System.out.println("[MAIN] Starting ClosedLoopBenchmarkRunner");
        System.out.println(
                "[MAIN] baseUrl=" + baseUrl + ", prompt=" + prompt + ", promptId=" + promptId);

        new ClosedLoopBenchmarkRunner(baseUrl, prompt, promptId).runClosedLoop();
    }

    /**
     * Paired t-testof p valuecompute(twosided). Simplified: returns only p-value, no confidence
     * interval.
     */
    private static double computePairedTTestPValue(List<double[]> results, int idx1, int idx2) {
        List<Double> deltas = new ArrayList<>();
        for (double[] r : results) {
            if (r[idx1] > 0 && r[idx2] > 0) {
                deltas.add(r[idx2] - r[idx1]);
            }
        }
        if (deltas.size() < 2) return 1.0;

        int n = deltas.size();
        double mean = deltas.stream().mapToDouble(d -> d).average().orElse(0);
        double variance = deltas.stream().mapToDouble(d -> Math.pow(d - mean, 2)).sum() / (n - 1);
        double std = Math.sqrt(variance);
        if (std == 0) return 1.0;

        double t = mean / (std / Math.sqrt(n));
        // Use Apache Commons TDistribution to compute p-value
        // Simplified: normal approximation (sufficiently accurate when n >= 15)
        int df = n - 1;
        // t-distribution CDF approximation
        double p = 2.0 * (1.0 - tDistributionCDF(Math.abs(t), df));
        return Math.min(1.0, Math.max(0.0, p));
    }

    /**
     * t-distribution CDF approximate calculation (using Student t distribution series expansion).
     */
    private static double tDistributionCDF(double t, int df) {
        // Approximate using incomplete Beta function
        double x = df / (df + t * t);
        double ibeta = incompleteBeta(0.5 * df, 0.5, x);
        return 1.0 - 0.5 * ibeta;
    }

    /** Incomplete Beta function continued fraction approximation. */
    private static double incompleteBeta(double a, double b, double x) {
        if (x <= 0) return 0;
        if (x >= 1) return 1;
        double lbeta = logGamma(a) + logGamma(b) - logGamma(a + b);
        double front = Math.exp(Math.log(x) * a + Math.log(1 - x) * b - lbeta) / a;
        if (x < (a + 1) / (a + b + 2)) {
            return front * betaCF(a, b, x);
        } else {
            return 1 - front * betaCF(b, a, 1 - x);
        }
    }

    private static double betaCF(double a, double b, double x) {
        int maxIter = 200;
        double epsilon = 1e-15;
        double qab = a + b;
        double qap = a + 1;
        double qam = a - 1;
        double c = 1;
        double d = 1 - qab * x / qap;
        if (Math.abs(d) < epsilon) d = epsilon;
        d = 1 / d;
        double result = d;
        for (int m = 1; m <= maxIter; m++) {
            int m2 = 2 * m;
            double aa = m * (b - m) * x / ((qam + m2) * (a + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < epsilon) d = epsilon;
            c = 1 + aa / c;
            if (Math.abs(c) < epsilon) c = epsilon;
            d = 1 / d;
            result *= d * c;
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
            d = 1 + aa * d;
            if (Math.abs(d) < epsilon) d = epsilon;
            c = 1 + aa / c;
            if (Math.abs(c) < epsilon) c = epsilon;
            d = 1 / d;
            double del = d * c;
            result *= del;
            if (Math.abs(del - 1) < epsilon) break;
        }
        return result;
    }

    private static double logGamma(double x) {
        double[] coef = {
            76.18009172947146,
            -86.50532032941677,
            24.01409824083091,
            -1.231739572450155,
            0.1208650973866179e-2,
            -0.5395239384953e-5
        };
        double y = x;
        double tmp = x + 5.5;
        tmp -= (x + 0.5) * Math.log(tmp);
        double ser = 1.000000000190015;
        for (double c : coef) {
            y += 1;
            ser += c / y;
        }
        return -tmp + Math.log(2.5066282746310005 * ser / x);
    }

    private record RoundResult(
            String tag,
            String originalPrompt,
            String enhancedPrompt,
            double vbenchScore,
            String conditionSource,
            Map<String, Double> dimensionScores) {
        RoundResult(
                String tag,
                String originalPrompt,
                String enhancedPrompt,
                double vbenchScore,
                String conditionSource) {
            this(tag, originalPrompt, enhancedPrompt, vbenchScore, conditionSource, Map.of());
        }
    }

    /** Minimal in-process StateEngine for closed-loop benchmark. */
    private static class SimpleStateEngine implements StateEngine {
        private final Map<String, StateSpace> spaces = new ConcurrentHashMap<>();
        private final Map<String, Map<String, StateEntry>> entries = new ConcurrentHashMap<>();

        @Override
        public StateSpace createStateSpace(
                String agentId,
                String name,
                String initialState,
                List<String> states,
                List<StateTransition> transitions,
                Map<String, String> metadata) {
            String spaceId = "space:" + UUID.randomUUID().toString().substring(0, 8);
            long now = System.currentTimeMillis();
            var space =
                    new StateSpace(
                            spaceId,
                            agentId,
                            name,
                            initialState,
                            List.copyOf(states),
                            List.copyOf(transitions),
                            metadata == null ? Map.of() : Map.copyOf(metadata),
                            now,
                            now);
            spaces.put(spaceId, space);
            entries.put(spaceId, new ConcurrentHashMap<>());
            return space;
        }

        @Override
        public Optional<StateSpace> getStateSpace(String spaceId) {
            return Optional.ofNullable(spaces.get(spaceId));
        }

        @Override
        public List<StateSpace> listStateSpacesByAgent(String agentId) {
            return spaces.values().stream().filter(s -> s.agentId().equals(agentId)).toList();
        }

        @Override
        public StateEntry read(String spaceId, String key) {
            Map<String, StateEntry> spaceEntries = entries.get(spaceId);
            return spaceEntries == null ? null : spaceEntries.get(key);
        }

        @Override
        public StateEntry write(
                String spaceId,
                String key,
                String value,
                String stateName,
                Map<String, String> metadata) {
            Map<String, StateEntry> spaceEntries =
                    entries.computeIfAbsent(spaceId, k -> new ConcurrentHashMap<>());
            StateEntry existing = spaceEntries.get(key);
            long now = System.currentTimeMillis();
            long version = existing == null ? 1 : existing.version() + 1;
            long createdAt = existing == null ? now : existing.createdAt();
            StateEntry entry =
                    new StateEntry(
                            key,
                            value,
                            stateName,
                            version,
                            metadata == null ? Map.of() : Map.copyOf(metadata),
                            createdAt,
                            now);
            spaceEntries.put(key, entry);
            return entry;
        }

        @Override
        public StateEntry transition(
                String spaceId, String key, String toState, Map<String, String> metadata) {
            Map<String, StateEntry> spaceEntries = entries.get(spaceId);
            if (spaceEntries == null) return null;
            StateEntry current = spaceEntries.get(key);
            if (current == null) return null;
            long now = System.currentTimeMillis();
            StateEntry updated =
                    new StateEntry(
                            key,
                            current.value(),
                            toState,
                            current.version() + 1,
                            metadata == null ? current.metadata() : Map.copyOf(metadata),
                            current.createdAt(),
                            now);
            spaceEntries.put(key, updated);
            return updated;
        }

        @Override
        public List<StateEntry> scan(String spaceId, String prefix, int limit) {
            Map<String, StateEntry> spaceEntries = entries.get(spaceId);
            if (spaceEntries == null) return List.of();
            return spaceEntries.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(prefix))
                    .limit(limit)
                    .map(Map.Entry::getValue)
                    .toList();
        }

        @Override
        public boolean deleteStateSpace(String spaceId) {
            spaces.remove(spaceId);
            entries.remove(spaceId);
            return true;
        }
    }

    /** Minimal CognitionEngine for condition construction. */
    private static class SimpleCognitionEngine implements CognitionEngine {
        @Override
        public Semantic deriveSemantic(
                List<String> viewIds, Semantic.SemanticType type, Map<String, Object> params) {
            return null;
        }

        @Override
        public Action deriveAction(
                List<String> semanticIds, Action.ActionType type, Map<String, Object> params) {
            try {
                String actionId = "action:" + type + ":" + System.currentTimeMillis();
                byte[] content = MAPPER.writeValueAsBytes(params);
                return new Action(actionId, List.copyOf(semanticIds), type, content, true);
            } catch (Exception e) {
                throw new RuntimeException("Failed to derive action", e);
            }
        }

        @Override
        public ActionPlan deriveActionPlan(
                List<String> semanticIds, PlanType planType, Map<String, Object> params) {
            return new ActionPlan(
                    "plan:" + System.currentTimeMillis(), List.of(), planType, Map.of());
        }

        @Override
        public ActionResult execute(String actionId, ExecutionContext context) {
            return new ActionResult(
                    actionId, false, "not implemented", Map.of(), System.currentTimeMillis());
        }

        @Override
        public PlanResult executePlan(String planId, ExecutionContext context) {
            return new PlanResult(
                    planId, false, List.of(), "not implemented", System.currentTimeMillis());
        }

        @Override
        public CognitiveAssetMetric cognitiveAssetMetric() {
            return new CognitiveAssetMetric(0, 0.0, 0.0);
        }

        @Override
        public CognitiveAssetMetric incrementalAssetUpdate(String viewId) {
            return new CognitiveAssetMetric(0, 0.0, 0.0);
        }
    }

    /** Minimal ExecutionContext. */
    private record SimpleExecutionContext(String promptId, Map<String, Object> params)
            implements ExecutionContext {
        @Override
        public Raw raw() {
            return null;
        }

        @Override
        public List<Chunk> chunks() {
            return List.of();
        }

        @Override
        public List<View> views() {
            return List.of();
        }

        @Override
        public Optional<Semantic> semantics() {
            return Optional.empty();
        }

        @Override
        public Map<String, Object> params() {
            return params;
        }
    }
}
