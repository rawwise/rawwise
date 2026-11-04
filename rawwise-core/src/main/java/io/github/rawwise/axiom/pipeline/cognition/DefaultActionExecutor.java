package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.orchestrator.CoordEngine;
import io.github.rawwise.axiom.orchestrator.LockHandle;
import io.github.rawwise.axiom.pipeline.KnowledgeDataAccessor;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.DefaultFeedbackAnnotation;
import io.github.rawwise.model.record.FeedbackAnnotation;
import io.github.rawwise.model.record.View;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultActionExecutor implements ActionExecutor {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    private final String executorId;
    private final Metastore metastore;
    private final KnowledgeStore knowledgeStore;
    private final KnowledgeDataAccessor dataAccessor;
    private final CorrectionCallback correctionCallback;
    private final CoordEngine coordEngine;
    private ActionExecutionLogger executionLogger;
    private static final Duration CORRECTION_LOCK_TTL = Duration.ofSeconds(30);

    public DefaultActionExecutor() {
        this(null, null, null, null);
    }

    public DefaultActionExecutor(String executorId) {
        this.executorId = executorId;
        this.metastore = null;
        this.knowledgeStore = null;
        this.dataAccessor = null;
        this.correctionCallback = null;
        this.coordEngine = null;
    }

    public DefaultActionExecutor(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            CorrectionCallback correctionCallback) {
        this(metastore, knowledgeStore, correctionCallback, null);
    }

    public DefaultActionExecutor(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            CorrectionCallback correctionCallback,
            CoordEngine coordEngine) {
        this.executorId = "default-executor-" + UUID.randomUUID().toString().substring(0, 8);
        this.metastore = metastore;
        this.knowledgeStore = knowledgeStore;
        this.dataAccessor =
                metastore != null && knowledgeStore != null
                        ? new KnowledgeDataAccessor(metastore, knowledgeStore)
                        : null;
        this.correctionCallback = correctionCallback;
        this.coordEngine = coordEngine;
    }

    public DefaultActionExecutor(
            Metastore metastore,
            KnowledgeStore knowledgeStore,
            CorrectionCallback correctionCallback,
            CoordEngine coordEngine,
            ActionExecutionLogger executionLogger) {
        this(metastore, knowledgeStore, correctionCallback, coordEngine);
        this.executionLogger = executionLogger;
    }

    @Override
    public String executorId() {
        return executorId;
    }

    @Override
    public Set<Action.ActionType> supportedActions() {
        return Set.of(Action.ActionType.values());
    }

    @Override
    public CognitionEngine.ActionResult execute(Action action, ExecutionContext context) {
        long start = System.currentTimeMillis();
        if (executionLogger != null) {
            executionLogger.logStart(action.actionId(), null, null, action.actionType());
        }
        try {
            String output = executeInternal(action, context, 0);
            Map<String, Object> artifacts = collectArtifacts(action, context);
            if (executionLogger != null) {
                executionLogger.logComplete(
                        action.actionId(),
                        null,
                        null,
                        action.actionType(),
                        start,
                        output,
                        true,
                        null);
            }
            return new CognitionEngine.ActionResult(
                    action.actionId(), true, output, artifacts, System.currentTimeMillis());
        } catch (Exception e) {
            if (executionLogger != null) {
                executionLogger.logComplete(
                        action.actionId(),
                        null,
                        null,
                        action.actionType(),
                        start,
                        null,
                        false,
                        e.getMessage());
            }
            log.warn("Action {} execution failed: {}", action.actionId(), e.getMessage());
            return new CognitionEngine.ActionResult(
                    action.actionId(), false, e.getMessage(), Map.of(), System.currentTimeMillis());
        }
    }

    public CognitionEngine.ActionResult execute(
            Action action, ExecutionContext context, int depth) {
        long start = System.currentTimeMillis();
        try {
            String output = executeInternal(action, context, depth);
            Map<String, Object> artifacts = collectArtifacts(action, context);

            return new CognitionEngine.ActionResult(
                    action.actionId(), true, output, artifacts, System.currentTimeMillis());
        } catch (Exception e) {
            log.warn("Action {} execution failed: {}", action.actionId(), e.getMessage());
            return new CognitionEngine.ActionResult(
                    action.actionId(), false, e.getMessage(), Map.of(), System.currentTimeMillis());
        }
    }

    @Override
    public CognitionEngine.PlanResult executePlan(ActionPlan plan, ExecutionContext context) {
        long start = System.currentTimeMillis();
        List<CognitionEngine.ActionResult> results;

        if (plan.planType() == CognitionEngine.PlanType.DAG && !plan.dependencies().isEmpty()) {
            results = executeDagPlan(plan, context);
        } else {
            results = executeChainPlan(plan, context);
        }

        boolean success = results.stream().allMatch(CognitionEngine.ActionResult::success);
        String summary = buildPlanSummary(plan, results, System.currentTimeMillis() - start);

        return new CognitionEngine.PlanResult(
                plan.planId(), success, results, summary, System.currentTimeMillis());
    }

    private List<CognitionEngine.ActionResult> executeChainPlan(
            ActionPlan plan, ExecutionContext context) {
        List<CognitionEngine.ActionResult> results = new ArrayList<>();
        for (Action action : plan.actions()) {
            CognitionEngine.ActionResult result = execute(action, context);
            results.add(result);
            if (!result.success()) {
                log.warn(
                        "Chain plan {} stopped at action {} due to failure",
                        plan.planId(),
                        action.actionId());
                break;
            }
        }
        return results;
    }

    private List<CognitionEngine.ActionResult> executeDagPlan(
            ActionPlan plan, ExecutionContext context) {
        List<Action> sorted = topologicalSort(plan.actions(), plan.dependencies());
        if (sorted == null) {
            log.error(
                    "DAG plan {} has cyclic dependencies, falling back to sequential execution",
                    plan.planId());
            sorted = plan.actions();
        }

        Map<String, CognitionEngine.ActionResult> completedResults = new LinkedHashMap<>();
        List<CognitionEngine.ActionResult> orderedResults = new ArrayList<>();

        for (Action action : sorted) {
            Set<String> deps = plan.dependencies().getOrDefault(action.actionId(), Set.of());
            boolean depsSatisfied = true;
            for (String depId : deps) {
                CognitionEngine.ActionResult depResult = completedResults.get(depId);
                if (depResult == null || !depResult.success()) {
                    depsSatisfied = false;
                    log.warn(
                            "DAG plan {}: action {} skipped, dependency {} not satisfied",
                            plan.planId(),
                            action.actionId(),
                            depId);
                    CognitionEngine.ActionResult skipped =
                            new CognitionEngine.ActionResult(
                                    action.actionId(),
                                    false,
                                    "dependency not satisfied: " + depId,
                                    Map.of(),
                                    System.currentTimeMillis());
                    completedResults.put(action.actionId(), skipped);
                    orderedResults.add(skipped);
                    break;
                }
            }

            if (depsSatisfied) {
                CognitionEngine.ActionResult result = execute(action, context);
                completedResults.put(action.actionId(), result);
                orderedResults.add(result);
            }
        }

        return orderedResults;
    }

    public List<Action> topologicalSort(
            List<Action> actions, Map<String, Set<String>> dependencies) {
        Map<String, Action> actionMap = new LinkedHashMap<>();
        for (Action a : actions) {
            actionMap.put(a.actionId(), a);
        }

        Map<String, Integer> inDegree = new LinkedHashMap<>();
        Map<String, Set<String>> adjacency = new LinkedHashMap<>();

        for (Action a : actions) {
            inDegree.put(a.actionId(), 0);
            adjacency.put(a.actionId(), new java.util.HashSet<>());
        }

        for (Map.Entry<String, Set<String>> entry : dependencies.entrySet()) {
            String target = entry.getKey();
            for (String source : entry.getValue()) {
                if (!actionMap.containsKey(source)) {
                    log.warn("Topological sort: dependency source {} not found in actions", source);
                    continue;
                }
                adjacency.get(source).add(target);
                inDegree.merge(target, 1, Integer::sum);
            }
        }

        java.util.Queue<String> queue = new java.util.ArrayDeque<>();
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.add(entry.getKey());
            }
        }

        List<Action> sorted = new ArrayList<>();
        while (!queue.isEmpty()) {
            String current = queue.poll();
            sorted.add(actionMap.get(current));
            for (String neighbor : adjacency.get(current)) {
                int newDegree = inDegree.merge(neighbor, -1, Integer::sum);
                if (newDegree == 0) {
                    queue.add(neighbor);
                }
            }
        }

        if (sorted.size() != actions.size()) {
            return null;
        }

        return sorted;
    }

    private String executeInternal(Action action, ExecutionContext context, int depth) {
        return switch (action.actionType()) {
            case RETRIEVE -> executeRetrieve(action, context);
            case TRANSFORM -> executeTransform(action, context);
            case NOTIFY -> executeNotify(action, context);
            case CORRECT -> executeCorrect(action, context, depth);
        };
    }

    private String executeRetrieve(Action action, ExecutionContext context) {
        if (dataAccessor == null) {
            return "retrieve:" + action.actionId() + ":no-dataAccessor";
        }
        try {
            var contentMap =
                    MAPPER.readValue(
                            action.content(),
                            new com.fasterxml.jackson.core.type.TypeReference<
                                    Map<String, Object>>() {});
            String targetViewId =
                    (String)
                            contentMap.getOrDefault(
                                    "targetViewId",
                                    contentMap.getOrDefault("retrieveSpec", Map.of())
                                                    instanceof Map m
                                            ? m.getOrDefault("queryTerms", "unknown")
                                            : "unknown");
            if (targetViewId == null) targetViewId = "unknown";

            var viewBytes = dataAccessor.readView(targetViewId);
            if (viewBytes.isPresent()) {
                return new String(viewBytes.get(), StandardCharsets.UTF_8);
            }

            return "retrieve:" + action.actionId() + ":view-not-found:" + targetViewId;
        } catch (Exception e) {
            log.warn("RETRIEVE failed for action {}: {}", action.actionId(), e.getMessage());
            return "retrieve:" + action.actionId() + ":error:" + e.getMessage();
        }
    }

    private String executeTransform(Action action, ExecutionContext context) {
        if (dataAccessor == null) {
            return "transform:" + action.actionId() + ":no-dataAccessor";
        }
        try {
            var contentMap =
                    MAPPER.readValue(
                            action.content(),
                            new com.fasterxml.jackson.core.type.TypeReference<
                                    Map<String, Object>>() {});
            String sourceViewId = (String) contentMap.getOrDefault("sourceViewId", "unknown");
            String transformSpec = contentMap.getOrDefault("transformSpec", Map.of()).toString();

            View sourceView = null;
            var viewBytes = dataAccessor.readView(sourceViewId);
            if (viewBytes.isPresent()) {
                sourceView = MAPPER.readValue(viewBytes.get(), View.class);
            }

            String newViewId = "view:" + UUID.randomUUID().toString().substring(0, 8);
            var newContent = new LinkedHashMap<String, Object>();
            newContent.put("viewId", newViewId);
            newContent.put("sourceViewId", sourceViewId);
            newContent.put("transformSpec", transformSpec);
            newContent.put("derivedAt", System.currentTimeMillis());
            if (sourceView != null && sourceView.content() != null) {
                newContent.put("sourceContentLength", sourceView.content().length);
            }

            byte[] newViewBytes = MAPPER.writeValueAsBytes(newContent);
            byte[] newViewKey = ("view:" + newViewId + ":v1").getBytes(StandardCharsets.UTF_8);
            knowledgeStore.put(newViewKey, newViewBytes);
            byte[] newLatestKey = ("view:latest:" + newViewId).getBytes(StandardCharsets.UTF_8);
            metastore.put(newLatestKey, "1".getBytes(StandardCharsets.UTF_8));

            return newViewId;
        } catch (Exception e) {
            log.warn("TRANSFORM failed for action {}: {}", action.actionId(), e.getMessage());
            return "transform:" + action.actionId() + ":error:" + e.getMessage();
        }
    }

    private String executeNotify(Action action, ExecutionContext context) {
        if (metastore == null) {
            return "notify:" + action.actionId() + ":no-metastore";
        }
        try {
            long timestamp = System.currentTimeMillis();
            String key = "notification:" + action.actionId() + ":" + timestamp;
            var node = MAPPER.createObjectNode();
            node.put("actionId", action.actionId());
            node.put("timestamp", timestamp);
            node.put("content", new String(action.content(), StandardCharsets.UTF_8));
            knowledgeStore.put(
                    key.getBytes(StandardCharsets.UTF_8), MAPPER.writeValueAsBytes(node));
            return "notified:" + key;
        } catch (Exception e) {
            log.warn("NOTIFY failed for action {}: {}", action.actionId(), e.getMessage());
            return "notify:" + action.actionId() + ":error:" + e.getMessage();
        }
    }

    private String executeCorrect(Action action, ExecutionContext context, int depth) {
        if (correctionCallback == null) {
            return "correct:" + action.actionId() + ":no-callback";
        }
        try {
            var contentMap =
                    MAPPER.readValue(
                            action.content(),
                            new com.fasterxml.jackson.core.type.TypeReference<
                                    Map<String, Object>>() {});
            String targetViewId = (String) contentMap.getOrDefault("targetViewId", "unknown");
            Object correctionValue =
                    contentMap.getOrDefault(
                            "correctionValue",
                            contentMap.getOrDefault("correctSpec", "auto-correction"));

            String annotationId = "ann:correct:" + UUID.randomUUID().toString().substring(0, 8);
            var correction =
                    new DefaultFeedbackAnnotation(
                            annotationId,
                            targetViewId,
                            "agent:corrector",
                            FeedbackAnnotation.AnnotationType.CORRECTION,
                            correctionValue,
                            0.8,
                            System.currentTimeMillis());

            LockHandle lock = null;
            try {
                if (coordEngine != null) {
                    lock =
                            coordEngine.acquire(
                                    executorId, "correct:" + targetViewId, CORRECTION_LOCK_TTL);
                }
                correctionCallback.onCorrection(targetViewId, correction, depth);
            } finally {
                if (lock != null) {
                    coordEngine.release(executorId, "correct:" + targetViewId, lock.fencingToken());
                }
            }
            return annotationId;
        } catch (Exception e) {
            log.warn("CORRECT failed for action {}: {}", action.actionId(), e.getMessage());
            return "correct:" + action.actionId() + ":error:" + e.getMessage();
        }
    }

    private Map<String, Object> collectArtifacts(Action action, ExecutionContext context) {
        Map<String, Object> artifacts = new LinkedHashMap<>();
        artifacts.put("executorId", executorId);
        artifacts.put("actionType", action.actionType().name());
        artifacts.put("contextViews", context.views().size());
        return artifacts;
    }

    private String buildPlanSummary(
            ActionPlan plan, List<CognitionEngine.ActionResult> results, long elapsedMs) {
        long successCount = results.stream().filter(CognitionEngine.ActionResult::success).count();
        return String.format(
                "Plan %s: %d/%d actions succeeded in %dms",
                plan.planId(), successCount, results.size(), elapsedMs);
    }
}
