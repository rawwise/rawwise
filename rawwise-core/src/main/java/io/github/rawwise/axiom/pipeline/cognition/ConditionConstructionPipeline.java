package io.github.rawwise.axiom.pipeline.cognition;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.memory.semantic.GraphEdge;
import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphException;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.axiom.pipeline.state.ContractViolation;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.model.record.Action;
import io.github.rawwise.model.record.View;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConditionConstructionPipeline {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    private final GraphEngine graphEngine;
    private final KnowledgeStore knowledgeStore;
    private final CognitionEngine cognitionEngine;
    private final TextConditionEnhancer textConditionEnhancer;
    private final ConditionReasoningService reasoningService;

    public ConditionConstructionPipeline(
            GraphEngine graphEngine,
            KnowledgeStore knowledgeStore,
            CognitionEngine cognitionEngine) {
        this(
                graphEngine,
                knowledgeStore,
                cognitionEngine,
                TextConditionEnhancer.noop(),
                ConditionReasoningService.noop());
    }

    public ConditionConstructionPipeline(
            GraphEngine graphEngine,
            KnowledgeStore knowledgeStore,
            CognitionEngine cognitionEngine,
            TextConditionEnhancer textConditionEnhancer) {
        this(
                graphEngine,
                knowledgeStore,
                cognitionEngine,
                textConditionEnhancer,
                ConditionReasoningService.noop());
    }

    public ConditionConstructionPipeline(
            GraphEngine graphEngine,
            KnowledgeStore knowledgeStore,
            CognitionEngine cognitionEngine,
            TextConditionEnhancer textConditionEnhancer,
            ConditionReasoningService reasoningService) {
        this.graphEngine = graphEngine;
        this.knowledgeStore = knowledgeStore;
        this.cognitionEngine = cognitionEngine;
        this.textConditionEnhancer =
                textConditionEnhancer == null
                        ? TextConditionEnhancer.noop()
                        : textConditionEnhancer;
        this.reasoningService =
                reasoningService == null ? ConditionReasoningService.noop() : reasoningService;
    }

    public Optional<Action> constructCondition(
            ContractViolation violation, ConditionConstructionSpec spec) {
        String entityId = violation.entryKey();

        GraphNode baseline = null;
        if (graphEngine != null) {
            Optional<GraphNode> baselineOpt;
            try {
                baselineOpt = graphEngine.getNode(spec.baselineGraphId(), entityId);
            } catch (GraphException e) {
                log.warn(
                        "GraphEngine.getNode failed for entityId={} in graph={}: {}",
                        entityId,
                        spec.baselineGraphId(),
                        e.getMessage());
                return Optional.empty();
            }

            if (baselineOpt.isEmpty()) {
                log.warn(
                        "Baseline node not found for entityId={} in graph={}",
                        entityId,
                        spec.baselineGraphId());
                return Optional.empty();
            }
            baseline = baselineOpt.get();
        }

        Map<String, Object> conditionParams = new HashMap<>();

        conditionParams.put("entity_id", entityId);
        conditionParams.put("violation_reason", violation.reason());
        conditionParams.put("drift_description", violation.reason());
        conditionParams.put("from_state", violation.fromState());
        conditionParams.put("to_state", violation.toState());

        if (baseline != null
                && ("text".equals(spec.conditionType()) || "hybrid".equals(spec.conditionType()))) {
            String textCondition = buildTextCondition(baseline);
            conditionParams.put("text_condition", textCondition);
            conditionParams.put("reference_properties", baseline.properties());
            applyTextConditionEnhancement(
                    entityId, violation, spec, conditionParams, textCondition);
        }

        if (baseline != null
                && ("embedding".equals(spec.conditionType())
                        || "hybrid".equals(spec.conditionType()))) {
            if (baseline.embeddingRef() != null && !baseline.embeddingRef().isBlank()) {
                Optional<float[]> referenceEmbedding =
                        loadEmbeddingFromView(baseline.embeddingRef());
                if (referenceEmbedding.isPresent()) {
                    conditionParams.put("reference_embedding", referenceEmbedding.get());
                    conditionParams.put("embedding_model", baseline.embeddingModel());
                } else {
                    log.warn(
                            "Reference embedding not found for viewId={}", baseline.embeddingRef());
                }
            }
        }

        conditionParams.put("condition_type", spec.conditionType());

        Action action = cognitionEngine.deriveAction(List.of(), spec.actionType(), conditionParams);

        return Optional.of(action);
    }

    /**
     * 带 LLM 推理的条件构造。
     *
     * <p>在模板条件构造的基础上，使用 ConditionReasoningService 基于 检索到的事实记忆、评价分数、违规信息和历史尝试，通过 LLM 推理 生成改进的条件。
     *
     * <p>如果 reasoning service 不可用，回退到模板条件构造。
     */
    public Optional<Action> constructConditionWithReasoning(
            ContractViolation violation,
            ConditionConstructionSpec spec,
            ConditionReasoningContext reasoningContext) {
        if (!reasoningService.isAvailable() || reasoningContext == null) {
            return constructCondition(violation, spec);
        }

        String entityId = violation.entryKey();

        GraphNode baseline = null;
        if (graphEngine != null) {
            try {
                Optional<GraphNode> baselineOpt =
                        graphEngine.getNode(spec.baselineGraphId(), entityId);
                if (baselineOpt.isPresent()) {
                    baseline = baselineOpt.get();
                }
            } catch (GraphException e) {
                log.warn("GraphEngine.getNode failed: {}", e.getMessage());
            }
        }

        // 1. 构建模板条件作为 LLM 的参考
        String templateCondition =
                baseline != null
                        ? buildTextCondition(baseline)
                        : reasoningContext.originalCondition();

        // 2. 调用 LLM 推理改进条件
        ConditionReasoningResult reasoningResult = reasoningService.reason(reasoningContext);
        String improvedCondition = reasoningResult.improvedCondition();

        if (reasoningResult.isImproved()) {
            log.info(
                    "LLM reasoning improved condition: method={}, original={} chars, improved={} chars",
                    reasoningResult.reasoningMethod(),
                    templateCondition.length(),
                    improvedCondition.length());
        }

        // 3. 构建 action params
        Map<String, Object> conditionParams = new HashMap<>();
        conditionParams.put("entity_id", entityId);
        conditionParams.put("violation_reason", violation.reason());
        conditionParams.put("from_state", violation.fromState());
        conditionParams.put("to_state", violation.toState());
        conditionParams.put("text_condition", improvedCondition);
        conditionParams.put("original_text_condition", templateCondition);
        conditionParams.put("condition_reasoned", reasoningResult.isImproved());
        conditionParams.put("condition_reasoning_method", reasoningResult.reasoningMethod());

        if (baseline != null) {
            conditionParams.put("reference_properties", baseline.properties());
            if (baseline.embeddingRef() != null && !baseline.embeddingRef().isBlank()) {
                Optional<float[]> referenceEmbedding =
                        loadEmbeddingFromView(baseline.embeddingRef());
                if (referenceEmbedding.isPresent()) {
                    conditionParams.put("reference_embedding", referenceEmbedding.get());
                    conditionParams.put("embedding_model", baseline.embeddingModel());
                }
            }
        }

        conditionParams.put("condition_type", spec.conditionType());

        Action action = cognitionEngine.deriveAction(List.of(), spec.actionType(), conditionParams);

        return Optional.of(action);
    }

    private void applyTextConditionEnhancement(
            String entityId,
            ContractViolation violation,
            ConditionConstructionSpec spec,
            Map<String, Object> conditionParams,
            String textCondition) {
        try {
            TextConditionEnhancement enhancement =
                    textConditionEnhancer.enhance(
                            textCondition,
                            new TextConditionEnhancementContext(
                                    entityId, violation, spec, conditionParams));
            if (enhancement != null && enhancement.enhanced()) {
                conditionParams.put("original_text_condition", textCondition);
                conditionParams.put("text_condition", enhancement.enhancedCondition());
                conditionParams.put("condition_enhanced", true);
                conditionParams.put("condition_enhancement_constraints", enhancement.constraints());
                conditionParams.put("condition_enhancement_source_ids", enhancement.sourceIds());
            }
        } catch (RuntimeException e) {
            log.warn("TextConditionEnhancer failed for entityId={}: {}", entityId, e.getMessage());
            conditionParams.put("condition_enhancement_failed", true);
            conditionParams.put("condition_enhancement_error", e.getMessage());
        }
    }

    private Optional<float[]> loadEmbeddingFromView(String viewId) {
        try {
            byte[] key = ("view:" + viewId).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> raw = knowledgeStore.get(key);
            if (raw.isEmpty()) {
                return Optional.empty();
            }
            View view = MAPPER.readValue(raw.get(), View.class);
            return view.embedding();
        } catch (IOException | RuntimeException e) {
            log.warn("Failed to load view embedding for viewId={}: {}", viewId, e.getMessage());
            return Optional.empty();
        }
    }

    private String buildTextCondition(GraphNode baseline) {
        StringBuilder sb = new StringBuilder();

        String name = baseline.properties().get("name");
        String description = baseline.properties().get("description");
        String type = baseline.type();

        if (name != null && !name.isBlank()) {
            sb.append("Keep the ").append(name.replace('_', ' '));
            if (description != null && !description.isBlank()) {
                sb.append(" (").append(description).append(")");
            }
            sb.append(" consistent across all frames.");
        } else if (description != null && !description.isBlank()) {
            sb.append("Maintain consistency for: ").append(description);
        } else {
            sb.append("Maintain consistency for entity type: ").append(type);
        }

        // Include relevant properties (clothing, hair, face, etc.)
        for (Map.Entry<String, String> prop : baseline.properties().entrySet()) {
            String key = prop.getKey();
            if (!"name".equals(key) && !"description".equals(key)) {
                sb.append(" ")
                        .append(key.replace('_', ' '))
                        .append(": ")
                        .append(prop.getValue())
                        .append(".");
            }
        }

        if (graphEngine != null) {
            try {
                for (String label : List.of("WEARS", "IN", "DOING", "HAS", "CONTAINS")) {
                    List<GraphEdge> edges =
                            graphEngine.getOutEdges(baseline.graphId(), baseline.nodeId(), label);
                    for (GraphEdge edge : edges) {
                        Optional<GraphNode> neighborOpt =
                                graphEngine.getNode(baseline.graphId(), edge.dstNodeId());
                        if (neighborOpt.isPresent()) {
                            GraphNode neighbor = neighborOpt.get();
                            String neighborName = neighbor.properties().get("name");
                            String neighborDesc = neighbor.properties().get("description");
                            String displayText =
                                    neighborName != null
                                            ? neighborName.replace('_', ' ')
                                            : neighborDesc;
                            if (displayText != null && !displayText.isBlank()) {
                                sb.append(" The ")
                                        .append(name != null ? name.replace('_', ' ') : "subject");
                                sb.append(" ").append(label.toLowerCase()).append(" ");
                                sb.append(displayText).append(".");
                            }
                        }
                    }
                }
            } catch (GraphException e) {
                log.warn(
                        "Failed to traverse graph neighbors for nodeId={}: {}",
                        baseline.nodeId(),
                        e.getMessage());
            }
        }

        return sb.toString();
    }
}
