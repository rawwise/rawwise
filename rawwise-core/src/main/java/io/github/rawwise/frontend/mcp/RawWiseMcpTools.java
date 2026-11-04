package io.github.rawwise.frontend.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.github.rawwise.axiom.memory.Depth;
import io.github.rawwise.axiom.memory.MemoryEntry;
import io.github.rawwise.axiom.memory.MemoryFacade;
import io.github.rawwise.axiom.memory.MemorySharePolicy;
import io.github.rawwise.axiom.memory.MemoryStatus;
import io.github.rawwise.axiom.memory.MemoryTier;
import io.github.rawwise.axiom.memory.semantic.GraphEngine;
import io.github.rawwise.axiom.memory.semantic.GraphNode;
import io.github.rawwise.axiom.memory.semantic.PathResult;
import io.github.rawwise.axiom.memory.semantic.TraversalDirection;
import io.github.rawwise.axiom.memory.semantic.VectorSearchHit;
import io.github.rawwise.axiom.pipeline.state.StateEngine;
import io.github.rawwise.axiom.pipeline.state.StateEntry;
import io.github.rawwise.axiom.pipeline.state.StateSpace;
import io.github.rawwise.axiom.pipeline.state.StateTransition;
import io.github.rawwise.frontend.backend.BackendClient;
import io.github.rawwise.proto.axiom.AxiomIngestRequest;
import io.github.rawwise.proto.axiom.AxiomProvenanceRequest;
import io.github.rawwise.proto.axiom.AxiomRederiveRequest;
import io.github.rawwise.proto.axiom.AxiomSearchRequest;
import io.github.rawwise.proto.axiom.AxiomServiceGrpc;
import io.github.rawwise.proto.axiom.ImplicitFeedbackRequest;
import io.github.rawwise.proto.axiom.RederiveScope;
import io.github.rawwise.server.JobState;
import io.github.rawwise.server.QueryType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class RawWiseMcpTools {

    private static final int QUERY_POLL_MAX_RETRIES = 30;
    private static final long QUERY_POLL_INTERVAL_MS = 1000;

    private final AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub;
    private final BackendClient backendClient;
    private final RawWiseMcpConfig config;
    private final MemoryFacade memoryFacade;
    private final GraphEngine graphEngine;
    private final StateEngine stateEngine;

    public RawWiseMcpTools(
            AxiomServiceGrpc.AxiomServiceBlockingStub axiomStub,
            BackendClient backendClient,
            RawWiseMcpConfig config,
            @Nullable MemoryFacade memoryFacade,
            @Nullable GraphEngine graphEngine,
            @Nullable StateEngine stateEngine) {
        this.axiomStub = axiomStub;
        this.backendClient = backendClient;
        this.config = config;
        this.memoryFacade = memoryFacade;
        this.graphEngine = graphEngine;
        this.stateEngine = stateEngine;
    }

    public List<Map<String, Object>> getToolDefinitions() {
        List<Map<String, Object>> tools = new ArrayList<>();
        addExistingTools(tools);
        addKnowledgePipelineTools(tools);
        addMemoryTools(tools);
        addGraphTools(tools);
        addStateTools(tools);
        return tools;
    }

    public Map<String, Object> call(String name, JsonNode arguments) {
        try {
            return switch (name) {
                case "rawwise_ingest" -> doIngest(arguments);
                case "rawwise_search" -> doSearch(arguments);
                case "rawwise_annotate" -> doAnnotate(arguments);
                case "rawwise_provenance" -> doProvenance(arguments);
                case "rawwise_query_sql" -> doQuerySql(arguments);
                case "rawwise_list_tables" -> doListTables();
                case "rawwise_cognize" -> doCognize(arguments);
                case "rawwise_rederive" -> doRederive(arguments);
                case "rawwise_implicit_feedback" -> doImplicitFeedback(arguments);
                case "rawwise_fetch_details" -> doFetchDetails(arguments);
                case "rawwise_diagnostics" -> doDiagnostics();
                case "rawwise_get_view" -> doGetById(arguments, "viewId");
                case "rawwise_get_chunk" -> doGetById(arguments, "chunkId");
                case "rawwise_get_semantic" -> doGetById(arguments, "semanticId");
                case "rawwise_get_action" -> doGetById(arguments, "actionId");
                case "rawwise_get_action_plan" -> doGetById(arguments, "planId");
                case "rawwise_memory_store" -> doMemoryStore(arguments);
                case "rawwise_memory_recall" -> doMemoryRecall(arguments);
                case "rawwise_memory_consolidate" -> doMemoryConsolidate(arguments);
                case "rawwise_memory_share" -> doMemoryShare(arguments);
                case "rawwise_memory_status" -> doMemoryStatus(arguments);
                case "rawwise_memory_decay" -> doMemoryDecay(arguments);
                case "rawwise_memory_reflect" -> doMemoryReflect(arguments);
                case "rawwise_graph_query_nodes" -> doGraphQueryNodes(arguments);
                case "rawwise_graph_find_paths" -> doGraphFindPaths(arguments);
                case "rawwise_graph_vector_search" -> doGraphVectorSearch(arguments);
                case "rawwise_graph_get_node" -> doGraphGetNode(arguments);
                case "rawwise_graph_neighbors" -> doGraphNeighbors(arguments);
                case "rawwise_state_create_space" -> doStateCreateSpace(arguments);
                case "rawwise_state_list_spaces" -> doStateListSpaces(arguments);
                case "rawwise_state_read" -> doStateRead(arguments);
                case "rawwise_state_write" -> doStateWrite(arguments);
                case "rawwise_state_transition" -> doStateTransition(arguments);
                case "rawwise_state_scan" -> doStateScan(arguments);
                default -> Map.of("error", "Unknown tool: " + name);
            };
        } catch (Exception e) {
            log.error("MCP tool '{}' failed: {}", name, e.getMessage(), e);
            return Map.of(
                    "error",
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    // ===== Tool Definitions =====

    private void addExistingTools(List<Map<String, Object>> tools) {
        if (config.toolsIngestEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_ingest",
                            "将知识写入 RawWise AI-Native 数据湖，自动触发派生管道 (Raw->Chunk->View->Embedding->JVector索引)。embedding 由 RawWise 服务端自动计算。",
                            schema(
                                    Map.of(
                                            "tableId",
                                            prop("string", "目标表ID，如 default.project_docs"),
                                            "key",
                                            prop("string", "文档/知识唯一标识"),
                                            "body",
                                            prop("string", "文档内容（UTF-8文本）"),
                                            "immutable",
                                            prop("boolean", "是否不可变，默认 false"),
                                            "properties",
                                            prop("object", "附加属性")),
                                    List.of("tableId", "key", "body"))));
        }
        if (config.toolsSearchEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_search",
                            "语义检索 RawWise 知识体，支持向量+关键词+图混合检索。返回摘要和血缘链，完整内容通过 rawwise_provenance 获取。",
                            schema(
                                    Map.of(
                                            "tableId",
                                            prop("string", "搜索目标表ID"),
                                            "query",
                                            prop("string", "自然语言查询"),
                                            "topK",
                                            prop("integer", "返回结果数，默认 10")),
                                    List.of("tableId", "query"))));
        }
        if (config.toolsAnnotateEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_annotate",
                            "对检索结果进行反馈标注，触发 FeedbackLoopCloser 级联重派生",
                            schema(
                                    Map.of(
                                            "viewId",
                                            prop("string", "被标注的 View ID"),
                                            "type",
                                            prop("string", "标注类型: POSITIVE/NEGATIVE/CORRECTION"),
                                            "value",
                                            prop("string", "标注值/修正文本"),
                                            "annotatorId",
                                            prop("string", "标注者标识"),
                                            "confidence",
                                            prop("number", "置信度 0.0-1.0，默认 1.0")),
                                    List.of("viewId", "type", "annotatorId"))));
        }
        if (config.toolsProvenanceEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_provenance",
                            "追溯 View 的完整派生血缘链 (Raw->Chunk->View->Semantic->Action)",
                            schema(
                                    Map.of("viewId", prop("string", "View ID")),
                                    List.of("viewId"))));
        }
        if (config.toolsSqlEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_query_sql",
                            "对 RawWise 数据湖执行 SQL 查询 (Spark SQL)。DDL 立即返回，DML/DQL 轮询等待结果（最多30s）",
                            schema(
                                    Map.of(
                                            "sql",
                                            prop("string", "SQL 语句 (SELECT/INSERT/CREATE/DROP)"),
                                            "timeoutSeconds",
                                            prop("integer", "超时秒数，默认 30，最大 300")),
                                    List.of("sql"))));
        }
        if (config.toolsListTablesEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_list_tables",
                            "列出 RawWise 中所有表及其 schema 信息",
                            schema(Map.of(), List.of())));
        }
    }

    private void addKnowledgePipelineTools(List<Map<String, Object>> tools) {
        if (config.toolsCognizeEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_cognize",
                            "五层认知检索：执行语义搜索并为每个结果构建完整多层响应 (L0 raw, L1 chunk, L2 view, L3 semantic, L4 action)",
                            schema(
                                    Map.of(
                                            "tableId",
                                            prop("string", "搜索目标表ID"),
                                            "query",
                                            prop("string", "自然语言查询"),
                                            "topK",
                                            prop("integer", "返回结果数，默认 5")),
                                    List.of("tableId", "query"))));
        }
        if (config.toolsRederiveEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_rederive",
                            "重新执行派生管道，支持全表/键范围/单键重派生",
                            schema(
                                    Map.of(
                                            "tableId",
                                            prop("string", "目标表ID"),
                                            "scope",
                                            prop(
                                                    "string",
                                                    "重派生范围: FULL_TABLE/KEY_RANGE/SINGLE_KEY"),
                                            "singleKey",
                                            prop("string", "SINGLE_KEY 模式下的目标键"),
                                            "forceReindex",
                                            prop("boolean", "是否强制重建索引，默认 false")),
                                    List.of("tableId", "scope"))));
        }
        if (config.toolsImplicitFeedbackEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_implicit_feedback",
                            "记录隐式反馈信号 (DWELL/CLICK/SKIP等)，用于 reranker 训练",
                            schema(
                                    Map.of(
                                            "viewId",
                                            prop("string", "被反馈的 View ID"),
                                            "signalType",
                                            prop("string", "信号类型: DWELL/CLICK/SKIP"),
                                            "dwellMs",
                                            prop("integer", "停留时长(毫秒)，DWELL信号使用")),
                                    List.of("viewId", "signalType"))));
        }
        if (config.toolsFetchDetailsEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_fetch_details",
                            "批量获取指定键的原始内容和摘要",
                            schema(
                                    Map.of(
                                            "tableId",
                                            prop("string", "目标表ID"),
                                            "entryKeys",
                                            prop("array", "要获取的键列表")),
                                    List.of("tableId", "entryKeys"))));
        }
        if (config.toolsDiagnosticsEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_diagnostics",
                            "返回系统诊断信息：MCP工具数量、各模块可用性",
                            schema(Map.of(), List.of())));
        }
        if (config.toolsGetViewEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_get_view",
                            "按 viewId 获取 View 层内容",
                            schemaTwoStr("tableId", "目标表ID", "viewId", "View ID")));
        }
        if (config.toolsGetChunkEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_get_chunk",
                            "按 chunkId 获取 Chunk 层内容",
                            schemaTwoStr("tableId", "目标表ID", "chunkId", "Chunk ID")));
        }
        if (config.toolsGetSemanticEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_get_semantic",
                            "按 semanticId 获取 Semantic 层内容",
                            schemaTwoStr("tableId", "目标表ID", "semanticId", "Semantic ID")));
        }
        if (config.toolsGetActionEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_get_action",
                            "按 actionId 获取 Action 层内容",
                            schemaTwoStr("tableId", "目标表ID", "actionId", "Action ID")));
        }
        if (config.toolsGetActionPlanEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_get_action_plan",
                            "按 planId 获取 Action Plan 层内容",
                            schemaTwoStr("tableId", "目标表ID", "planId", "Plan ID")));
        }
    }

    private void addMemoryTools(List<Map<String, Object>> tools) {
        if (config.toolsMemoryStoreEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_memory_store",
                            "将内容存入记忆系统，指定记忆层级和重要性",
                            schema(
                                    Map.of(
                                            "content",
                                            prop("object", "记忆内容(key-value)"),
                                            "memoryTier",
                                            prop(
                                                    "string",
                                                    "记忆层级: WORKING/EPISODIC/SEMANTIC/PROCEDURAL"),
                                            "importance",
                                            prop("number", "重要性 0.0-1.0，默认 0.5"),
                                            "agentId",
                                            prop("string", "Agent ID，默认 default")),
                                    List.of("content", "memoryTier"))));
        }
        if (config.toolsMemoryRecallEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_memory_recall",
                            "从记忆系统召回相关记忆",
                            schema(
                                    Map.of(
                                            "query",
                                            prop("string", "召回查询"),
                                            "memoryTier",
                                            prop(
                                                    "string",
                                                    "记忆层级: WORKING/EPISODIC/SEMANTIC/PROCEDURAL"),
                                            "depth",
                                            prop(
                                                    "string",
                                                    "下钻深度: ABSTRACT=摘要/DETAIL=原文，兼容旧值 L0/L1/L2（缺省 ABSTRACT）"),
                                            "loadTier",
                                            prop("string", "兼容旧参数: L0/L1/L2（depth 缺省时生效）"),
                                            "topK",
                                            prop("integer", "返回数量，默认 5"),
                                            "agentId",
                                            prop("string", "Agent ID，默认 default")),
                                    List.of("query", "memoryTier"))));
        }
        if (config.toolsMemoryConsolidateEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_memory_consolidate",
                            "将记忆从一层整合到另一层（如 WORKING->EPISODIC）",
                            schema(
                                    Map.of(
                                            "fromTier",
                                            prop("string", "源层级"),
                                            "toTier",
                                            prop("string", "目标层级"),
                                            "agentId",
                                            prop("string", "Agent ID，默认 default")),
                                    List.of("fromTier", "toTier"))));
        }
        if (config.toolsMemoryShareEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_memory_share",
                            "在 Agent 之间共享记忆条目",
                            schema(
                                    Map.of(
                                            "toAgentId",
                                            prop("string", "目标 Agent ID"),
                                            "entryId",
                                            prop("string", "记忆条目 ID"),
                                            "ttlMs",
                                            prop("integer", "TTL(毫秒)"),
                                            "revocable",
                                            prop("boolean", "是否可撤销"),
                                            "permission",
                                            prop("string", "权限"),
                                            "agentId",
                                            prop("string", "源 Agent ID，默认 default")),
                                    List.of(
                                            "toAgentId",
                                            "entryId",
                                            "ttlMs",
                                            "revocable",
                                            "permission"))));
        }
        if (config.toolsMemoryStatusEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_memory_status",
                            "获取 Agent 的记忆状态统计",
                            schema(
                                    Map.of("agentId", prop("string", "Agent ID，默认 default")),
                                    List.of())));
        }
        if (config.toolsMemoryDecayEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_memory_decay",
                            "对超过指定时长的记忆执行衰减",
                            schema(
                                    Map.of(
                                            "ageMs",
                                            prop("integer", "衰减阈值(毫秒)"),
                                            "agentId",
                                            prop("string", "Agent ID，默认 default")),
                                    List.of("ageMs"))));
        }
        if (config.toolsMemoryReflectEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_memory_reflect",
                            "对查询执行反思，返回最相关的记忆条目",
                            schema(
                                    Map.of(
                                            "query",
                                            prop("string", "反思查询"),
                                            "agentId",
                                            prop("string", "Agent ID，默认 default")),
                                    List.of("query"))));
        }
    }

    private void addGraphTools(List<Map<String, Object>> tools) {
        if (config.toolsGraphQueryNodesEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_graph_query_nodes",
                            "查询图中符合类型和属性过滤条件的节点",
                            schema(
                                    Map.of(
                                            "graphId",
                                            prop("string", "图 ID"),
                                            "type",
                                            prop("string", "节点类型过滤"),
                                            "propertyFilter",
                                            prop("object", "属性过滤条件"),
                                            "limit",
                                            prop("integer", "返回数量限制，默认 100")),
                                    List.of("graphId"))));
        }
        if (config.toolsGraphFindPathsEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_graph_find_paths",
                            "在图中查找两个节点之间的路径",
                            schema(
                                    Map.of(
                                            "graphId",
                                            prop("string", "图 ID"),
                                            "srcNodeId",
                                            prop("string", "源节点 ID"),
                                            "dstNodeId",
                                            prop("string", "目标节点 ID"),
                                            "maxHops",
                                            prop("integer", "最大跳数，默认 5")),
                                    List.of("graphId", "srcNodeId", "dstNodeId"))));
        }
        if (config.toolsGraphVectorSearchEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_graph_vector_search",
                            "在图中执行向量语义搜索",
                            schema(
                                    Map.of(
                                            "graphId",
                                            prop("string", "图 ID"),
                                            "query",
                                            prop("string", "自然语言查询"),
                                            "topK",
                                            prop("integer", "返回数量，默认 10")),
                                    List.of("graphId", "query"))));
        }
        if (config.toolsGraphGetNodeEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_graph_get_node",
                            "按 nodeId 获取图节点",
                            schemaTwoStr("graphId", "图 ID", "nodeId", "节点 ID")));
        }
        if (config.toolsGraphNeighborsEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_graph_neighbors",
                            "获取节点的邻居",
                            schema(
                                    Map.of(
                                            "graphId",
                                            prop("string", "图 ID"),
                                            "nodeId",
                                            prop("string", "节点 ID"),
                                            "direction",
                                            prop("string", "遍历方向: OUT/IN/BOTH"),
                                            "label",
                                            prop("string", "边标签过滤"),
                                            "maxHops",
                                            prop("integer", "最大跳数，默认 1"),
                                            "limit",
                                            prop("integer", "返回数量限制，默认 100")),
                                    List.of("graphId", "nodeId", "direction"))));
        }
    }

    private void addStateTools(List<Map<String, Object>> tools) {
        if (config.toolsStateCreateSpaceEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_state_create_space",
                            "创建状态空间",
                            schema(
                                    Map.of(
                                            "agentId",
                                            prop("string", "Agent ID"),
                                            "name",
                                            prop("string", "状态空间名称"),
                                            "initialState",
                                            prop("string", "初始状态"),
                                            "states",
                                            prop("array", "状态列表"),
                                            "transitions",
                                            prop("array", "状态转移列表"),
                                            "metadata",
                                            prop("object", "元数据")),
                                    List.of("agentId", "name", "initialState", "states"))));
        }
        if (config.toolsStateListSpacesEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_state_list_spaces",
                            "列出 Agent 的所有状态空间",
                            schema(
                                    Map.of("agentId", prop("string", "Agent ID")),
                                    List.of("agentId"))));
        }
        if (config.toolsStateReadEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_state_read",
                            "读取状态空间中的键值",
                            schemaTwoStr("spaceId", "状态空间 ID", "key", "键")));
        }
        if (config.toolsStateWriteEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_state_write",
                            "写入状态空间中的键值",
                            schema(
                                    Map.of(
                                            "spaceId",
                                            prop("string", "状态空间 ID"),
                                            "key",
                                            prop("string", "键"),
                                            "value",
                                            prop("string", "值"),
                                            "stateName",
                                            prop("string", "状态名称"),
                                            "metadata",
                                            prop("object", "元数据")),
                                    List.of("spaceId", "key", "value", "stateName"))));
        }
        if (config.toolsStateTransitionEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_state_transition",
                            "执行状态转移",
                            schema(
                                    Map.of(
                                            "spaceId",
                                            prop("string", "状态空间 ID"),
                                            "key",
                                            prop("string", "键"),
                                            "toState",
                                            prop("string", "目标状态"),
                                            "metadata",
                                            prop("object", "元数据")),
                                    List.of("spaceId", "key", "toState"))));
        }
        if (config.toolsStateScanEnabled()) {
            tools.add(
                    toolDef(
                            "rawwise_state_scan",
                            "扫描状态空间中匹配前缀的键值",
                            schema(
                                    Map.of(
                                            "spaceId",
                                            prop("string", "状态空间 ID"),
                                            "prefix",
                                            prop("string", "键前缀"),
                                            "limit",
                                            prop("integer", "返回数量限制，默认 100")),
                                    List.of("spaceId"))));
        }
    }

    // ===== Existing Tool Handlers =====

    private Map<String, Object> doIngest(JsonNode args) {
        String tableId = args.get("tableId").asText();
        String key = args.get("key").asText();
        String body = args.has("body") ? args.get("body").asText() : "";
        boolean immutable = args.has("immutable") && args.get("immutable").asBoolean();
        Map<String, String> properties = new LinkedHashMap<>();
        if (args.has("properties") && args.get("properties").isObject()) {
            var props = args.get("properties");
            var iter = props.fields();
            while (iter.hasNext()) {
                var entry = iter.next();
                properties.put(entry.getKey(), entry.getValue().asText());
            }
        }
        var request =
                AxiomIngestRequest.newBuilder()
                        .setTableId(tableId)
                        .setKey(key)
                        .setBody(ByteString.copyFromUtf8(body))
                        .setImmutable(immutable)
                        .putAllProperties(properties)
                        .build();
        var response = axiomStub.ingest(request);
        return Map.of(
                "recordKey",
                response.getRecordKey(),
                "chunkCount",
                response.getChunkCount(),
                "status",
                response.getStatus());
    }

    private Map<String, Object> doSearch(JsonNode args) {
        String tableId = args.get("tableId").asText();
        String query = args.get("query").asText();
        int topK = args.has("topK") ? args.get("topK").asInt() : 10;
        var request =
                AxiomSearchRequest.newBuilder()
                        .setTableId(tableId)
                        .setQuery(query)
                        .setTopK(topK)
                        .setSnapshotId(0)
                        .build();
        var response = axiomStub.search(request);
        List<Map<String, Object>> results = new ArrayList<>();
        for (var item : response.getResultsList()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("viewId", item.getViewId());
            result.put("score", item.getScore());
            result.put("summary", item.getSummary());
            if (item.hasProvenance()) {
                Map<String, Object> provenance = new LinkedHashMap<>();
                provenance.put("rawKey", item.getProvenance().getRawKey());
                List<Map<String, Object>> chain = new ArrayList<>();
                for (var link : item.getProvenance().getChainList()) {
                    chain.add(
                            Map.of(
                                    "from",
                                    link.getFrom(),
                                    "fromLayer",
                                    link.getFromLayer(),
                                    "to",
                                    link.getTo(),
                                    "toLayer",
                                    link.getToLayer()));
                }
                provenance.put("chain", chain);
                result.put("provenance", provenance);
            }
            results.add(result);
        }
        return Map.of("results", results);
    }

    private Map<String, Object> doAnnotate(JsonNode args) {
        String viewId = args.get("viewId").asText();
        String type = args.get("type").asText();
        String value = args.has("value") ? args.get("value").asText() : "";
        String annotatorId = args.get("annotatorId").asText();
        double confidence = args.has("confidence") ? args.get("confidence").asDouble() : 1.0;
        var request =
                io.github.rawwise.proto.axiom.AxiomAnnotateRequest.newBuilder()
                        .setViewId(viewId)
                        .setType(type)
                        .setValue(value)
                        .setAnnotatorId(annotatorId)
                        .setConfidence(confidence)
                        .build();
        var response = axiomStub.annotate(request);
        return Map.of(
                "success", response.getSuccess(), "adjustedScore", response.getAdjustedScore());
    }

    private Map<String, Object> doProvenance(JsonNode args) {
        String viewId = args.get("viewId").asText();
        var request = AxiomProvenanceRequest.newBuilder().setViewId(viewId).build();
        var response = axiomStub.getProvenance(request);
        List<Map<String, Object>> chain = new ArrayList<>();
        for (var link : response.getFullChainList()) {
            chain.add(
                    Map.of(
                            "from",
                            link.getFrom(),
                            "fromLayer",
                            link.getFromLayer(),
                            "to",
                            link.getTo(),
                            "toLayer",
                            link.getToLayer()));
        }
        return Map.of(
                "viewId", response.getViewId(), "rawKey", response.getRawKey(), "fullChain", chain);
    }

    private Map<String, Object> doQuerySql(JsonNode args) {
        String sql = args.get("sql").asText();
        int timeoutSeconds =
                args.has("timeoutSeconds") ? Math.min(args.get("timeoutSeconds").asInt(), 300) : 30;
        return executeSql(sql, timeoutSeconds);
    }

    private Map<String, Object> doListTables() {
        try {
            var clusterStatus = backendClient.getClusterStatus();
            if (clusterStatus == null) {
                return Map.of(
                        "tables", List.of(), "note", "Backend not available for table listing");
            }
            return Map.of(
                    "tables",
                    List.of(),
                    "note",
                    "Table listing via MCP uses cluster metadata. For full schema, use rawwise_query_sql with SHOW TABLES.");
        } catch (Exception e) {
            log.warn("Failed to list tables via MCP: {}", e.getMessage(), e);
            return Map.of("tables", List.of(), "error", e.getMessage());
        }
    }

    // ===== Knowledge Pipeline Tool Handlers =====

    private Map<String, Object> doCognize(JsonNode args) {
        String tableId = args.get("tableId").asText();
        String query = args.get("query").asText();
        int topK = args.has("topK") ? args.get("topK").asInt() : 5;
        var searchRequest =
                AxiomSearchRequest.newBuilder()
                        .setTableId(tableId)
                        .setQuery(query)
                        .setTopK(topK)
                        .setSnapshotId(0)
                        .build();
        var searchResponse = axiomStub.search(searchRequest);
        List<Map<String, Object>> cognitiveResults = new ArrayList<>();
        for (var item : searchResponse.getResultsList()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("viewId", item.getViewId());
            result.put("score", item.getScore());
            result.put("summary", item.getSummary());
            List<Map<String, Object>> layers = new ArrayList<>();
            layers.add(
                    Map.of(
                            "layer",
                            "L2_view",
                            "id",
                            item.getViewId(),
                            "summary",
                            item.getSummary()));
            try {
                var provRequest =
                        AxiomProvenanceRequest.newBuilder().setViewId(item.getViewId()).build();
                var provResponse = axiomStub.getProvenance(provRequest);
                result.put("rawKey", provResponse.getRawKey());
                for (var link : provResponse.getFullChainList()) {
                    layers.add(
                            Map.of(
                                    "layer",
                                    link.getToLayer(),
                                    "from",
                                    link.getFrom(),
                                    "to",
                                    link.getTo(),
                                    "fromLayer",
                                    link.getFromLayer()));
                }
                layers.add(Map.of("layer", "L0_raw", "id", provResponse.getRawKey()));
            } catch (Exception e) {
                log.debug(
                        "Provenance fetch failed for {}: {}", item.getViewId(), e.getMessage(), e);
            }
            result.put("layers", layers);
            cognitiveResults.add(result);
        }
        return Map.of("query", query, "results", cognitiveResults);
    }

    private Map<String, Object> doRederive(JsonNode args) {
        String tableId = args.get("tableId").asText();
        String scopeStr = args.get("scope").asText().toUpperCase();
        RederiveScope scope;
        try {
            scope = RederiveScope.valueOf(scopeStr);
        } catch (IllegalArgumentException e) {
            return Map.of(
                    "error",
                    "Invalid scope: " + scopeStr + ", must be FULL_TABLE/KEY_RANGE/SINGLE_KEY");
        }
        boolean forceReindex = args.has("forceReindex") && args.get("forceReindex").asBoolean();
        var builder =
                AxiomRederiveRequest.newBuilder()
                        .setTableId(tableId)
                        .setScope(scope)
                        .setForceReindex(forceReindex);
        if (args.has("singleKey") && !args.get("singleKey").isNull()) {
            builder.setSingleKey(args.get("singleKey").asText());
        }
        var response = axiomStub.rederive(builder.build());
        return Map.of(
                "totalKeys",
                response.getTotalKeys(),
                "rederivedCount",
                response.getRederivedCount(),
                "failedCount",
                response.getFailedCount(),
                "status",
                response.getStatus());
    }

    private Map<String, Object> doImplicitFeedback(JsonNode args) {
        String viewId = args.get("viewId").asText();
        String signalType = args.get("signalType").asText();
        long dwellMs = args.has("dwellMs") ? args.get("dwellMs").asLong() : 0;
        var request =
                ImplicitFeedbackRequest.newBuilder()
                        .setViewId(viewId)
                        .setSignalType(signalType)
                        .setDwellMs(dwellMs)
                        .build();
        var response = axiomStub.recordImplicitFeedback(request);
        return Map.of("accepted", response.getAccepted());
    }

    private Map<String, Object> doFetchDetails(JsonNode args) {
        String tableId = args.get("tableId").asText();
        List<String> keys = new ArrayList<>();
        if (args.has("entryKeys") && args.get("entryKeys").isArray()) {
            for (var key : args.get("entryKeys")) {
                keys.add("'" + key.asText().replace("'", "''") + "'");
            }
        }
        if (keys.isEmpty()) {
            return Map.of("error", "entryKeys must not be empty");
        }
        String sql = "SELECT * FROM " + tableId + " WHERE id IN (" + String.join(",", keys) + ")";
        return executeSql(sql, 30);
    }

    private Map<String, Object> doDiagnostics() {
        Map<String, Object> diagnostics = new LinkedHashMap<>();
        diagnostics.put("mcpToolsCount", getToolDefinitions().size());
        diagnostics.put("axiomStubAvailable", axiomStub != null);
        diagnostics.put("memoryFacadeAvailable", memoryFacade != null);
        diagnostics.put("graphEngineAvailable", graphEngine != null);
        diagnostics.put("stateEngineAvailable", stateEngine != null);
        return diagnostics;
    }

    private Map<String, Object> doGetById(JsonNode args, String idParamName) {
        String tableId = args.get("tableId").asText();
        String id = args.get(idParamName).asText();
        String sql = "SELECT * FROM " + tableId + " WHERE id = '" + id.replace("'", "''") + "'";
        var result = executeSql(sql, 30);
        if ("SUCCEEDED".equals(result.get("status"))) {
            @SuppressWarnings("unchecked")
            List<List<Object>> rows = (List<List<Object>>) result.getOrDefault("rows", List.of());
            if (rows != null && !rows.isEmpty()) {
                Map<String, Object> found = new LinkedHashMap<>();
                found.put("found", true);
                found.put("rows", rows);
                found.put("rowCount", result.get("rowCount"));
                return found;
            }
            return Map.of("found", false, "message", idParamName + " '" + id + "' not found");
        }
        return result;
    }

    // ===== Memory Tool Handlers =====

    private Map<String, Object> doMemoryStore(JsonNode args) {
        if (memoryFacade == null) {
            return Map.of(
                    "error", "Memory module not available. Enable rawwise.context.enabled=true");
        }
        Map<String, Object> content = new LinkedHashMap<>();
        if (args.has("content") && args.get("content").isObject()) {
            var contentNode = args.get("content");
            var iter = contentNode.fields();
            while (iter.hasNext()) {
                var entry = iter.next();
                content.put(entry.getKey(), jsonNodeToObject(entry.getValue()));
            }
        }
        MemoryTier tier = MemoryTier.valueOf(args.get("memoryTier").asText().toUpperCase());
        double importance = args.has("importance") ? args.get("importance").asDouble() : 0.5;
        String agentId = args.has("agentId") ? args.get("agentId").asText() : "default";
        MemoryEntry entry = memoryFacade.store(agentId, content, tier, importance);
        return memoryEntryToMap(entry);
    }

    private Map<String, Object> doMemoryRecall(JsonNode args) {
        if (memoryFacade == null) {
            return Map.of(
                    "error", "Memory module not available. Enable rawwise.context.enabled=true");
        }
        String query = args.get("query").asText();
        MemoryTier tier = MemoryTier.valueOf(args.get("memoryTier").asText().toUpperCase());
        String depthStr = args.has("depth") ? args.get("depth").asText() : null;
        if ((depthStr == null || depthStr.isBlank()) && args.has("loadTier")) {
            depthStr = args.get("loadTier").asText();
        }
        Depth depth = parseDepth(depthStr);
        int topK = args.has("topK") ? args.get("topK").asInt() : 5;
        String agentId = args.has("agentId") ? args.get("agentId").asText() : "default";
        List<MemoryEntry> entries = memoryFacade.recall(agentId, query, topK, tier, depth);
        List<Map<String, Object>> results = new ArrayList<>();
        for (MemoryEntry entry : entries) {
            results.add(memoryEntryToMap(entry));
        }
        return Map.of("results", results);
    }

    private Map<String, Object> doMemoryConsolidate(JsonNode args) {
        if (memoryFacade == null) {
            return Map.of(
                    "error", "Memory module not available. Enable rawwise.context.enabled=true");
        }
        MemoryTier fromTier = MemoryTier.valueOf(args.get("fromTier").asText().toUpperCase());
        MemoryTier toTier = MemoryTier.valueOf(args.get("toTier").asText().toUpperCase());
        String agentId = args.has("agentId") ? args.get("agentId").asText() : "default";
        memoryFacade.consolidate(agentId, fromTier, toTier);
        return Map.of(
                "success", true, "message", "Consolidated from " + fromTier + " to " + toTier);
    }

    private Map<String, Object> doMemoryShare(JsonNode args) {
        if (memoryFacade == null) {
            return Map.of(
                    "error", "Memory module not available. Enable rawwise.context.enabled=true");
        }
        String toAgentId = args.get("toAgentId").asText();
        String entryId = args.get("entryId").asText();
        long ttlMs = args.get("ttlMs").asLong();
        boolean revocable = args.get("revocable").asBoolean();
        String permission = args.get("permission").asText();
        String agentId = args.has("agentId") ? args.get("agentId").asText() : "default";
        MemorySharePolicy policy =
                new MemorySharePolicy(Duration.ofMillis(ttlMs), revocable, permission);
        memoryFacade.share(agentId, toAgentId, entryId, policy);
        return Map.of("success", true, "message", "Shared entry " + entryId + " to " + toAgentId);
    }

    private Map<String, Object> doMemoryStatus(JsonNode args) {
        if (memoryFacade == null) {
            return Map.of(
                    "error", "Memory module not available. Enable rawwise.context.enabled=true");
        }
        String agentId = args.has("agentId") ? args.get("agentId").asText() : "default";
        MemoryStatus status = memoryFacade.status(agentId);
        return Map.of(
                "agentId",
                status.agentId(),
                "workingCount",
                status.workingCount(),
                "episodicCount",
                status.episodicCount(),
                "semanticCount",
                status.semanticCount(),
                "proceduralCount",
                status.proceduralCount(),
                "totalCount",
                status.totalCount());
    }

    private Map<String, Object> doMemoryDecay(JsonNode args) {
        if (memoryFacade == null) {
            return Map.of(
                    "error", "Memory module not available. Enable rawwise.context.enabled=true");
        }
        long ageMs = args.get("ageMs").asLong();
        String agentId = args.has("agentId") ? args.get("agentId").asText() : "default";
        memoryFacade.decay(agentId, Duration.ofMillis(ageMs));
        return Map.of(
                "success", true, "message", "Decay applied for entries older than " + ageMs + "ms");
    }

    private Map<String, Object> doMemoryReflect(JsonNode args) {
        if (memoryFacade == null) {
            return Map.of(
                    "error", "Memory module not available. Enable rawwise.context.enabled=true");
        }
        String query = args.get("query").asText();
        String agentId = args.has("agentId") ? args.get("agentId").asText() : "default";
        Optional<MemoryEntry> result = memoryFacade.reflect(agentId, query);
        if (result.isPresent()) {
            return memoryEntryToMap(result.get());
        }
        return Map.of("found", false, "message", "No relevant memory found for reflection");
    }

    // ===== Graph Tool Handlers =====

    private Map<String, Object> doGraphQueryNodes(JsonNode args) {
        if (graphEngine == null) {
            return Map.of(
                    "error", "Graph engine not available. Enable rawwise.context.enabled=true");
        }
        String graphId = args.get("graphId").asText();
        String type =
                args.has("type") && !args.get("type").isNull() ? args.get("type").asText() : null;
        Map<String, String> propertyFilter = new LinkedHashMap<>();
        if (args.has("propertyFilter") && args.get("propertyFilter").isObject()) {
            var pf = args.get("propertyFilter");
            var iter = pf.fields();
            while (iter.hasNext()) {
                var entry = iter.next();
                propertyFilter.put(entry.getKey(), entry.getValue().asText());
            }
        }
        int limit = args.has("limit") ? args.get("limit").asInt() : 100;
        List<GraphNode> nodes = graphEngine.queryNodes(graphId, type, propertyFilter, limit);
        List<Map<String, Object>> results = new ArrayList<>();
        for (GraphNode node : nodes) {
            results.add(graphNodeToMap(node));
        }
        return Map.of("results", results);
    }

    private Map<String, Object> doGraphFindPaths(JsonNode args) {
        if (graphEngine == null) {
            return Map.of(
                    "error", "Graph engine not available. Enable rawwise.context.enabled=true");
        }
        String graphId = args.get("graphId").asText();
        String srcNodeId = args.get("srcNodeId").asText();
        String dstNodeId = args.get("dstNodeId").asText();
        int maxHops = args.has("maxHops") ? args.get("maxHops").asInt() : 5;
        List<PathResult> paths = graphEngine.findPaths(graphId, srcNodeId, dstNodeId, maxHops);
        List<Map<String, Object>> results = new ArrayList<>();
        for (PathResult path : paths) {
            results.add(pathResultToMap(path));
        }
        return Map.of("results", results);
    }

    private Map<String, Object> doGraphVectorSearch(JsonNode args) {
        if (graphEngine == null) {
            return Map.of(
                    "error", "Graph engine not available. Enable rawwise.context.enabled=true");
        }
        String graphId = args.get("graphId").asText();
        String query = args.get("query").asText();
        int topK = args.has("topK") ? args.get("topK").asInt() : 10;
        List<VectorSearchHit> hits = graphEngine.vectorSearch(graphId, query, topK);
        List<Map<String, Object>> results = new ArrayList<>();
        for (VectorSearchHit hit : hits) {
            Map<String, Object> hitMap = new LinkedHashMap<>();
            hitMap.put("score", hit.score());
            hitMap.put("node", graphNodeToMap(hit.node()));
            results.add(hitMap);
        }
        return Map.of("results", results);
    }

    private Map<String, Object> doGraphGetNode(JsonNode args) {
        if (graphEngine == null) {
            return Map.of(
                    "error", "Graph engine not available. Enable rawwise.context.enabled=true");
        }
        String graphId = args.get("graphId").asText();
        String nodeId = args.get("nodeId").asText();
        Optional<GraphNode> node = graphEngine.getNode(graphId, nodeId);
        if (node.isPresent()) {
            return graphNodeToMap(node.get());
        }
        return Map.of(
                "found",
                false,
                "message",
                "Node '" + nodeId + "' not found in graph '" + graphId + "'");
    }

    private Map<String, Object> doGraphNeighbors(JsonNode args) {
        if (graphEngine == null) {
            return Map.of(
                    "error", "Graph engine not available. Enable rawwise.context.enabled=true");
        }
        String graphId = args.get("graphId").asText();
        String nodeId = args.get("nodeId").asText();
        TraversalDirection direction =
                TraversalDirection.valueOf(args.get("direction").asText().toUpperCase());
        String label =
                args.has("label") && !args.get("label").isNull()
                        ? args.get("label").asText()
                        : null;
        int maxHops = args.has("maxHops") ? args.get("maxHops").asInt() : 1;
        int limit = args.has("limit") ? args.get("limit").asInt() : 100;
        List<GraphNode> neighbors =
                graphEngine.neighbors(graphId, nodeId, label, maxHops, limit, direction);
        List<Map<String, Object>> results = new ArrayList<>();
        for (GraphNode node : neighbors) {
            results.add(graphNodeToMap(node));
        }
        return Map.of("results", results);
    }

    // ===== State Tool Handlers =====

    private Map<String, Object> doStateCreateSpace(JsonNode args) {
        if (stateEngine == null) {
            return Map.of("error", "State engine not available");
        }
        String agentId = args.get("agentId").asText();
        String name = args.get("name").asText();
        String initialState = args.get("initialState").asText();
        List<String> states = new ArrayList<>();
        if (args.has("states") && args.get("states").isArray()) {
            for (var s : args.get("states")) {
                states.add(s.asText());
            }
        }
        List<StateTransition> transitions = new ArrayList<>();
        if (args.has("transitions") && args.get("transitions").isArray()) {
            for (var t : args.get("transitions")) {
                String fromState = t.has("fromState") ? t.get("fromState").asText() : "";
                String toState = t.has("toState") ? t.get("toState").asText() : "";
                String guard = t.has("guardExpression") ? t.get("guardExpression").asText() : null;
                transitions.add(new StateTransition(fromState, toState, guard, Map.of()));
            }
        }
        Map<String, String> metadata = new LinkedHashMap<>();
        if (args.has("metadata") && args.get("metadata").isObject()) {
            var meta = args.get("metadata");
            var iter = meta.fields();
            while (iter.hasNext()) {
                var entry = iter.next();
                metadata.put(entry.getKey(), entry.getValue().asText());
            }
        }
        try {
            StateSpace space =
                    stateEngine.createStateSpace(
                            agentId, name, initialState, states, transitions, metadata);
            return stateSpaceToMap(space);
        } catch (Exception e) {
            log.warn("State createStateSpace failed: agentId={}", agentId, e);
            return Map.of("error", e.getMessage());
        }
    }

    private Map<String, Object> doStateListSpaces(JsonNode args) {
        if (stateEngine == null) {
            return Map.of("error", "State engine not available");
        }
        String agentId = args.get("agentId").asText();
        try {
            List<StateSpace> spaces = stateEngine.listStateSpacesByAgent(agentId);
            List<Map<String, Object>> results = new ArrayList<>();
            for (StateSpace space : spaces) {
                results.add(stateSpaceToMap(space));
            }
            return Map.of("results", results);
        } catch (Exception e) {
            log.warn("State listSpaces failed: agentId={}", agentId, e);
            return Map.of("error", e.getMessage());
        }
    }

    private Map<String, Object> doStateRead(JsonNode args) {
        if (stateEngine == null) {
            return Map.of("error", "State engine not available");
        }
        String spaceId = args.get("spaceId").asText();
        String key = args.get("key").asText();
        try {
            StateEntry entry = stateEngine.read(spaceId, key);
            return stateEntryToMap(entry);
        } catch (Exception e) {
            log.warn("State read failed: spaceId={}, key={}", spaceId, key, e);
            return Map.of("error", e.getMessage());
        }
    }

    private Map<String, Object> doStateWrite(JsonNode args) {
        if (stateEngine == null) {
            return Map.of("error", "State engine not available");
        }
        String spaceId = args.get("spaceId").asText();
        String key = args.get("key").asText();
        String value = args.get("value").asText();
        String stateName = args.get("stateName").asText();
        Map<String, String> metadata = new LinkedHashMap<>();
        if (args.has("metadata") && args.get("metadata").isObject()) {
            var meta = args.get("metadata");
            var iter = meta.fields();
            while (iter.hasNext()) {
                var entry = iter.next();
                metadata.put(entry.getKey(), entry.getValue().asText());
            }
        }
        try {
            StateEntry entry = stateEngine.write(spaceId, key, value, stateName, metadata);
            return stateEntryToMap(entry);
        } catch (Exception e) {
            log.warn("State write failed: spaceId={}, key={}", spaceId, key, e);
            return Map.of("error", e.getMessage());
        }
    }

    private Map<String, Object> doStateTransition(JsonNode args) {
        if (stateEngine == null) {
            return Map.of("error", "State engine not available");
        }
        String spaceId = args.get("spaceId").asText();
        String key = args.get("key").asText();
        String toState = args.get("toState").asText();
        Map<String, String> metadata = new LinkedHashMap<>();
        if (args.has("metadata") && args.get("metadata").isObject()) {
            var meta = args.get("metadata");
            var iter = meta.fields();
            while (iter.hasNext()) {
                var entry = iter.next();
                metadata.put(entry.getKey(), entry.getValue().asText());
            }
        }
        try {
            StateEntry entry = stateEngine.transition(spaceId, key, toState, metadata);
            return stateEntryToMap(entry);
        } catch (Exception e) {
            log.warn("State transition failed: spaceId={}, key={}", spaceId, key, e);
            return Map.of("error", e.getMessage());
        }
    }

    private Map<String, Object> doStateScan(JsonNode args) {
        if (stateEngine == null) {
            return Map.of("error", "State engine not available");
        }
        String spaceId = args.get("spaceId").asText();
        String prefix =
                args.has("prefix") && !args.get("prefix").isNull()
                        ? args.get("prefix").asText()
                        : "";
        int limit = args.has("limit") ? args.get("limit").asInt() : 100;
        try {
            List<StateEntry> entries = stateEngine.scan(spaceId, prefix, limit);
            List<Map<String, Object>> results = new ArrayList<>();
            for (StateEntry entry : entries) {
                results.add(stateEntryToMap(entry));
            }
            return Map.of("results", results);
        } catch (Exception e) {
            log.warn("State scan failed: spaceId={}", spaceId, e);
            return Map.of("error", e.getMessage());
        }
    }

    // ===== Helpers =====

    private Map<String, Object> executeSql(String sql, int timeoutSeconds) {
        var submitResult = backendClient.submitQuery(sql, QueryType.SQL, Map.of());
        if (!submitResult.success()) {
            return Map.of(
                    "jobId",
                    submitResult.jobId(),
                    "status",
                    "FAILED",
                    "error",
                    submitResult.errorMessage());
        }
        String jobId = submitResult.jobId();
        for (int i = 0; i < Math.min(QUERY_POLL_MAX_RETRIES, timeoutSeconds); i++) {
            try {
                Thread.sleep(QUERY_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Map.of("jobId", jobId, "status", "INTERRUPTED");
            }
            var jobState = backendClient.getJobStatus(jobId);
            if (jobState == JobState.SUCCEEDED) {
                var result = backendClient.getQueryResult(jobId);
                List<Map<String, String>> columns = new ArrayList<>();
                if (result.columns() != null) {
                    for (var col : result.columns()) {
                        columns.add(Map.of("name", col.name(), "type", col.type()));
                    }
                }
                List<List<Object>> rows = result.rows() != null ? result.rows() : List.of();
                return Map.of(
                        "jobId",
                        jobId,
                        "status",
                        "SUCCEEDED",
                        "columns",
                        columns,
                        "rows",
                        rows,
                        "rowCount",
                        result.rowCount());
            }
            if (jobState == JobState.FAILED || jobState == JobState.CANCELLED) {
                return Map.of("jobId", jobId, "status", jobState.name());
            }
        }
        return Map.of(
                "jobId",
                jobId,
                "status",
                "TIMED_OUT",
                "message",
                "Query did not complete within " + timeoutSeconds + "s. Use jobId to poll later.");
    }

    private static Object jsonNodeToObject(JsonNode node) {
        if (node.isTextual()) return node.asText();
        if (node.isInt() || node.isLong()) return node.asLong();
        if (node.isDouble() || node.isFloat()) return node.asDouble();
        if (node.isBoolean()) return node.asBoolean();
        return node.toString();
    }

    private static Depth parseDepth(String raw) {
        // 统一走 Depth.resolve 兼容映射（缺省→ABSTRACT、L0→ABSTRACT、L1/L2→DETAIL）
        return Depth.resolve(raw);
    }

    private static Map<String, Object> memoryEntryToMap(MemoryEntry entry) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("entryId", entry.entryId());
        map.put("agentId", entry.agentId());
        map.put("memoryTier", entry.memoryTier().name());
        map.put("depth", entry.depth().name());
        map.put("content", entry.content());
        map.put("importance", entry.importance());
        map.put("decayFactor", entry.decayFactor());
        map.put("createdAt", entry.createdAt());
        map.put("updatedAt", entry.updatedAt());
        if (entry.relevanceScore() != null) {
            map.put("relevanceScore", entry.relevanceScore());
        }
        return map;
    }

    private static Map<String, Object> graphNodeToMap(GraphNode node) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("nodeId", node.nodeId());
        map.put("graphId", node.graphId());
        map.put("type", node.type());
        map.put("properties", node.properties());
        map.put("importance", node.importance());
        map.put("decayFactor", node.decayFactor());
        map.put("embeddingRef", node.embeddingRef());
        map.put("embeddingModel", node.embeddingModel());
        map.put("embeddingText", node.embeddingText());
        map.put("version", node.version());
        map.put("createdAt", node.createdAt());
        map.put("updatedAt", node.updatedAt());
        return map;
    }

    private static Map<String, Object> pathResultToMap(PathResult path) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("graphId", path.graphId());
        map.put("srcNodeId", path.srcNodeId());
        map.put("dstNodeId", path.dstNodeId());
        map.put("nodeIds", path.nodeIds());
        map.put("edgeIds", path.edgeIds());
        map.put("edgeLabels", path.edgeLabels());
        map.put("totalWeight", path.totalWeight());
        map.put("hopCount", path.hopCount());
        return map;
    }

    private static Map<String, Object> stateSpaceToMap(StateSpace space) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("spaceId", space.spaceId());
        map.put("agentId", space.agentId());
        map.put("name", space.name());
        map.put("initialState", space.initialState());
        map.put("states", space.states());
        List<Map<String, Object>> transitions = new ArrayList<>();
        for (StateTransition t : space.transitions()) {
            Map<String, Object> tm = new LinkedHashMap<>();
            tm.put("fromState", t.fromState());
            tm.put("toState", t.toState());
            tm.put("guardExpression", t.guardExpression());
            tm.put("metadata", t.metadata());
            transitions.add(tm);
        }
        map.put("transitions", transitions);
        map.put("metadata", space.metadata());
        map.put("createdAt", space.createdAt());
        map.put("updatedAt", space.updatedAt());
        return map;
    }

    private static Map<String, Object> stateEntryToMap(StateEntry entry) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("key", entry.key());
        map.put("value", entry.value());
        map.put("stateName", entry.stateName());
        map.put("version", entry.version());
        map.put("metadata", entry.metadata());
        map.put("createdAt", entry.createdAt());
        map.put("updatedAt", entry.updatedAt());
        return map;
    }

    private static Map<String, Object> prop(String type, String description) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type);
        m.put("description", description);
        return m;
    }

    private static Map<String, Object> schema(
            Map<String, Object> properties, List<String> required) {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("type", "object");
        s.put("properties", properties);
        s.put("required", required);
        return s;
    }

    private static Map<String, Object> schemaTwoStr(
            String key1, String desc1, String key2, String desc2) {
        return schema(
                Map.of(key1, prop("string", desc1), key2, prop("string", desc2)),
                List.of(key1, key2));
    }

    private static Map<String, Object> toolDef(
            String name, String description, Map<String, Object> inputSchema) {
        Map<String, Object> tool = new LinkedHashMap<>();
        tool.put("name", name);
        tool.put("description", description);
        tool.put("inputSchema", inputSchema);
        return tool;
    }
}
