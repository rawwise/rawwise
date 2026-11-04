# RawWise

[English](README.md) | [中文](README.zh-CN.md)

**认知数据引擎（Cognitive Data Engine）** - 把关系数据、向量计算与 AI 认知统一为同一条可溯源语义记录的 AI 原生数据引擎。

所谓*认知*数据引擎，可以这样理解：写入时就在引擎内部完成语义派生，每一个派生结果都能追溯回它的来源，反馈能回流并触发重新派生。RawWise 把这三点做进同一套引擎，同时提供 OLTP 点查、OLAP 分析和混合语义搜索——Agent、应用和分析人员通过 SQL、HTTP 或 MCP 读取同一份语义记录，而不必各自拼装一套技术栈。

***

## 1. 什么才算"认知"

一个加了 embedding 字段的数据库不是认知数据引擎。RawWise 按三个属性设计；去掉其中任何一个，剩下的就只是一个数据库、一个向量库，或者一段 RAG 脚本。

| 属性        | 要求                            | RawWise 的做法                                           |
| --------- | ----------------------------- | ----------------------------------------------------- |
| **派生内建**  | 语义在写入时由存储引擎内部产生，而不是靠引擎外的流水线拼装 | 每次写入触发：分块 → 摘要 → 向量化 → 索引 → 实体抽取 → 知识图谱构建             |
| **认知可溯源** | 每个派生结果都能追溯到确切的原始字节，以及产生它的模型版本 | Raw → Chunk → View → Semantic → Action 血缘链，可按单条搜索结果查询 |
| **反馈闭环**  | 纠正能改变后续行为，认知资产随时间累积           | 显式标注与隐式信号触发受影响 View 的级联重派生                            |

第三条最难被复制。它让数据引擎从被动的存储，变成一台越用越准的系统。

## 2. 为什么不直接拼现成的组件？

| 方案                          | 它给你什么                     | 它缺什么                           |
| --------------------------- | ------------------------- | ------------------------------ |
| 关系数据库 + 向量扩展                | 一个进程里同时有 SQL 和 ANN        | 没有派生管线、没有血缘、没有反馈闭环             |
| 向量数据库                       | 对 embedding 的高效 ANN 检索    | embedding 是死胡同：回不到原文，没有摘要，没有事务 |
| Lakehouse / lakebase 式 OLTP | 开放文件格式 + SQL，湖存储上的事务行     | 没有写入时认知、没有 Agent 记忆、没有状态       |
| RAG 框架                      | 给 prompt 做检索的胶水           | 没有存储引擎、没有一致性、知识不会沉淀            |
| **RawWise**                 | 在关系、向量、分析数据之上同时提供派生、血缘与反馈 | -                              |

## 3. 谁在用它

引擎的身份不由消费者定义：同一份语义记录、同一个事务、同一套血缘服务于所有客户端，差的只是接口。

| 使用者 | 它得到什么 | 接口 |
|---|---|---|
| **Agent 运行时**（LangChain、AutoGen、编码 Agent、工作流 Agent） | 知识库、四层记忆、带守卫的状态、图谱遍历 | MCP |
| **AI 应用** | 写入即派生、混合语义搜索、血缘追溯、反馈纠正 | HTTP |
| **数据应用** | 标准 SQL DDL/DML、FastPath 点查、Spark SQL 分析 | HTTP SQL |
| **数据与 AI 工程** | 派生管线、embedding、用于 reranker 训练的隐式反馈信号 | HTTP / 配置 |
| **审计与治理** | 单条结果的完整血缘，绑定到产生它的模型版本 | 血缘 API |

## 4. 语义记录模型

每一次摄入都会生成一条 **SemanticRecord**：一个可追溯对象，包含五层，每一层由前一层派生而来。

```
Raw ───────► Chunk ───────► View ───────► Semantic ───────► Action
不可变        可寻址          多模型版本      结构化抽象        动作可达性
原始字节      语义片段        计算视图        标签/实体/关系    给 Agent 的声明
```

| 层            | 含义                                                            |
| ------------ | ------------------------------------------------------------- |
| **Raw**      | 系统边界捕获的不可变原始字节。写入一次，永不修改。                                     |
| **Chunk**    | 可寻址的语义片段：文本段落、图像区域、表格行、代码块。每个 chunk 指回它的 raw 来源。              |
| **View**     | 特定模型（及模型版本）在一组 chunk 上的计算结果。多个 View 共存——模型升级只会新增 View，不会覆盖历史。 |
| **Semantic** | 对 View 的结构化抽象：标签、实体、分类、关联。与产生它的 View 版本绑定。                    |
| **Action**   | 基于这些 View，Agent 可以做什么：操作名、参数约束、预期效果。                          |

这个模型与模态解耦：同一条派生链在文本、图像、音频之间不变，变化的只是分块器和 embedding 模型。当前以文本落地；图像区域与音频片段按同一记录模型接入，共享同一份血缘和同一个反馈回路。

## 5. 快速开始

### 前置条件

- **JDK 21**（JDK，不是 JRE）
- **Maven 3.9+** 需在 `PATH` 中——仓库没有附带 Maven wrapper
- 默认 Embedded profile 预留 6 GB JVM 堆（`RAWWISE_EMBEDDED_HEAP`）

### 构建与启动

```bash
# 1. 安装 JDK 21 与 Maven 3.9+，然后确认
java -version   # 应显示 21
mvn -v

# 2. 构建（仅首次）
mvn package -pl rawwise-core -DskipTests -q

# 3. 启动 RawWise（单进程 Embedded 模式：Frontend + Backend + Metastore 同 JVM）
#    请使用此脚本，而不是直接 java -cp 启动：它会开启 context（记忆/图谱 API）、
#    IR/FastPath 管线以及 Embedded Metastore 的装配。
bash bin/rawwise-start.sh

# 4. 创建认知表
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"CREATE TABLE knowledge (id STRING, content STRING) USING rawwise TBLPROPERTIES ('\''primary-key'\''='\''id'\'', '\''ai-native'\''='\''true'\'')"}'

# 5. 写入知识（自动触发派生管线）
curl -s -X POST http://localhost:8080/api/axiom/ingest \
  -H "Content-Type: application/json" \
  -d '{"tableId":"default.knowledge","key":"doc:1","body":"RawWise is a cognitive data engine with built-in vector search and knowledge derivation."}'

# 6. 语义搜索（noop 模式下为关键词排序，summary 为空）
curl -s -X POST http://localhost:8080/api/axiom/search \
  -H "Content-Type: application/json" \
  -d '{"tableId":"default.knowledge","query":"what is rawwise","topK":5}'

# 7. 追溯血缘
curl -s http://localhost:8080/api/axiom/provenance/view:<viewId>
```

### 不接外部模型时，哪些能力可用

全新检出默认使用 `noop` 提供方（embedding、摘要、实体抽取、事实抽取）。存储、SQL、OLTP/OLAP、检索链路、记忆、状态机与血缘都可用，AI 质量则按可预期的方式降级。

| 能力 | 默认（`noop`） | 如何开启 |
|---|---|---|
| `search` 排序 | 实际按关键词排序（图谱有数据时叠加）；向量全为零，没有语义召回 | 在 `.env` 设置 `RAWWISE_EMBEDDING_TYPE=http` 及 endpoint / model / api-key |
| 搜索结果的 `summary` | 空字符串 | 设置 `RAWWISE_SUMMARIZATION_TYPE=http` 及 endpoint / model / api-key |
| 知识图谱 | 空：实体抽取与事实抽取默认关闭 | `RAWWISE_ENTITY_EXTRACTION_ENABLED=true`、`RAWWISE_FACT_EXTRACTION_ENABLED=true` |
| 记忆 / 状态 / 图谱 API | 由 `bin/rawwise-start.sh` 开启（`-Drawwise.context.enabled=true`） | - |
| 多查询扩展 | 默认开启 | - |
| Reranker 与查询改写 | 默认关闭 | 见「配置」与 `.env.example` |

将 `.env.example` 复制为 `.env` 并填入真实提供方，启动脚本会自动加载。

## 6. 它替代了什么

| 问题             | 传统方案                      | RawWise                                               |
| -------------- | ------------------------- | ----------------------------------------------------- |
| 事务和分析需要两套系统    | MySQL + ClickHouse，ETL 同步 | 一套引擎：FastPath 点查 + Spark SQL 分析，共享存储                  |
| 结构化查询与语义搜索割裂   | 关系库 + Milvus，数据双写         | 同一张表：SQL 精确查询 + 向量语义搜索，单次写入                           |
| 写入后要另外构建向量索引   | 应用层调 embedding API 再写向量库  | 写入即派生，零应用代码                                           |
| 范围更新要全表扫描重写    | 扫描匹配行、修改、重写 Parquet（秒到分钟） | Lazy Update：谓词+新值追加到 NWL 日志（毫秒级），读时动态合并，异步 Compaction |
| 搜索结果无法溯源       | 黑盒，不知道结果从哪来               | 全链路血缘：Raw → Chunk → View → Semantic → Action          |
| 用户反馈无法改进检索     | 重新训练整个模型                  | 标注单条结果，级联重派生，实时生效                                     |
| Agent 没有持久记忆   | 每次对话从零开始                  | 四层记忆（工作/情景/语义/程序），跨会话持久                               |
| 多 Agent 无法共享知识 | 各自维护独立知识库                 | 带权限与 TTL 的记忆共享                                        |

## 7. 架构

```
                    ┌─────────────────────────────────┐
                    │         RawWise Engine           │
                    │                                  │
  SQL Query ──────► │  ┌──────────┐  ┌──────────────┐ │
  (OLTP point)      │  │ FastPath  │  │ Spark SQL    │ │
                    │  │ (LMDB)    │  │ (Catalyst)   │ │
  SQL Query ──────► │  │ PK lookup │  │ OLAP analysis│ │
  (OLAP scan)       │  └────┬─────┘  └──────┬───────┘ │
                    │       │   Lazy Update  │         │
                    │       └───────┬────────┘         │
  Ingest ─────────► │  ┌──────────┐│┌──────────────┐  │
                    │  │ WAL +    │││ 认知派生      │  │
                    │  │ LMDB     │││ Raw->Chunk-> │  │
                    │  │ (hot)    │││ View->Embed->│  │
                    │  │          │││ JVector      │  │
                    │  └──────────┘│└──────────────┘  │
                    │       │      │       │          │
  Semantic Search ► │  ┌────┴──────┴───────┴──────┐  │
                    │  │  RRF Fusion (Vector +    │  │
                    │  │  Keyword + Graph +        │  │
                    │  │  Semantic + Action)       │  │
                    │  └───────────────────────────┘  │
                    │                                  │
  Memory / State ─► │  ┌───────────────────────────┐  │
                    │  │  Memory (4 tiers) + State │  │
                    │  │  Machine + Knowledge Graph│  │
                    │  └───────────────────────────┘  │
                    └─────────────────────────────────┘

  Traditional:  MySQL + ClickHouse + Milvus + ES + Redis (5 systems, ETL sync)
  RawWise:      One engine, one transaction, one copy of data
```

每个请求都会被路由到能满足它的最便宜的执行路径：

| 负载      | 执行路径                         | 行为                 |
| ------- | ---------------------------- | ------------------ |
| 点查 / 点写 | FastPath → LMDB              | 主键等值，绕过 Spark SQL  |
| 范围更新    | NWL 追加 + 异步 Compaction       | 谓词与新值毫秒级落日志，读时动态合并 |
| 分析查询    | Spark SQL Catalyst           | 全表扫描、过滤、聚合、Join    |
| 混合搜索    | Vector + Keyword + Graph RRF | 跨表语义召回             |

## 8. 接口

### 8.1 MCP（推荐 Agent 使用）

RawWise 在 `http://localhost:8080/mcp` 暴露 **34 个 MCP 工具**。

MCP 客户端配置：

```json
{
  "rawwise": {
    "url": "http://localhost:8080/mcp",
    "transport": "http"
  }
}
```

除 `memory.decay` 与 `memory.reflect` 外，所有工具默认启用；这两个需通过 `rawwise.mcp.tools.memory.decay.enabled=true` / `rawwise.mcp.tools.memory.reflect.enabled=true` 显式开启。

#### 知识流水线（16 个工具）

| 工具                          | 必填                              | 可选                          | 说明                                                 |
| --------------------------- | ------------------------------- | --------------------------- | -------------------------------------------------- |
| `rawwise_ingest`            | `tableId`, `key`, `body`        | `immutable`, `properties`   | 写入数据，自动触发 Raw → Chunk → View → Embedding → JVector |
| `rawwise_search`            | `tableId`, `query`              | `topK`                      | 混合搜索（向量 + 关键词 + 图谱 RRF）                            |
| `rawwise_cognize`           | `tableId`, `query`              | `topK`                      | 五层认知召回                                             |
| `rawwise_annotate`          | `viewId`, `type`, `annotatorId` | `value`, `confidence`       | POSITIVE / NEGATIVE / CORRECTION 反馈                |
| `rawwise_provenance`        | `viewId`                        | -                           | 完整派生血缘                                             |
| `rawwise_rederive`          | `tableId`, `scope`              | `singleKey`, `forceReindex` | 级联重派生                                              |
| `rawwise_query_sql`         | `sql`                           | `timeoutSeconds`            | 执行 SQL（DDL 立即返回，DML 轮询）                            |
| `rawwise_list_tables`       | -                               | -                           | 列出所有表                                              |
| `rawwise_implicit_feedback` | `viewId`, `signalType`          | `dwellMs`                   | DWELL / CLICK / SKIP，用于 reranker 训练                |
| `rawwise_get_view`          | `tableId`, `viewId`             | -                           | 获取 View 层内容                                        |
| `rawwise_get_chunk`         | `tableId`, `chunkId`            | -                           | 获取 Chunk 层内容                                       |
| `rawwise_get_semantic`      | `tableId`, `semanticId`         | -                           | 获取 Semantic 层内容                                    |
| `rawwise_get_action`        | `tableId`, `actionId`           | -                           | 获取 Action 层内容                                      |
| `rawwise_get_action_plan`   | `tableId`, `planId`             | -                           | 获取 ActionPlan 内容                                   |
| `rawwise_fetch_details`     | `tableId`, `entryKeys`          | -                           | 批量获取 raw\_text + view\_summary                     |
| `rawwise_diagnostics`       | -                               | -                           | 流水线诊断                                              |

#### 记忆（7 个工具）

| 工具                           | 必填                                                         | 可选                      | 说明                                               |
| ---------------------------- | ---------------------------------------------------------- | ----------------------- | ------------------------------------------------ |
| `rawwise_memory_store`       | `content`, `memoryTier`                                    | `importance`, `agentId` | 存储记忆（WORKING / EPISODIC / SEMANTIC / PROCEDURAL） |
| `rawwise_memory_recall`      | `query`, `memoryTier`, `loadTier`                          | `topK`, `agentId`       | 召回记忆（L0/L1/L2 加载层级）                              |
| `rawwise_memory_consolidate` | `fromTier`, `toTier`                                       | `agentId`               | 在不同层级之间整合记忆                                      |
| `rawwise_memory_share`       | `toAgentId`, `entryId`, `ttlMs`, `revocable`, `permission` | `agentId`               | 将记忆分享给另一个 Agent                                  |
| `rawwise_memory_status`      | -                                                          | `agentId`               | 查询记忆状态                                           |
| `rawwise_memory_decay`       | `ageMs`                                                    | `agentId`               | 衰减旧记忆（默认关闭）                                      |
| `rawwise_memory_reflect`     | `query`                                                    | `agentId`               | 对记忆进行反思（默认关闭）                                    |

#### 图谱（5 个工具）

| 工具                            | 必填                                  | 可选                                | 说明                    |
| ----------------------------- | ----------------------------------- | --------------------------------- | --------------------- |
| `rawwise_graph_query_nodes`   | `graphId`                           | `type`, `propertyFilter`, `limit` | 查询节点                  |
| `rawwise_graph_find_paths`    | `graphId`, `srcNodeId`, `dstNodeId` | `maxHops`                         | 查找节点间路径               |
| `rawwise_graph_vector_search` | `graphId`, `query`                  | `topK`                            | 图谱向量搜索                |
| `rawwise_graph_get_node`      | `graphId`, `nodeId`                 | -                                 | 获取节点详情                |
| `rawwise_graph_neighbors`     | `graphId`, `nodeId`, `direction`    | `label`, `maxHops`, `limit`       | 获取邻居（OUT / IN / BOTH） |

#### 状态机（6 个工具）

| 工具                           | 必填                                          | 可选                        | 说明             |
| ---------------------------- | ------------------------------------------- | ------------------------- | -------------- |
| `rawwise_state_create_space` | `agentId`, `name`, `initialState`, `states` | `transitions`, `metadata` | 创建状态空间         |
| `rawwise_state_list_spaces`  | `agentId`                                   | -                         | 列出 Agent 的状态空间 |
| `rawwise_state_read`         | `spaceId`, `key`                            | -                         | 读取状态条目         |
| `rawwise_state_write`        | `spaceId`, `key`, `value`, `stateName`      | `metadata`                | 写入状态（直接设置）     |
| `rawwise_state_transition`   | `spaceId`, `key`, `toState`                 | `metadata`                | 状态迁移（校验守卫）     |
| `rawwise_state_scan`         | `spaceId`                                   | `prefix`, `limit`         | 按前缀扫描          |

### 8.2 HTTP API

Base URL：`http://localhost:8080`

| 端点                               | 方法   | 用途                       |
| -------------------------------- | ---- | ------------------------ |
| `/api/axiom/ingest`              | POST | 写入知识（自动派生）               |
| `/api/axiom/search`              | POST | 语义搜索                     |
| `/api/axiom/cognize`             | POST | 五层认知                     |
| `/api/axiom/annotate`            | POST | 反馈（级联重派生）                |
| `/api/axiom/provenance/{viewId}` | GET  | 追溯派生链                    |
| `/api/axiom/fetch-details`       | POST | 批量获取 raw\_text + summary |
| `/api/v1/axiom/memory/store`     | POST | 存储记忆                     |
| `/api/v1/axiom/memory/recall`    | POST | 召回记忆                     |
| `/api/v1/axiom/state/spaces`     | POST | 创建状态空间                   |
| `/api/query`                     | POST | 执行 SQL                   |

### 8.3 如何选择工具

| 你的目标           | 使用此工具                                                                                                                | 原因                                           |
| -------------- | -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- |
| 存入新知识或文档       | `rawwise_ingest`                                                                                                     | 写入即派生 Raw → Chunk → View → Semantic → Action |
| 从已有知识回答问题      | `rawwise_search`                                                                                                     | 混合（向量 + 关键词 + 图谱）语义召回                        |
| 需要答案背后的多层细节    | `rawwise_cognize`                                                                                                    | 返回 L0 原始 + L1 分块 + L2 摘要 + L3 语义 + L4 动作     |
| 读写结构化行         | `rawwise_query_sql`                                                                                                  | 统一存储上的标准 SQL                                 |
| 查找有哪些表         | `rawwise_list_tables`                                                                                                | 返回表与 schema                                  |
| 解释结果为何出现       | `rawwise_provenance`                                                                                                 | View 的完整派生血缘                                 |
| 用户说结果对/错       | `rawwise_annotate`                                                                                                   | 触发级联重派生                                      |
| 追踪停留/点击/跳过     | `rawwise_implicit_feedback`                                                                                          | 供 reranker 训练                                |
| 跨会话记住用户事实      | `rawwise_memory_store` / `rawwise_memory_recall`                                                                     | 四层持久记忆                                       |
| 将短期记忆提升为长期     | `rawwise_memory_consolidate`                                                                                         | 例如任务完成后 WORKING → SEMANTIC                   |
| 与其他 Agent 共享知识 | `rawwise_memory_share`                                                                                               | 带权限与 TTL 的跨 Agent 记忆                         |
| 建模多步骤工作流       | `rawwise_state_create_space` + `rawwise_state_transition`                                                            | 带守卫的状态机                                      |
| 探索实体关系         | `rawwise_graph_query_nodes` / `rawwise_graph_neighbors` / `rawwise_graph_find_paths`                                 | 知识图谱遍历                                       |
| 读取某一层的具体内容     | `rawwise_get_view` / `rawwise_get_chunk` / `rawwise_get_semantic` / `rawwise_get_action` / `rawwise_get_action_plan` | 精确到层                                         |

**如何选择层**：`rawwise_search` 之后，只需要摘要就直接用返回的 `summary`；需要完整原文就用 `entryKey` 调 `rawwise_fetch_details`；需要派生链就用 `viewId` 调 `rawwise_provenance`。

**读路径 vs 写路径**：写操作（`rawwise_ingest`、`rawwise_query_sql` 的 INSERT、`rawwise_memory_store`、`rawwise_state_write`）立即返回或返回 jobId 供轮询；读操作（`rawwise_search`、`rawwise_query_sql` 的 SELECT、`rawwise_memory_recall`、`rawwise_state_read`）同步返回数据。

### 8.4 记忆模式

```bash
# 存储用户偏好
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{"content":"User prefers concise technical answers","memoryTier":"EPISODIC","importance":0.8}'

# 回答前先召回
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/recall \
  -H "Content-Type: application/json" \
  -d '{"query":"user answer style preference","memoryTier":"EPISODIC","loadTier":"L1","topK":3}'

# 任务执行中存储工作记忆
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{"content":"Currently debugging auth module","memoryTier":"WORKING"}'

# 任务完成后将工作记忆整合为语义记忆
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/consolidate \
  -H "Content-Type: application/json" \
  -d '{"fromTier":"WORKING","toTier":"SEMANTIC"}'
```

### 8.5 反馈模式

```bash
# 用户表示结果不正确
curl -s -X POST http://localhost:8080/api/axiom/annotate \
  -H "Content-Type: application/json" \
  -d '{"viewId":"view:abc123","type":"NEGATIVE","value":"-5","annotatorId":"agent"}'
```

随后 RawWise 自动闭环：

1. 调整该条目的排序权重。
2. 用新参数重新派生受影响的 View。
3. 下次搜索返回更优结果。

## 9. 核心概念

| 概念                       | 说明                                                        |
| ------------------------ | --------------------------------------------------------- |
| **认知数据引擎**               | 写入即派生、派生可溯源、反馈在引擎内闭环的存储引擎。                                |
| **SemanticRecord**       | 原子数据单元：一份原始来源，加上派生出的 Chunk / View / Semantic / Action 各层。 |
| **认知表（Cognitive Table）** | 用 `TBLPROPERTIES ('ai-native'='true')` 声明的表。写入自动触发派生。     |
| **View**                 | 搜索返回的表示层。多个模型版本共存。                                        |
| **血缘（Provenance）**       | 从 Raw 到 Action 的完整链路，每条搜索结果均可溯源。                          |
| **记忆层级**                 | WORKING（当前任务）、EPISODIC（事件）、SEMANTIC（事实）、PROCEDURAL（技能）。   |
| **加载层级**                 | L0（原始）、L1（摘要）、L2（仅元数据），控制召回深度。                            |
| **级联重派生**                | 反馈触发受影响 View 的重新派生，持续改进后续结果。                              |

## 10. 性能

YCSB（Yahoo! Cloud Serving Benchmark）通过 HTTP API 端到端测量延迟（p99，微秒），所有工作负载均通过 `correctness=true` 一致性校验。

**单节点嵌入式模式**（FastPath + LMDB）：

| 负载     | 混合比           | median (us) | p99 (us) |
| ------ | ------------- | ----------- | -------- |
| C（纯读）  | 100%R         | 105         | **200**  |
| D（读最新） | 95%R / 5%Scan | 98          | 1,050    |
| B（读为主） | 95%R / 5%W    | 128         | 5,275    |
| A（读重）  | 50%R / 50%W   | 211         | **934**  |

**分布式 8-VM 集群**（5 个 BE 节点 + Frontend + Metastore，4 线程，局域网）：

| 负载     | 混合比         | median (us) | p99 (us) |
| ------ | ----------- | ----------- | -------- |
| C（纯读）  | 100%R       | 6,582       | 9,301    |
| B（读为主） | 95%R / 5%W  | 6,919       | 18,504   |
| A（读重）  | 50%R / 50%W | 12,770      | 22,495   |

> **注**：分布式数据包含网络 RTT（两跳 gRPC：Client → Frontend → BE Node），且运行在 VM 上（约为物理机性能的 50-70%）。单节点数据在 Apple Silicon 本机测得、无网络跳数。两者不宜直接对比——分布式部署的价值在于高并发和大数据量下的横向扩展。

## 11. 运维

`rawwise-start.sh` 以**单进程 Embedded 模式**运行（Frontend + Backend + Metastore 同 JVM），数据持久化在 `~/.rawwise/data/`。它是受支持的启动入口：除了拉起进程，还会设置记忆、图谱与 FastPath 所需的开关。

| 命令                            | 说明                        |
| ----------------------------- | ------------------------- |
| `bash bin/rawwise-start.sh`   | 启动（默认端口 8080，Embedded 模式） |
| `bash bin/rawwise-stop.sh`    | 优雅停止                      |
| `bash bin/rawwise-restart.sh` | 停止 + 清理锁 + 启动             |
| `bash bin/rawwise-status.sh`  | 查看状态                      |
| `bash bin/rawwise-clean.sh`   | 删除所有数据                    |

## 12. 配置

所有配置通过系统属性（`-Drawwise.*`）设置，关键项：

| 配置键                                | 默认值      | 说明                                       |
| ---------------------------------- | -------- | ---------------------------------------- |
| `rawwise.axiom.enabled`            | `true`   | 启用认知能力                                   |
| `rawwise.mcp.enabled`              | `true`   | 启用 `/mcp` 端点                             |
| `rawwise.frontend.ir.enabled`      | `false`  | 启用 IR 流水线（FastPath）                      |
| `rawwise.context.enabled`          | `false`  | 启用上下文/会话（记忆相关 API 依赖）                    |
| `rawwise.embedding.provider.type`  | `noop`   | embedding 提供方（noop / http / stub / onnx） |
| `rawwise.axiom.search.path`        | `hybrid` | 搜索路径（hybrid / vector / keyword）          |
| `rawwise.axiom.multiquery.enabled` | `true`   | 多查询扩展（3 个变体）                             |
| `rawwise.axiom.rrf.k`              | `60`     | RRF 融合常数                                 |
| `rawwise.summarization.provider.type` | `noop` | 摘要提供方（noop / http）                 |
| `rawwise.entity.extraction.enabled` | `false` | 实体抽取并写入知识图谱                        |

> 上表是应用默认值。`bin/rawwise-start.sh` 会覆盖其中若干项（`context.enabled=true`、`frontend.ir.enabled=true`），使记忆、图谱与 FastPath 开箱可用。

## 13. 技能

RawWise 在 `rawwise-skills/` 提供 Agent 可用的技能定义（知识流水线、记忆管理、状态机、反馈闭环、SQL 操作）。详见 [rawwise-skills/](rawwise-skills/)。

***

## 贡献

欢迎外部贡献，但必须先签署 CLA：被合入的贡献及其全部相关权益归 Owner（即 `@rawwise` GitHub 账号的运营者）所有。详见 [CONTRIBUTING.md](CONTRIBUTING.md) 与 [CLA.md](CLA.md)。

## 安全

安全漏洞请私下报告，详见 [SECURITY.md](SECURITY.md)；请勿公开提交 issue。

## 许可证

Copyright (c) 2026 RawWise. 保留所有权利。

本项目为**专有 / source-available** 项目，不是开源项目。未授予任何许可：未经版权持有人事先书面许可，不得使用、复制、修改、分发或以商业目的利用本仓库的任何部分。完整条款见 [COPYRIGHT](COPYRIGHT)。

授权或商业合作请联系：GitHub [@rawwise](https://github.com/rawwise) · <352270501+rawwise@users.noreply.github.com>
