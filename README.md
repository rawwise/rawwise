# RawWise

[English](README.md) | [中文](README.zh-CN.md)

**Cognitive Data Engine** - An AI-native data engine that unifies relational data, vector computation, and AI cognition into one traceable semantic record model.

A *cognitive* data engine is a storage engine that derives meaning at write time, keeps every derived artifact traceable to its source, and lets feedback flow back into re-derivation. RawWise does all three inside one engine that also serves OLTP point queries, OLAP analytics, and hybrid semantic search - so agents, applications, and analysts all read the same record model through SQL, HTTP, or MCP instead of a stitched-together stack.

***

## 1. What Makes It "Cognitive"

A database with an embedding column is not a cognitive data engine. RawWise is designed against three properties; remove any one of them and what remains is a database, a vector store, or a RAG script.

| Property                     | What it requires                                                                                      | How RawWise implements it                                                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **Derivation is native**     | Meaning is produced inside the storage engine on write, not by a pipeline assembled outside it        | Every write triggers chunking → summarization → vectorization → indexing → entity extraction → knowledge graph construction |
| **Cognition is traceable**   | Every derived artifact can be traced to the exact source bytes and the model version that produced it | Provenance chain Raw → Chunk → View → Semantic → Action, queryable per search result                                        |
| **Feedback closes the loop** | Corrections change future behavior, so cognitive assets accumulate over time                          | Explicit annotations and implicit signals trigger cascade re-derivation of the affected Views                               |

The third property is the hardest to copy. It is what turns a data engine from a passive store into a system that improves with use.

## 2. Why Not Just Combine Existing Tools?

| Alternative                      | What it gives you                                                            | What it leaves out                                                              |
| -------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Relational DB + vector extension | SQL and ANN in one process                                                   | No derivation pipeline, no lineage, no feedback loop                            |
| Vector database                  | Fast ANN over embeddings                                                     | Embeddings are dead ends: no link to raw content, no summaries, no transactions |
| Lakehouse / lakebase-style OLTP  | Open files + SQL, transactional rows on lake storage                         | No write-time cognition, no agent memory, no state                              |
| RAG framework                    | Retrieval glue for prompts                                                   | No storage engine, no consistency, nothing accumulates as knowledge             |
| **RawWise**                      | Derivation + lineage + feedback over relational, vector, and analytical data | -                                                                               |

## 3. Who Uses It

The engine is not defined by its consumer: the same record, the same transaction, and the same lineage serve every client. Only the interface differs.

| Consumer | What it gets | Interface |
|---|---|---|
| **Agent runtimes** (LangChain, AutoGen, coding agents, workflow agents) | Knowledge base, four-tier memory, guarded state, graph traversal | MCP |
| **AI applications** | Ingest-and-derive, hybrid semantic search, provenance, feedback correction | HTTP |
| **Data applications** | Standard SQL DDL/DML, FastPath point queries, Spark SQL analytics | HTTP SQL |
| **Data and AI engineering** | Derivation pipeline, embeddings, implicit-feedback signals for reranker training | HTTP / config |
| **Audit and governance** | Per-result lineage, bound to the model version that produced it | Provenance API |

## 4. The Semantic Record Model

Every ingestion becomes a **SemanticRecord**: one traceable object with five layers, each derived from the one before it.

```
Raw ───────► Chunk ───────► View ───────► Semantic ───────► Action
immutable     addressable     per-model      structured       affordance
bytes         segments        representation abstraction      for agents
```

| Layer        | What it is                                                                                                                                          |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Raw**      | Immutable bytes captured at the system boundary. Written once, never modified.                                                                      |
| **Chunk**    | Addressable semantic segments: text spans, image regions, table rows, code blocks. Each chunk points back to its raw source.                        |
| **View**     | The result of one model (and model version) over a set of chunks. Multiple Views coexist - a model upgrade adds Views, it never overwrites history. |
| **Semantic** | Structured abstraction over Views: tags, entities, classifications, relations. Bound to the View version that produced it.                          |
| **Action**   | What an agent can do given these Views: operation names, parameter constraints, expected effects.                                                   |

The model is modality-agnostic: the chain does not change between text, image, and audio - only the chunker and the embedding model do. Text is what ships today; image regions and audio segments join the same record model with the same lineage and the same feedback loop.

## 5. Quick Start

### Requirements

- **JDK 21** (JDK, not JRE)
- **Maven 3.9+** on `PATH` - the repository ships no Maven wrapper
- The default embedded profile reserves a 6 GB JVM heap (`RAWWISE_EMBEDDED_HEAP`)

### Build and run

```bash
# 1. Install JDK 21 and Maven 3.9+, then verify
java -version   # must report 21
mvn -v

# 2. Build (first time only)
mvn package -pl rawwise-core -DskipTests -q

# 3. Start RawWise (single-process embedded mode: Frontend + Backend + Metastore in one JVM)
#    Use this script rather than a bare `java -cp` launch: it enables context
#    (memory/graph APIs), the IR/FastPath pipeline, and embedded Metastore wiring.
bash bin/rawwise-start.sh

# 4. Create a cognitive table
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"CREATE TABLE knowledge (id STRING, content STRING) USING rawwise TBLPROPERTIES ('\''primary-key'\''='\''id'\'', '\''ai-native'\''='\''true'\'')"}'

# 5. Ingest knowledge (auto-triggers the derivation pipeline)
curl -s -X POST http://localhost:8080/api/axiom/ingest \
  -H "Content-Type: application/json" \
  -d '{"tableId":"default.knowledge","key":"doc:1","body":"RawWise is a cognitive data engine with built-in vector search and knowledge derivation."}'

# 6. Semantic search (with noop providers: keyword-ranked, empty summary)
curl -s -X POST http://localhost:8080/api/axiom/search \
  -H "Content-Type: application/json" \
  -d '{"tableId":"default.knowledge","query":"what is rawwise","topK":5}'

# 7. Trace provenance
curl -s http://localhost:8080/api/axiom/provenance/view:<viewId>
```

### What works without external models

A fresh checkout starts with `noop` providers for embedding, summarization, entity extraction, and fact extraction. Storage, SQL, OLTP/OLAP, retrieval plumbing, memory, state machine, and provenance all work; AI quality degrades in predictable ways.

| Capability | Default (`noop`) | How to enable |
|---|---|---|
| `search` ranking | Effectively keyword-ranked (+ graph if populated); vectors are all zeros, so there is no semantic recall | Set `RAWWISE_EMBEDDING_TYPE=http` with endpoint / model / api-key in `.env` |
| `summary` in search results | Empty string | Set `RAWWISE_SUMMARIZATION_TYPE=http` with endpoint / model / api-key |
| Knowledge graph | Empty: entity and fact extraction are off by default | `RAWWISE_ENTITY_EXTRACTION_ENABLED=true`, `RAWWISE_FACT_EXTRACTION_ENABLED=true` |
| Memory / state / graph APIs | Enabled by `bin/rawwise-start.sh` (`-Drawwise.context.enabled=true`) | - |
| Multi-query expansion | On by default | - |
| Reranker and query rewrite | Off by default | See Configuration and `.env.example` |

Copy `.env.example` to `.env`, fill in real providers, and the start script picks it up automatically.

## 6. What It Replaces

| Problem                                               | Traditional approach                                                   | RawWise                                                                                                 |
| ----------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Transactions and analytics need two systems           | MySQL + ClickHouse, ETL sync                                           | One engine: FastPath point queries + Spark SQL analytics over shared storage                            |
| Structured queries and semantic search are fragmented | Relational DB + Milvus, dual writes                                    | Same table: SQL exact query + vector semantic search from a single write                                |
| Vector indexes must be built after writing            | App layer calls embedding API and writes to a vector DB                | Derivation on write, zero application code                                                              |
| Range updates require a full scan and rewrite         | Scan matching rows, modify, rewrite Parquet files (seconds to minutes) | Lazy Update: append predicate + new value to an NWL log (milliseconds), merge on read, async compaction |
| Search results cannot be traced                       | Black box, unknown origin                                              | Full lineage: Raw → Chunk → View → Semantic → Action                                                    |
| Feedback cannot improve retrieval quality             | Retrain the whole model                                                | Annotate one result, cascade re-derive, effective immediately                                           |
| Agents have no persistent memory                      | Start from scratch every conversation                                  | Four-tier memory (working/episodic/semantic/procedural), cross-session                                  |
| Multiple agents cannot share knowledge                | Each keeps an isolated knowledge base                                  | Memory sharing with permissions and TTL                                                                 |

## 7. Architecture

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
                    │  │ WAL +    │││ Cognition    │  │
                    │  │ LMDB     │││ Derivation   │  │
                    │  │ (hot)    │││ Raw->Chunk-> │  │
                    │  │          │││ View->Embed->│  │
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

Every request is routed to the cheapest execution path that can satisfy it:

| Workload              | Execution path                | Behavior                                                               |
| --------------------- | ----------------------------- | ---------------------------------------------------------------------- |
| Point SELECT / INSERT | FastPath → LMDB               | PK equality, bypasses Spark SQL                                        |
| Range UPDATE          | NWL append + async compaction | Predicate and new value logged in milliseconds; reads merge on the fly |
| Analytical SELECT     | Spark SQL Catalyst            | Full scan, filter, aggregate, join                                     |
| Hybrid search         | Vector + keyword + graph RRF  | Cross-table semantic recall                                            |

## 8. Interfaces

### 8.1 MCP (recommended for agents)

RawWise exposes **34 MCP tools** at `http://localhost:8080/mcp`.

MCP client config:

```json
{
  "rawwise": {
    "url": "http://localhost:8080/mcp",
    "transport": "http"
  }
}
```

All tools are enabled by default except `memory.decay` and `memory.reflect`, which require `rawwise.mcp.tools.memory.decay.enabled=true` / `rawwise.mcp.tools.memory.reflect.enabled=true`.

#### Knowledge pipeline (16 tools)

| Tool                        | Required                        | Optional                    | What it does                                                       |
| --------------------------- | ------------------------------- | --------------------------- | ------------------------------------------------------------------ |
| `rawwise_ingest`            | `tableId`, `key`, `body`        | `immutable`, `properties`   | Write data, auto-triggers Raw → Chunk → View → Embedding → JVector |
| `rawwise_search`            | `tableId`, `query`              | `topK`                      | Hybrid search (vector + keyword + graph RRF)                       |
| `rawwise_cognize`           | `tableId`, `query`              | `topK`                      | Five-layer cognition recall                                        |
| `rawwise_annotate`          | `viewId`, `type`, `annotatorId` | `value`, `confidence`       | POSITIVE / NEGATIVE / CORRECTION feedback                          |
| `rawwise_provenance`        | `viewId`                        | -                           | Full derivation lineage                                            |
| `rawwise_rederive`          | `tableId`, `scope`              | `singleKey`, `forceReindex` | Cascade re-derivation                                              |
| `rawwise_query_sql`         | `sql`                           | `timeoutSeconds`            | Execute SQL (DDL immediate, DML polls)                             |
| `rawwise_list_tables`       | -                               | -                           | List all tables                                                    |
| `rawwise_implicit_feedback` | `viewId`, `signalType`          | `dwellMs`                   | DWELL / CLICK / SKIP for reranker training                         |
| `rawwise_get_view`          | `tableId`, `viewId`             | -                           | Get View layer content                                             |
| `rawwise_get_chunk`         | `tableId`, `chunkId`            | -                           | Get Chunk layer content                                            |
| `rawwise_get_semantic`      | `tableId`, `semanticId`         | -                           | Get Semantic layer content                                         |
| `rawwise_get_action`        | `tableId`, `actionId`           | -                           | Get Action layer content                                           |
| `rawwise_get_action_plan`   | `tableId`, `planId`             | -                           | Get ActionPlan content                                             |
| `rawwise_fetch_details`     | `tableId`, `entryKeys`          | -                           | Batch fetch raw\_text + view\_summary                              |
| `rawwise_diagnostics`       | -                               | -                           | Pipeline diagnostics                                               |

#### Memory (7 tools)

| Tool                         | Required                                                   | Optional                | What it does                                              |
| ---------------------------- | ---------------------------------------------------------- | ----------------------- | --------------------------------------------------------- |
| `rawwise_memory_store`       | `content`, `memoryTier`                                    | `importance`, `agentId` | Store memory (WORKING / EPISODIC / SEMANTIC / PROCEDURAL) |
| `rawwise_memory_recall`      | `query`, `memoryTier`, `loadTier`                          | `topK`, `agentId`       | Recall memory (L0/L1/L2 load tier)                        |
| `rawwise_memory_consolidate` | `fromTier`, `toTier`                                       | `agentId`               | Consolidate memories between tiers                        |
| `rawwise_memory_share`       | `toAgentId`, `entryId`, `ttlMs`, `revocable`, `permission` | `agentId`               | Share memory with another agent                           |
| `rawwise_memory_status`      | -                                                          | `agentId`               | Query memory status                                       |
| `rawwise_memory_decay`       | `ageMs`                                                    | `agentId`               | Decay old memories (disabled by default)                  |
| `rawwise_memory_reflect`     | `query`                                                    | `agentId`               | Reflect on memories (disabled by default)                 |

#### Graph (5 tools)

| Tool                          | Required                            | Optional                          | What it does                    |
| ----------------------------- | ----------------------------------- | --------------------------------- | ------------------------------- |
| `rawwise_graph_query_nodes`   | `graphId`                           | `type`, `propertyFilter`, `limit` | Query nodes                     |
| `rawwise_graph_find_paths`    | `graphId`, `srcNodeId`, `dstNodeId` | `maxHops`                         | Find paths between nodes        |
| `rawwise_graph_vector_search` | `graphId`, `query`                  | `topK`                            | Graph vector search             |
| `rawwise_graph_get_node`      | `graphId`, `nodeId`                 | -                                 | Get node details                |
| `rawwise_graph_neighbors`     | `graphId`, `nodeId`, `direction`    | `label`, `maxHops`, `limit`       | Get neighbors (OUT / IN / BOTH) |

#### State machine (6 tools)

| Tool                         | Required                                    | Optional                  | What it does                 |
| ---------------------------- | ------------------------------------------- | ------------------------- | ---------------------------- |
| `rawwise_state_create_space` | `agentId`, `name`, `initialState`, `states` | `transitions`, `metadata` | Create a state space         |
| `rawwise_state_list_spaces`  | `agentId`                                   | -                         | List an agent's state spaces |
| `rawwise_state_read`         | `spaceId`, `key`                            | -                         | Read a state entry           |
| `rawwise_state_write`        | `spaceId`, `key`, `value`, `stateName`      | `metadata`                | Write state (direct set)     |
| `rawwise_state_transition`   | `spaceId`, `key`, `toState`                 | `metadata`                | Transition (validates guard) |
| `rawwise_state_scan`         | `spaceId`                                   | `prefix`, `limit`         | Scan by prefix               |

### 8.2 HTTP API

Base URL: `http://localhost:8080`

| Endpoint                         | Method | Purpose                           |
| -------------------------------- | ------ | --------------------------------- |
| `/api/axiom/ingest`              | POST   | Write knowledge (auto-derivation) |
| `/api/axiom/search`              | POST   | Semantic search                   |
| `/api/axiom/cognize`             | POST   | Five-layer cognition              |
| `/api/axiom/annotate`            | POST   | Feedback (cascade re-derive)      |
| `/api/axiom/provenance/{viewId}` | GET    | Trace the derivation chain        |
| `/api/axiom/fetch-details`       | POST   | Batch fetch raw\_text + summary   |
| `/api/v1/axiom/memory/store`     | POST   | Store memory                      |
| `/api/v1/axiom/memory/recall`    | POST   | Recall memory                     |
| `/api/v1/axiom/state/spaces`     | POST   | Create a state space              |
| `/api/query`                     | POST   | Execute SQL                       |

### 8.3 Choosing a tool

| Your goal                               | Use this tool                                                                                                        | Why                                                           |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| Store new knowledge or a document       | `rawwise_ingest`                                                                                                     | Auto-derives Raw → Chunk → View → Semantic → Action on write  |
| Answer a question from stored knowledge | `rawwise_search`                                                                                                     | Hybrid (vector + keyword + graph) semantic recall             |
| Get multi-layer detail behind an answer | `rawwise_cognize`                                                                                                    | Returns L0 raw + L1 chunk + L2 view + L3 semantic + L4 action |
| Read or write structured rows           | `rawwise_query_sql`                                                                                                  | Standard SQL over the unified store                           |
| Find what tables exist                  | `rawwise_list_tables`                                                                                                | Returns tables and schema                                     |
| Explain why a result appeared           | `rawwise_provenance`                                                                                                 | Full derivation lineage of a View                             |
| Say a result was wrong or right         | `rawwise_annotate`                                                                                                   | Triggers cascade re-derivation                                |
| Track dwell / click / skip              | `rawwise_implicit_feedback`                                                                                          | Feeds reranker training                                       |
| Remember facts across sessions          | `rawwise_memory_store` / `rawwise_memory_recall`                                                                     | Four-tier persistent memory                                   |
| Promote short-term memory to long-term  | `rawwise_memory_consolidate`                                                                                         | e.g. WORKING → SEMANTIC after a task                          |
| Share knowledge with another agent      | `rawwise_memory_share`                                                                                               | Cross-agent memory with permission and TTL                    |
| Model a multi-step workflow             | `rawwise_state_create_space` + `rawwise_state_transition`                                                            | Guarded state machine                                         |
| Explore entity relationships            | `rawwise_graph_query_nodes` / `rawwise_graph_neighbors` / `rawwise_graph_find_paths`                                 | Knowledge graph traversal                                     |
| Read one specific layer                 | `rawwise_get_view` / `rawwise_get_chunk` / `rawwise_get_semantic` / `rawwise_get_action` / `rawwise_get_action_plan` | Layer-precise access                                          |

**Choosing a layer**: after `rawwise_search`, use the returned `summary` if that is enough. For full original text, call `rawwise_fetch_details` with the `entryKey`. For the derivation chain, call `rawwise_provenance` with the `viewId`.

**Read path vs write path**: writes (`rawwise_ingest`, `rawwise_query_sql` INSERT, `rawwise_memory_store`, `rawwise_state_write`) return immediately or return a jobId to poll. Reads (`rawwise_search`, `rawwise_query_sql` SELECT, `rawwise_memory_recall`, `rawwise_state_read`) return synchronously.

### 8.4 Memory pattern

```bash
# Store a user preference
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{"content":"User prefers concise technical answers","memoryTier":"EPISODIC","importance":0.8}'

# Recall before responding
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/recall \
  -H "Content-Type: application/json" \
  -d '{"query":"user answer style preference","memoryTier":"EPISODIC","loadTier":"L1","topK":3}'

# Store working memory during a task
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{"content":"Currently debugging auth module","memoryTier":"WORKING"}'

# Consolidate working memory into semantic memory after the task
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/consolidate \
  -H "Content-Type: application/json" \
  -d '{"fromTier":"WORKING","toTier":"SEMANTIC"}'
```

### 8.5 Feedback pattern

```bash
# The user indicates a result was wrong
curl -s -X POST http://localhost:8080/api/axiom/annotate \
  -H "Content-Type: application/json" \
  -d '{"viewId":"view:abc123","type":"NEGATIVE","value":"-5","annotatorId":"agent"}'
```

RawWise then closes the loop on its own:

1. Adjusts the rank bias for this entry.
2. Re-derives the affected View with the new parameters.
3. Returns better results on the next search.

## 9. Key Concepts

| Concept                   | Description                                                                                                                    |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **Cognitive Data Engine** | A storage engine that derives meaning on write, traces every derived artifact, and closes the feedback loop inside the engine. |
| **SemanticRecord**        | The atomic unit: one raw source plus its derived Chunk / View / Semantic / Action layers.                                      |
| **Cognitive Table**       | A table declared with `TBLPROPERTIES ('ai-native'='true')`. Writes to it auto-trigger derivation.                              |
| **View**                  | The representation layer returned by search. Multiple model versions coexist.                                                  |
| **Provenance**            | The full chain from Raw to Action. Every search result is traceable.                                                           |
| **Memory Tier**           | WORKING (current task), EPISODIC (events), SEMANTIC (facts), PROCEDURAL (skills).                                              |
| **Load Tier**             | L0 (raw), L1 (summary), L2 (metadata only). Controls recall depth.                                                             |
| **Cascade Re-derive**     | Feedback triggers re-derivation of affected Views, improving future results.                                                   |

## 10. Performance

YCSB (Yahoo! Cloud Serving Benchmark) end-to-end latency measured over the HTTP API (p99, microseconds). All workloads pass `correctness=true`.

**Single-node embedded mode** (FastPath + LMDB):

| Workload        | Mix           | median (us) | p99 (us) |
| --------------- | ------------- | ----------- | -------- |
| C (read-only)   | 100%R         | 105         | **200**  |
| D (read latest) | 95%R / 5%Scan | 98          | 1,050    |
| B (read-mostly) | 95%R / 5%W    | 128         | 5,275    |
| A (read-heavy)  | 50%R / 50%W   | 211         | **934**  |

**Distributed 8-VM cluster** (5 BE nodes + Frontend + Metastore, 4 threads, LAN):

| Workload        | Mix         | median (us) | p99 (us) |
| --------------- | ----------- | ----------- | -------- |
| C (read-only)   | 100%R       | 6,582       | 9,301    |
| B (read-mostly) | 95%R / 5%W  | 6,919       | 18,504   |
| A (read-heavy)  | 50%R / 50%W | 12,770      | 22,495   |

> **Note**: the distributed numbers include network RTT (two-hop gRPC: client → frontend → BE node) and run on VMs (\~50-70% of physical machine performance). Single-node numbers run locally on Apple Silicon with no network hop. They are not apples-to-apples: distributed deployment buys horizontal scaling for high concurrency and large data.

## 11. Operations

`rawwise-start.sh` runs in **single-process embedded mode** (Frontend + Backend + Metastore in one JVM). Data persists at `~/.rawwise/data/`. It is the supported entry point: besides starting the process, it sets the flags required by memory, graph, and FastPath.

| Command                       | Description                              |
| ----------------------------- | ---------------------------------------- |
| `bash bin/rawwise-start.sh`   | Start (default port 8080, embedded mode) |
| `bash bin/rawwise-stop.sh`    | Graceful stop                            |
| `bash bin/rawwise-restart.sh` | Stop + clean locks + start               |
| `bash bin/rawwise-status.sh`  | Check status                             |
| `bash bin/rawwise-clean.sh`   | Delete all data                          |

## 12. Configuration

All configuration is set via system properties (`-Drawwise.*`). Key ones:

| Key                                | Default  | Description                                          |
| ---------------------------------- | -------- | ---------------------------------------------------- |
| `rawwise.axiom.enabled`            | `true`   | Enable cognitive features                            |
| `rawwise.mcp.enabled`              | `true`   | Enable the MCP endpoint at `/mcp`                    |
| `rawwise.frontend.ir.enabled`      | `false`  | Enable the IR pipeline (FastPath)                    |
| `rawwise.context.enabled`          | `false`  | Enable context / sessions (required for memory APIs) |
| `rawwise.embedding.provider.type`  | `noop`   | Embedding provider (noop / http / stub / onnx)       |
| `rawwise.axiom.search.path`        | `hybrid` | Search path (hybrid / vector / keyword)              |
| `rawwise.axiom.multiquery.enabled` | `true`   | Multi-query expansion (3 variants)                   |
| `rawwise.axiom.rrf.k`              | `60`     | RRF fusion constant                                  |
| `rawwise.summarization.provider.type` | `noop` | Summarization provider (noop / http)              |
| `rawwise.entity.extraction.enabled` | `false` | Entity extraction into the knowledge graph          |

> Defaults above are the application defaults. `bin/rawwise-start.sh` overrides several of them (`context.enabled=true`, `frontend.ir.enabled=true`) so that memory, graph, and FastPath work out of the box.

## 13. Skills

RawWise ships agent-usable skill definitions in `rawwise-skills/` (knowledge pipeline, memory management, state machine, feedback loop, SQL operations). See [rawwise-skills/](rawwise-skills/).

***

## Contributing

External contributions are welcome, but a signed CLA is required: accepted contributions and all related rights become the property of the Owner (the individual operating the `@rawwise` GitHub account). See [CONTRIBUTING.md](CONTRIBUTING.md) and [CLA.md](CLA.md).

## Security

Please report vulnerabilities privately — see [SECURITY.md](SECURITY.md). Do not open public issues for security reports.

## License

Copyright (c) 2026 RawWise. All rights reserved.

This project is **proprietary / source-available**, not open source. No license is granted: you may not use, copy, modify, distribute, or commercially exploit any part of this repository without prior written permission from the copyright holder. See [COPYRIGHT](COPYRIGHT) for the full terms.

For licensing or commercial inquiries: GitHub [@rawwise](https://github.com/rawwise) · <352270501+rawwise@users.noreply.github.com>
