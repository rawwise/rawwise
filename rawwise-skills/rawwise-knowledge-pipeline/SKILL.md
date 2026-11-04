---
name: rawwise-knowledge-pipeline
description: Ingest knowledge into the RawWise cognitive data engine, search semantically, cognize across five layers, and trace full provenance. Use when you need to store text for later semantic retrieval, search existing knowledge, or understand derivation lineage.
---

# Knowledge Pipeline Skill

## When to Use

- Storing documents, conversations, or any text for later semantic search
- Searching knowledge base with natural language queries
- Understanding how a search result was derived (provenance)
- Running five-layer cognition (Raw/Chunk/View/Semantic/Action)
- Batch fetching details for multiple entries

## Prerequisites

RawWise running at `http://localhost:8080`. An AI-Native table must exist:

```bash
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"CREATE TABLE kb (id STRING, content STRING) USING rawwise TBLPROPERTIES ('\''primary-key'\''='\''id'\'', '\''ai-native'\''='\''true'\'')"}'
```

## Actions

### Ingest Knowledge

Write data to RawWise. Auto-triggers derivation: Raw -> Chunk -> View -> Embedding -> JVector.

**MCP Tool**: `rawwise_ingest`
**HTTP**: `POST /api/axiom/ingest`

```bash
curl -s -X POST http://localhost:8080/api/axiom/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "tableId": "default.kb",
    "key": "doc:001",
    "body": "RawWise is a cognitive data engine with built-in vector search.",
    "properties": {"source": "docs", "author": "team"}
  }'
```

Response: `{"recordKey": "doc:001", "chunkCount": 3, "status": "OK"}`

### Semantic Search

Hybrid search: vector + keyword + graph RRF fusion. Returns summaries, not raw text.

**MCP Tool**: `rawwise_search`
**HTTP**: `POST /api/axiom/search`

```bash
curl -s -X POST http://localhost:8080/api/axiom/search \
  -H "Content-Type: application/json" \
  -d '{"tableId": "default.kb", "query": "what is rawwise", "topK": 5}'
```

Response:
```json
{
  "results": [
    {"entryKey": "doc:001", "viewId": "view:abc", "score": 0.89, "summary": "RawWise is a cognitive data engine..."}
  ]
}
```

### Five-Layer Cognition

Multi-channel recall across all five layers with RRF fusion.

**MCP Tool**: `rawwise_cognize`
**HTTP**: `POST /api/axiom/cognize`

```bash
curl -s -X POST http://localhost:8080/api/axiom/cognize \
  -H "Content-Type: application/json" \
  -d '{
    "tableId": "default.kb",
    "query": "agent memory architecture",
    "topK": 5
  }'
```

### Trace Provenance

Get full derivation chain for a search result.

**MCP Tool**: `rawwise_provenance`
**HTTP**: `GET /api/axiom/provenance/{viewId}`

```bash
curl -s http://localhost:8080/api/axiom/provenance/view:abc
```

### Batch Fetch Details

Two-stage loading: get raw_text + view_summary for multiple entries.

**MCP Tool**: `rawwise_fetch_details`
**HTTP**: `POST /api/axiom/fetch-details`

```bash
curl -s -X POST http://localhost:8080/api/axiom/fetch-details \
  -H "Content-Type: application/json" \
  -d '{"tableId": "default.kb", "entryKeys": ["doc:001", "doc:002", "doc:003"]}'
```

### Get Layer Content

Access individual layers of the five-layer stack. Available via MCP tools only.

| Layer | MCP Tool |
|-------|----------|
| View (summary) | `rawwise_get_view` |
| Chunk | `rawwise_get_chunk` |
| Semantic | `rawwise_get_semantic` |
| Action | `rawwise_get_action` |
| ActionPlan | `rawwise_get_action_plan` |

## Tips

- Use `fetch_details` instead of individual `get_view` calls when you need multiple entries
- Search returns `viewId` - use it for provenance, annotation, and layer access
- Ingest is async - derivation happens in background, search may not find it immediately
- Use `diagnostics` MCP tool to check pipeline status if search returns empty
