---
name: rawwise-memory-management
description: Store, recall, consolidate, and share agent memories across sessions using RawWise's four-tier memory system (WORKING/EPISODIC/SEMANTIC/PROCEDURAL). Use when you need persistent memory across conversations, recall past interactions, or share knowledge between agents.
---

# Memory Management Skill

## When to Use

- Remembering user preferences across sessions
- Storing task context during multi-step work
- Consolidating working memories into long-term semantic knowledge
- Sharing memories with other agents
- Recalling relevant past experiences before responding

## Memory Tiers

| Tier | Purpose | Example |
|------|---------|---------|
| `WORKING` | Current task context | "Debugging auth module, line 42 has NPE" |
| `EPISODIC` | Events and interactions | "User asked about Python performance on July 6" |
| `SEMANTIC` | Facts and knowledge | "User's codebase uses Java 21 and Spark 4.0" |
| `PROCEDURAL` | Skills and procedures | "To deploy: package.sh -> scp -> rawwise-start.sh" |

## Load Tiers (Recall Depth)

| Load Tier | Returns | Use When |
|-----------|---------|----------|
| `L0` | Raw full content | Need complete text |
| `L1` | Summary/View | Most common - balanced detail |
| `L2` | Metadata only | Quick scan, filtering |

## Prerequisites

RawWise running at `http://localhost:8080` with `-Drawwise.context.enabled=true` (included in `bin/dev-startup.sh`).

## Actions

### Store Memory

**MCP Tool**: `rawwise_memory_store`
**HTTP**: `POST /api/v1/axiom/memory/store`

```bash
# Store user preference
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{
    "content": "User prefers concise technical answers without emojis",
    "memoryTier": "EPISODIC",
    "importance": 0.8
  }'

# Store working memory
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{
    "content": "Currently refactoring AuthFilter.java, added production safety check",
    "memoryTier": "WORKING"
  }'

# Store procedural knowledge
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{
    "content": "To restart RawWise: bash bin/rawwise-restart.sh --profile=dogfood",
    "memoryTier": "PROCEDURAL"
  }'
```

### Recall Memory

**MCP Tool**: `rawwise_memory_recall`
**HTTP**: `POST /api/v1/axiom/memory/recall`

```bash
# Recall user preferences
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/recall \
  -H "Content-Type: application/json" \
  -d '{
    "query": "user answer style preference",
    "memoryTier": "EPISODIC",
    "loadTier": "L1",
    "topK": 3
  }'

# Recall relevant procedures
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/recall \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how to restart server",
    "memoryTier": "PROCEDURAL",
    "loadTier": "L0",
    "topK": 1
  }'
```

### Consolidate Memories

Move memories from one tier to another (e.g., working -> semantic after task completion).

**MCP Tool**: `rawwise_memory_consolidate`
**HTTP**: `POST /api/v1/axiom/memory/consolidate`

```bash
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/consolidate \
  -H "Content-Type: application/json" \
  -d '{"fromTier": "WORKING", "toTier": "SEMANTIC"}'
```

### Share Memory with Other Agents

**MCP Tool**: `rawwise_memory_share`
**HTTP**: `POST /api/v1/axiom/memory/share`

```bash
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/share \
  -H "Content-Type: application/json" \
  -d '{
    "toAgentId": "agent-b",
    "entryId": "mem:abc123",
    "ttlMs": 3600000,
    "revocable": true,
    "permission": "READ"
  }'
```

### Check Memory Status

**MCP Tool**: `rawwise_memory_status`
**HTTP**: `GET /api/v1/axiom/memory/status`

```bash
curl -s http://localhost:8080/api/v1/axiom/memory/status
```

## Recommended Patterns

### Pattern: Session Start
At the beginning of a conversation, recall EPISODIC and SEMANTIC memories:
```bash
# Recall who the user is and their preferences
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/recall \
  -H "Content-Type: application/json" \
  -d '{"query": "user profile preferences", "memoryTier": "SEMANTIC", "loadTier": "L1", "topK": 5}'

# Recall recent episodes
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/recall \
  -H "Content-Type: application/json" \
  -d '{"query": "recent conversations", "memoryTier": "EPISODIC", "loadTier": "L1", "topK": 3}'
```

### Pattern: During Task
Store WORKING memories as you go:
```bash
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{"content": "Found bug in LmdbInstance.java:1049, closeUnsafe() not releasing lock", "memoryTier": "WORKING"}'
```

### Pattern: Task Complete
Consolidate WORKING -> SEMANTIC and store EPISODIC:
```bash
# Consolidate
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/consolidate \
  -H "Content-Type: application/json" \
  -d '{"fromTier": "WORKING", "toTier": "SEMANTIC"}'

# Store episode
curl -s -X POST http://localhost:8080/api/v1/axiom/memory/store \
  -H "Content-Type: application/json" \
  -d '{"content": "Fixed LMDB lock release bug in closeUnsafe(), added awaitTermination", "memoryTier": "EPISODIC", "importance": 0.7}'
```
