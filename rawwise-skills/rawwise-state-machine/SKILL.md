---
name: rawwise-state-machine
description: Create state spaces with states, transitions, and guard expressions for agent workflow management. Use when you need to track task progress with state transitions, enforce valid transitions, or manage per-key state across agent invocations.
---

# State Machine Skill

## When to Use

- Tracking multi-step task progress (e.g., draft -> review -> published)
- Enforcing workflow transitions (can't go from "draft" to "published" directly)
- Managing per-entity state across agent sessions
- Coordinating state between multiple agents

## Prerequisites

RawWise running at `http://localhost:8080` with `-Drawwise.context.enabled=true` (included in `bin/dev-startup.sh`). Agent identity is derived from authentication (anonymous in dev mode).

## Actions

### Create State Space

**MCP Tool**: `rawwise_state_create_space`
**HTTP**: `POST /api/v1/axiom/state/spaces`

```bash
curl -s -X POST http://localhost:8080/api/v1/axiom/state/spaces \
  -H "Content-Type: application/json" \
  -d '{
    "name": "document-workflow",
    "initialState": "draft",
    "states": ["draft", "review", "approved", "published", "rejected"],
    "transitions": [
      {"fromState": "draft", "toState": "review"},
      {"fromState": "review", "toState": "approved"},
      {"fromState": "review", "toState": "rejected"},
      {"fromState": "approved", "toState": "published"},
      {"fromState": "rejected", "toState": "draft"}
    ]
  }'
```

### Write State (Direct Set)

Bypasses transition validation. Use for initialization or admin operations.

**MCP Tool**: `rawwise_state_write`
**HTTP**: `PUT /api/v1/axiom/state/spaces/{spaceId}/entries/{key}`

```bash
curl -s -X PUT http://localhost:8080/api/v1/axiom/state/spaces/space123/entries/doc:001 \
  -H "Content-Type: application/json" \
  -d '{"value": "Document content here", "stateName": "draft"}'
```

### Transition State (Validated)

Validates against transition rules. Rejects invalid transitions.

**MCP Tool**: `rawwise_state_transition`
**HTTP**: `POST /api/v1/axiom/state/spaces/{spaceId}/transition`

```bash
curl -s -X POST http://localhost:8080/api/v1/axiom/state/spaces/space123/transition \
  -H "Content-Type: application/json" \
  -d '{"key": "doc:001", "toState": "review"}'
```

If transition is invalid (e.g., draft -> published), returns error.

### Read State

**MCP Tool**: `rawwise_state_read`
**HTTP**: `GET /api/v1/axiom/state/spaces/{spaceId}/entries/{key}`

```bash
curl -s http://localhost:8080/api/v1/axiom/state/spaces/space123/entries/doc:001
```

Response: `{"key": "doc:001", "value": "...", "stateName": "review", "version": 2}`

### Scan State Entries

**MCP Tool**: `rawwise_state_scan`
**HTTP**: `GET /api/v1/axiom/state/spaces/{spaceId}/entries?prefix=doc:&limit=10`

```bash
curl -s "http://localhost:8080/api/v1/axiom/state/spaces/space123/entries?prefix=doc:&limit=10"
```

### List State Spaces

**MCP Tool**: `rawwise_state_list_spaces`
**HTTP**: `GET /api/v1/axiom/state/spaces?agentId=agent-a`

```bash
curl -s http://localhost:8080/api/v1/axiom/state/spaces
```

## Example: Document Review Workflow

```bash
# 1. Create state space
SPACE=$(curl -s -X POST http://localhost:8080/api/v1/axiom/state/spaces \
  -H "Content-Type: application/json" \
  -d '{
    "name": "doc-review",
    "initialState": "draft",
    "states": ["draft", "in_review", "approved", "rejected", "published"],
    "transitions": [
      {"fromState": "draft", "toState": "in_review"},
      {"fromState": "in_review", "toState": "approved"},
      {"fromState": "in_review", "toState": "rejected"},
      {"fromState": "approved", "toState": "published"},
      {"fromState": "rejected", "toState": "draft"}
    ]
  }' | jq -r '.spaceId')

# 2. Create document in draft state
curl -s -X PUT "http://localhost:8080/api/v1/axiom/state/spaces/$SPACE/entries/doc:001" \
  -H "Content-Type: application/json" \
  -d '{"value": "Document body", "stateName": "draft"}'

# 3. Submit for review
curl -s -X POST "http://localhost:8080/api/v1/axiom/state/spaces/$SPACE/transition" \
  -H "Content-Type: application/json" \
  -d '{"key": "doc:001", "toState": "in_review"}'

# 4. Approve
curl -s -X POST "http://localhost:8080/api/v1/axiom/state/spaces/$SPACE/transition" \
  -H "Content-Type: application/json" \
  -d '{"key": "doc:001", "toState": "approved"}'

# 5. Publish
curl -s -X POST "http://localhost:8080/api/v1/axiom/state/spaces/$SPACE/transition" \
  -H "Content-Type: application/json" \
  -d '{"key": "doc:001", "toState": "published"}'

# 6. Verify state
curl -s "http://localhost:8080/api/v1/axiom/state/spaces/$SPACE/entries/doc:001"
# -> {"stateName": "published", "version": 4}
```

## Tips

- State is per-key within a space - different keys can be in different states
- `version` increments on each transition - use for optimistic concurrency
- Transition validation is server-side - invalid transitions return HTTP 400
- Use `scan` with prefix to batch-check states of related entries
- State spaces are isolated per agent - use `agentId` to namespace
