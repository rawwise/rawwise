---
name: rawwise-feedback-loop
description: Annotate search results with positive/negative feedback to trigger cascade rederivation, improving future search quality. Use when search results are wrong or suboptimal and you want RawWise to learn from the correction.
---

# Feedback Loop Skill

## When to Use

- Search returned irrelevant results -> annotate NEGATIVE
- Search returned perfect results -> annotate POSITIVE
- User corrected a search result -> annotate CORRECTION
- Track implicit signals (dwell time, click, skip) for reranker training

## Actions

### Annotate (Explicit Feedback)

Triggers `FeedbackLoopCloser.cascadeRederive` - adjusts rank bias and re-derives the View.

**MCP Tool**: `rawwise_annotate`
**HTTP**: `POST /api/axiom/annotate`

```bash
# Negative feedback (result was wrong)
curl -s -X POST http://localhost:8080/api/axiom/annotate \
  -H "Content-Type: application/json" \
  -d '{
    "viewId": "view:abc123",
    "type": "NEGATIVE",
    "value": "-5",
    "annotatorId": "agent",
    "confidence": 0.9
  }'

# Positive feedback (result was great)
curl -s -X POST http://localhost:8080/api/axiom/annotate \
  -H "Content-Type: application/json" \
  -d '{
    "viewId": "view:def456",
    "type": "POSITIVE",
    "value": "3",
    "annotatorId": "agent"
  }'

# Correction (free text)
curl -s -X POST http://localhost:8080/api/axiom/annotate \
  -H "Content-Type: application/json" \
  -d '{
    "viewId": "view:ghi789",
    "type": "CORRECTION",
    "value": "The correct answer is Java 21, not Java 17",
    "annotatorId": "agent"
  }'
```

### Implicit Feedback

Record user behavior signals for reranker training.

**MCP Tool**: `rawwise_implicit_feedback`
**HTTP**: `POST /api/axiom/feedback/implicit`

```bash
# User spent 30s reading (positive signal)
curl -s -X POST http://localhost:8080/api/axiom/feedback/implicit \
  -H "Content-Type: application/json" \
  -d '{"viewId": "view:abc123", "signalType": "DWELL", "dwellMs": 30000}'

# User clicked (positive signal)
curl -s -X POST http://localhost:8080/api/axiom/feedback/implicit \
  -H "Content-Type: application/json" \
  -d '{"viewId": "view:abc123", "signalType": "CLICK"}'

# User skipped (negative signal)
curl -s -X POST http://localhost:8080/api/axiom/feedback/implicit \
  -H "Content-Type: application/json" \
  -d '{"viewId": "view:abc123", "signalType": "SKIP"}'
```

### Force Rederive

Manually trigger re-derivation of specific entries or entire table.

**MCP Tool**: `rawwise_rederive`
**HTTP**: `POST /api/axiom/rederive`

```bash
# Re-derive a single entry
curl -s -X POST http://localhost:8080/api/axiom/rederive \
  -H "Content-Type: application/json" \
  -d '{"tableId": "default.kb", "scope": "SINGLE_KEY", "singleKey": "doc:001", "forceReindex": true}'

# Re-derive entire table
curl -s -X POST http://localhost:8080/api/axiom/rederive \
  -H "Content-Type: application/json" \
  -d '{"tableId": "default.kb", "scope": "FULL_TABLE"}'
```

## How It Works

```
User feedback (NEGATIVE/CORRECTION)
    |
    v
FeedbackLoopCloser.shouldCascade()
    |  (threshold check or LLM judgment)
    v
cascadeRederive(viewId)
    |
    +---> 1. Load View + source Chunks
    +---> 2. Adjust rank bias (inject/exclude from candidates)
    +---> 3. Re-derive View with new parameters
    +---> 4. Re-embed if needed
    +---> 5. Update index
    |
    v
Next search returns better results
```

## Tips

- `viewId` comes from search results - always capture it when searching
- NEGATIVE with value "-5" is stronger than "-1"; use confidence to indicate certainty
- CORRECTION type accepts free text - LLM analyzes sentiment severity
- Implicit feedback accumulates over time for reranker training
- `forceReindex=true` bypasses the "skip if already derived" check
