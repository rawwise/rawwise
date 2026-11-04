# RawWise Skills

RawWise 对外暴露的 Skill 定义。Agent 读取后可直接通过 MCP 或 HTTP API 使用 RawWise。

## Skills

| Skill | Description |
|-------|-------------|
| [rawwise-knowledge-pipeline](rawwise-knowledge-pipeline/SKILL.md) | Ingest, search, cognize, trace provenance |
| [rawwise-memory-management](rawwise-memory-management/SKILL.md) | Store, recall, consolidate, share agent memory |
| [rawwise-feedback-loop](rawwise-feedback-loop/SKILL.md) | Annotate results, trigger cascade rederivation |
| [rawwise-sql-operations](rawwise-sql-operations/SKILL.md) | Create tables, insert, query via SQL |
| [rawwise-state-machine](rawwise-state-machine/SKILL.md) | Create state spaces, transitions, guards |

## Connection

All skills assume RawWise is running at `http://localhost:8080`.

- **MCP**: `http://localhost:8080/mcp` (34 tools)
- **HTTP API**: `http://localhost:8080/api/axiom/*`, `http://localhost:8080/api/query`

See [../README.md](../README.md) for full API reference.
