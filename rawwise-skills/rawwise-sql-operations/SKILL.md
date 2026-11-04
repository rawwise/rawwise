---
name: rawwise-sql-operations
description: Create tables, insert data, and query using SQL via RawWise's HTTP API with FastPath optimization. Use when you need structured CRUD operations, table management, or point queries with sub-millisecond latency.
---

# SQL Operations Skill

## When to Use

- Creating tables (DDL)
- Inserting structured data (DML)
- Querying by primary key (FastPath: ~100us latency)
- Listing tables and databases
- Standard SQL SELECT/INSERT/UPDATE/DELETE

## Actions

### Execute SQL

**MCP Tool**: `rawwise_query_sql`
**HTTP**: `POST /api/query`

All SQL returns a jobId. Poll until `state = SUCCEEDED`. DDL completes quickly with empty result.

```bash
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM default.users WHERE id = '\''user001'\''"}'
```

### Create Table

```bash
# Standard table with primary key (enables FastPath)
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE users (id STRING, name STRING, age INT) USING rawwise TBLPROPERTIES ('\''primary-key'\''='\''id'\'')"}'}

# AI-Native table (enables auto-derivation + semantic search)
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "CREATE TABLE docs (id STRING, content STRING) USING rawwise TBLPROPERTIES ('\''primary-key'\''='\''id'\'', '\''ai-native'\''='\''true'\'')"}'}
```

### Insert Data

Single-row INSERT uses FastPath (sub-millisecond write, but still returns a jobId to poll):

```bash
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO users VALUES ('\''user001'\'', '\''Alice'\'', 30)"}'}
```

Multi-row INSERT goes through Spark SQL:

```bash
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO users VALUES ('\''user002'\'', '\''Bob'\'', 25), ('\''user003'\'', '\''Carol'\'', 35)"}'}
```

### Query Data

Point query (FastPath, ~100us):
```bash
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users WHERE id = '\''user001'\''"}'
```

Full scan:
```bash
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users"}'
```

### Poll for Results

DML/DQL returns a jobId. Poll until state = SUCCEEDED:

```bash
# Get status
curl -s http://localhost:8080/api/query/{jobId}/status

# Get result
curl -s http://localhost:8080/api/query/{jobId}/result
```

### List Tables

**MCP Tool**: `rawwise_list_tables`

```bash
# Via SQL
curl -s -X POST http://localhost:8080/api/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SHOW TABLES"}'

# Or via API
curl -s http://localhost:8080/api/tables
```

## FastPath Optimization

FastPath bypasses Spark SQL for point queries, reading directly from LMDB:

```
SQL: SELECT * FROM t WHERE pk = ?
  -> CatalystQueryParser
  -> ScanQueryIR (pkCols=[id])
  -> RouteDecider -> FAST_PATH
  -> gRPC PointQueryRequest
  -> DynamicMmapStore.find()
  -> JSON response
```

**Requirements for FastPath**:
1. Table must have `primary-key` in TBLPROPERTIES
2. Query must be `SELECT ... WHERE pk_col = literal`
3. IR must be enabled (`-Drawwise.frontend.ir.enabled=true`, default: false)

## Tips

- DDL (CREATE/DROP TABLE) returns empty result, not 404
- Single-row INSERT is 56x faster than Spark SQL path (9ms vs 508ms)
- Use `USING rawwise` in CREATE TABLE to use RawWise storage (not default Parquet)
- For semantic search on table data, add `'ai-native'='true'` to TBLPROPERTIES
