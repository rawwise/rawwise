package io.github.rawwise.frontend.mcp;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RawWiseMcpPrompts {

    public List<Map<String, Object>> getPromptDefinitions() {
        return List.of(
                promptDef(
                        "rawwise_sql_guide",
                        "RawWise SQL dialect guide with USING rawwise, TBLPROPERTIES, and FastPath optimization tips",
                        List.of()));
    }

    public Map<String, Object> get(String name) {
        if ("rawwise_sql_guide".equals(name)) {
            return getSqlGuide();
        }
        return Map.of("error", "Unknown prompt: " + name);
    }

    private Map<String, Object> getSqlGuide() {
        String guide =
                """
            # RawWise SQL Guide

            ## Table Creation
            RawWise supports `USING rawwise` with TBLPROPERTIES for AI-Native tables:

            ```sql
            CREATE TABLE my_table (
                id STRING,
                content STRING,
                metadata STRING
            ) USING rawwise
            TBLPROPERTIES (
                'primary-key' = 'id',
                'ai-native' = 'true',
                'derivation.strategy' = 'auto'
            );
            ```

            ## FastPath Optimization
            Point queries on primary-key columns bypass Spark SQL for sub-10ms latency:
            ```sql
            SELECT * FROM my_table WHERE id = 'some-key';
            ```
            Prerequisites: table must have `primary-key` TBLPROPERTIES and query must be
            `SELECT ... WHERE pk_col = literal`.

            ## INSERT
            Single-row inserts use FastPath (~9ms):
            ```sql
            INSERT INTO my_table VALUES ('key1', 'content1', 'meta1');
            ```
            Multi-row inserts fall back to Spark SQL batch path.

            ## AI-Native Tables
            Tables with `ai-native=true` trigger AutoDerivationHook on write:
            Raw -> Chunk -> View -> Embedding -> JVector index.
            Use `rawwise_search` tool for semantic retrieval instead of SQL LIKE.

            ## Supported SQL
            - SELECT (with WHERE, GROUP BY, ORDER BY, LIMIT)
            - INSERT INTO ... VALUES
            - CREATE TABLE / DROP TABLE
            - SHOW TABLES / DESCRIBE TABLE
            """;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("description", "RawWise SQL dialect guide");
        result.put(
                "messages",
                List.of(Map.of("role", "user", "content", Map.of("type", "text", "text", guide))));
        return result;
    }

    private static Map<String, Object> promptDef(
            String name, String description, List<Map<String, Object>> arguments) {
        Map<String, Object> prompt = new LinkedHashMap<>();
        prompt.put("name", name);
        prompt.put("description", description);
        prompt.put("arguments", arguments);
        return prompt;
    }
}
