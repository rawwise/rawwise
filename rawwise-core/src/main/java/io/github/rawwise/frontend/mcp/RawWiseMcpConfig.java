package io.github.rawwise.frontend.mcp;

import java.util.Map;

public record RawWiseMcpConfig(
        boolean enabled,
        String endpoint,
        int maxConcurrentRequests,
        boolean toolsIngestEnabled,
        boolean toolsSearchEnabled,
        boolean toolsAnnotateEnabled,
        boolean toolsProvenanceEnabled,
        boolean toolsSqlEnabled,
        boolean toolsListTablesEnabled,
        boolean toolsCognizeEnabled,
        boolean toolsRederiveEnabled,
        boolean toolsImplicitFeedbackEnabled,
        boolean toolsFetchDetailsEnabled,
        boolean toolsDiagnosticsEnabled,
        boolean toolsGetViewEnabled,
        boolean toolsGetChunkEnabled,
        boolean toolsGetSemanticEnabled,
        boolean toolsGetActionEnabled,
        boolean toolsGetActionPlanEnabled,
        boolean toolsMemoryStoreEnabled,
        boolean toolsMemoryRecallEnabled,
        boolean toolsMemoryConsolidateEnabled,
        boolean toolsMemoryShareEnabled,
        boolean toolsMemoryStatusEnabled,
        boolean toolsMemoryDecayEnabled,
        boolean toolsMemoryReflectEnabled,
        boolean toolsGraphQueryNodesEnabled,
        boolean toolsGraphFindPathsEnabled,
        boolean toolsGraphVectorSearchEnabled,
        boolean toolsGraphGetNodeEnabled,
        boolean toolsGraphNeighborsEnabled,
        boolean toolsStateCreateSpaceEnabled,
        boolean toolsStateListSpacesEnabled,
        boolean toolsStateReadEnabled,
        boolean toolsStateWriteEnabled,
        boolean toolsStateTransitionEnabled,
        boolean toolsStateScanEnabled,
        boolean resourcesEnabled,
        boolean promptsEnabled) {

    public static RawWiseMcpConfig fromSystemProperties() {
        return new RawWiseMcpConfig(
                Boolean.parseBoolean(System.getProperty("rawwise.mcp.enabled", "true")),
                System.getProperty("rawwise.mcp.endpoint", "/mcp"),
                Integer.parseInt(System.getProperty("rawwise.mcp.max-concurrent-requests", "50")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.ingest.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.search.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.annotate.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.provenance.enabled", "true")),
                Boolean.parseBoolean(System.getProperty("rawwise.mcp.tools.sql.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.list-tables.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.cognize.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.rederive.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.implicit-feedback.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.fetch-details.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.diagnostics.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.get-view.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.get-chunk.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.get-semantic.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.get-action.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.get-action-plan.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.memory.store.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.memory.recall.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.memory.consolidate.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.memory.share.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.memory.status.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.memory.decay.enabled", "false")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.memory.reflect.enabled", "false")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.graph.query.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.graph.paths.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty(
                                "rawwise.mcp.tools.graph.vector-search.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.graph.get-node.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.graph.neighbors.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.state.create-space.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.state.list-spaces.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.state.read.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.state.write.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.state.transition.enabled", "true")),
                Boolean.parseBoolean(
                        System.getProperty("rawwise.mcp.tools.state.scan.enabled", "true")),
                Boolean.parseBoolean(System.getProperty("rawwise.mcp.resources.enabled", "false")),
                Boolean.parseBoolean(System.getProperty("rawwise.mcp.prompts.enabled", "false")));
    }

    public static RawWiseMcpConfig fromOptionsMap(Map<String, Object> config) {
        return new RawWiseMcpConfig(
                getBool(config, "rawwise.mcp.enabled", true),
                getString(config, "rawwise.mcp.endpoint", "/mcp"),
                getInt(config, "rawwise.mcp.max-concurrent-requests", 50),
                getBool(config, "rawwise.mcp.tools.ingest.enabled", true),
                getBool(config, "rawwise.mcp.tools.search.enabled", true),
                getBool(config, "rawwise.mcp.tools.annotate.enabled", true),
                getBool(config, "rawwise.mcp.tools.provenance.enabled", true),
                getBool(config, "rawwise.mcp.tools.sql.enabled", true),
                getBool(config, "rawwise.mcp.tools.list-tables.enabled", true),
                getBool(config, "rawwise.mcp.tools.cognize.enabled", true),
                getBool(config, "rawwise.mcp.tools.rederive.enabled", true),
                getBool(config, "rawwise.mcp.tools.implicit-feedback.enabled", true),
                getBool(config, "rawwise.mcp.tools.fetch-details.enabled", true),
                getBool(config, "rawwise.mcp.tools.diagnostics.enabled", true),
                getBool(config, "rawwise.mcp.tools.get-view.enabled", true),
                getBool(config, "rawwise.mcp.tools.get-chunk.enabled", true),
                getBool(config, "rawwise.mcp.tools.get-semantic.enabled", true),
                getBool(config, "rawwise.mcp.tools.get-action.enabled", true),
                getBool(config, "rawwise.mcp.tools.get-action-plan.enabled", true),
                getBool(config, "rawwise.mcp.tools.memory.store.enabled", true),
                getBool(config, "rawwise.mcp.tools.memory.recall.enabled", true),
                getBool(config, "rawwise.mcp.tools.memory.consolidate.enabled", true),
                getBool(config, "rawwise.mcp.tools.memory.share.enabled", true),
                getBool(config, "rawwise.mcp.tools.memory.status.enabled", true),
                getBool(config, "rawwise.mcp.tools.memory.decay.enabled", false),
                getBool(config, "rawwise.mcp.tools.memory.reflect.enabled", false),
                getBool(config, "rawwise.mcp.tools.graph.query.enabled", true),
                getBool(config, "rawwise.mcp.tools.graph.paths.enabled", true),
                getBool(config, "rawwise.mcp.tools.graph.vector-search.enabled", true),
                getBool(config, "rawwise.mcp.tools.graph.get-node.enabled", true),
                getBool(config, "rawwise.mcp.tools.graph.neighbors.enabled", true),
                getBool(config, "rawwise.mcp.tools.state.create-space.enabled", true),
                getBool(config, "rawwise.mcp.tools.state.list-spaces.enabled", true),
                getBool(config, "rawwise.mcp.tools.state.read.enabled", true),
                getBool(config, "rawwise.mcp.tools.state.write.enabled", true),
                getBool(config, "rawwise.mcp.tools.state.transition.enabled", true),
                getBool(config, "rawwise.mcp.tools.state.scan.enabled", true),
                getBool(config, "rawwise.mcp.resources.enabled", false),
                getBool(config, "rawwise.mcp.prompts.enabled", false));
    }

    private static boolean getBool(Map<String, Object> config, String key, boolean def) {
        Object v = config.get(key);
        if (v instanceof Boolean b) return b;
        if (v != null) return Boolean.parseBoolean(v.toString());
        return def;
    }

    private static int getInt(Map<String, Object> config, String key, int def) {
        Object v = config.get(key);
        if (v instanceof Number n) return n.intValue();
        if (v != null) return Integer.parseInt(v.toString());
        return def;
    }

    private static String getString(Map<String, Object> config, String key, String def) {
        Object v = config.get(key);
        return v != null ? v.toString() : def;
    }
}
