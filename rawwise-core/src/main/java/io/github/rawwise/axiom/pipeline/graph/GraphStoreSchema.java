package io.github.rawwise.axiom.pipeline.graph;

import java.nio.charset.StandardCharsets;

public final class GraphStoreSchema {

    public static final String NODE_PREFIX = "graph:node:";
    public static final String EDGE_OUT_PREFIX = "graph:edge:out:";
    public static final String EDGE_IN_PREFIX = "graph:edge:in:";
    public static final String IDX_ENTITY_TO_DIA = "graph:idx:entity_to_dia:";
    public static final String IDX_TYPE_TO_ENTITY = "graph:idx:type_to_entity:";
    public static final String IDX_NAME_TO_ENTITY = "graph:idx:name_to_entity:";
    public static final String IDX_DIA_TO_ENTITY = "graph:idx:dia_to_entity:";

    private GraphStoreSchema() {}

    public static byte[] nodeKey(String entityId) {
        return (NODE_PREFIX + entityId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] outEdgeKey(String srcId, String relType, String dstId) {
        return (EDGE_OUT_PREFIX + srcId + ":" + relType + ":" + dstId)
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] outEdgePrefix(String srcId) {
        return (EDGE_OUT_PREFIX + srcId + ":").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] inEdgeKey(String dstId, String relType, String srcId) {
        return (EDGE_IN_PREFIX + dstId + ":" + relType + ":" + srcId)
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] inEdgePrefix(String dstId) {
        return (EDGE_IN_PREFIX + dstId + ":").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] entityToDiaKey(String entityId, String diaId, int turnIdx) {
        return (IDX_ENTITY_TO_DIA + entityId + ":" + diaId + ":" + turnIdx)
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] entityToDiaPrefix(String entityId) {
        return (IDX_ENTITY_TO_DIA + entityId + ":").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] typeToEntityKey(String entityType, String entityId) {
        return (IDX_TYPE_TO_ENTITY + entityType + ":" + entityId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] nameToEntityKey(String sampleId, String canonicalName) {
        return (IDX_NAME_TO_ENTITY + sampleId + ":" + canonicalName)
                .getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] diaToEntityKey(String sampleId, String diaId) {
        return (IDX_DIA_TO_ENTITY + sampleId + ":" + diaId).getBytes(StandardCharsets.UTF_8);
    }
}
