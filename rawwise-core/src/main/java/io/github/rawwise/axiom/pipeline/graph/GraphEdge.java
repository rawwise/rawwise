package io.github.rawwise.axiom.pipeline.graph;

import io.github.rawwise.axiom.pipeline.entity.Relation;

public record GraphEdge(
        String srcId,
        String relType,
        String dstId,
        String temporal,
        double confidence,
        double weight) {

    public static GraphEdge fromRelation(Relation relation) {
        return new GraphEdge(
                relation.head(),
                relation.relType(),
                relation.tail(),
                relation.temporal(),
                relation.confidence(),
                relation.confidence());
    }
}
