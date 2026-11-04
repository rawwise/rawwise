package io.github.rawwise.axiom.pipeline.entity;

import lombok.Builder;

@Builder
public record Relation(
        String head, String relType, String tail, String temporal, double confidence) {}
