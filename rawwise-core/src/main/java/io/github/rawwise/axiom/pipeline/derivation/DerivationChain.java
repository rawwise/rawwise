package io.github.rawwise.axiom.pipeline.derivation;

public record DerivationChain(
        String sourceId,
        Layer sourceLayer,
        String derivedId,
        Layer derivedLayer,
        String deriveOp,
        long timestamp) {}
