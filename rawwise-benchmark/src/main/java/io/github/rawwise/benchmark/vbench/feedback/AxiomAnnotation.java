package io.github.rawwise.benchmark.vbench.feedback;

public record AxiomAnnotation(
        String viewId, String type, String value, String annotatorId, double confidence) {}
