package io.github.rawwise.engine.vector;

public record JVectorConfig(
        String mode,
        int dimension,
        int graphDegree,
        int searchTopK,
        int efSearch,
        String basePath,
        int buildThreshold,
        int mergeThreshold,
        int maxVectors) {
    public JVectorConfig {
        if (mode == null || mode.isBlank()) mode = "disk";
        if (dimension <= 0) dimension = 768;
        if (graphDegree <= 0) graphDegree = 32;
        if (searchTopK <= 0) searchTopK = 10;
        if (efSearch <= 0) efSearch = 50;
        if (basePath == null || basePath.isBlank()) basePath = "/tmp/rawwise/vector";
        if (buildThreshold <= 0) buildThreshold = 10000;
        if (mergeThreshold <= 0) mergeThreshold = 10000;
        if (maxVectors <= 0) maxVectors = 500000;
    }
}
