package io.github.rawwise.engine.vector;

public record IvfPqConfig(
        int dimension,
        int nlist,
        int m,
        int nprobe,
        int retrainThreshold,
        int kmeansMaxIter,
        int mergeThreshold,
        long seed,
        int restarts) {
    public IvfPqConfig {
        if (dimension <= 0) dimension = 768;
        if (nlist <= 0) nlist = 1024;
        if (m <= 0) m = 64;
        if (nprobe <= 0) nprobe = 32;
        if (retrainThreshold <= 0) retrainThreshold = 10;
        if (kmeansMaxIter <= 0) kmeansMaxIter = 20;
        if (mergeThreshold <= 0) mergeThreshold = 10000;
        if (seed == 0) seed = 42;
        if (restarts <= 0) restarts = 3;
    }
}
