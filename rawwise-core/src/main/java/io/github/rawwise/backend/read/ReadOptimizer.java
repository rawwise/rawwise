package io.github.rawwise.backend.read;

public interface ReadOptimizer {

    String name();

    ReadPlan optimize(ReadPlan plan);

    default int priority() {
        return 0;
    }
}
