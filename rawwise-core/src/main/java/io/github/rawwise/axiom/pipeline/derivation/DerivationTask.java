package io.github.rawwise.axiom.pipeline.derivation;

public interface DerivationTask {
    void runFull();

    void runLightweight();

    String tableId();

    String recordKey();
}
