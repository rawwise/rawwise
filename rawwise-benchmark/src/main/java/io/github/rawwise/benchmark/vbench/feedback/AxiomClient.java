package io.github.rawwise.benchmark.vbench.feedback;

public interface AxiomClient {
    String ingest(AxiomIngest ingest);

    void annotate(AxiomAnnotation annotation);
}
