package io.github.rawwise.benchmark.vbench.feedback;

import java.util.Map;

public record AxiomIngest(
        String tableId,
        String key,
        String body,
        boolean immutable,
        Map<String, String> properties) {}
