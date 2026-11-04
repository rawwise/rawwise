package io.github.rawwise.axiom.pipeline.graph.distributed;

import java.util.Optional;

@FunctionalInterface
public interface NameIndexResolver {

    Optional<String> resolveEntityId(String sampleId, String canonicalName);

    static NameIndexResolver empty() {
        return (sampleId, canonicalName) -> Optional.empty();
    }
}
