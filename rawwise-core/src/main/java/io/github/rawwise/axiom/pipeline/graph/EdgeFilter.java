package io.github.rawwise.axiom.pipeline.graph;

import java.util.Collections;
import java.util.Set;
import lombok.Builder;

@Builder
public record EdgeFilter(Set<String> allowedRelTypes, double minWeight) {

    public EdgeFilter {
        allowedRelTypes =
                allowedRelTypes == null ? Set.of() : Collections.unmodifiableSet(allowedRelTypes);
    }

    public static EdgeFilter allowAll() {
        return new EdgeFilter(Set.of(), 0.0);
    }

    public boolean accept(GraphEdge edge) {
        if (!allowedRelTypes.isEmpty() && !allowedRelTypes.contains(edge.relType())) {
            return false;
        }
        return edge.weight() >= minWeight;
    }
}
