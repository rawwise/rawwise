package io.github.rawwise.axiom.memory.semantic;

import java.util.List;
import java.util.Map;

public record ExtractionResponse(List<NodeDef> nodes, List<EdgeDef> edges) {
    public record NodeDef(String id, String type, Map<String, String> properties) {
        public NodeDef {
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }
    }

    public record EdgeDef(
            String src, String label, String dst, Map<String, String> properties, float weight) {
        public EdgeDef {
            properties = properties == null ? Map.of() : Map.copyOf(properties);
        }
    }
}
