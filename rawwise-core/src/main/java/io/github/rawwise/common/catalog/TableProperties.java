package io.github.rawwise.common.catalog;

import java.util.Map;

public record TableProperties(Map<String, String> props) {
    public static TableProperties empty() {
        return new TableProperties(Map.of());
    }
}
