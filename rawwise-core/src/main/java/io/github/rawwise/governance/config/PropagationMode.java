package io.github.rawwise.governance.config;

public enum PropagationMode {
    QUORUM,
    ALL;

    public static PropagationMode fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("propagationMode must not be blank");
        }
        return switch (value.toUpperCase().trim()) {
            case "QUORUM" -> QUORUM;
            case "ALL" -> ALL;
            default ->
                    throw new IllegalArgumentException(
                            "propagationMode must be QUORUM or ALL, got: " + value);
        };
    }
}
