package io.github.rawwise.engine.txn;

public enum TxnMode {
    DEFERRED,
    WRITE_THROUGH;

    public boolean isWriteThrough() {
        return this == WRITE_THROUGH;
    }

    public boolean isDeferred() {
        return this == DEFERRED;
    }

    public static TxnMode fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("TxnMode value must not be null or blank");
        }
        return switch (value.toUpperCase().trim()) {
            case "DEFERRED" -> DEFERRED;
            case "WRITE_THROUGH" -> WRITE_THROUGH;
            default -> throw new IllegalArgumentException("Unknown TxnMode: " + value);
        };
    }
}
