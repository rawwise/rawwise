package io.github.rawwise.engine.storage.recovery;

/**
 * Record capturing a single WAL replay failure.
 *
 * <p>Note: the {@code key} field uses {@code byte[]} which has reference-based {@code
 * equals()}/{@code hashCode()} in record semantics. Instances should be stored in lists, not used
 * as map keys or set elements.
 */
public record ReplayError(
        String tableId, long walSequence, byte[] key, String errorMessage, long timestamp) {
    public static ReplayError create(
            String tableId, long walSequence, byte[] key, String errorMessage) {
        return new ReplayError(tableId, walSequence, key, errorMessage, System.currentTimeMillis());
    }

    public String toLogLine() {
        return String.format(
                "table=%s seq=%d key_len=%d error=%s ts=%d",
                tableId, walSequence, key != null ? key.length : 0, errorMessage, timestamp);
    }
}
