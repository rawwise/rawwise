package io.github.rawwise.engine.clean;

public record RewriteFileAction(
        String filePath,
        String expireColumn,
        String logicalType,
        long expiryThresholdMs,
        long estimatedExpiredBytes,
        long totalFileBytes)
        implements CleanAction {

    public RewriteFileAction {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("filePath must not be null or empty");
        }
        if (expireColumn == null || expireColumn.isEmpty()) {
            throw new IllegalArgumentException("expireColumn must not be null or empty");
        }
        if (expiryThresholdMs < 0) {
            throw new IllegalArgumentException("expiryThresholdMs must not be negative");
        }
        if (estimatedExpiredBytes < 0) {
            throw new IllegalArgumentException("estimatedExpiredBytes must not be negative");
        }
        if (totalFileBytes < 0) {
            throw new IllegalArgumentException("totalFileBytes must not be negative");
        }
    }

    @Override
    public ActionType actionType() {
        return ActionType.REWRITE_FILE;
    }
}
