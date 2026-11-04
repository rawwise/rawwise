package io.github.rawwise.engine.clean;

public record DeleteFileAction(String filePath, long estimatedExpiredBytes) implements CleanAction {

    public DeleteFileAction {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("filePath must not be null or empty");
        }
        if (estimatedExpiredBytes < 0) {
            throw new IllegalArgumentException("estimatedExpiredBytes must not be negative");
        }
    }

    @Override
    public ActionType actionType() {
        return ActionType.DELETE_FILE;
    }
}
