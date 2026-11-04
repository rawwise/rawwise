package io.github.rawwise.engine.clean;

public record ExpireSnapshotAction(long snapshotId) implements CleanAction {

    public ExpireSnapshotAction {
        if (snapshotId < 0) {
            throw new IllegalArgumentException("snapshotId must not be negative");
        }
    }

    @Override
    public ActionType actionType() {
        return ActionType.EXPIRE_SNAPSHOT;
    }
}
