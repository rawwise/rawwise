package io.github.rawwise.model.op.commit;

import io.github.rawwise.meta.PbTableOperation;
import java.util.List;

public record CommitRequest(
        String tableId,
        long baseSnapshotId,
        List<PbTableOperation> operations,
        CommitContext context) {
    public CommitRequest {
        if (tableId == null || tableId.isEmpty()) {
            throw new IllegalArgumentException("tableId must not be null or empty");
        }
        if (operations == null) {
            operations = List.of();
        }
        if (context == null) {
            context = CommitContext.of(CommitContext.CommitSource.MANUAL, "default");
        }
    }

    public static CommitRequest of(
            String tableId,
            long baseSnapshotId,
            List<PbTableOperation> operations,
            CommitContext context) {
        return new CommitRequest(tableId, baseSnapshotId, operations, context);
    }

    public static CommitRequest empty(String tableId, CommitContext context) {
        return new CommitRequest(tableId, -1L, List.of(), context);
    }
}
