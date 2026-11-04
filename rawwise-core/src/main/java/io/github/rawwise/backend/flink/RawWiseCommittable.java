package io.github.rawwise.backend.flink;

import java.io.Serializable;
import java.util.List;

public record RawWiseCommittable(
        String tableId,
        List<String> pendingIds,
        long txnId,
        long checkpointId,
        long schemaVersion,
        long minSeq,
        long maxSeq,
        long checkpointTs)
        implements Serializable {
    private static final long serialVersionUID = 1L;

    public RawWiseCommittable {
        pendingIds = List.copyOf(pendingIds);
    }

    public boolean isEmpty() {
        return pendingIds.isEmpty();
    }

    public static RawWiseCommittable empty(
            String tableId, long txnId, long checkpointId, long schemaVersion) {
        return new RawWiseCommittable(
                tableId,
                List.of(),
                txnId,
                checkpointId,
                schemaVersion,
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                0L);
    }
}
