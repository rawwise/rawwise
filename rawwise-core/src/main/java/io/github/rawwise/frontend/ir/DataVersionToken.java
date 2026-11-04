package io.github.rawwise.frontend.ir;

import java.io.Serializable;
import java.util.Objects;

public record DataVersionToken(String tableId, long commitTs) implements Serializable {

    private static final long serialVersionUID = 1L;

    public DataVersionToken {
        if (tableId == null || tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (commitTs < 0) {
            throw new IllegalArgumentException("commitTs must be non-negative: " + commitTs);
        }
    }

    public static DataVersionToken of(String tableId, long commitTs) {
        return new DataVersionToken(tableId, commitTs);
    }

    public long toSnapshotTs() {
        return commitTs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataVersionToken that)) return false;
        return commitTs == that.commitTs && tableId.equals(that.tableId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, commitTs);
    }
}
