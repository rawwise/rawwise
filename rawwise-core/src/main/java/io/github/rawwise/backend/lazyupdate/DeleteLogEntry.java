package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.model.expression.Expression;
import java.io.Serializable;
import java.util.Set;

/** Log entry for range DELETE operations in the Lazy Update mechanism. */
public record DeleteLogEntry(
        String entryId,
        String tableId,
        Expression predicate,
        long transactionId,
        long sequenceNumber,
        long createdAtMillis)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public DeleteLogEntry {
        if (entryId == null || entryId.isBlank())
            throw new IllegalArgumentException("entryId must not be blank");
        if (tableId == null || tableId.isBlank())
            throw new IllegalArgumentException("tableId must not be blank");
    }

    public Set<String> updatedColumns() {
        return Set.of();
    }

    public boolean isDelete() {
        return true;
    }
}
