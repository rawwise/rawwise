package io.github.rawwise.engine.txn;

import io.github.rawwise.backend.lazyupdate.DeleteLogEntry;
import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionBuilder;
import io.github.rawwise.model.expression.ExpressionUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/** Predicate-level conflict detector for Lazy Update range operations. */
@Slf4j
public final class PredicateConflictDetector {

    private PredicateConflictDetector() {}

    public sealed interface PendingLogEntry
            permits PendingLogEntry.UpdatePending, PendingLogEntry.DeletePending {

        String tableId();

        Expression predicate();

        Set<String> updatedColumns();

        boolean isDelete();

        record UpdatePending(String tableId, Expression predicate, Set<String> updatedColumns)
                implements PendingLogEntry {
            public UpdatePending {
                if (tableId == null || tableId.isBlank())
                    throw new IllegalArgumentException("tableId must not be blank");
                if (updatedColumns == null) updatedColumns = Set.of();
            }

            @Override
            public boolean isDelete() {
                return false;
            }
        }

        record DeletePending(String tableId, Expression predicate) implements PendingLogEntry {
            public DeletePending {
                if (tableId == null || tableId.isBlank())
                    throw new IllegalArgumentException("tableId must not be blank");
            }

            @Override
            public Set<String> updatedColumns() {
                return Set.of();
            }

            @Override
            public boolean isDelete() {
                return true;
            }
        }
    }

    public sealed interface CommittedLogEntry
            permits CommittedLogEntry.UpdateCommitted, CommittedLogEntry.DeleteCommitted {

        String entryId();

        Expression predicate();

        Set<String> updatedColumns();

        long transactionId();

        long sequenceNumber();

        boolean isDelete();

        record UpdateCommitted(UpdateLogEntry entry) implements CommittedLogEntry {
            @Override
            public String entryId() {
                return entry.entryId();
            }

            @Override
            public Expression predicate() {
                return entry.predicate();
            }

            @Override
            public Set<String> updatedColumns() {
                return entry.updates().keySet();
            }

            @Override
            public long transactionId() {
                return entry.transactionId();
            }

            @Override
            public long sequenceNumber() {
                return entry.sequenceNumber();
            }

            @Override
            public boolean isDelete() {
                return false;
            }
        }

        record DeleteCommitted(DeleteLogEntry entry) implements CommittedLogEntry {
            @Override
            public String entryId() {
                return entry.entryId();
            }

            @Override
            public Expression predicate() {
                return entry.predicate();
            }

            @Override
            public Set<String> updatedColumns() {
                return Set.of();
            }

            @Override
            public long transactionId() {
                return entry.transactionId();
            }

            @Override
            public long sequenceNumber() {
                return entry.sequenceNumber();
            }

            @Override
            public boolean isDelete() {
                return true;
            }
        }
    }

    public static ConflictReport detectPredicateVsPredicate(
            List<PendingLogEntry> pendingEntries,
            List<CommittedLogEntry> committedLogs,
            long currentTxnId) {

        if (pendingEntries == null || pendingEntries.isEmpty()) return ConflictReport.noConflict();
        if (committedLogs == null || committedLogs.isEmpty()) return ConflictReport.noConflict();

        List<ConflictReport.ConflictEntry> conflicts = new ArrayList<>();

        for (PendingLogEntry pending : pendingEntries) {
            for (CommittedLogEntry committed : committedLogs) {
                if (committed.transactionId() == currentTxnId) continue;
                if (!ExpressionUtils.predicatesConflict(pending.predicate(), committed.predicate()))
                    continue;

                if (pending.isDelete() && committed.isDelete()) {
                    log.debug("Delete-Delete idempotent: txn={}", currentTxnId);
                    continue;
                }

                if (pending.isDelete() || committed.isDelete()) {
                    log.warn("Predicate conflict (DELETE involved): txn={}", currentTxnId);
                    conflicts.add(
                            new ConflictReport.ConflictEntry(
                                    ConflictReport.ConflictType.WRITE_WRITE,
                                    ("pred:" + committed.entryId())
                                            .getBytes(StandardCharsets.UTF_8),
                                    -1,
                                    committed.sequenceNumber(),
                                    committed.transactionId()));
                    continue;
                }

                if (columnsOverlap(pending.updatedColumns(), committed.updatedColumns())) {
                    log.warn("Predicate conflict (overlapping columns): txn={}", currentTxnId);
                    conflicts.add(
                            new ConflictReport.ConflictEntry(
                                    ConflictReport.ConflictType.WRITE_WRITE,
                                    ("pred:" + committed.entryId())
                                            .getBytes(StandardCharsets.UTF_8),
                                    -1,
                                    committed.sequenceNumber(),
                                    committed.transactionId()));
                } else {
                    log.debug("Predicates overlap but column sets disjoint: txn={}", currentTxnId);
                }
            }
        }

        if (conflicts.isEmpty()) return ConflictReport.noConflict();
        return ConflictReport.multipleConflicts(
                conflicts,
                "Predicate-predicate conflict: " + conflicts.size() + " committed log(s) overlap");
    }

    public static ConflictReport detectPredicateVsPointWrites(
            List<PendingLogEntry> pendingEntries,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            Set<String> keyColumnNames,
            long currentTxnId) {

        if (pendingEntries == null || pendingEntries.isEmpty()) return ConflictReport.noConflict();
        if (writeIntents == null || writeIntents.isEmpty()) return ConflictReport.noConflict();

        List<ConflictReport.ConflictEntry> conflicts = new ArrayList<>();

        for (PendingLogEntry pending : pendingEntries) {
            for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
                byte[] keyBytes = intent.keyBytes();

                boolean keyInRange;
                if (pending.predicate() == null) {
                    keyInRange = true;
                } else {
                    keyInRange =
                            couldKeyBeInPredicateRange(
                                    pending.predicate(), keyBytes, keyColumnNames);
                }

                if (keyInRange) {
                    log.warn("Predicate-point conflict: txn={}", currentTxnId);
                    conflicts.add(
                            new ConflictReport.ConflictEntry(
                                    ConflictReport.ConflictType.WRITE_WRITE,
                                    keyBytes,
                                    intent.baseVersion(),
                                    -1,
                                    -1));
                }
            }
        }

        if (conflicts.isEmpty()) return ConflictReport.noConflict();
        return ConflictReport.multipleConflicts(
                conflicts,
                "Predicate-point conflict: " + conflicts.size() + " point write(s) overlap");
    }

    private static boolean columnsOverlap(Set<String> cols1, Set<String> cols2) {
        if (cols1 == null || cols2 == null) return true;
        if (cols1.isEmpty() || cols2.isEmpty()) return false;
        Set<String> smaller = cols1.size() <= cols2.size() ? cols1 : cols2;
        Set<String> larger = cols1.size() <= cols2.size() ? cols2 : cols1;
        for (String col : smaller) {
            if (larger.contains(col)) return true;
        }
        return false;
    }

    private static boolean couldKeyBeInPredicateRange(
            Expression predicate, byte[] keyBytes, Set<String> keyColumnNames) {
        Set<String> predCols = predicate.getReferencedColumns();
        boolean referencesPk = predCols.stream().anyMatch(keyColumnNames::contains);
        if (!referencesPk) return true;
        if (keyColumnNames.size() == 1) {
            String pkCol = keyColumnNames.iterator().next();
            String keyStr = new String(keyBytes, StandardCharsets.UTF_8);
            Comparable<?> keyValue = tryParseNumeric(keyStr);
            Expression keyEq = ExpressionBuilder.eq(pkCol, keyValue);
            return ExpressionUtils.predicatesConflict(predicate, keyEq);
        }
        return true;
    }

    private static Comparable<?> tryParseNumeric(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException ignored) {
        }
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException ignored) {
        }
        return s;
    }
}
