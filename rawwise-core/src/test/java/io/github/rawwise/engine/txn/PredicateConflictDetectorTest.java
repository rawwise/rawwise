package io.github.rawwise.engine.txn;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.backend.lazyupdate.DeleteLogEntry;
import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.ExpressionBuilder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PredicateConflictDetectorTest {

    // -- decision matrix: all 5 combinations --
    @Nested
    @DisplayName("Decision matrix")
    class Matrix {
        @Test
        void overlappingSameColsConflict() {
            assertTrue(
                    detect(
                                    update("t", gt("age", 10), Set.of("class")),
                                    upd("t", gt("age", 12), Map.of("class", "4"), 1, 1))
                            .hasConflict());
        }

        @Test
        void overlappingDisjointColsNoConflict() {
            assertFalse(
                    detect(
                                    update("t", gt("age", 10), Set.of("class")),
                                    upd(
                                            "t",
                                            gt("age", 12),
                                            new java.util.HashMap<>(Map.of("score", "50")),
                                            1,
                                            1))
                            .hasConflict());
        }

        @Test
        void intersectingColSetsConflict() {
            assertTrue(
                    detect(
                                    update("t", gt("age", 10), Set.of("class", "score")),
                                    upd(
                                            "t",
                                            gt("age", 12),
                                            Map.of("score", "50", "region", "EU"),
                                            1,
                                            1))
                            .hasConflict());
        }

        @Test
        void updateVsDeleteConflict() {
            assertTrue(
                    detect(
                                    update("t", gt("age", 10), Set.of("class")),
                                    del("t", gt("age", 12), 1, 1))
                            .hasConflict());
        }

        @Test
        void deleteVsUpdateConflict() {
            assertTrue(
                    detect(
                                    delete("t", gt("age", 10)),
                                    upd("t", gt("age", 12), Map.of("class", "4"), 1, 1))
                            .hasConflict());
        }

        @Test
        void deleteVsDeleteIdempotent() {
            assertFalse(
                    detect(delete("t", gt("age", 10)), del("t", gt("age", 12), 1, 1))
                            .hasConflict());
        }

        @Test
        void disjointPredicatesNoConflict() {
            assertFalse(
                    detect(
                                    update("t", gt("age", 10), Set.of("class")),
                                    upd("t", lt("age", 9), Map.of("class", "2"), 1, 1))
                            .hasConflict());
        }

        @Test
        void eqInsideRange() {
            assertTrue(
                    detect(
                                    update("t", ExpressionBuilder.eq("age", 5), Set.of("class")),
                                    upd("t", gt("age", 3), Map.of("class", "4"), 1, 1))
                            .hasConflict());
        }

        @Test
        void eqOutsideRange() {
            assertFalse(
                    detect(
                                    update("t", ExpressionBuilder.eq("age", 5), Set.of("class")),
                                    upd("t", lt("age", 3), Map.of("class", "2"), 1, 1))
                            .hasConflict());
        }
    }

    // -- self-transaction exclusion --
    @Nested
    @DisplayName("Self-transaction skip")
    class SelfTxn {
        @Test
        void ownSkipped() {
            long tid = 42L;
            assertFalse(
                    detect(
                                    update("t", gt("age", 10), Set.of("class")),
                                    upd("t", gt("age", 10), Map.of("class", "4"), tid, 1),
                                    tid)
                            .hasConflict());
        }

        @Test
        void ownSkippedOtherDetected() {
            long tid = 42L;
            assertTrue(
                    detect(
                                    update("t", gt("age", 10), Set.of("class")),
                                    upd("t", gt("age", 15), Map.of("class", "5"), 99L, 2),
                                    tid)
                            .hasConflict());
        }
    }

    // -- predicate vs point write --
    @Nested
    @DisplayName("Predicate vs Point Write")
    class PredVsPoint {
        @Test
        void conservativeConflict() {
            assertTrue(
                    point(
                                    update("t", gt("age", 10), Set.of("class")),
                                    List.of(w("t", "k1", 5)),
                                    Set.of(),
                                    100L)
                            .hasConflict());
        }

        @Test
        void pkKeyInRange() {
            assertTrue(
                    point(
                                    update("t", gt("id", 10), Set.of("class")),
                                    List.of(w("t", "15", 5)),
                                    Set.of("id"),
                                    100L)
                            .hasConflict());
        }

        @Test
        void pkKeyNotInRange() {
            assertFalse(
                    point(
                                    update("t", gt("id", 20), Set.of("class")),
                                    List.of(w("t", "15", 5)),
                                    Set.of("id"),
                                    100L)
                            .hasConflict());
        }

        @Test
        void nullPredCoversAll() {
            assertTrue(
                    point(
                                    new PredicateConflictDetector.PendingLogEntry.UpdatePending(
                                            "t", null, Set.of("class")),
                                    List.of(w("t", "any", 5)),
                                    Set.of("id"),
                                    100L)
                            .hasConflict());
        }

        @Test
        void emptyIntents() {
            assertFalse(
                    PredicateConflictDetector.detectPredicateVsPointWrites(
                                    List.of(update("t", gt("age", 10), Set.of("class"))),
                                    List.of(),
                                    Set.of(),
                                    100L)
                            .hasConflict());
        }
    }

    // -- edge cases --
    @Nested
    @DisplayName("Edge cases")
    class Edge {
        @Test
        void emptyPending() {
            assertFalse(
                    detect(null, upd("t", gt("age", 10), Map.of("class", "4"), 1, 1), 100L)
                            .hasConflict());
        }

        @Test
        void emptyCommitted() {
            assertFalse(
                    PredicateConflictDetector.detectPredicateVsPredicate(
                                    List.of(update("t", gt("age", 10), Set.of("class"))),
                                    null,
                                    100L)
                            .hasConflict());
        }

        @Test
        void multiPending() {
            assertTrue(
                    PredicateConflictDetector.detectPredicateVsPredicate(
                                    List.of(
                                            update("t", gt("age", 10), Set.of("class")),
                                            update("t", lt("age", 5), Set.of("score"))),
                                    List.of(upd("t", gt("age", 8), Map.of("class", "4"), 1, 1)),
                                    100L)
                            .hasConflict());
        }

        @Test
        void compositePred() {
            assertTrue(
                    PredicateConflictDetector.detectPredicateVsPredicate(
                                    List.of(
                                            update(
                                                    "t",
                                                    ExpressionBuilder.and(
                                                            gt("age", 10),
                                                            ExpressionBuilder.eq("region", "US")),
                                                    Set.of("class"))),
                                    List.of(upd("t", gt("age", 12), Map.of("class", "5"), 1, 1)),
                                    100L)
                            .hasConflict());
        }
    }

    // -- helpers --

    static ConflictReport detect(
            PredicateConflictDetector.PendingLogEntry p,
            PredicateConflictDetector.CommittedLogEntry c) {
        return detect(p, c, 100L);
    }

    static ConflictReport detect(
            PredicateConflictDetector.PendingLogEntry p,
            PredicateConflictDetector.CommittedLogEntry c,
            long tid) {
        return PredicateConflictDetector.detectPredicateVsPredicate(
                p == null ? null : List.of(p), c == null ? null : List.of(c), tid);
    }

    static ConflictReport point(
            PredicateConflictDetector.PendingLogEntry p,
            List<TxnWALRecord.TxnWriteIntent> intents,
            Set<String> cols,
            long tid) {
        return PredicateConflictDetector.detectPredicateVsPointWrites(
                List.of(p), intents, cols, tid);
    }

    static PredicateConflictDetector.PendingLogEntry update(String t, Expression e, Set<String> c) {
        return new PredicateConflictDetector.PendingLogEntry.UpdatePending(t, e, c);
    }

    static PredicateConflictDetector.PendingLogEntry delete(String t, Expression e) {
        return new PredicateConflictDetector.PendingLogEntry.DeletePending(t, e);
    }

    static PredicateConflictDetector.CommittedLogEntry upd(
            String t, Expression e, Map<String, Comparable<?>> u, long tid, long seq) {
        return new PredicateConflictDetector.CommittedLogEntry.UpdateCommitted(
                new UpdateLogEntry(
                        "e-" + seq,
                        t,
                        e,
                        new java.util.HashMap<>(u),
                        tid,
                        seq,
                        System.currentTimeMillis()));
    }

    static PredicateConflictDetector.CommittedLogEntry del(
            String t, Expression e, long tid, long seq) {
        return new PredicateConflictDetector.CommittedLogEntry.DeleteCommitted(
                new DeleteLogEntry("d-" + seq, t, e, tid, seq, System.currentTimeMillis()));
    }

    static TxnWALRecord.TxnWriteIntent w(String t, String k, long bv) {
        return new TxnWALRecord.TxnWriteIntent(
                99L,
                Set.of(t),
                t,
                0,
                0L,
                IsolationLevel.READ_COMMITTED,
                0,
                k.getBytes(StandardCharsets.UTF_8),
                new byte[0],
                TxnWALRecord.WriteType.UPSERT,
                bv,
                0,
                0L);
    }

    static Expression gt(String c, int v) {
        return ExpressionBuilder.gt(c, v);
    }

    static Expression lt(String c, int v) {
        return ExpressionBuilder.lt(c, v);
    }
}
