package io.github.rawwise.model.expression;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ExpressionUtilsTest {

    // --- canFullyCover ---------------------------------------------------

    @Test
    void canFullyCoverSingleUpdateLogCoversEverything() {
        // Update: SET x=1 (no predicate = covers all rows)
        UpdateLogEntry entry = new UpdateLogEntry("e1", "t1", null, Map.of("x", 1), 1L, 1L, 1000L);

        // Read: age > 8 — the null-predicate entry covers everything
        Expression read = ExpressionBuilder.gt("age", 8);
        assertThat(ExpressionUtils.canFullyCover(read, List.of(entry))).isTrue();
    }

    @Test
    void canFullyCoverExactMatch() {
        // Update: SET class=4 WHERE age > 10
        UpdateLogEntry entry = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);

        // Read: age > 10 — exactly covered
        Expression read = ExpressionBuilder.gt("age", 10);
        assertThat(ExpressionUtils.canFullyCover(read, List.of(entry))).isTrue();
    }

    @Test
    void canFullyCoverPartialCoverageReturnsFalse() {
        // Update: SET class=4 WHERE age > 10
        UpdateLogEntry entry = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);

        // Read: age > 8 — only partially covered (8 < age <= 10 not covered)
        Expression read = ExpressionBuilder.gt("age", 8);
        assertThat(ExpressionUtils.canFullyCover(read, List.of(entry))).isFalse();
    }

    @Test
    void canFullyCoverMultipleUpdatesFullyCover() {
        // U1 (seq=1): SET class=4 WHERE age > 8 (broad, covers everything first)
        // U2 (seq=2): SET class=3 WHERE age > 10 (narrow, overridden by U1)
        UpdateLogEntry e1 = entry("e1", ExpressionBuilder.gt("age", 8), Map.of("class", 4), 1L);
        UpdateLogEntry e2 = entry("e2", ExpressionBuilder.gt("age", 10), Map.of("class", 3), 2L);

        // Read: age > 8 — fully covered by U1 (first entry covers the full range)
        Expression read = ExpressionBuilder.gt("age", 8);
        assertThat(ExpressionUtils.canFullyCover(read, List.of(e1, e2))).isTrue();
    }

    @Test
    void canFullyCoverNarrowPredicateWithMatchingUpdateFirst() {
        // U1 (seq=1): SET class=5 WHERE age > 20
        // U2 (seq=2): SET class=4 WHERE age > 10
        UpdateLogEntry e1 = entry("e1", ExpressionBuilder.gt("age", 20), Map.of("class", 5), 1L);
        UpdateLogEntry e2 = entry("e2", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 2L);

        // Read: age > 20 — covered by U1 exactly
        Expression read = ExpressionBuilder.gt("age", 20);
        assertThat(ExpressionUtils.canFullyCover(read, List.of(e1, e2))).isTrue();
    }

    @Test
    void canFullyCoverEmptyUpdateLogs() {
        Expression read = ExpressionBuilder.gt("age", 8);
        assertThat(ExpressionUtils.canFullyCover(read, List.of())).isFalse();
    }

    @Test
    void canFullyCoverNullReadCondition() {
        // null read = select all rows
        UpdateLogEntry entry = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);
        // Only rows with age > 10 covered; null read means all rows
        assertThat(ExpressionUtils.canFullyCover(null, List.of(entry))).isFalse();
    }

    @Test
    void canFullyCoverNullReadWithNullPredicate() {
        UpdateLogEntry entry = new UpdateLogEntry("e1", "t1", null, Map.of("x", 1), 1L, 1L, 1000L);
        assertThat(ExpressionUtils.canFullyCover(null, List.of(entry))).isTrue();
    }

    // --- resolveUpdates -------------------------------------------------

    @Test
    void resolveUpdatesSingleEntry() {
        UpdateLogEntry entry = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);
        Expression read = ExpressionBuilder.gt("age", 8);

        ExpressionUtils.ResolvedUpdates result =
                ExpressionUtils.resolveUpdates(read, List.of(entry));

        assertThat(result.slices()).hasSize(1);
        assertThat(result.slices().get(0).updates()).containsEntry("class", 4);
        assertThat(result.isFullyCovered()).isFalse();
    }

    @Test
    void resolveUpdatesExactMatchFullyCovered() {
        UpdateLogEntry entry = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);
        Expression read = ExpressionBuilder.gt("age", 10);

        ExpressionUtils.ResolvedUpdates result =
                ExpressionUtils.resolveUpdates(read, List.of(entry));

        assertThat(result.slices()).hasSize(1);
        // age>10 AND NOT(age>10) simplifies to false with NOT inversion
        assertThat(result.isFullyCovered()).isTrue();
        assertThat(result.remainingCondition().alwaysFalse()).isTrue();
    }

    @Test
    void resolveUpdatesMultipleOverlapping() {
        // U1 (seq=1): SET class=4 WHERE age > 10
        // U2 (seq=2): SET class=5 WHERE age > 20
        UpdateLogEntry e1 = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);
        UpdateLogEntry e2 = entry("e2", ExpressionBuilder.gt("age", 20), Map.of("class", 5), 2L);

        // Read: age > 8
        Expression read = ExpressionBuilder.gt("age", 8);

        ExpressionUtils.ResolvedUpdates result =
                ExpressionUtils.resolveUpdates(read, List.of(e1, e2));

        // Descending order: e2 (seq=2, age>20) carves out first -> slice[0]={age>20, class=5}
        // e1 (seq=1, age>10) carves out remaining -> slice[1]={age>10 AND age<=20, class=4}
        // remaining: age>8 AND age<=10
        assertThat(result.slices()).hasSize(2);
        assertThat(result.slices().get(0).updates()).containsEntry("class", 5);
        assertThat(result.slices().get(1).updates()).containsEntry("class", 4);
        assertThat(result.isFullyCovered()).isFalse();
    }

    @Test
    void resolveUpdatesRangeOverlapDescendingSliceOrder() {
        // Bug-1 regression: late entries must carve out first so they override
        // earlier entries for overlapping ranges.
        // U1 (seq=1): SET class=4 WHERE age > 10
        // U2 (seq=2): SET class=5 WHERE age > 20
        UpdateLogEntry e1 = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);
        UpdateLogEntry e2 = entry("e2", ExpressionBuilder.gt("age", 20), Map.of("class", 5), 2L);

        Expression read = ExpressionBuilder.gt("age", 8);

        ExpressionUtils.ResolvedUpdates result =
                ExpressionUtils.resolveUpdates(read, List.of(e1, e2));

        // slice[0] = age>20 -> class=5 (late entry overrides)
        // slice[1] = age>10 AND age<=20 -> class=4
        assertThat(result.slices()).hasSize(2);
        assertThat(result.slices().get(0).updates()).containsEntry("class", 5);
        assertThat(result.slices().get(1).updates()).containsEntry("class", 4);
    }

    @Test
    void resolveUpdatesInputOrderIndependent() {
        // Bug-1: resolveUpdates must sort internally, not depend on caller's order.
        // Same entries as resolveUpdatesRangeOverlapDescendingSliceOrder but reversed input.
        UpdateLogEntry e1 = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);
        UpdateLogEntry e2 = entry("e2", ExpressionBuilder.gt("age", 20), Map.of("class", 5), 2L);

        Expression read = ExpressionBuilder.gt("age", 8);

        ExpressionUtils.ResolvedUpdates resultAsc =
                ExpressionUtils.resolveUpdates(read, List.of(e1, e2));
        ExpressionUtils.ResolvedUpdates resultDesc =
                ExpressionUtils.resolveUpdates(read, List.of(e2, e1));

        // Both input orders must produce the same result.
        assertThat(resultAsc.slices()).hasSize(resultDesc.slices().size());
        for (int i = 0; i < resultAsc.slices().size(); i++) {
            assertThat(resultAsc.slices().get(i).updates())
                    .isEqualTo(resultDesc.slices().get(i).updates());
        }
    }

    @Test
    void simplifyConditionRangePredicateIntersection() {
        // Bug-1 prerequisite: simplify must reduce "age>8 AND age>20" to "age>20".
        Expression ageGt8 = ExpressionBuilder.gt("age", 8);
        Expression ageGt20 = ExpressionBuilder.gt("age", 20);
        Expression conjunction = ExpressionBuilder.and(ageGt8, ageGt20);
        Expression simplified = ExpressionUtils.simplifyCondition(conjunction);

        // The simplified form must not be alwaysFalse (the ranges overlap).
        assertThat(simplified.alwaysFalse()).isFalse();
        // Evaluating against a value in the overlap (age=25) must be true.
        assertThat(
                        simplified.evaluate(
                                io.github.rawwise.model.record.MapRecord.fromMap(
                                        java.util.Map.of("age", 25))))
                .isTrue();
        // Evaluating against a value in the gap (age=9) must be false.
        assertThat(
                        simplified.evaluate(
                                io.github.rawwise.model.record.MapRecord.fromMap(
                                        java.util.Map.of("age", 9))))
                .isFalse();
    }

    @Test
    void resolveUpdatesNullPredicateCoversAll() {
        UpdateLogEntry entry =
                new UpdateLogEntry("e1", "t1", null, Map.of("class", 5), 1L, 1L, 1000L);
        Expression read = ExpressionBuilder.gt("age", 8);

        ExpressionUtils.ResolvedUpdates result =
                ExpressionUtils.resolveUpdates(read, List.of(entry));

        assertThat(result.slices()).hasSize(1);
        assertThat(result.isFullyCovered()).isTrue();
        assertThat(result.remainingCondition().alwaysFalse()).isTrue();
    }

    @Test
    void resolveUpdatesEmptyUpdateLogs() {
        Expression read = ExpressionBuilder.gt("age", 8);
        ExpressionUtils.ResolvedUpdates result = ExpressionUtils.resolveUpdates(read, List.of());

        assertThat(result.slices()).isEmpty();
        assertThat(result.isFullyCovered()).isFalse();
        assertThat(result.remainingCondition().alwaysFalse()).isFalse();
    }

    @Test
    void resolveUpdatesNullReadCondition() {
        UpdateLogEntry entry = entry("e1", ExpressionBuilder.gt("age", 10), Map.of("class", 4), 1L);

        ExpressionUtils.ResolvedUpdates result =
                ExpressionUtils.resolveUpdates(null, List.of(entry));

        assertThat(result.slices()).hasSize(1);
        assertThat(result.isFullyCovered()).isFalse();
    }

    // --- simplifyCondition ----------------------------------------------

    @Test
    void simplifyConditionNullReturnsTrue() {
        Expression result = ExpressionUtils.simplifyCondition(null);
        assertThat(result).isInstanceOf(Literal.class);
        assertThat(result.alwaysTrue()).isTrue();
    }

    @Test
    void simplifyConditionAlwaysTrueReturnsLiteralTrue() {
        Expression alwaysTrue = new Literal(true);
        Expression result = ExpressionUtils.simplifyCondition(alwaysTrue);
        assertThat(result).isInstanceOf(Literal.class);
        assertThat(result.alwaysTrue()).isTrue();
    }

    @Test
    void simplifyConditionPassesThroughNormalExpression() {
        Expression expr = ExpressionBuilder.gt("age", 10);
        Expression result = ExpressionUtils.simplifyCondition(expr);
        // GT expression passes through because it's not trivially true/false
        assertThat(result).isNotNull();
        assertThat(result.getType()).isEqualTo(Expression.ExpressionType.GREATER_THAN);
    }

    // --- helpers --------------------------------------------------------

    private static UpdateLogEntry entry(
            String id, Expression predicate, Map<String, Comparable<?>> updates, long seq) {
        return new UpdateLogEntry(
                id, "test_table", predicate, updates, seq, seq, System.currentTimeMillis());
    }
}
