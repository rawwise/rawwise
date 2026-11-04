package io.github.rawwise.model.expression;

import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExpressionUtils {

    private static final io.micrometer.core.instrument.Counter PK_EQ_HIT =
            io.micrometer.core.instrument.Metrics.counter("rawwise.pk_equality", "result", "hit");
    private static final io.micrometer.core.instrument.Counter PK_EQ_MISS =
            io.micrometer.core.instrument.Metrics.counter("rawwise.pk_equality", "result", "miss");

    private static final class ExtractResult {
        static final ExtractResult NON_PK_CONDITION = new ExtractResult(Set.of(), true);
        final Set<String> keys;
        final boolean rejected;

        ExtractResult(Set<String> keys, boolean rejected) {
            this.keys = keys;
            this.rejected = rejected;
        }

        static ExtractResult foundPk(String key) {
            return new ExtractResult(Set.of(key), false);
        }

        static ExtractResult foundAllPks(Set<String> keys) {
            return new ExtractResult(keys, false);
        }
    }

    public static boolean isPrimaryKeyEquals(Expression expression, Set<String> primaryKeys) {
        ExtractResult result = extractPrimaryKeyConditions(expression, primaryKeys);
        if (result.rejected) {
            PK_EQ_MISS.increment();
            return false;
        }
        boolean isPkEq =
                result.keys.size() == primaryKeys.size()
                        && primaryKeys.stream().allMatch(result.keys::contains);
        if (isPkEq) {
            PK_EQ_HIT.increment();
        } else {
            PK_EQ_MISS.increment();
        }
        return isPkEq;
    }

    private static ExtractResult extractPrimaryKeyConditions(
            Expression expression, Set<String> primaryKeys) {
        if (expression instanceof Eq eq) {
            String column = extractEqColumn(eq);
            if (column == null) {
                return ExtractResult.NON_PK_CONDITION;
            }
            if (primaryKeys.contains(column)) {
                return ExtractResult.foundPk(column);
            }
            return ExtractResult.NON_PK_CONDITION;
        }
        if (expression instanceof And and) {
            Set<String> allKeys = new java.util.HashSet<>();
            for (Expression child : and.children()) {
                ExtractResult r = extractPrimaryKeyConditions(child, primaryKeys);
                if (r.rejected) {
                    return ExtractResult.NON_PK_CONDITION;
                }
                allKeys.addAll(r.keys);
            }
            return ExtractResult.foundAllPks(allKeys);
        }
        return ExtractResult.NON_PK_CONDITION;
    }

    private static String extractEqColumn(Eq eq) {
        if (eq.getLeft() instanceof ColumnRef colRef && eq.getRight() instanceof Literal) {
            return colRef.getColumnName();
        }
        if (eq.getLeft() instanceof Literal && eq.getRight() instanceof ColumnRef colRef) {
            return colRef.getColumnName();
        }
        return null;
    }

    private static Comparable<?> extractEqValue(Eq eq) {
        if (eq.getLeft() instanceof ColumnRef && eq.getRight() instanceof Literal lit)
            return lit.getValue();
        if (eq.getLeft() instanceof Literal lit && eq.getRight() instanceof ColumnRef)
            return lit.getValue();
        return null;
    }

    public static boolean isAllEqPredicate(Expression expr) {
        if (expr instanceof Eq) return true;
        if (expr instanceof And and) {
            return and.children().stream().allMatch(ExpressionUtils::isAllEqPredicate);
        }
        return false;
    }

    // determine if expression only involves primary key
    public static boolean involvesOnlyPrimaryKeys(Expression expression, Set<String> primaryKeys) {
        Set<String> referencedColumns = expression.getReferencedColumns();
        return primaryKeys.containsAll(referencedColumns);
    }

    // expression and computation
    public static Expression merge(Expression expr1, Expression expr2) {
        if (expr1 == null) {
            return expr2;
        }
        if (expr2 == null) {
            return expr1;
        }

        return new And(expr1, expr2);
    }

    // expression
    public static boolean contains(Expression expr1, Expression expr2) {
        // todo determination based on column references and value ranges,actual implementation
        // needs more complex logic analysis,if comparing the same column, can determine directly
        if (expr1 instanceof Eq eq1 && expr2 instanceof Eq eq2) {
            if (eq1.getLeft().equals(eq2.getLeft())) {
                return eq1.getRight().equals(eq2.getRight());
            }
        }

        return false;
    }

    // intersect query and update log
    public static SplitQuery splitQueryByUpdate(Expression query, Expression update) {
        // intersection:query AND update(can directly apply updated value parts)
        Expression intersection = ExpressionBuilder.and(query, update);

        // difference:query AND NOT(update)(needs to read original data parts)
        Expression difference = ExpressionBuilder.and(query, ExpressionBuilder.not(update));

        return new SplitQuery(intersection, difference);
    }

    public static Map<String, Comparable<?>> extractPrimaryKeyValues(
            Expression expression, Set<String> primaryKeys) {
        Map<String, Comparable<?>> result = new HashMap<>();
        collectPkValues(expression, primaryKeys, result);
        return result;
    }

    private static void collectPkValues(
            Expression expression, Set<String> primaryKeys, Map<String, Comparable<?>> result) {
        if (expression instanceof Eq eq) {
            String column = extractEqColumn(eq);
            Comparable<?> value = extractEqValue(eq);
            if (column != null && value != null && primaryKeys.contains(column)) {
                result.put(column, value);
            }
        } else if (expression instanceof And and) {
            for (Expression child : and.children()) {
                collectPkValues(child, primaryKeys, result);
            }
        }
    }

    public static Expression buildEqPredicate(Map<String, Comparable<?>> pkValues) {
        if (pkValues == null || pkValues.isEmpty()) {
            throw new IllegalArgumentException("Primary key values must not be empty");
        }
        List<Expression> eqs = new ArrayList<>();
        for (var entry : pkValues.entrySet()) {
            eqs.add(new Eq(new ColumnRef(entry.getKey()), new Literal(entry.getValue())));
        }
        if (eqs.size() == 1) return eqs.get(0);
        return new And(eqs);
    }

    public record SplitQuery(Expression intersection, Expression difference) {}

    /**
     * Check whether Update Log entries can fully cover the read condition, meaning no data file
     * scan is necessary - every row matching {@code read Condition} is also covered by at least one
     * Update Log predicate.
     *
     * <p>Algorithm: iteratively subtract each Update Log predicate from a "remaining" condition.
     * When the remaining reduces to {@code always False()}, full coverage is achieved. The check is
     * bounded and uses algebraic simplification at each step to detect unreachability early.
     *
     * @param read Condition the query's read predicate (may be {@code null} to mean "all rows")
     * @param update Logs Update Log entries sorted by sequence number ascending
     * @return {@code true} if the data file scan can be completely skipped
     */
    public static boolean canFullyCover(Expression readCondition, List<UpdateLogEntry> updateLogs) {
        Expression remaining = simplifyCondition(readCondition);

        for (UpdateLogEntry entry : updateLogs) {
            Expression predicate = entry.predicate();
            if (predicate == null) {
                // no predicate means the update applies to ALL rows
                return true;
            }

            // remaining = remaining AND NOT(predicate)
            remaining = ExpressionBuilder.and(remaining, ExpressionBuilder.not(predicate));
            remaining = simplifyCondition(remaining);

            if (remaining.alwaysFalse()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Resolve which Update Log entries apply to which portions of the read condition.
     *
     * <p>Update Logs are processed in <b>descending</b> sequence order so that later entries carve
     * out their portions first. Each entry's predicate is intersected with the <em>remaining</em>
     * condition; the intersection portion receives the entry's updates. The remainder is passed to
     * the next entry. This guarantees that later Update Logs naturally override earlier ones for
     * overlapping ranges.
     *
     * <p>The input list is sorted internally by sequence number descending; callers may pass
     * entries in any order.
     *
     * @param read Condition the query's read predicate (may be {@code null})
     * @param update Logs Update Log entries (order will be normalized internally; callers may pass
     *     any order)
     * @return resolved slices plus the remaining condition that needs data file scan
     */
    public static ResolvedUpdates resolveUpdates(
            Expression readCondition, List<UpdateLogEntry> updateLogs) {
        Expression remaining = simplifyCondition(readCondition);
        List<ResolvedSlice> slices = new ArrayList<>();

        List<UpdateLogEntry> sorted = new ArrayList<>(updateLogs);
        sorted.sort(Comparator.comparingLong(UpdateLogEntry::sequenceNumber).reversed());

        for (UpdateLogEntry entry : sorted) {
            Expression predicate = entry.predicate();
            if (predicate == null) {
                slices.add(new ResolvedSlice(simplifyCondition(readCondition), entry.updates()));
                return new ResolvedUpdates(slices, new Literal(false));
            }

            Expression intersection = ExpressionBuilder.and(remaining, predicate);
            intersection = simplifyCondition(intersection);

            if (!intersection.alwaysFalse()) {
                slices.add(new ResolvedSlice(intersection, entry.updates()));
            }

            remaining = ExpressionBuilder.and(remaining, ExpressionBuilder.not(predicate));
            remaining = simplifyCondition(remaining);

            if (remaining.alwaysFalse()) {
                break;
            }
        }

        return new ResolvedUpdates(slices, remaining);
    }

    /**
     * Simplify an expression: treat {@code null} as "always true" and run the expression's own
     * {@code simplify()} method, returning a trivially-true marker when the result covers
     * everything.
     */
    public static Expression simplifyCondition(Expression condition) {
        if (condition == null) {
            return new Literal(true);
        }
        Expression simplified = condition.simplify();
        if (simplified.alwaysTrue()) {
            return new Literal(true);
        }
        return simplified;
    }

    /**
     * Check whether two range predicates conflict (i.e., their effective ranges overlap). Two
     * predicates conflict if there exists at least one row that satisfies both predicates
     * simultaneously.
     *
     * <p>Algorithm: compute {@code simplify(pred A AND pred B)}. If the result reduces to {@code
     * always False()}, the predicates are disjoint (no overlap). Otherwise they conflict.
     *
     * <h3>Examples</h3>
     *
     * <pre>{@code
     * predicates Conflict(age>10, age>12) → true (age>12 ⊆ age>10)
     * predicates Conflict(age>10, age<9) → false (10>=9, Gt/Lt contradiction)
     * predicates Conflict(age>11, age<9) → false (11>=9, Gt/Lt contradiction)
     * predicates Conflict(age>10, age<15) → true (10<age<15, overlap)
     * predicates Conflict(age=5, age>3) → true (5>3, Eq compatible with Gt)
     * predicates Conflict(age=5, age<3) → false (5>=3, Eq/Lt contradiction)
     * predicates Conflict(age>10, region='US') → true (different columns, both satisfiable)
     * predicates Conflict(null, age>10) → true (null means ALL rows)
     * }</pre>
     *
     * @param pred A first predicate (may be {@code null} to mean "all rows")
     * @param pred B second predicate (may be {@code null} to mean "all rows")
     * @return {@code true} if the predicates overlap (conflict)
     */
    public static boolean predicatesConflict(Expression predA, Expression predB) {
        if (predA == null || predB == null) {
            return true;
        }
        Expression conjunction = ExpressionBuilder.and(predA, predB);
        Expression simplified = conjunction.simplify();
        if (simplified.alwaysFalse()) {
            return false;
        }
        return true;
    }

    /**
     * A single resolved slice: a portion of the read condition together with the concrete column
     * updates that should be applied to rows matching that portion.
     */
    public record ResolvedSlice(
            Expression applicableCondition, Map<String, Comparable<?>> updates) {
        public ResolvedSlice {
            applicableCondition =
                    java.util.Objects.requireNonNull(applicableCondition, "applicableCondition");
            updates = Map.copyOf(updates);
        }
    }

    /**
     * The result of resolving updates against a read condition.
     *
     * @param slices ordered list of (condition, updates) pairs derived from Update Logs
     * @param remaining Condition the part of the original read condition that still requires a data
     *     file scan; {@link Literal#false} when everything is covered
     */
    public record ResolvedUpdates(List<ResolvedSlice> slices, Expression remainingCondition) {
        public ResolvedUpdates {
            slices = List.copyOf(slices);
            remainingCondition =
                    java.util.Objects.requireNonNull(remainingCondition, "remainingCondition");
        }

        /** Convenience: true when no data file scan is needed. */
        public boolean isFullyCovered() {
            return remainingCondition.alwaysFalse();
        }
    }
}
