package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.model.expression.*;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class UpdateLogDataRow {

    private UpdateLogDataRow() {}

    public static List<Map<String, Comparable<?>>> generateFromPredicate(
            Expression predicate,
            Map<String, Comparable<?>> updates,
            Map<String, Comparable<?>> schemaDefaults) {
        List<Map<String, Comparable<?>>> rows = new ArrayList<>();

        if (ExpressionUtils.isAllEqPredicate(predicate)) {
            Map<String, Comparable<?>> row = new LinkedHashMap<>(schemaDefaults);
            applyPredicateToRow(predicate, row);
            row.putAll(updates);
            rows.add(row);
            log.debug("generateFromPredicate: generated 1 row from Eq predicate");
        } else {
            log.warn("generateFromPredicate: non-Eq predicate {}, cannot generate rows", predicate);
        }

        return rows;
    }

    static void applyPredicateToRow(Expression expr, Map<String, Comparable<?>> row) {
        if (expr instanceof Eq eq) {
            applyEqToRow(eq, row);
        } else if (expr instanceof And and) {
            for (Expression child : and.children()) {
                applyPredicateToRow(child, row);
            }
        }
    }

    private static void applyEqToRow(Eq eq, Map<String, Comparable<?>> row) {
        if (eq.getLeft() instanceof ColumnRef colRef && eq.getRight() instanceof Literal lit) {
            row.put(colRef.getColumnName(), lit.getValue());
        } else if (eq.getLeft() instanceof Literal lit
                && eq.getRight() instanceof ColumnRef colRef) {
            row.put(colRef.getColumnName(), lit.getValue());
        }
    }
}
