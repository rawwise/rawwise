package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.*;
import java.util.stream.Collectors;

public class And implements LogicalExpression {

    private final List<Expression> children;

    public And(Expression... children) {
        this.children = Arrays.asList(children);
    }

    public And(List<Expression> children) {
        this.children = new ArrayList<>(children);
    }

    @Override
    public boolean evaluate(Record row) {
        for (Expression child : children) {
            if (!child.evaluate(row)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.AND;
    }

    @Override
    public Set<String> getReferencedColumns() {
        Set<String> columns = new HashSet<>();
        for (Expression child : children) {
            columns.addAll(child.getReferencedColumns());
        }
        return columns;
    }

    @Override
    public Expression simplify() {
        List<Expression> flat = flattenAnds(children);
        List<Expression> simplified = new ArrayList<>();

        for (Expression child : flat) {
            Expression simplifiedChild = child.simplify();

            if (simplifiedChild.alwaysFalse()) {
                return new Literal(false);
            }

            if (!simplifiedChild.alwaysTrue()) {
                simplified.add(simplifiedChild);
            }
        }

        if (simplified.isEmpty()) {
            return new Literal(true);
        }

        for (int i = 0; i < simplified.size(); i++) {
            for (int j = i + 1; j < simplified.size(); j++) {
                if (areContradictory(simplified.get(i), simplified.get(j))) {
                    return new Literal(false);
                }
            }
        }

        if (simplified.size() == 1) {
            return simplified.get(0);
        } else {
            return new And(simplified);
        }
    }

    private static List<Expression> flattenAnds(List<Expression> exprs) {
        List<Expression> result = new ArrayList<>();
        for (Expression e : exprs) {
            if (e instanceof And and) {
                result.addAll(flattenAnds(and.children));
            } else {
                result.add(e);
            }
        }
        return result;
    }

    private static boolean areContradictory(Expression a, Expression b) {
        return areOppositePair(a, b, Gt.class, LtEq.class)
                || areOppositePair(b, a, Gt.class, LtEq.class)
                || areOppositePair(a, b, GtEq.class, Lt.class)
                || areOppositePair(b, a, GtEq.class, Lt.class)
                || areOppositePair(a, b, GtEq.class, LtEq.class)
                || areOppositePair(b, a, GtEq.class, LtEq.class)
                || areOppositePair(a, b, Gt.class, Lt.class)
                || areOppositePair(b, a, Gt.class, Lt.class)
                || areOppositePair(a, b, Lt.class, GtEq.class)
                || areOppositePair(b, a, Lt.class, GtEq.class)
                || areOppositePair(a, b, LtEq.class, Gt.class)
                || areOppositePair(b, a, LtEq.class, Gt.class)
                || areOppositePair(a, b, Eq.class, NotEq.class)
                || areOppositePair(b, a, Eq.class, NotEq.class)
                || areOppositePair(a, b, Eq.class, Lt.class)
                || areOppositePair(b, a, Eq.class, Lt.class)
                || areOppositePair(a, b, Eq.class, LtEq.class)
                || areOppositePair(b, a, Eq.class, LtEq.class)
                || areOppositePair(a, b, Eq.class, Gt.class)
                || areOppositePair(b, a, Eq.class, Gt.class)
                || areOppositePair(a, b, Eq.class, GtEq.class)
                || areOppositePair(b, a, Eq.class, GtEq.class);
    }

    private static boolean areOppositePair(
            Expression a, Expression b, Class<?> clsA, Class<?> clsB) {
        if (!clsA.isInstance(a) || !clsB.isInstance(b)) {
            return false;
        }
        BinaryComparison bcA = (BinaryComparison) a;
        BinaryComparison bcB = (BinaryComparison) b;

        Expression leftA = bcA.getLeft();
        Expression leftB = bcB.getLeft();
        Expression rightA = bcA.getRight();
        Expression rightB = bcB.getRight();

        if (leftA instanceof ColumnRef
                && leftB instanceof ColumnRef
                && rightA instanceof Literal
                && rightB instanceof Literal) {
            String colA = ((ColumnRef) leftA).getColumnName();
            String colB = ((ColumnRef) leftB).getColumnName();
            if (!colA.equals(colB)) {
                return false;
            }
            Comparable valA = ((Literal) rightA).getValue();
            Comparable valB = ((Literal) rightB).getValue();

            if (valA instanceof Number && valB instanceof Number) {
                double da = ((Number) valA).doubleValue();
                double db = ((Number) valB).doubleValue();
                if (clsA == Eq.class && clsB == NotEq.class) {
                    return da == db;
                }
                if (clsA == Gt.class && clsB == LtEq.class) {
                    return da >= db;
                }
                if (clsA == GtEq.class && clsB == Lt.class) {
                    return da >= db;
                }
                if (clsA == GtEq.class && clsB == LtEq.class) {
                    return da > db;
                }
                if (clsA == Gt.class && clsB == Lt.class) {
                    return da >= db;
                }
                if (clsA == Eq.class && clsB == Lt.class) {
                    return da >= db;
                }
                if (clsA == Eq.class && clsB == LtEq.class) {
                    return da > db;
                }
                if (clsA == Eq.class && clsB == Gt.class) {
                    return da <= db;
                }
                if (clsA == Eq.class && clsB == GtEq.class) {
                    return da < db;
                }
            }
            int cmp = safeCompare(valA, valB);
            if (clsA == Eq.class && clsB == NotEq.class) {
                return cmp == 0;
            }
            if (clsA == Gt.class && clsB == LtEq.class) {
                return cmp >= 0;
            }
            if (clsA == GtEq.class && clsB == Lt.class) {
                return cmp >= 0;
            }
            if (clsA == GtEq.class && clsB == LtEq.class) {
                return cmp > 0;
            }
            if (clsA == Gt.class && clsB == Lt.class) {
                return cmp >= 0;
            }
            if (clsA == Eq.class && clsB == Lt.class) {
                return cmp >= 0;
            }
            if (clsA == Eq.class && clsB == LtEq.class) {
                return cmp > 0;
            }
            if (clsA == Eq.class && clsB == Gt.class) {
                return cmp <= 0;
            }
            if (clsA == Eq.class && clsB == GtEq.class) {
                return cmp < 0;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static int safeCompare(Comparable a, Comparable b) {
        if (a == null || b == null) return -1;
        try {
            return a.compareTo(b);
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public boolean alwaysTrue() {
        return children.stream().allMatch(Expression::alwaysTrue);
    }

    @Override
    public boolean alwaysFalse() {
        return children.stream().anyMatch(Expression::alwaysFalse);
    }

    @Override
    public Expression copy() {
        List<Expression> copiedChildren =
                children.stream().map(Expression::copy).collect(Collectors.toList());
        return new And(copiedChildren);
    }

    @Override
    public String toString() {
        return children.stream()
                .map(Expression::toString)
                .collect(Collectors.joining(" AND ", "(", ")"));
    }

    @Override
    public List<Expression> children() {
        return Collections.unmodifiableList(children);
    }
}
