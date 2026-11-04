package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.*;
import java.util.stream.Collectors;

public class Or implements LogicalExpression {

    private final List<Expression> children;

    public Or(Expression... children) {
        this.children = Arrays.asList(children);
    }

    public Or(List<Expression> children) {
        this.children = new ArrayList<>(children);
    }

    @Override
    public boolean evaluate(Record row) {
        for (Expression child : children) {
            if (child.evaluate(row)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.OR;
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
        List<Expression> simplified = new ArrayList<>();

        for (Expression child : children) {
            Expression simplifiedChild = child.simplify();

            if (simplifiedChild.alwaysTrue()) {
                return new Literal(true);
            }

            if (!simplifiedChild.alwaysFalse()) {
                simplified.add(simplifiedChild);
            }
        }

        if (simplified.isEmpty()) {
            return new Literal(false);
        } else if (simplified.size() == 1) {
            return simplified.getFirst();
        } else {
            return new Or(simplified);
        }
    }

    @Override
    public boolean alwaysTrue() {
        return children.stream().anyMatch(Expression::alwaysTrue);
    }

    @Override
    public boolean alwaysFalse() {
        return children.stream().allMatch(Expression::alwaysFalse);
    }

    @Override
    public Expression copy() {
        List<Expression> copiedChildren =
                children.stream().map(Expression::copy).collect(Collectors.toList());
        return new Or(copiedChildren);
    }

    @Override
    public String toString() {
        return children.stream()
                .map(Expression::toString)
                .collect(Collectors.joining(" OR ", "(", ")"));
    }

    @Override
    public List<Expression> children() {
        return Collections.unmodifiableList(children);
    }
}
