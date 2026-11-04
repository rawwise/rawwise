package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class Not implements LogicalExpression {

    private final Expression child;

    public Not(Expression child) {
        this.child = child;
    }

    @Override
    public boolean evaluate(Record row) {
        return !child.evaluate(row);
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.NOT;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return child.getReferencedColumns();
    }

    @Override
    public Expression simplify() {
        Expression simplifiedChild = child.simplify();

        if (simplifiedChild instanceof Not) {
            return ((Not) simplifiedChild).child.simplify();
        } else if (simplifiedChild.alwaysTrue()) {
            return new Literal(false);
        } else if (simplifiedChild.alwaysFalse()) {
            return new Literal(true);
        } else if (simplifiedChild instanceof Gt gt) {
            return new LtEq(gt.getLeft(), gt.getRight()).simplify();
        } else if (simplifiedChild instanceof Lt lt) {
            return new GtEq(lt.getLeft(), lt.getRight()).simplify();
        } else if (simplifiedChild instanceof GtEq ge) {
            return new Lt(ge.getLeft(), ge.getRight()).simplify();
        } else if (simplifiedChild instanceof LtEq le) {
            return new Gt(le.getLeft(), le.getRight()).simplify();
        } else if (simplifiedChild instanceof Eq eq) {
            return new NotEq(eq.getLeft(), eq.getRight()).simplify();
        } else if (simplifiedChild instanceof NotEq ne) {
            return new Eq(ne.getLeft(), ne.getRight()).simplify();
        } else {
            return new Not(simplifiedChild);
        }
    }

    @Override
    public Expression copy() {
        return new Not(child.copy());
    }

    @Override
    public String toString() {
        return "NOT " + child.toString();
    }

    @Override
    public List<Expression> children() {
        return Collections.singletonList(child);
    }
}
