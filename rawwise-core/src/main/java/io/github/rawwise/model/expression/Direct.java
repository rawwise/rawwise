package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Collections;
import java.util.Set;

public class Direct implements Expression {
    private final boolean value;

    public Direct(boolean value) {
        this.value = value;
    }

    @Override
    public boolean evaluate(Record row) {
        return value;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.DIRECT;
    }

    @Override
    public Set<String> getReferencedColumns() {
        return Collections.emptySet();
    }

    @Override
    public Expression simplify() {
        return this;
    }

    @Override
    public boolean alwaysTrue() {
        return value;
    }

    @Override
    public boolean alwaysFalse() {
        return !value;
    }

    @Override
    public Expression copy() {
        return new Direct(value);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
