package io.github.rawwise.model.expression;

import io.github.rawwise.model.record.Record;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class Literal implements Expression {
    private final Comparable<?> value;

    public Literal(Comparable<?> value) {
        this.value = value;
    }

    public Literal(boolean value) {
        this.value = value;
    }

    public Comparable<?> getValue() {
        return value;
    }

    @Override
    public boolean evaluate(Record row) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        return value != null;
    }

    @Override
    public ExpressionType getType() {
        return ExpressionType.LITERAL;
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
        return value instanceof Boolean bool && bool;
    }

    @Override
    public boolean alwaysFalse() {
        return value instanceof Boolean bool && !bool;
    }

    @Override
    public Expression copy() {
        return new Literal(value);
    }

    @Override
    public String toString() {
        if (value instanceof String) {
            return "'" + value + "'";
        }
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Literal literal = (Literal) obj;
        return Objects.equals(value, literal.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
