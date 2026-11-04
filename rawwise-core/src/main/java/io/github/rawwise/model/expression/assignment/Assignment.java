package io.github.rawwise.model.expression.assignment;

import io.github.rawwise.model.expression.Expression;
import java.util.Set;

public interface Assignment extends Expression {

    String getTargetColumn();

    Expression getValueExpression();

    Set<String> getReferencedColumns();

    // check if assigning to self(:age = age + 1)
    boolean selfAssignment();
}
