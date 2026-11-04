package io.github.rawwise.frontend.ir.adaptor;

import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.StructType;

public interface IRAdaptor<E> {
    E adapt(QueryIR ir);

    Object adaptPredicate(Expression predicate);

    Object adaptSchema(StructType schema);
}
