package io.github.rawwise.backend;

import io.github.rawwise.model.op.Op;
import java.util.List;

public interface Planner<C, O extends Op<?>> {
    List<O> plan(C context);

    default List<O> plan() {
        return plan(null);
    }
}
