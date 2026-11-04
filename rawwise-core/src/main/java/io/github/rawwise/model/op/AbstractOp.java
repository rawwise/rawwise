package io.github.rawwise.model.op;

import io.github.rawwise.model.record.RawWiseTable;

public abstract class AbstractOp<R extends OpResult> implements Op<R> {
    protected final RawWiseTable table;

    protected AbstractOp(RawWiseTable table) {
        this.table = table;
    }
}
