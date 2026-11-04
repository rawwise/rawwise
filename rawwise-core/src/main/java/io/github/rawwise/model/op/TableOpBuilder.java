package io.github.rawwise.model.op;

import io.github.rawwise.model.record.RawWiseTable;

public interface TableOpBuilder<R extends OpResult, O extends Op<R>> {

    O build();

    RawWiseTable table();
}
