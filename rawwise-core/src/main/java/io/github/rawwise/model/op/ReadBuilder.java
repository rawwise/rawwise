package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.RawWiseTable;
import java.util.List;

public interface ReadBuilder<R extends Read, B extends ReadBuilder<R, B>>
        extends TableOpBuilder<Read.Result, R> {

    B withTable(RawWiseTable table);

    B withDataBlocks(List<DataBlock> dataBlocks);

    B withPredicate(Expression predicate);

    @Override
    RawWiseTable table();
}
