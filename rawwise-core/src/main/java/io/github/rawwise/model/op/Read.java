package io.github.rawwise.model.op;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.Record;
import java.util.List;
import java.util.Optional;

public interface Read extends Op<Read.Result> {

    List<DataBlock> getDataBlocks();

    long estimatedSize();

    Expression getPushedPredicate();

    boolean isPrimaryKeyLookup();

    @Override
    Result exec();

    interface Result extends OpResult {

        long recordCount();

        long bytesRead();

        List<Record> data();

        Optional<Object> singleResult();

        boolean hasMore();
    }
}
