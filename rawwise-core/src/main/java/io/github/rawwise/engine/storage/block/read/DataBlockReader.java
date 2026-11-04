package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.QueryCapability;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.reader.QueryReader;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Scan;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface DataBlockReader extends QueryReader {
    boolean supports(DataBlock block);

    Find.Result find(Find find, List<DataBlock> blocks) throws IOException;

    Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException;

    @Override
    default Set<QueryCapability> capabilities() {
        return Set.of(QueryCapability.POINT_LOOKUP, QueryCapability.RANGE_SCAN);
    }
}
