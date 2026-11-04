package io.github.rawwise.engine.storage.table.read;

import io.github.rawwise.engine.storage.QueryCapability;
import io.github.rawwise.engine.storage.reader.QueryReader;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Read;
import io.github.rawwise.model.op.Scan;
import java.io.IOException;
import java.util.Set;

public interface TableReader extends QueryReader {

    Read.Result read(Read read) throws IOException;

    Find.Result find(Find find) throws IOException;

    Scan.Result scan(Scan scan) throws IOException;

    @Override
    default Set<QueryCapability> capabilities() {
        return Set.of(
                QueryCapability.POINT_LOOKUP,
                QueryCapability.RANGE_SCAN,
                QueryCapability.FULL_READ);
    }
}
