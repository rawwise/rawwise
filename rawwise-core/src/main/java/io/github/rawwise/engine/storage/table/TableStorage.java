package io.github.rawwise.engine.storage.table;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.model.op.Read;
import io.github.rawwise.model.op.Write;
import io.github.rawwise.model.record.TableId;
import java.io.IOException;
import java.util.List;

public interface TableStorage extends AutoCloseable {

    TableId tableId();

    Write.Result write(Write write) throws IOException;

    Read.Result read(Read read) throws IOException;

    List<DataBlock> currentDataBlocks();

    void flush() throws IOException;
}
