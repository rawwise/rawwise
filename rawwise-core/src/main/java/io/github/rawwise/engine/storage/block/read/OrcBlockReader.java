package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.block.ColdDataBlock;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.metrics.ReaderMetrics;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Scan;
import java.io.IOException;
import java.util.List;

public class OrcBlockReader extends AbstractColdFormatBlockReader {
    public OrcBlockReader() {
        super(DataBlock.Format.ORC);
    }

    public OrcBlockReader(ReaderMetrics metrics) {
        super(DataBlock.Format.ORC, metrics);
    }

    @Override
    protected Find.Result findVisible(Find find, List<ColdDataBlock> blocks) throws IOException {
        return Find.Result.error("ORC reader is not implemented yet", null);
    }

    @Override
    protected Scan.Result scanVisible(Scan scan, List<ColdDataBlock> blocks) throws IOException {
        return Scan.Result.error("ORC reader is not implemented yet", null);
    }
}
