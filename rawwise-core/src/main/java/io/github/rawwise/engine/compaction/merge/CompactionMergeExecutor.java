package io.github.rawwise.engine.compaction.merge;

import io.github.rawwise.engine.compaction.merger.OutputWriter;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CompactionMergeExecutor implements OutputWriter {

    private final OutputWriter delegate;
    private long tombstoneSkipped;

    public CompactionMergeExecutor(OutputWriter delegate) {
        this.delegate = delegate;
    }

    @Override
    public void write(byte[] key, byte[] value) throws IOException {
        delegate.write(key, value);
    }

    @Override
    public void write(
            byte[] key,
            byte[] value,
            long sequenceNumber,
            long commitTimestamp,
            boolean isTombstone)
            throws IOException {
        if (isTombstone) {
            tombstoneSkipped++;
            return;
        }
        delegate.write(key, value, sequenceNumber, commitTimestamp, false);
    }

    @Override
    public long writtenRows() {
        return delegate.writtenRows();
    }

    public long tombstoneSkipped() {
        return tombstoneSkipped;
    }

    @Override
    public void close() throws Exception {
        if (tombstoneSkipped > 0) {
            log.info("Compaction tombstone collapse: {} deleted keys filtered", tombstoneSkipped);
        }
        delegate.close();
    }
}
