package io.github.rawwise.engine.compaction.merger;

import java.io.IOException;

public interface OutputWriter extends AutoCloseable {

    void write(byte[] key, byte[] value) throws IOException;

    void write(
            byte[] key,
            byte[] value,
            long sequenceNumber,
            long commitTimestamp,
            boolean isTombstone)
            throws IOException;

    long writtenRows();
}
