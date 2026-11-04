package io.github.rawwise.engine.compaction.merger;

import io.github.rawwise.engine.storage.reader.CursorReader;
import java.io.IOException;
import java.util.Arrays;

public interface SortedFileReader extends CursorReader, Comparable<SortedFileReader> {

    byte[] currentKey() throws IOException;

    byte[] currentValue() throws IOException;

    long currentSequenceNumber() throws IOException;

    long currentCommitTimestamp() throws IOException;

    boolean currentIsTombstone() throws IOException;

    default VersionedValue currentVersionedValue() throws IOException {
        return VersionedValue.mutable(
                currentValue(),
                currentSequenceNumber(),
                currentCommitTimestamp(),
                currentIsTombstone());
    }

    byte[] cachedKey();

    @Override
    default int compareTo(SortedFileReader other) {
        byte[] thisKey = this.cachedKey();
        byte[] otherKey = other.cachedKey();
        if (thisKey == null || otherKey == null) {
            throw new IllegalStateException("cachedKey() is null - advance() must be called first");
        }
        return Arrays.compare(thisKey, otherKey);
    }
}
