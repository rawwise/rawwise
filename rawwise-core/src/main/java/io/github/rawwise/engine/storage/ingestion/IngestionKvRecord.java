package io.github.rawwise.engine.storage.ingestion;

import java.util.Arrays;

public record IngestionKvRecord(
        byte[] key, byte[] value, long sequenceNumber, long commitTimestamp, boolean tombstone) {

    public IngestionKvRecord(byte[] key, byte[] value, long sequenceNumber, long commitTimestamp) {
        this(key.clone(), value.clone(), sequenceNumber, commitTimestamp, false);
    }

    IngestionKvRecord(
            byte[] key,
            byte[] value,
            long sequenceNumber,
            long commitTimestamp,
            boolean tombstone,
            boolean skipCopy) {
        this(
                skipCopy ? key : key.clone(),
                skipCopy ? value : value.clone(),
                sequenceNumber,
                commitTimestamp,
                tombstone);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IngestionKvRecord that)) return false;
        return Arrays.equals(key, that.key)
                && Arrays.equals(value, that.value)
                && sequenceNumber == that.sequenceNumber
                && commitTimestamp == that.commitTimestamp
                && tombstone == that.tombstone;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + Long.hashCode(sequenceNumber);
        result = 31 * result + Long.hashCode(commitTimestamp);
        result = 31 * result + Boolean.hashCode(tombstone);
        return result;
    }
}
