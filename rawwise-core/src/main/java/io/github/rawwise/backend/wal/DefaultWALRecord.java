package io.github.rawwise.backend.wal;

public class DefaultWALRecord implements WALRecord {
    private final long logSequenceNum;
    private final byte[] data;
    private final long timestamp;
    private final long commitId;
    private final String uniqTableIdentifier;
    private final String marker;

    public DefaultWALRecord(
            long logSequenceNum,
            byte[] data,
            long timestamp,
            long commitId,
            String uniqTableIdentifier,
            String marker) {
        this.logSequenceNum = logSequenceNum;
        this.data = data;
        this.timestamp = timestamp;
        this.commitId = commitId;
        this.uniqTableIdentifier = uniqTableIdentifier;
        this.marker = marker;
    }

    @Override
    public long sequence() {
        return logSequenceNum;
    }

    @Override
    public String marker() {
        return marker;
    }

    @Override
    public byte[] value() {
        return data;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public long commitId() {
        return commitId;
    }

    @Override
    public String uniqTableIdentifier() {
        return uniqTableIdentifier;
    }
}
