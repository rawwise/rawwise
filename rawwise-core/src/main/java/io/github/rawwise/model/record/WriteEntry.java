package io.github.rawwise.model.record;

import io.github.rawwise.engine.storage.codec.ValueCodec;

public record WriteEntry(byte[] key, byte[] encodedValue, boolean valuePreEncoded) {

    public WriteEntry(byte[] key, byte[] encodedValue) {
        this(key, encodedValue, false);
    }

    public static WriteEntry fromRecord(Record record, RecordCodec recordCodec) {
        byte[] key = record.key().encoded();
        byte[] encoded = recordCodec.encode(record);
        return new WriteEntry(key, encoded, true);
    }

    public static WriteEntry tombstone(byte[] key, long commitTimestamp, ValueCodec valueCodec) {
        byte[] encoded = valueCodec.encodeTombstone(commitTimestamp);
        return new WriteEntry(key, encoded, true);
    }
}
