package io.github.rawwise.engine.storage.codec;

import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Schema;
import java.util.Map;

public interface ValueCodec {
    byte TAG_NULL = 0;
    byte TAG_VALUE = 1;
    byte TAG_TOMBSTONE = 2;

    byte[] encodeNull();

    byte[] encodeTombstone(long commitTimestamp);

    byte[] encodeValue(byte[] payload);

    byte extractTag(byte[] stored);

    byte[] extractPayload(byte[] stored);

    boolean isNull(byte[] stored);

    boolean isTombstone(byte[] stored);

    long decodeTombstoneTimestamp(byte[] stored);

    byte[] encodeFields(Schema schema, Map<String, Object> fields);

    byte[] encodeFieldsArray(Schema schema, Object[] values);

    Map<String, Record.Value> decodeFields(Schema schema, byte[] payload);

    Object[] decodeFieldsToArray(Schema schema, byte[] payload);

    byte[] encodePrimaryKey(Schema schema, Map<String, Object> primaryKeyValues);
}
