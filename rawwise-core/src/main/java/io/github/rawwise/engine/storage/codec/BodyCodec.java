package io.github.rawwise.engine.storage.codec;

import io.github.rawwise.model.record.Schema;
import java.util.Map;

/**
 * Pluggable row body encoding for Record payloads.
 *
 * <p>A {@code BodyCodec} defines an alternative strategy for encoding/decoding row body bytes
 * within a Record. Unlike the default ValueCodec which decodes ALL fields, a BodyCodec can support
 * lazy field-level access ({@link #decodeField}) to avoid decoding unused columns during
 * projection-heavy workloads.
 *
 * <p>The codec name returned by {@link #name()} is stored in {@code TableMeta.properties} under key
 * {@code "body.codec"} and persisted into Record flags at write time.
 */
public interface BodyCodec {

    /**
     * Unique name for this codec, used as the key in {@code TableMeta.properties} ({@code
     * body.codec=<name>}) and within {@link BodyCodecRegistry} for lookup.
     */
    String name();

    /**
     * Encode a complete row of field values into a byte array.
     *
     * @param schema the table schema at the time of encoding
     * @param fields field name to value mapping
     * @return encoded body bytes
     */
    byte[] encode(Schema schema, Map<String, Object> fields);

    /**
     * Decode all fields from the encoded body bytes.
     *
     * @param schema the resolved table schema
     * @param data encoded body bytes
     * @return field name to value mapping for all columns
     */
    Map<String, Object> decode(Schema schema, byte[] data);

    /**
     * Decode a single field from the encoded body bytes without decoding other fields. This is the
     * key optimization for projection pushdown.
     *
     * @param schema the resolved table schema
     * @param data encoded body bytes
     * @param fieldName the name of the field to decode
     * @return the decoded value, or null if the field is null or not found
     */
    Object decodeField(Schema schema, byte[] data, String fieldName);
}
