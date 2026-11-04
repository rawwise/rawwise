package io.github.rawwise.engine.storage.codec;

import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.DefaultRecord;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

@Slf4j
public final class RecordDecoder {

    private static final DecodeResult NOT_FOUND = new DecodeResult.NotFound();
    private static final DecodeResult TOMBSTONE = new DecodeResult.Tombstone();
    private static final XXHash64 HASHER = XXHashFactory.fastestInstance().hash64();

    private RecordDecoder() {}

    public static DecodeResult decodeNullableWrapped(
            byte[] nullableWrappedBytes,
            RecordCodec recordCodec,
            RecordCodec.SchemaResolver schemaResolver) {
        if (nullableWrappedBytes == null) {
            return NOT_FOUND;
        }
        if (NullableValueCodec.isTombstone(nullableWrappedBytes)) {
            return TOMBSTONE;
        }
        byte[] decoded = NullableValueCodec.decode(nullableWrappedBytes);
        if (decoded == null) {
            return NOT_FOUND;
        }
        if (!recordCodec.isRecordCodecFormat(decoded)) {
            log.warn(
                    "RecordDecoder: bytes not in RecordCodec format after NullableValueCodec.decode, skipping");
            return NOT_FOUND;
        }
        try {
            Record record = recordCodec.decode(decoded, schemaResolver);
            return new DecodeResult.Found(record, nullableWrappedBytes);
        } catch (Exception e) {
            log.warn(
                    "RecordDecoder: failed to decode record, treating as not found: {}",
                    e.getMessage());
            return NOT_FOUND;
        }
    }

    /**
     * Decode a NullableValueCodec-wrapped byte array into a projected Record.
     *
     * <p>When the body is encoded with a BodyCodec (FLAG_BODY_CODEC set), this method uses {@link
     * BodyCodec#decodeField} to decode only the projected fields, avoiding full row decode
     * overhead. Falls back to full decode + projectCopy when the flag is not set.
     *
     * @param nullableWrappedBytes the NullableValueCodec-wrapped Record bytes
     * @param recordCodec the RecordCodec for flag extraction
     * @param schemaResolver resolves Schema from (tableId, schemaVersion)
     * @param bodyCodec the BodyCodec for per-field lazy decode (may be null)
     * @param projections the set of projected column names
     * @return decode result with a Record containing only projected fields
     */
    public static DecodeResult decodeNullableWrappedProjected(
            byte[] nullableWrappedBytes,
            RecordCodec recordCodec,
            RecordCodec.SchemaResolver schemaResolver,
            BodyCodec bodyCodec,
            List<String> projections) {
        if (nullableWrappedBytes == null) {
            return NOT_FOUND;
        }
        if (NullableValueCodec.isTombstone(nullableWrappedBytes)) {
            return TOMBSTONE;
        }
        byte[] decoded = NullableValueCodec.decode(nullableWrappedBytes);
        if (decoded == null) {
            return NOT_FOUND;
        }
        if (!recordCodec.isRecordCodecFormat(decoded)) {
            log.warn(
                    "RecordDecoder: bytes not in RecordCodec format after NullableValueCodec.decode, skipping");
            return NOT_FOUND;
        }

        // Check FLAG_BODY_CODEC for projection optimization
        int flags = recordCodec.extractFlags(decoded);
        boolean useBodyCodec = (flags & DefaultRecordCodec.FLAG_BODY_CODEC) != 0;

        if (useBodyCodec && bodyCodec != null && projections != null && !projections.isEmpty()) {
            return decodeProjectedWithBodyCodec(
                    decoded, schemaResolver, bodyCodec, projections, nullableWrappedBytes);
        }

        // Fallback: full decode + projectCopy
        try {
            Record record = recordCodec.decode(decoded, schemaResolver);
            if (projections != null && !projections.isEmpty()) {
                Record projected = Records.projectCopy(record, projections);
                record.body().evict();
                return new DecodeResult.Found(projected, nullableWrappedBytes);
            }
            return new DecodeResult.Found(record, nullableWrappedBytes);
        } catch (Exception e) {
            log.warn("RecordDecoder: failed to decode projected record: {}", e.getMessage());
            return NOT_FOUND;
        }
    }

    /** Decode only projected fields using BodyCodec.decodeField(). */
    private static DecodeResult decodeProjectedWithBodyCodec(
            byte[] data,
            RecordCodec.SchemaResolver schemaResolver,
            BodyCodec bodyCodec,
            List<String> projections,
            byte[] sourceBytes) {
        // Extract header fields for Record reconstruction
        if (data.length < DefaultRecordCodec.HEADER_PREFIX_MIN) {
            log.warn("RecordDecoder: data too short for header extraction in projection path");
            return NOT_FOUND;
        }

        try {
            java.nio.ByteBuffer buf =
                    java.nio.ByteBuffer.wrap(data).order(java.nio.ByteOrder.BIG_ENDIAN);
            buf.position(
                    DefaultRecordCodec.MAGIC_LEN
                            + DefaultRecordCodec.VERSION_LEN
                            + DefaultRecordCodec.FLAGS_LEN
                            + DefaultRecordCodec.KEY_LEN_FIELD_SIZE);

            byte[] keyBytes =
                    new byte[buf.getInt(buf.position() - DefaultRecordCodec.KEY_LEN_FIELD_SIZE)];
            buf.get(keyBytes);

            long tableId = buf.getLong();
            long schemaVersion = buf.getLong();
            long sequenceNumber = buf.getLong();
            long commitTimestamp = buf.getLong();

            int kindOrdinal = buf.get() & 0xFF;
            Record.Kind[] kinds = Record.Kind.values();
            Record.Kind kind =
                    (kindOrdinal < kinds.length) ? kinds[kindOrdinal] : Record.Kind.INSERT;
            boolean visible = buf.get() == 1;

            // Extract body bytes
            int bodyStart = buf.position();
            int checksumEnd = data.length - 8;
            if (checksumEnd < bodyStart) {
                return NOT_FOUND;
            }

            // Validate xxHash64 checksum (same as DefaultRecordCodec.decode does)
            java.nio.ByteBuffer storedChecksumBuf =
                    java.nio.ByteBuffer.wrap(data, checksumEnd, 8)
                            .order(java.nio.ByteOrder.BIG_ENDIAN);
            long storedChecksum = storedChecksumBuf.getLong();
            long computedChecksum = HASHER.hash(data, 0, checksumEnd, 0x97C3BE15L);
            if (storedChecksum != computedChecksum) {
                log.warn(
                        "RecordDecoder: checksum mismatch in projected decode path: "
                                + "expected {}, got {}",
                        storedChecksum,
                        computedChecksum);
                return NOT_FOUND;
            }

            int bodyLen = checksumEnd - bodyStart;
            if (bodyLen <= 0) {
                return NOT_FOUND;
            }
            byte[] bodyBytes = new byte[bodyLen];
            java.lang.System.arraycopy(data, bodyStart, bodyBytes, 0, bodyLen);

            Schema schema = schemaResolver.resolve(tableId, schemaVersion);
            if (schema == null) {
                log.warn(
                        "RecordDecoder: failed to resolve schema for tableId={}, version={}",
                        tableId,
                        schemaVersion);
                return NOT_FOUND;
            }

            // Build projected field values
            Map<String, Object> projectedValues = new LinkedHashMap<>();
            for (String proj : projections) {
                Object value = bodyCodec.decodeField(schema, bodyBytes, proj);
                projectedValues.put(proj, value);
            }

            Record.Key key = Records.keyOf(keyBytes);
            Record.Header header =
                    Record.Header.of(
                            key,
                            tableId,
                            schemaVersion,
                            sequenceNumber,
                            commitTimestamp,
                            kind,
                            visible);

            // Build a projected Schema with only the requested fields
            Schema projectedSchema = schema.project(projections);

            // Use DefaultBody with projected values (already decoded, no lazy decode needed)
            Object[] valuesArray = new Object[projectedSchema.fields().size()];
            for (int i = 0; i < projectedSchema.fields().size(); i++) {
                valuesArray[i] = projectedValues.get(projectedSchema.fields().get(i).name());
            }
            Record.Body body = new DefaultBody(projectedSchema, valuesArray);

            Record record = new DefaultRecord(key, body, header);
            return new DecodeResult.Found(record, sourceBytes);
        } catch (Exception e) {
            log.warn("RecordDecoder: projection decode failed: {}", e.getMessage());
            return NOT_FOUND;
        }
    }

    public static DecodeResult decodeRaw(
            byte[] rawBytes, RecordCodec recordCodec, RecordCodec.SchemaResolver schemaResolver) {
        if (rawBytes == null) {
            return NOT_FOUND;
        }
        if (!recordCodec.isRecordCodecFormat(rawBytes)) {
            log.warn("RecordDecoder: raw bytes not in RecordCodec format, skipping");
            return NOT_FOUND;
        }
        try {
            Record record = recordCodec.decode(rawBytes, schemaResolver);
            return new DecodeResult.Found(record, null);
        } catch (Exception e) {
            log.warn(
                    "RecordDecoder: failed to decode raw record, treating as not found: {}",
                    e.getMessage());
            return NOT_FOUND;
        }
    }

    public sealed interface DecodeResult {
        boolean found();

        boolean isTombstone();

        record NotFound() implements DecodeResult {
            @Override
            public boolean found() {
                return false;
            }

            @Override
            public boolean isTombstone() {
                return false;
            }
        }

        record Tombstone() implements DecodeResult {
            @Override
            public boolean found() {
                return false;
            }

            @Override
            public boolean isTombstone() {
                return true;
            }
        }

        record Found(Record record, byte[] sourceBytes) implements DecodeResult {
            @Override
            public boolean found() {
                return true;
            }

            @Override
            public boolean isTombstone() {
                return false;
            }
        }
    }
}
