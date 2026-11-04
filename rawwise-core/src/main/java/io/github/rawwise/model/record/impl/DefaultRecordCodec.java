package io.github.rawwise.model.record.impl;

import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.codec.ValueCodec;
import io.github.rawwise.model.record.CgEncodedBody;
import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.DefaultRecord;
import io.github.rawwise.model.record.ImmutabilitySource;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;

public class DefaultRecordCodec implements RecordCodec {
    private static final byte[] MAGIC = {0x52, 0x45, 0x43};
    private static final byte VERSION = 0x10;
    private static final int FLAG_HAS_SCHEMA_REF = 0x01;
    private static final int FLAG_HAS_KEY = 0x02;
    public static final int FLAG_BODY_CODEC = 0x04;
    private static final int FLAG_COMPACT_DELETE = 0x08;
    private static final int FLAG_KEY_IN_STORAGE = 0x10;
    public static final int FLAG_HAS_IMMUTABLE = 0x20;

    public static final int MAGIC_LEN = 3;
    public static final int VERSION_LEN = 1;
    public static final int FLAGS_LEN = 1;
    public static final int KEY_LEN_FIELD_SIZE = 4;
    private static final int HEADER_PREFIX =
            MAGIC_LEN + VERSION_LEN + FLAGS_LEN + KEY_LEN_FIELD_SIZE;
    public static final int HEADER_PREFIX_MIN =
            MAGIC_LEN + VERSION_LEN + FLAGS_LEN + KEY_LEN_FIELD_SIZE;
    private static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;

    private static final XXHash64 HASHER = XXHashFactory.fastestInstance().hash64();

    private final ValueCodec valueCodec;
    private final BodyCodec bodyCodec;

    public DefaultRecordCodec(ValueCodec valueCodec) {
        this(valueCodec, null);
    }

    public DefaultRecordCodec(ValueCodec valueCodec, BodyCodec bodyCodec) {
        this.valueCodec = valueCodec;
        this.bodyCodec = bodyCodec;
    }

    /** Returns the configured Body Codec, may be null if body codec is not active. */
    public BodyCodec getBodyCodec() {
        return bodyCodec;
    }

    @Override
    public boolean isRecordCodecFormat(byte[] data) {
        return isRecordCodecFormatStatic(data);
    }

    public static boolean isRecordCodecFormatStatic(byte[] data) {
        return data != null
                && data.length >= 3
                && data[0] == MAGIC[0]
                && data[1] == MAGIC[1]
                && data[2] == MAGIC[2];
    }

    @Override
    public byte[] encode(Record record) {
        Record.Header header = record.header();
        byte[] keyBytes = record.key().encoded();
        boolean isCompactDelete = header.kind() == Record.Kind.DELETE;

        int flags = FLAG_HAS_SCHEMA_REF | FLAG_HAS_KEY;
        if (isCompactDelete) flags |= FLAG_COMPACT_DELETE;
        boolean useBodyCodec = bodyCodec != null && !isCompactDelete;
        if (useBodyCodec) flags |= FLAG_BODY_CODEC;
        flags |= FLAG_HAS_IMMUTABLE;

        byte[] bodyBytes = null;
        if (!isCompactDelete) {
            if (useBodyCodec) {
                bodyBytes =
                        bodyCodec.encode(record.body().schema(), extractValuesMap(record.body()));
            } else {
                bodyBytes =
                        valueCodec.encodeFieldsArray(
                                record.body().schema(), extractValuesArray(record.body()));
            }
        }
        int bodySize = (bodyBytes != null) ? bodyBytes.length : 0;

        int headerSize = HEADER_PREFIX + keyBytes.length + 8 + 8 + 8 + 8 + 1 + 1 + 1 + 1;
        int total = headerSize + bodySize + 8;

        ByteBuffer buf = ByteBuffer.allocate(total).order(BYTE_ORDER);
        buf.put(MAGIC);
        buf.put(VERSION);
        buf.put((byte) flags);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putLong(header.tableId());
        buf.putLong(header.schemaVersion());
        buf.putLong(header.sequenceNumber());
        buf.putLong(header.commitTimestamp());
        buf.put((byte) header.kind().ordinal());
        buf.put((byte) (header.visible() ? 1 : 0));
        buf.put((byte) (header.immutable() ? 1 : 0));
        buf.put((byte) header.immutableSource().ordinal());
        if (bodyBytes != null) buf.put(bodyBytes);

        long checksum = HASHER.hash(buf.array(), 0, buf.position(), 0x97C3BE15L);
        buf.putLong(checksum);

        return Arrays.copyOf(buf.array(), buf.position());
    }

    @Override
    public Record decode(byte[] data, RecordCodec.SchemaResolver schemaResolver) {
        if (data == null || data.length < 5) {
            throw new IllegalArgumentException("Data too short for Record header");
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);

        byte[] magic = new byte[3];
        buf.get(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new IllegalArgumentException("Invalid Record magic bytes");
        }

        byte version = buf.get();
        if ((version >> 4) > (VERSION >> 4)) {
            throw new UnsupportedOperationException(
                    "Unsupported Record format version: " + version);
        }

        int flags = buf.get() & 0xFF;
        boolean compactDelete = (flags & FLAG_COMPACT_DELETE) != 0;
        boolean useBodyCodec = (flags & FLAG_BODY_CODEC) != 0;
        boolean hasImmutable = (flags & FLAG_HAS_IMMUTABLE) != 0;

        int keyLen = buf.getInt();
        if (keyLen < 0 || keyLen > data.length) {
            throw new IllegalArgumentException("Invalid key length: " + keyLen);
        }
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);

        long tableId = buf.getLong();
        long schemaVersion = buf.getLong();
        long sequenceNumber = buf.getLong();
        long commitTimestamp = buf.getLong();

        int kindOrdinal = buf.get() & 0xFF;
        Record.Kind[] kinds = Record.Kind.values();
        if (kindOrdinal >= kinds.length) {
            throw new IllegalArgumentException("Invalid Record.Kind ordinal: " + kindOrdinal);
        }
        Record.Kind kind = kinds[kindOrdinal];
        boolean visible = buf.get() == 1;

        boolean immutable = false;
        ImmutabilitySource immutableSource = ImmutabilitySource.DEFAULT;
        if (hasImmutable) {
            immutable = buf.get() == 1;
            int srcOrdinal = buf.get() & 0xFF;
            ImmutabilitySource[] sources = ImmutabilitySource.values();
            immutableSource =
                    (srcOrdinal < sources.length)
                            ? sources[srcOrdinal]
                            : ImmutabilitySource.DEFAULT;
        }

        Record.Key key = Records.keyOf(keyBytes);
        Schema schema = schemaResolver.resolve(tableId, schemaVersion);

        Record.Body body;
        if (compactDelete) {
            body = new DefaultBody(schema, new Object[0]);
        } else {
            int bodyStart = buf.position();
            int checksumEnd = data.length - 8;
            int bodyLen = checksumEnd - bodyStart;
            if (bodyLen < 0) {
                throw new IllegalArgumentException("Record body length is negative: " + bodyLen);
            }
            byte[] bodyBytes = new byte[bodyLen];
            buf.get(bodyBytes);
            if (useBodyCodec && bodyCodec != null) {
                body = new CgEncodedBody(schema, bodyCodec, bodyBytes);
            } else {
                body = new DefaultBody(schema, bodyBytes, valueCodec);
            }
        }

        Record.Header header =
                new Record.Header(
                        key,
                        tableId,
                        schemaVersion,
                        sequenceNumber,
                        commitTimestamp,
                        kind,
                        visible,
                        immutable,
                        immutableSource);

        long storedChecksum = buf.getLong();
        long computedChecksum = HASHER.hash(data, 0, data.length - 8, 0x97C3BE15L);
        if (storedChecksum != computedChecksum) {
            throw new IllegalArgumentException(
                    "Record checksum mismatch: expected "
                            + storedChecksum
                            + ", got "
                            + computedChecksum);
        }

        return new DefaultRecord(key, body, header);
    }

    @Override
    public byte[] extractKey(byte[] data) {
        if (!isRecordCodecFormatStatic(data)) {
            throw new IllegalArgumentException("Data does not have RecordCodec MAGIC prefix");
        }
        if (data.length < HEADER_PREFIX) {
            throw new IllegalArgumentException("Data too short for Record header");
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        buf.position(HEADER_PREFIX - KEY_LEN_FIELD_SIZE);
        int keyLen = buf.getInt();
        validateKeyLen(keyLen, data.length);
        byte[] key = new byte[keyLen];
        buf.get(key);
        return key;
    }

    @Override
    public long extractSequenceNumber(byte[] data) {
        if (!isRecordCodecFormatStatic(data)) {
            throw new IllegalArgumentException("Data does not have RecordCodec MAGIC prefix");
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        buf.position(HEADER_PREFIX - KEY_LEN_FIELD_SIZE);
        int keyLen = buf.getInt();
        validateKeyLen(keyLen, data.length);
        buf.position(HEADER_PREFIX + keyLen + 8 + 8);
        return buf.getLong();
    }

    @Override
    public long extractCommitTimestamp(byte[] data) {
        if (!isRecordCodecFormatStatic(data)) {
            throw new IllegalArgumentException("Data does not have RecordCodec MAGIC prefix");
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        buf.position(HEADER_PREFIX - KEY_LEN_FIELD_SIZE);
        int keyLen = buf.getInt();
        validateKeyLen(keyLen, data.length);
        buf.position(HEADER_PREFIX + keyLen + 8 + 8 + 8);
        return buf.getLong();
    }

    @Override
    public Record.Kind extractKind(byte[] data) {
        if (!isRecordCodecFormatStatic(data)) {
            throw new IllegalArgumentException("Data does not have RecordCodec MAGIC prefix");
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        buf.position(HEADER_PREFIX - KEY_LEN_FIELD_SIZE);
        int keyLen = buf.getInt();
        validateKeyLen(keyLen, data.length);
        buf.position(HEADER_PREFIX + keyLen + 8 + 8 + 8 + 8);
        int kindOrdinal = buf.get() & 0xFF;
        Record.Kind[] kinds = Record.Kind.values();
        if (kindOrdinal >= kinds.length) {
            throw new IllegalArgumentException("Invalid Record.Kind ordinal: " + kindOrdinal);
        }
        return kinds[kindOrdinal];
    }

    @Override
    public boolean extractVisibility(byte[] data) {
        if (!isRecordCodecFormatStatic(data)) {
            throw new IllegalArgumentException("Data does not have RecordCodec MAGIC prefix");
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        buf.position(HEADER_PREFIX - KEY_LEN_FIELD_SIZE);
        int keyLen = buf.getInt();
        validateKeyLen(keyLen, data.length);
        buf.position(HEADER_PREFIX + keyLen + 8 + 8 + 8 + 8 + 1);
        return buf.get() == 1;
    }

    @Override
    public int extractFlags(byte[] data) {
        if (!isRecordCodecFormatStatic(data)) {
            throw new IllegalArgumentException("Data does not have RecordCodec MAGIC prefix");
        }
        if (data.length <= MAGIC_LEN + VERSION_LEN) {
            throw new IllegalArgumentException("Data too short: no flags field");
        }
        return data[MAGIC_LEN + VERSION_LEN] & 0xFF;
    }

    public static boolean extractImmutableStatic(byte[] data) {
        if (!isRecordCodecFormatStatic(data) || data.length <= MAGIC_LEN + VERSION_LEN) {
            return false;
        }
        int flags = data[MAGIC_LEN + VERSION_LEN] & 0xFF;
        if ((flags & FLAG_HAS_IMMUTABLE) == 0) {
            return false;
        }
        try {
            ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
            buf.position(HEADER_PREFIX - KEY_LEN_FIELD_SIZE);
            int keyLen = buf.getInt();
            validateKeyLen(keyLen, data.length);
            int immutablePos = HEADER_PREFIX + keyLen + 8 + 8 + 8 + 8 + 1 + 1;
            if (data.length <= immutablePos) {
                return false;
            }
            buf.position(immutablePos);
            return buf.get() == 1;
        } catch (Exception e) {
            return false;
        }
    }

    public static ImmutabilitySource extractImmutabilitySourceStatic(byte[] data) {
        if (!isRecordCodecFormatStatic(data) || data.length <= MAGIC_LEN + VERSION_LEN) {
            return ImmutabilitySource.DEFAULT;
        }
        int flags = data[MAGIC_LEN + VERSION_LEN] & 0xFF;
        if ((flags & FLAG_HAS_IMMUTABLE) == 0) {
            return ImmutabilitySource.DEFAULT;
        }
        try {
            ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
            buf.position(HEADER_PREFIX - KEY_LEN_FIELD_SIZE);
            int keyLen = buf.getInt();
            validateKeyLen(keyLen, data.length);
            int sourcePos = HEADER_PREFIX + keyLen + 8 + 8 + 8 + 8 + 1 + 1 + 1;
            if (data.length <= sourcePos) {
                return ImmutabilitySource.DEFAULT;
            }
            buf.position(sourcePos);
            int srcOrdinal = buf.get() & 0xFF;
            ImmutabilitySource[] sources = ImmutabilitySource.values();
            return (srcOrdinal < sources.length) ? sources[srcOrdinal] : ImmutabilitySource.DEFAULT;
        } catch (Exception e) {
            return ImmutabilitySource.DEFAULT;
        }
    }

    private static void validateKeyLen(int keyLen, int dataLength) {
        if (keyLen < 0 || keyLen > dataLength - HEADER_PREFIX) {
            throw new IllegalArgumentException(
                    "Invalid key length: " + keyLen + " for data length " + dataLength);
        }
    }

    public record ExtractedHeader(
            byte[] keyBytes,
            long tableId,
            long schemaVersion,
            long sequenceNumber,
            long commitTimestamp,
            Record.Kind kind,
            boolean visible,
            boolean immutable,
            ImmutabilitySource immutableSource,
            int bodyOffset,
            int checksumEnd) {}

    public static ExtractedHeader extractHeaderStatic(byte[] data) {
        if (!isRecordCodecFormatStatic(data) || data.length < HEADER_PREFIX_MIN) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(BYTE_ORDER);
        buf.position(MAGIC_LEN + VERSION_LEN + FLAGS_LEN);
        int keyLen = buf.getInt();
        validateKeyLen(keyLen, data.length);
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);

        long tableId = buf.getLong();
        long schemaVersion = buf.getLong();
        long sequenceNumber = buf.getLong();
        long commitTimestamp = buf.getLong();

        int kindOrdinal = buf.get() & 0xFF;
        Record.Kind[] kinds = Record.Kind.values();
        Record.Kind kind = (kindOrdinal < kinds.length) ? kinds[kindOrdinal] : Record.Kind.INSERT;
        boolean visible = buf.get() == 1;

        int flags = data[MAGIC_LEN + VERSION_LEN] & 0xFF;
        boolean hasImmutable = (flags & FLAG_HAS_IMMUTABLE) != 0;
        boolean immutable = false;
        ImmutabilitySource immutableSource = ImmutabilitySource.DEFAULT;
        if (hasImmutable) {
            immutable = buf.get() == 1;
            int srcOrdinal = buf.get() & 0xFF;
            ImmutabilitySource[] sources = ImmutabilitySource.values();
            immutableSource =
                    (srcOrdinal < sources.length)
                            ? sources[srcOrdinal]
                            : ImmutabilitySource.DEFAULT;
        }

        int bodyOffset = buf.position();
        int checksumEnd = data.length - 8;

        return new ExtractedHeader(
                keyBytes,
                tableId,
                schemaVersion,
                sequenceNumber,
                commitTimestamp,
                kind,
                visible,
                immutable,
                immutableSource,
                bodyOffset,
                checksumEnd);
    }

    /**
     * Peek commit Timestamp from raw record bytes (already unwrapped, not Nullable Value
     * Codec-wrapped).
     *
     * @param raw Record Bytes unwrapped record bytes
     * @return commit Timestamp from the header
     * @throws Illegal Argument Exception if data is not in Record Codec format or is malformed
     */
    public static long peekCommitTimestamp(byte[] rawRecordBytes) {
        if (!isRecordCodecFormatStatic(rawRecordBytes)
                || rawRecordBytes.length < HEADER_PREFIX_MIN) {
            throw new IllegalArgumentException(
                    "Data does not have RecordCodec MAGIC prefix or is too short");
        }
        ByteBuffer buf = ByteBuffer.wrap(rawRecordBytes).order(BYTE_ORDER);
        buf.position(MAGIC_LEN + VERSION_LEN + FLAGS_LEN);
        int keyLen = buf.getInt();
        validateKeyLen(keyLen, rawRecordBytes.length);
        buf.position(buf.position() + keyLen + 8 + 8 + 8);
        return buf.getLong();
    }

    private Object[] extractValuesArray(Record.Body body) {
        if (body instanceof DefaultBody db) {
            return db.getRawValues();
        }
        if (body instanceof CgEncodedBody cgb) {
            return cgb.getRawValues();
        }
        Object[] arr = new Object[body.schema().fields().size()];
        for (int i = 0; i < arr.length; i++) {
            Record.Value v = body.valueAt(i);
            arr[i] = v.isNull() ? null : v.raw();
        }
        return arr;
    }

    private Map<String, Object> extractValuesMap(Record.Body body) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (var field : body.schema().fields()) {
            Record.Value v = body.value(field.name());
            map.put(field.name(), v.isNull() ? null : v.raw());
        }
        return map;
    }
}
