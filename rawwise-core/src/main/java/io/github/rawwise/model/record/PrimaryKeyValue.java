package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * A schema-aware primary key value that binds {@link Primary Key} field names to their runtime
 * values, before encoding to storage bytes.
 *
 * <p>Implements {@link Record.Key} so it can be passed wherever a storage key is expected. Call
 * {@link #encode()} to produce an {@link Records.Encoded Key} for storage operations.
 */
public record PrimaryKeyValue(PrimaryKey keyDef, Map<String, Comparable> values)
        implements Record.Key, Serializable {

    public PrimaryKeyValue {
        Objects.requireNonNull(keyDef, "keyDef");
        Objects.requireNonNull(values, "values");
        if (values.isEmpty() && !keyDef.isEmpty()) {
            throw new IllegalArgumentException("values must not be empty when keyDef is non-empty");
        }
    }

    public static PrimaryKeyValue of(PrimaryKey keyDef, Map<String, Comparable> values) {
        return new PrimaryKeyValue(keyDef, values);
    }

    public static PrimaryKeyValue of(String field, Comparable value) {
        return new PrimaryKeyValue(PrimaryKey.of(field), Map.of(field, value));
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    /** Encode this primary key value to a storage key using the given codec. */
    public Records.EncodedKey encode(io.github.rawwise.engine.storage.table.key.KeyCodec codec) {
        try {
            return new Records.EncodedKey(codec.encodePrimaryKey(values));
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to encode primary key", e);
        }
    }

    @Override
    public byte[] encoded() {
        throw new UnsupportedOperationException(
                "PrimaryKeyValue.encoded() is not supported — use encode(KeyCodec) to produce an EncodedKey");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrimaryKeyValue that)) return false;
        return keyDef.equals(that.keyDef) && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyDef, values);
    }
}
