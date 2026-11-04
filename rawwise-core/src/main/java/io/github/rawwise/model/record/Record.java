package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public sealed interface Record extends SemanticRecord permits DefaultRecord, MapRecord {

    Key key();

    Header header();

    Body body();

    @Override
    default Raw raw() {
        return Raw.of(key(), header(), body());
    }

    @Override
    default List<Chunk> chunks() {
        return List.of();
    }

    @Override
    default List<View> views() {
        return List.of();
    }

    @Override
    default Optional<Semantic> semantics() {
        return Optional.empty();
    }

    @Override
    default Optional<Action> actions() {
        return Optional.empty();
    }

    sealed interface Key extends Serializable permits PrimaryKeyValue, Records.EncodedKey {
        byte[] encoded();
    }

    record Header(
            Key key,
            long tableId,
            long schemaVersion,
            long sequenceNumber,
            long commitTimestamp,
            Kind kind,
            boolean visible,
            boolean immutable,
            ImmutabilitySource immutableSource)
            implements Serializable {

        public static Header of(
                Key key,
                long tableId,
                long schemaVersion,
                long sequenceNumber,
                long commitTimestamp,
                Kind kind,
                boolean visible) {
            return new Header(
                    key,
                    tableId,
                    schemaVersion,
                    sequenceNumber,
                    commitTimestamp,
                    kind,
                    visible,
                    false,
                    ImmutabilitySource.DEFAULT);
        }
    }

    interface Body {
        Schema schema();

        Value value(String fieldName);

        Value value(FieldId fieldId);

        boolean has(String fieldName);

        boolean has(FieldId fieldId);

        default Value valueAt(int index) {
            return value(schema().fields().get(index).name());
        }

        default void evict() {}

        default Map<String, Object> toMap() {
            Map<String, Object> map = new java.util.LinkedHashMap<>();
            for (var field : schema().fields()) {
                Value v = value(field.name());
                map.put(field.name(), v.isNull() ? null : v.raw());
            }
            return map;
        }
    }

    record FieldId(int id, String name) {}

    interface Value {
        RawWiseType type();

        boolean isNull();

        Object raw();

        byte[] rawBytes();

        default Comparable<?> comparable() {
            Object r = raw();
            if (r == null) throw new ClassCastException("NULL value is not Comparable");
            if (r instanceof Comparable<?> c) return c;
            if (r instanceof byte[] bytes) return ComparableByteArray.wrap(bytes);
            throw new ClassCastException(
                    "Value of type " + type() + " is not Comparable: " + r.getClass());
        }
    }

    enum Kind {
        INSERT,
        UPDATE,
        DELETE
    }
}
