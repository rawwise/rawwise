package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public record PrimaryKey(List<String> fields) implements Serializable {

    public PrimaryKey {
        if (fields != null) {
            fields = List.copyOf(fields);
        } else {
            fields = List.of();
        }
    }

    public static PrimaryKey of(String... fieldNames) {
        return new PrimaryKey(List.of(fieldNames));
    }

    public static PrimaryKey of(List<String> fieldNames) {
        return new PrimaryKey(fieldNames);
    }

    public static PrimaryKey none() {
        return new PrimaryKey(List.of());
    }

    public static PrimaryKey fromSchema(Schema schema) {
        return schema.primaryKeyConstraint()
                .map(pk -> new PrimaryKey(new ArrayList<>(pk.fieldNames())))
                .orElse(none());
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }

    public int size() {
        return fields.size();
    }

    public boolean contains(String fieldName) {
        return fields.contains(fieldName);
    }

    public List<String> fields() {
        return Collections.unmodifiableList(fields);
    }

    @Override
    public String toString() {
        return "PK" + fields;
    }
}
