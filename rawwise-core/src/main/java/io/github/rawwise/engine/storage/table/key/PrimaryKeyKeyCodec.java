package io.github.rawwise.engine.storage.table.key;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PrimaryKeyKeyCodec implements KeyCodec {
    private final List<String> primaryKeyFields;
    private final KeyCodec delegate;

    public PrimaryKeyKeyCodec(List<String> primaryKeyFields) {
        this(primaryKeyFields, new DefaultKeyCodec());
    }

    public PrimaryKeyKeyCodec(List<String> primaryKeyFields, KeyCodec delegate) {
        Objects.requireNonNull(primaryKeyFields, "primaryKeyFields cannot be null");
        Objects.requireNonNull(delegate, "delegate cannot be null");
        if (primaryKeyFields.isEmpty()) {
            throw new IllegalArgumentException("primaryKeyFields cannot be empty");
        }
        this.primaryKeyFields = List.copyOf(primaryKeyFields);
        this.delegate = delegate;
    }

    @Override
    public byte[] encodePrimaryKey(Map<String, Comparable> primaryKeyValues) throws IOException {
        Objects.requireNonNull(primaryKeyValues, "primaryKeyValues cannot be null");
        if (primaryKeyValues.isEmpty()) {
            throw new IllegalArgumentException("Primary key values cannot be empty");
        }
        LinkedHashMap<String, Comparable> orderedValues = new LinkedHashMap<>();
        for (String field : primaryKeyFields) {
            if (!primaryKeyValues.containsKey(field)) {
                throw new IllegalArgumentException("Missing primary key field: " + field);
            }
            orderedValues.put(field, primaryKeyValues.get(field));
        }
        if (primaryKeyValues.size() != orderedValues.size()) {
            throw new IllegalArgumentException(
                    "Primary key values contain unexpected fields: " + primaryKeyValues.keySet());
        }
        return delegate.encodePrimaryKey(orderedValues);
    }
}
