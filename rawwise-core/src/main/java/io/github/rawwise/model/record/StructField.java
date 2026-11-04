package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public record StructField(
        int fieldId,
        String name,
        RawWiseType dataType,
        String comment,
        Map<String, String> metadata)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    public Optional<String> defaultValue() {
        if (metadata != null && metadata.containsKey("default")) {
            String val = metadata.get("default");
            return (val != null && !val.isEmpty()) ? Optional.of(val) : Optional.empty();
        }
        return Optional.empty();
    }

    public static final int UNASSIGNED_FIELD_ID = 0;

    public StructField {
        if (fieldId != UNASSIGNED_FIELD_ID && fieldId < 0)
            throw new IllegalArgumentException(
                    "fieldId must be UNASSIGNED_FIELD_ID(0) or positive, got: " + fieldId);
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("Field name cannot be empty");
        if (dataType == null) throw new IllegalArgumentException("Field data type cannot be null");
        comment = comment != null ? comment : "";
        metadata = metadata != null ? Map.copyOf(metadata) : Map.of();
    }

    public StructField(String name, RawWiseType dataType) {
        this(UNASSIGNED_FIELD_ID, name, dataType, "", Map.of());
    }

    public StructField(String name, RawWiseType dataType, String comment) {
        this(UNASSIGNED_FIELD_ID, name, dataType, comment, Map.of());
    }

    public StructField(
            String name, RawWiseType dataType, String comment, Map<String, String> metadata) {
        this(UNASSIGNED_FIELD_ID, name, dataType, comment, metadata);
    }

    public StructField withComment(String newComment) {
        return new StructField(fieldId, name, dataType, newComment, metadata);
    }

    public StructField withMetadata(String key, String value) {
        Map<String, String> newMetadata = new HashMap<>(metadata);
        newMetadata.put(key, value);
        return new StructField(fieldId, name, dataType, comment, newMetadata);
    }

    public StructField withFieldId(int newFieldId) {
        if (newFieldId < 0) {
            throw new IllegalArgumentException("fieldId must be non-negative, got: " + newFieldId);
        }
        return new StructField(newFieldId, name, dataType, comment, metadata);
    }

    public StructField withName(String newName) {
        if (newName == null || newName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be empty");
        }
        return new StructField(fieldId, newName, dataType, comment, metadata);
    }

    public StructField withDataType(RawWiseType newDataType) {
        if (newDataType == null) {
            throw new IllegalArgumentException("Field data type cannot be null");
        }
        return new StructField(fieldId, name, newDataType, comment, metadata);
    }

    public boolean hasAssignedFieldId() {
        return fieldId != UNASSIGNED_FIELD_ID;
    }
}
