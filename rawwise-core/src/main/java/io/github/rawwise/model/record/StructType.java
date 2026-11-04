package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public record StructType(List<StructField> fields, boolean isNullable, String description)
        implements ComplexType, Serializable {

    private static final long serialVersionUID = 1L;

    public StructType {
        if (fields == null || fields.isEmpty())
            throw new IllegalArgumentException("Struct must have at least one field");
        Set<String> fieldNames = new HashSet<>();
        Set<Integer> assignedFieldIds = new HashSet<>();
        for (StructField field : fields) {
            if (!fieldNames.add(field.name())) {
                throw new IllegalArgumentException("Duplicate field name: " + field.name());
            }
            if (field.hasAssignedFieldId() && !assignedFieldIds.add(field.fieldId())) {
                throw new IllegalArgumentException(
                        "Duplicate field id: "
                                + field.fieldId()
                                + " on field '"
                                + field.name()
                                + "'");
            }
        }
        fields = List.copyOf(fields);
        description =
                description != null ? description : "Struct with " + fields.size() + " fields";
    }

    public StructType(List<StructField> fields) {
        this(fields, true, null);
    }

    public String name() {
        return "struct<"
                + fields.stream()
                        .map(f -> f.name() + ": " + f.dataType().name())
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("")
                + ">";
    }

    public int id() {
        return 102;
    }

    public <T> T accept(TypeVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public RawWiseType copyWithNullable(boolean nullable) {
        return new StructType(fields, nullable, description);
    }

    public Optional<StructField> field(String name) {
        return fields.stream().filter(f -> f.name().equals(name)).findFirst();
    }

    public RawWiseType fieldType(String name) {
        return field(name)
                .map(StructField::dataType)
                .orElseThrow(() -> new IllegalArgumentException("Field not found: " + name));
    }

    public boolean hasField(String name) {
        return fields.stream().anyMatch(f -> f.name().equals(name));
    }

    public Optional<StructField> findFieldById(int fieldId) {
        return fields.stream()
                .filter(f -> f.hasAssignedFieldId() && f.fieldId() == fieldId)
                .findFirst();
    }

    public StructType addField(StructField field) {
        if (field == null) throw new IllegalArgumentException("Field cannot be null");
        if (hasField(field.name())) {
            throw new IllegalArgumentException("Field already exists: " + field.name());
        }
        if (field.hasAssignedFieldId()) {
            Optional<StructField> existing = findFieldById(field.fieldId());
            if (existing.isPresent()) {
                throw new IllegalArgumentException(
                        "Field id already assigned: "
                                + field.fieldId()
                                + " (existing field: '"
                                + existing.get().name()
                                + "')");
            }
        }
        List<StructField> newFields = new ArrayList<>(fields);
        newFields.add(field);
        return new StructType(newFields, isNullable, description);
    }

    public StructType dropField(String name) {
        if (name == null) throw new IllegalArgumentException("Field name cannot be null");
        if (!hasField(name)) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        if (fields.size() == 1) {
            throw new IllegalArgumentException("Cannot drop the last field: " + name);
        }
        List<StructField> newFields = fields.stream().filter(f -> !f.name().equals(name)).toList();
        return new StructType(newFields, isNullable, description);
    }

    public StructType renameField(String oldName, String newName) {
        if (oldName == null) throw new IllegalArgumentException("Old field name cannot be null");
        if (newName == null) throw new IllegalArgumentException("New field name cannot be null");
        if (!hasField(oldName)) {
            throw new IllegalArgumentException("Field not found: " + oldName);
        }
        if (oldName.equals(newName)) {
            return this;
        }
        if (hasField(newName)) {
            throw new IllegalArgumentException("Field already exists: " + newName);
        }
        return replaceField(oldName, f -> f.withName(newName));
    }

    public StructType updateFieldType(String name, RawWiseType newType) {
        if (name == null) throw new IllegalArgumentException("Field name cannot be null");
        if (newType == null) throw new IllegalArgumentException("New type cannot be null");
        if (!hasField(name)) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        return replaceField(name, f -> f.withDataType(newType));
    }

    private StructType replaceField(String name, Function<StructField, StructField> replacer) {
        List<StructField> newFields =
                fields.stream().map(f -> f.name().equals(name) ? replacer.apply(f) : f).toList();
        return new StructType(newFields, isNullable, description);
    }

    public StructType project(Set<String> fieldNames) {
        List<StructField> projected =
                fields.stream().filter(f -> fieldNames.contains(f.name())).toList();
        return new StructType(projected, isNullable, description);
    }
}
