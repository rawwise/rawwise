package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record Schema(
        StructType structType,
        Map<String, String> properties,
        long version,
        List<Constraint> constraints,
        List<ColumnGroup> columnGroups)
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Set<String> RESERVED_HIDDEN_COLUMN_NAMES =
            Set.of("_row_number", "_hidden_pk", "_commit_ts", "_op_type");

    public Schema {
        if (structType == null) throw new IllegalArgumentException("Struct type cannot be null");
        properties = properties != null ? Map.copyOf(properties) : Map.of();
        constraints = constraints != null ? List.copyOf(constraints) : List.of();
        columnGroups = columnGroups != null ? List.copyOf(columnGroups) : List.of();

        structType = ensurePkFieldsNonNullable(structType, constraints);
        validateConstraints(structType, constraints);
        validateSinglePrimaryKey(constraints);
        validateColumnGroups(structType, columnGroups);
        validateHiddenColumnNames(structType);
    }

    private static StructType ensurePkFieldsNonNullable(
            StructType st, List<Constraint> constraints) {
        Set<String> pkFieldNames =
                constraints.stream()
                        .filter(PrimaryKeyConstraint.class::isInstance)
                        .map(PrimaryKeyConstraint.class::cast)
                        .flatMap(pk -> pk.fieldNames().stream())
                        .collect(Collectors.toSet());
        if (pkFieldNames.isEmpty()) {
            return st;
        }
        List<StructField> adjusted = new ArrayList<>();
        boolean changed = false;
        for (StructField f : st.fields()) {
            if (pkFieldNames.contains(f.name()) && f.dataType().isNullable()) {
                adjusted.add(
                        new StructField(
                                f.fieldId(),
                                f.name(),
                                f.dataType().copyWithNullable(false),
                                f.comment(),
                                f.metadata()));
                changed = true;
            } else {
                adjusted.add(f);
            }
        }
        return changed ? new StructType(adjusted) : st;
    }

    private static void validateConstraints(StructType structType, List<Constraint> constraints) {
        for (Constraint constraint : constraints) {
            if (constraint instanceof PrimaryKeyConstraint pkConstraint) {
                for (String fieldName : pkConstraint.fieldNames()) {
                    if (structType.field(fieldName).isEmpty()) {
                        throw new IllegalArgumentException(
                                "Primary key field '" + fieldName + "' does not exist in schema");
                    }
                }
            }
        }
    }

    private static void validateSinglePrimaryKey(List<Constraint> constraints) {
        long primaryKeyCount =
                constraints.stream().filter(c -> c instanceof PrimaryKeyConstraint).count();
        if (primaryKeyCount > 1) {
            throw new IllegalArgumentException("Schema can only have one primary key constraint");
        }
    }

    private static void validateColumnGroups(
            StructType structType, List<ColumnGroup> columnGroups) {
        Set<String> cgNames = new HashSet<>();
        for (ColumnGroup cg : columnGroups) {
            if (!cgNames.add(cg.name())) {
                throw new IllegalArgumentException("Duplicate ColumnGroup name: " + cg.name());
            }
        }
        Set<String> assignedColumns = new HashSet<>();
        for (ColumnGroup cg : columnGroups) {
            for (String colName : cg.columnNames()) {
                if (structType.field(colName).isEmpty()) {
                    throw new IllegalArgumentException(
                            "ColumnGroup '"
                                    + cg.name()
                                    + "' references column '"
                                    + colName
                                    + "' which does not exist in schema");
                }
                if (!assignedColumns.add(colName)) {
                    throw new IllegalArgumentException(
                            "Column overlap across ColumnGroups: column '"
                                    + colName
                                    + "' appears in multiple groups");
                }
            }
        }
    }

    private static void validateHiddenColumnNames(StructType structType) {
        List<String> conflicting = new ArrayList<>();
        for (StructField field : structType.fields()) {
            if (RESERVED_HIDDEN_COLUMN_NAMES.contains(field.name())) {
                conflicting.add(field.name());
            }
        }
        if (!conflicting.isEmpty()) {
            log.warn(
                    "Schema contains fields that conflict with reserved hidden column names: {}. "
                            + "This may cause unexpected behavior if hidden columns are enabled.",
                    conflicting);
        }
    }

    public Schema(StructType structType) {
        this(structType, Map.of(), 1L, List.of(), List.of());
    }

    public Schema(StructType structType, long version) {
        this(structType, Map.of(), version, List.of(), List.of());
    }

    public List<StructField> fields() {
        return structType.fields();
    }

    public Optional<StructField> field(String name) {
        return structType.field(name);
    }

    public RawWiseType fieldType(String name) {
        return structType.fieldType(name);
    }

    /** Returns the index of the field with the given name, or -1 if not found. */
    public int fieldIndex(String name) {
        List<StructField> fields = structType.fields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    public Optional<PrimaryKeyConstraint> primaryKeyConstraint() {
        return constraints.stream()
                .filter(PrimaryKeyConstraint.class::isInstance)
                .map(PrimaryKeyConstraint.class::cast)
                .findFirst();
    }

    public List<String> primaryKeys() {
        return primaryKeyConstraint().map(PrimaryKeyConstraint::fieldNames).orElse(List.of());
    }

    public List<StructField> primaryKeyFields() {
        return primaryKeys().stream().map(pk -> structType.field(pk).orElseThrow()).toList();
    }

    public boolean isPrimaryKeyField(String fieldName) {
        return primaryKeys().contains(fieldName);
    }

    public boolean hasPrimaryKey() {
        return primaryKeyConstraint().isPresent();
    }

    public boolean isCompositePrimaryKey() {
        return primaryKeyConstraint().map(PrimaryKeyConstraint::isComposite).orElse(false);
    }

    public int primaryKeyCount() {
        return primaryKeys().size();
    }

    public Optional<ColumnGroup> columnGroup(String name) {
        return columnGroups.stream().filter(cg -> cg.name().equals(name)).findFirst();
    }

    public Optional<ColumnGroup> columnGroupForColumn(String columnName) {
        return columnGroups.stream().filter(cg -> cg.containsColumn(columnName)).findFirst();
    }

    public Schema withColumnGroups(List<ColumnGroup> newColumnGroups) {
        List<ColumnGroup> resolved = new ArrayList<>();
        for (ColumnGroup newCg : newColumnGroups) {
            Optional<ColumnGroup> existingCg = columnGroup(newCg.name());
            if (existingCg.isPresent()) {
                boolean columnsChanged =
                        !existingCg.get().columnNames().equals(newCg.columnNames());
                boolean encodingChanged = existingCg.get().encoding() != newCg.encoding();
                if (columnsChanged || encodingChanged) {
                    throw new IllegalArgumentException(
                            "ColumnGroup '"
                                    + newCg.name()
                                    + "' already exists with different content. "
                                    + "Column Group does not support internal column changes. "
                                    + "Delete the existing group first and create a new one with a different name.");
                }
                List<RawWiseType> resolvedTypes =
                        newCg.columnTypes().isEmpty()
                                ? existingCg.get().columnTypes()
                                : newCg.columnTypes();
                List<Integer> resolvedFieldIds =
                        newCg.columnFieldIds().isEmpty()
                                ? existingCg.get().columnFieldIds()
                                : newCg.columnFieldIds();
                resolved.add(
                        new ColumnGroup(
                                newCg.name(),
                                newCg.columnNames(),
                                resolvedTypes,
                                newCg.encoding(),
                                existingCg.get().schemaVersion(),
                                resolvedFieldIds));
            } else {
                resolved.add(newCg);
            }
        }
        return new Schema(structType, properties, version + 1, constraints, resolved);
    }

    public Schema withDroppedColumnGroup(String columnGroupName) {
        if (columnGroupName == null)
            throw new IllegalArgumentException("ColumnGroup name cannot be null");
        List<ColumnGroup> newColumnGroups =
                columnGroups.stream().filter(cg -> !cg.name().equals(columnGroupName)).toList();
        if (newColumnGroups.size() == columnGroups.size()) {
            throw new IllegalArgumentException("ColumnGroup not found: " + columnGroupName);
        }
        return new Schema(structType, properties, version + 1, constraints, newColumnGroups);
    }

    public Schema withPrimaryKeyConstraint(PrimaryKeyConstraint newConstraint) {
        List<Constraint> newConstraints = new ArrayList<>(constraints);
        newConstraints.removeIf(PrimaryKeyConstraint.class::isInstance);
        newConstraints.add(newConstraint);
        return new Schema(structType, properties, version + 1, newConstraints, columnGroups);
    }

    public Schema withPrimaryKey(String fieldName) {
        PrimaryKeyConstraint existing = primaryKeyConstraint().orElse(null);
        List<String> newFieldNames;
        if (existing != null) {
            newFieldNames = new ArrayList<>(existing.fieldNames());
            if (!newFieldNames.contains(fieldName)) {
                newFieldNames.add(fieldName);
            }
        } else {
            newFieldNames = List.of(fieldName);
        }
        return withPrimaryKeyConstraint(new PrimaryKeyConstraint(newFieldNames));
    }

    public Schema withPrimaryKeys(List<String> fieldNames) {
        return withPrimaryKeyConstraint(new PrimaryKeyConstraint(fieldNames));
    }

    public Schema withoutPrimaryKey() {
        List<Constraint> newConstraints =
                constraints.stream().filter(c -> !(c instanceof PrimaryKeyConstraint)).toList();
        return new Schema(structType, properties, version + 1, newConstraints, columnGroups);
    }

    public Schema withProperty(String key, String value) {
        Map<String, String> newProperties = new HashMap<>(properties);
        newProperties.put(key, value);
        return new Schema(structType, newProperties, version, constraints, columnGroups);
    }

    public Schema withVersion(long newVersion) {
        return new Schema(structType, properties, newVersion, constraints, columnGroups);
    }

    public Schema withConstraint(Constraint constraint) {
        List<Constraint> newConstraints = new ArrayList<>(constraints);
        newConstraints.add(constraint);
        return new Schema(structType, properties, version + 1, newConstraints, columnGroups);
    }

    public Schema project(List<String> fieldNames) {
        Set<String> nameSet = Set.copyOf(fieldNames);
        StructType projectedType = structType.project(nameSet);
        List<Constraint> projectedConstraints =
                constraints.stream()
                        .filter(c -> nameSet.containsAll(c.referencedFieldNames()))
                        .toList();
        List<ColumnGroup> projectedColumnGroups =
                columnGroups.stream().filter(cg -> nameSet.containsAll(cg.columnNames())).toList();
        return new Schema(
                projectedType, properties, version, projectedConstraints, projectedColumnGroups);
    }

    private int nextFieldId() {
        return structType.fields().stream()
                        .filter(StructField::hasAssignedFieldId)
                        .mapToInt(StructField::fieldId)
                        .max()
                        .orElse(0)
                + 1;
    }

    public Schema withAddedField(StructField field) {
        if (field == null) throw new IllegalArgumentException("Field cannot be null");
        StructField resolved;
        if (!field.hasAssignedFieldId()) {
            resolved = field.withFieldId(nextFieldId());
        } else {
            resolved = field;
        }
        StructType newStructType = structType.addField(resolved);
        return new Schema(newStructType, properties, version + 1, constraints, columnGroups);
    }

    public Schema withAddedField(String name, RawWiseType dataType) {
        return withAddedField(new StructField(name, dataType));
    }

    public Schema withDroppedField(String name) {
        if (name == null) throw new IllegalArgumentException("Field name cannot be null");
        if (structType.field(name).isEmpty()) {
            throw new IllegalArgumentException("Field not found: " + name);
        }
        if (isPrimaryKeyField(name)) {
            throw new IllegalArgumentException(
                    "Cannot drop primary key field '"
                            + name
                            + "'. Remove primary key constraint first.");
        }
        Optional<ColumnGroup> cg = columnGroupForColumn(name);
        if (cg.isPresent()) {
            throw new IllegalArgumentException(
                    "Cannot drop field '"
                            + name
                            + "' that belongs to ColumnGroup '"
                            + cg.get().name()
                            + "'. Remove from ColumnGroup first.");
        }
        StructType newStructType = structType.dropField(name);
        return new Schema(newStructType, properties, version + 1, constraints, columnGroups);
    }

    public Schema withRenamedField(String oldName, String newName) {
        if (oldName == null) throw new IllegalArgumentException("Old field name cannot be null");
        if (newName == null) throw new IllegalArgumentException("New field name cannot be null");
        if (oldName.equals(newName)) {
            return this;
        }
        StructType newStructType = structType.renameField(oldName, newName);
        List<Constraint> newConstraints = updateConstraintsOnRename(constraints, oldName, newName);
        List<ColumnGroup> newColumnGroups =
                updateColumnGroupsOnRename(columnGroups, oldName, newName);
        return new Schema(newStructType, properties, version + 1, newConstraints, newColumnGroups);
    }

    public Schema withUpdatedFieldType(String name, RawWiseType newType) {
        if (name == null) throw new IllegalArgumentException("Field name cannot be null");
        if (newType == null) throw new IllegalArgumentException("New type cannot be null");
        StructType newStructType = structType.updateFieldType(name, newType);
        List<ColumnGroup> newColumnGroups =
                updateColumnGroupsOnTypeChange(columnGroups, name, newType);
        return new Schema(newStructType, properties, version + 1, constraints, newColumnGroups);
    }

    private static List<Constraint> updateConstraintsOnRename(
            List<Constraint> constraints, String oldName, String newName) {
        List<Constraint> updated = new ArrayList<>();
        for (Constraint c : constraints) {
            if (c instanceof PrimaryKeyConstraint pk) {
                List<String> newFieldNames =
                        pk.fieldNames().stream()
                                .map(fn -> fn.equals(oldName) ? newName : fn)
                                .toList();
                updated.add(new PrimaryKeyConstraint(newFieldNames));
            } else {
                updated.add(c);
            }
        }
        return updated;
    }

    private static List<ColumnGroup> updateColumnGroupsOnRename(
            List<ColumnGroup> columnGroups, String oldName, String newName) {
        List<ColumnGroup> updated = new ArrayList<>();
        for (ColumnGroup cg : columnGroups) {
            if (cg.containsColumn(oldName)) {
                List<String> newColumnNames =
                        cg.columnNames().stream()
                                .map(cn -> cn.equals(oldName) ? newName : cn)
                                .toList();
                updated.add(
                        new ColumnGroup(
                                cg.name(),
                                newColumnNames,
                                cg.columnTypes(),
                                cg.encoding(),
                                cg.schemaVersion(),
                                cg.columnFieldIds()));
            } else {
                updated.add(cg);
            }
        }
        return updated;
    }

    private static List<ColumnGroup> updateColumnGroupsOnTypeChange(
            List<ColumnGroup> columnGroups, String fieldName, RawWiseType newType) {
        List<ColumnGroup> updated = new ArrayList<>();
        for (ColumnGroup cg : columnGroups) {
            if (cg.containsColumn(fieldName)) {
                int idx = cg.columnNames().indexOf(fieldName);
                List<RawWiseType> newColTypes = new ArrayList<>(cg.columnTypes());
                if (!newColTypes.isEmpty() && idx >= 0 && idx < newColTypes.size()) {
                    newColTypes.set(idx, newType);
                }
                updated.add(
                        new ColumnGroup(
                                cg.name(),
                                cg.columnNames(),
                                List.copyOf(newColTypes),
                                cg.encoding(),
                                cg.schemaVersion() + 1,
                                cg.columnFieldIds()));
            } else {
                updated.add(cg);
            }
        }
        return updated;
    }

    public static SchemaBuilder builder() {
        return new SchemaBuilder();
    }

    public static class SchemaBuilder {
        private final List<StructField> fields = new ArrayList<>();
        private final Map<String, String> properties = new HashMap<>();
        private final List<Constraint> constraints = new ArrayList<>();
        private long version = 1L;

        public SchemaBuilder field(String name, RawWiseType dataType) {
            fields.add(new StructField(name, dataType));
            return this;
        }

        public SchemaBuilder field(String name, RawWiseType dataType, String comment) {
            fields.add(new StructField(name, dataType, comment));
            return this;
        }

        public SchemaBuilder field(int fieldId, String name, RawWiseType dataType) {
            if (fieldId <= 0) {
                throw new IllegalArgumentException(
                        "Explicit fieldId must be positive, got: "
                                + fieldId
                                + ". Use field(String, RawWiseType) for unassigned fields.");
            }
            fields.add(new StructField(fieldId, name, dataType, "", Map.of()));
            return this;
        }

        public SchemaBuilder field(int fieldId, String name, RawWiseType dataType, String comment) {
            if (fieldId <= 0) {
                throw new IllegalArgumentException(
                        "Explicit fieldId must be positive, got: "
                                + fieldId
                                + ". Use field(String, RawWiseType, String) for unassigned fields.");
            }
            fields.add(new StructField(fieldId, name, dataType, comment, Map.of()));
            return this;
        }

        public SchemaBuilder field(StructField field) {
            fields.add(field);
            return this;
        }

        public SchemaBuilder fields(List<StructField> fieldList) {
            fields.addAll(fieldList);
            return this;
        }

        public SchemaBuilder primaryKey(String fieldName) {
            Optional<PrimaryKeyConstraint> existing =
                    constraints.stream()
                            .filter(PrimaryKeyConstraint.class::isInstance)
                            .map(PrimaryKeyConstraint.class::cast)
                            .findFirst();
            if (existing.isPresent()) {
                List<String> newFieldNames = new ArrayList<>(existing.get().fieldNames());
                if (!newFieldNames.contains(fieldName)) {
                    newFieldNames.add(fieldName);
                }
                constraints.removeIf(PrimaryKeyConstraint.class::isInstance);
                constraints.add(new PrimaryKeyConstraint(newFieldNames));
            } else {
                constraints.add(new PrimaryKeyConstraint(List.of(fieldName)));
            }
            return this;
        }

        public SchemaBuilder primaryKeys(String... fieldNames) {
            constraints.removeIf(PrimaryKeyConstraint.class::isInstance);
            constraints.add(new PrimaryKeyConstraint(Arrays.asList(fieldNames)));
            return this;
        }

        public SchemaBuilder primaryKeyConstraint(PrimaryKeyConstraint constraint) {
            constraints.removeIf(PrimaryKeyConstraint.class::isInstance);
            constraints.add(constraint);
            return this;
        }

        public SchemaBuilder constraint(Constraint constraint) {
            constraints.add(constraint);
            return this;
        }

        public SchemaBuilder property(String key, String value) {
            properties.put(key, value);
            return this;
        }

        public SchemaBuilder version(long version) {
            this.version = version;
            return this;
        }

        public Schema build() {
            List<StructField> adjustedFields = assignFieldIds(fields);
            adjustedFields = ensurePkFieldsNonNullable(adjustedFields);
            StructType structType = new StructType(adjustedFields);
            return new Schema(structType, properties, version, constraints, List.of());
        }

        private List<StructField> ensurePkFieldsNonNullable(List<StructField> fieldList) {
            Set<String> pkFieldNames =
                    constraints.stream()
                            .filter(PrimaryKeyConstraint.class::isInstance)
                            .map(PrimaryKeyConstraint.class::cast)
                            .flatMap(pk -> pk.fieldNames().stream())
                            .collect(Collectors.toSet());
            if (pkFieldNames.isEmpty()) {
                return fieldList;
            }
            List<StructField> adjusted = new ArrayList<>();
            for (StructField f : fieldList) {
                if (pkFieldNames.contains(f.name()) && f.dataType().isNullable()) {
                    adjusted.add(
                            new StructField(
                                    f.fieldId(),
                                    f.name(),
                                    f.dataType().copyWithNullable(false),
                                    f.comment(),
                                    f.metadata()));
                } else {
                    adjusted.add(f);
                }
            }
            return adjusted;
        }

        private static List<StructField> assignFieldIds(List<StructField> fields) {
            Set<Integer> explicitIds = new HashSet<>();
            for (StructField f : fields) {
                if (f.hasAssignedFieldId()) {
                    explicitIds.add(f.fieldId());
                }
            }
            int nextId = 1;
            List<StructField> assigned = new ArrayList<>();
            for (StructField f : fields) {
                if (f.hasAssignedFieldId()) {
                    assigned.add(f);
                } else {
                    while (explicitIds.contains(nextId)) {
                        nextId++;
                    }
                    explicitIds.add(nextId);
                    assigned.add(f.withFieldId(nextId));
                    nextId++;
                }
            }
            return assigned;
        }
    }
}
