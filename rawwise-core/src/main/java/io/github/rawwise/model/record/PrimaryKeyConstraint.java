package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public record PrimaryKeyConstraint(
        String name, List<String> fieldNames, boolean enforced, String description)
        implements Constraint, Serializable {

    private static final long serialVersionUID = 1L;

    public PrimaryKeyConstraint {
        if (fieldNames == null || fieldNames.isEmpty()) {
            throw new IllegalArgumentException("Primary key must have at least one field");
        }
        if (name == null || name.trim().isEmpty()) {
            name = "PK_" + String.join("_", fieldNames);
        }
        fieldNames = List.copyOf(fieldNames);
        description =
                description != null
                        ? description
                        : String.format("Primary key constraint on %s", fieldNames);
    }

    public PrimaryKeyConstraint(List<String> fieldNames) {
        this(null, fieldNames, true, null);
    }

    public PrimaryKeyConstraint(String name, List<String> fieldNames) {
        this(name, fieldNames, true, null);
    }

    public boolean isComposite() {
        return fieldNames.size() > 1;
    }

    @Override
    public Set<String> referencedFieldNames() {
        return Set.copyOf(fieldNames);
    }
}
