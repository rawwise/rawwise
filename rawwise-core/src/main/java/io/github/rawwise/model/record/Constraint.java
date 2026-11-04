package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.Set;

public sealed interface Constraint extends Serializable permits PrimaryKeyConstraint {
    String name();

    boolean enforced();

    String description();

    default Set<String> referencedFieldNames() {
        return Set.of();
    }
}
