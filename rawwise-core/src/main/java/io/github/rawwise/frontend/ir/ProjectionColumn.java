package io.github.rawwise.frontend.ir;

import io.github.rawwise.model.record.RawWiseType;
import java.io.Serializable;

public record ProjectionColumn(
        String name,
        int fieldId,
        RawWiseType type, // nullable after proto deserialization; resolve via TableMetaIR.schema
        String alias)
        implements Serializable {
    public ProjectionColumn(String name, int fieldId, RawWiseType type) {
        this(name, fieldId, type, null);
    }
}
