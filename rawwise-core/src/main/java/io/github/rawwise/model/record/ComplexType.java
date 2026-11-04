package io.github.rawwise.model.record;

import java.io.Serializable;

public sealed interface ComplexType extends RawWiseType, Serializable
        permits ArrayType, MapType, StructType {
    default TypeCategory category() {
        return TypeCategory.COMPLEX;
    }
}
