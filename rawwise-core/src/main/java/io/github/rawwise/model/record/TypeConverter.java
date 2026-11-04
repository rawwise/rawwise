package io.github.rawwise.model.record;

public interface TypeConverter<T> {
    RawWiseType toRawWiseType(T externalType);

    T fromRawWiseType(RawWiseType dataType);
}
