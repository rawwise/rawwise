package io.github.rawwise.model.record;

import java.util.Map;

public interface ColumnGroupCodec {

    byte[] encode(ColumnGroup cg, Map<String, Object> values);

    Map<String, Object> decode(ColumnGroup cg, byte[] data);

    Object decodeField(ColumnGroup cg, byte[] data, String columnName);
}
