package io.github.rawwise.engine.storage.table.key;

import java.io.IOException;
import java.util.Map;

public interface KeyCodec {

    byte[] encodePrimaryKey(Map<String, Comparable> primaryKeyValues) throws IOException;
}
