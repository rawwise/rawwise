package io.github.rawwise.engine.storage;

import io.github.rawwise.model.record.TableId;

public interface TablePathResolver {

    String resolveDataPath(String tableId);

    String resolveTmpPath(String tableId);

    String resolveManifestPath(String tableId);

    default String resolveTablePath(String tableId) {
        return TableId.parseLenient(tableId).toPath();
    }
}
