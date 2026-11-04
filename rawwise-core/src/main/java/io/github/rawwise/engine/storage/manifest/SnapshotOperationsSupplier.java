package io.github.rawwise.engine.storage.manifest;

import java.io.IOException;
import java.io.Serializable;

@FunctionalInterface
public interface SnapshotOperationsSupplier extends Serializable {
    SnapshotOperations get() throws IOException;
}
