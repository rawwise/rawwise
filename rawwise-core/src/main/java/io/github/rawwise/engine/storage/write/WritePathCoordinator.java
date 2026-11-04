package io.github.rawwise.engine.storage.write;

public interface WritePathCoordinator {
    boolean ensureWritable(String tableIdentifier, byte[] key);

    boolean isLmdbBacked();
}
