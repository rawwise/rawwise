package io.github.rawwise.engine.storage.flush;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.vector.VectorFlushListener;

public interface FlushCoordinator {
    FlushPlan plan(String tableId, LmdbInstance instance, String remoteBasePath);

    void beforeFlush(String tableId, LmdbInstance instance);

    void afterFlush(String tableId, LmdbInstance instance, FlushPlan plan);

    void commitFlush(String tableId, LmdbInstance instance, FlushPlan plan);

    void onFlushFailure(String tableId, LmdbInstance instance, Throwable cause);

    void setSnapshotManager(SnapshotManager snapshotManager);

    void addFlushListener(VectorFlushListener listener);
}
