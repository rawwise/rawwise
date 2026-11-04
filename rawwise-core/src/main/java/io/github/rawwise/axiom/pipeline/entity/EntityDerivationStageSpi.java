package io.github.rawwise.axiom.pipeline.entity;

import io.github.rawwise.model.record.SemanticRecord;
import java.util.List;

public interface EntityDerivationStageSpi {
    void onWrite(SemanticRecord record, String tableId, long snapshotId);

    default void onWrite(
            SemanticRecord record, String tableId, long snapshotId, List<String> chunkIds) {
        onWrite(record, tableId, snapshotId);
    }

    void shutdown();
}
