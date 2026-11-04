package io.github.rawwise.axiom.pipeline.entity;

import io.github.rawwise.model.record.SemanticRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class NoOpEntityDerivationStage implements EntityDerivationStageSpi {

    public void onWrite(SemanticRecord record, String tableId, long snapshotId) {
        log.debug(
                "NoOpEntityDerivationStage: entity extraction disabled, skipping table={}",
                tableId);
    }

    public void shutdown() {}
}
