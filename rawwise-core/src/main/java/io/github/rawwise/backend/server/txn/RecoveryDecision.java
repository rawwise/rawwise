package io.github.rawwise.backend.server.txn;

import io.github.rawwise.engine.txn.ingest.BatchState;
import java.util.Objects;

public record RecoveryDecision(DecisionType type, String detail, BatchState recoveryState) {

    public enum DecisionType {
        ALREADY_COMMITTED,
        ALREADY_ABORTED,
        NEEDS_RECOVERY,
        NEEDS_ABORT,
        ACTIVE_BATCH
    }

    public RecoveryDecision {
        Objects.requireNonNull(type, "type must not be null");
        if (detail == null) {
            detail = "";
        }
    }
}
