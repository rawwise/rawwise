package io.github.rawwise.governance;

import io.github.rawwise.axiom.pipeline.feedback.FeedbackSignal;

public interface GovernanceEngine {

    boolean checkPermission(String principal, String entityId, String layer, Operation op);

    ProvenanceChain provenance(String entityId);

    ContinuityReport continuityCheck(long snapshotId);

    FeedbackReceipt submitFeedback(FeedbackSignal signal);

    enum Operation {
        READ,
        WRITE,
        DERIVE,
        EVALUATE,
        ANNOTATE,
        FEEDBACK,
        DELETE,
        OWNERSHIP
    }

    record ProvenanceChain(String entityId, String chainJson, long timestamp) {}

    record ContinuityReport(long snapshotId, boolean continuous, String details, long timestamp) {}

    record FeedbackReceipt(String feedbackId, boolean accepted, String reason, long timestamp) {}
}
