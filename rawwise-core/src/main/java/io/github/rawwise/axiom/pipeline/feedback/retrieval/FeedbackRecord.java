package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.Builder;

@Builder
public record FeedbackRecord(
        String recordId,
        String query,
        List<String> retrievedHitIds,
        String userExpectedHitId,
        FeedbackType feedbackType,
        String correctionText,
        Instant timestamp,
        Map<String, String> metadata) {}
