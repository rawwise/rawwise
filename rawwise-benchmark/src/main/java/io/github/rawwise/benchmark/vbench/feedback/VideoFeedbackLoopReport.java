package io.github.rawwise.benchmark.vbench.feedback;

public record VideoFeedbackLoopReport(
        String promptId, int ingestedRecordCount, int autoFeedbackCount, int llmFeedbackCount) {}
