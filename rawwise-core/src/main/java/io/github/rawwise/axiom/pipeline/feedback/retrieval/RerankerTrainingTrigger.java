package io.github.rawwise.axiom.pipeline.feedback.retrieval;

public record RerankerTrainingTrigger(long feedbackCount, long lastTrainAtMs, String version) {}
