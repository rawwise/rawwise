package io.github.rawwise.axiom.orchestrator;

public record SeqHandle(String seqId, long currentValue, long createdAt) {}
