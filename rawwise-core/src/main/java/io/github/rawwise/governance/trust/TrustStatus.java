package io.github.rawwise.governance.trust;

public record TrustStatus(
        long totalTransitions,
        long verifiedTransitions,
        long failedVerifications,
        long lastVerificationAt) {}
