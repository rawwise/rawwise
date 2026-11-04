package io.github.rawwise.governance.trust;

import java.util.List;

public record VerificationResult(
        boolean valid,
        String contextId,
        String entryKey,
        int version,
        String computedHash,
        String storedHash,
        List<String> errors) {
    public static VerificationResult ok(
            String contextId,
            String entryKey,
            int version,
            String computedHash,
            String storedHash) {
        return new VerificationResult(
                true, contextId, entryKey, version, computedHash, storedHash, List.of());
    }

    public static VerificationResult fail(
            String contextId,
            String entryKey,
            int version,
            String computedHash,
            String storedHash,
            List<String> errors) {
        return new VerificationResult(
                false, contextId, entryKey, version, computedHash, storedHash, errors);
    }
}
