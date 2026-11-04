package io.github.rawwise.axiom.memory.semantic;

public record ExtractionPolicy(
        int batchSize,
        int maxRetries,
        long retryDelayMs,
        String extractionPrompt,
        boolean overwriteExisting,
        double minImportance,
        int maxTokensResponse,
        String lastExtractionCursor) {
    public static final ExtractionPolicy DEFAULT =
            new ExtractionPolicy(50, 3, 1000, null, false, 0.0, 4096, null);

    public ExtractionPolicy {
        if (batchSize <= 0) batchSize = 50;
        if (maxRetries < 0) maxRetries = 3;
        if (retryDelayMs <= 0) retryDelayMs = 1000;
        if (maxTokensResponse <= 0) maxTokensResponse = 4096;
    }
}
