package io.github.rawwise.axiom.memory;

import java.io.IOException;

public interface SummarizationService {

    SummarizationResult summarize(String content, SummarizationRequest request) throws IOException;

    boolean isAvailable();

    record SummarizationRequest(
            String model,
            int maxTokensL0,
            int maxTokensL1,
            String contentType,
            String systemPrompt) {
        public SummarizationRequest {
            if (model == null) model = "default";
            if (maxTokensL0 <= 0) maxTokensL0 = 100;
            if (maxTokensL1 <= 0) maxTokensL1 = 2000;
            if (contentType == null) contentType = "text/plain";
            if (systemPrompt == null) systemPrompt = "";
        }
    }

    record SummarizationResult(
            String l0Abstract,
            String l1Overview,
            long l0TokenCount,
            long l1TokenCount,
            String model,
            long timestamp) {}
}
