package io.github.rawwise.axiom.memory;

public class NoOpSummarizationService implements SummarizationService {

    @Override
    public SummarizationResult summarize(String content, SummarizationRequest request) {
        return new SummarizationResult("", "", 0, 0, "noop", System.currentTimeMillis());
    }

    @Override
    public boolean isAvailable() {
        return false;
    }
}
