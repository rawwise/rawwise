package io.github.rawwise.benchmark.vbench.memory;

import java.util.List;

public record PromptEnhancement(
        String originalPrompt,
        String enhancedPrompt,
        List<String> constraints,
        List<String> sourceMemoryIds) {
    public boolean enhanced() {
        return !constraints.isEmpty();
    }
}
