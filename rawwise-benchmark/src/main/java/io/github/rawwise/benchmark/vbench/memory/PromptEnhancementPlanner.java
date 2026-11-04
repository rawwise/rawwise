package io.github.rawwise.benchmark.vbench.memory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public final class PromptEnhancementPlanner {
    private final PromptMemoryClient memoryClient;
    private final int topK;

    public PromptEnhancementPlanner(PromptMemoryClient memoryClient) {
        this(memoryClient, 5);
    }

    public PromptEnhancementPlanner(PromptMemoryClient memoryClient, int topK) {
        this.memoryClient = memoryClient;
        this.topK = topK;
    }

    public PromptEnhancement enhance(String prompt) {
        if (prompt == null || prompt.isBlank() || memoryClient == null) {
            return new PromptEnhancement(prompt, prompt, List.of(), List.of());
        }
        List<PromptMemoryRecord> memories = memoryClient.recall(prompt, topK);
        Set<String> constraints = new LinkedHashSet<>();
        List<String> sourceIds = new ArrayList<>();
        for (PromptMemoryRecord memory : memories) {
            if (memory == null || memory.summary() == null) {
                continue;
            }
            sourceIds.add(memory.memoryId());
            String summary = normalize(memory.summary());
            if (summary.contains("temporal_flickering")
                    || summary.contains("temporal stability")
                    || summary.contains("smooth motion")) {
                constraints.add("Maintain stable lighting and smooth motion across all frames.");
            }
            if (summary.contains("subject_consistency")
                    || summary.contains("overall_consistency")
                    || summary.contains("subject consistent")) {
                constraints.add(
                        "Keep the subject consistent across frames, including clothing, pose continuity, and identity.");
            }
            if (summary.contains("aesthetic_quality") || summary.contains("aesthetic quality")) {
                constraints.add(
                        "Preserve visual appeal while improving consistency; avoid dull colors or low-quality composition.");
            }
        }
        List<String> constraintList = List.copyOf(constraints);
        String enhancedPrompt = buildEnhancedPrompt(prompt, constraintList);
        return new PromptEnhancement(
                prompt, enhancedPrompt, constraintList, List.copyOf(sourceIds));
    }

    private String normalize(String text) {
        return text.toLowerCase(Locale.ROOT)
                .replace(" _ ", "_")
                .replace(" - ", "-")
                .replace(" / ", "/");
    }

    private String buildEnhancedPrompt(String prompt, List<String> constraints) {
        if (constraints.isEmpty()) {
            return prompt;
        }
        StringBuilder sb = new StringBuilder(prompt.strip());
        sb.append("\n\nRawWise learned constraints from prior feedback:\n");
        for (String constraint : constraints) {
            sb.append("- ").append(constraint).append('\n');
        }
        return sb.toString().strip();
    }
}
