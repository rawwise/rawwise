package io.github.rawwise.benchmark.vbench.memory;

import java.util.List;

public interface PromptMemoryClient {
    List<PromptMemoryRecord> recall(String prompt, int topK);
}
