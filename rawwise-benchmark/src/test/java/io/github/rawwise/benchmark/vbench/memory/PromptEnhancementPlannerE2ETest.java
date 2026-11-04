package io.github.rawwise.benchmark.vbench.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.DefaultExecutionContext;
import io.github.rawwise.axiom.pipeline.cognition.TextConditionEnhancement;
import io.github.rawwise.axiom.pipeline.cognition.TextConditionEnhancementContext;
import io.github.rawwise.benchmark.vbench.executors.CogVideoXClient;
import io.github.rawwise.benchmark.vbench.executors.VideoGenerationActionExecutor;
import io.github.rawwise.model.record.Action;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PromptEnhancementPlannerE2ETest {

    @Test
    void plannerBuildsPromptConstraintsFromRawWiseFeedbackMemory() {
        PromptEnhancementPlanner planner =
                new PromptEnhancementPlanner(new FakePromptMemoryClient());

        PromptEnhancement enhancement =
                planner.enhance("A young woman in a red jacket walking down a street");

        assertTrue(
                enhancement
                        .enhancedPrompt()
                        .contains("Maintain stable lighting and smooth motion"));
        assertTrue(
                enhancement.enhancedPrompt().contains("Keep the subject consistent across frames"));
        assertTrue(enhancement.enhancedPrompt().contains("Preserve visual appeal"));
        assertEquals(3, enhancement.constraints().size());
        assertEquals(2, enhancement.sourceMemoryIds().size());
    }

    @Test
    void adapterConvertsPlannerOutputIntoCoreTextConditionEnhancement() {
        var adapter =
                new PromptPlannerTextConditionEnhancer(
                        new PromptEnhancementPlanner(new FakePromptMemoryClient()));

        TextConditionEnhancement enhancement =
                adapter.enhance(
                        "entity=character_A, clothing=red jacket",
                        new TextConditionEnhancementContext("character_A", null, null, Map.of()));

        assertTrue(enhancement.enhanced());
        assertTrue(enhancement.enhancedCondition().contains("RawWise learned constraints"));
        assertEquals(3, enhancement.constraints().size());
        assertEquals(2, enhancement.sourceIds().size());
    }

    @Test
    void videoGenerationExecutorUsesPlannerEnhancedPrompt() {
        CapturingCogVideoXClient client = new CapturingCogVideoXClient();
        VideoGenerationActionExecutor executor =
                new VideoGenerationActionExecutor(
                        client, new PromptEnhancementPlanner(new FakePromptMemoryClient()));
        Action action =
                new Action("act-1", List.of(), Action.ActionType.CORRECT, new byte[0], true);
        DefaultExecutionContext context =
                new DefaultExecutionContext(
                        null,
                        List.of(),
                        List.of(),
                        Optional.empty(),
                        Map.of("prompt", "A young woman in a red jacket walking down a street"));

        CognitionEngine.ActionResult result = executor.execute(action, context);

        assertTrue(result.success());
        assertTrue(client.lastPrompt.contains("Maintain stable lighting and smooth motion"));
        assertTrue(result.artifacts().containsKey("rawise_prompt_enhanced"));
        assertEquals(true, result.artifacts().get("rawise_prompt_enhanced"));
    }

    private static final class FakePromptMemoryClient implements PromptMemoryClient {
        @Override
        public List<PromptMemoryRecord> recall(String prompt, int topK) {
            return List.of(
                    new PromptMemoryRecord(
                            "rec:vbench:prompt_001:evaluation",
                            0.91,
                            "temporal_flickering improved by 0.2021; overall_consistency improved by 0.074; "
                                    + "LLM judge: temporal stability improved; aesthetic quality regressed"),
                    new PromptMemoryRecord(
                            "rec:vbench:prompt_001:rawwise",
                            0.78,
                            "subject_consistency high; background_consistency high; red jacket walking street"));
        }
    }

    private static final class CapturingCogVideoXClient extends CogVideoXClient {
        private String lastPrompt;

        @Override
        public GenerationResponse generate(GenerationRequest request) {
            this.lastPrompt = request.prompt();
            return new GenerationResponse(
                    ("VIDEO:" + request.prompt()).getBytes(StandardCharsets.UTF_8),
                    1,
                    "capture",
                    Map.of());
        }
    }
}
