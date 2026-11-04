package io.github.rawwise.benchmark.vbench.feedback;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;

/**
 * Local rule-based LLM judge substitute. in productioncanreplaced withrealactualmultimodal
 * LLM:Input prompt,key framesand VBench score,outputstructuredfeedback.
 */
public final class RuleBasedLlmVideoFeedbackProvider implements LlmVideoFeedbackProvider {

    @Override
    public List<AxiomAnnotation> judge(
            String baselineViewId, String rawwiseViewId, JsonNode result) {
        List<AxiomAnnotation> annotations = new ArrayList<>();
        JsonNode delta = result.path("delta");
        double temporal = delta.path("temporal_flickering").asDouble(0.0);
        double aesthetic = delta.path("aesthetic_quality").asDouble(0.0);
        if (temporal > 0.05) {
            annotations.add(
                    new AxiomAnnotation(
                            rawwiseViewId,
                            "POSITIVE",
                            "LLM judge: temporal stability improved; keep smooth motion and stable lighting constraints.",
                            "llm-video-judge",
                            0.70));
        }
        if (aesthetic < -0.005) {
            annotations.add(
                    new AxiomAnnotation(
                            rawwiseViewId,
                            "NEGATIVE",
                            "LLM judge: aesthetic quality regressed; strengthen visual appeal without sacrificing consistency.",
                            "llm-video-judge",
                            0.65));
        }
        return annotations;
    }
}
