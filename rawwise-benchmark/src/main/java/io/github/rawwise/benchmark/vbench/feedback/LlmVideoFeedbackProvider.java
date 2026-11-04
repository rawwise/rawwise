package io.github.rawwise.benchmark.vbench.feedback;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public interface LlmVideoFeedbackProvider {
    List<AxiomAnnotation> judge(String baselineViewId, String rawwiseViewId, JsonNode result);
}
