package io.github.rawwise.axiom.pipeline.reformulation;

public class StubLlmClient implements LlmClient {

    private static final String PARAPHRASE_RESPONSE =
            "[\"query rephrased v1\",\"query rephrased v2\",\"query rephrased v3\",\"query rephrased v4\"]";

    @Override
    public String name() {
        return "stub";
    }

    @Override
    public String complete(String prompt, String modelName, int maxTokens) {
        if (prompt != null && prompt.contains("diverse paraphrases")) {
            return PARAPHRASE_RESPONSE;
        }
        return "stub-response";
    }
}
