package io.github.rawwise.axiom.memory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpSummarizationService implements SummarizationService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String endpoint;
    private final String apiKey;
    private final String defaultModel;
    private final HttpClient httpClient;

    public HttpSummarizationService(String endpoint, String apiKey, String defaultModel) {
        this.endpoint = endpoint;
        this.apiKey = apiKey;
        this.defaultModel = defaultModel;
        this.httpClient =
                HttpClient.newBuilder()
                        .connectTimeout(Duration.ofSeconds(10))
                        .proxy(java.net.ProxySelector.of(null)) // explicitly disable system proxy
                        .build();
    }

    @Override
    public SummarizationResult summarize(String content, SummarizationRequest request)
            throws IOException {
        String model = request.model().equals("default") ? defaultModel : request.model();
        ObjectNode requestBody = MAPPER.createObjectNode();
        requestBody.put("model", model);

        ArrayNode messages = requestBody.putArray("messages");
        // system Prompt non-empty,caller provided complete prompt template,pass directly to user
        // message
        // (strategy D fact extraction and other scenarios needing custom prompt)
        if (!request.systemPrompt().isEmpty()) {
            messages.add(
                    MAPPER.createObjectNode()
                            .put("role", "user")
                            .put("content", request.systemPrompt() + "\n\n" + content));
        } else {
            messages.add(
                    MAPPER.createObjectNode()
                            .put("role", "user")
                            .put(
                                    "content",
                                    "Extract key facts from this text in 1-2 sentences (max ~100 tokens). "
                                            + "Focus on entities, actions, and attributes. Output plain text only, no JSON.\n\n"
                                            + content));
        }

        HttpRequest httpRequest =
                HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + apiKey)
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        MAPPER.writeValueAsString(requestBody)))
                        .timeout(Duration.ofSeconds(60))
                        .build();

        try {
            log.debug("Calling LLM API model={} contentLength={}", model, content.length());
            HttpResponse<String> response =
                    httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                log.error(
                        "LLM API returned status={} body={}",
                        response.statusCode(),
                        response.body());
                throw new IOException(
                        "LLM API returned status "
                                + response.statusCode()
                                + ": "
                                + response.body());
            }
            JsonNode root = MAPPER.readTree(response.body());
            String contentStr =
                    root.path("choices").path(0).path("message").path("content").asText("");
            // LLM returns plain text summary (not JSON), use directly as L0 abstract
            String l0Abstract = contentStr.trim();
            SummarizationResult result =
                    new SummarizationResult(
                            l0Abstract,
                            "", // L1 overview not needed for embedding
                            estimateTokens(l0Abstract),
                            0,
                            model,
                            System.currentTimeMillis());
            log.info(
                    "LLM summarization complete model={} l0Tokens={} l1Tokens={}",
                    model,
                    result.l0TokenCount(),
                    result.l1TokenCount());
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("LLM API call interrupted", e);
        }
    }

    @Override
    public boolean isAvailable() {
        return endpoint != null && !endpoint.isEmpty() && apiKey != null && !apiKey.isEmpty();
    }

    private long estimateTokens(String text) {
        return (long) Math.ceil(text.length() / 4.0);
    }
}
