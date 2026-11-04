package io.github.rawwise.axiom.pipeline.reformulation;

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
public class HttpLlmClient implements LlmClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String endpoint;
    private final String apiKey;
    private final Duration timeout;
    private final int maxRetries;
    private final HttpClient httpClient;
    private final StubLlmClient stubDelegate;

    public HttpLlmClient(String endpoint, String apiKey, Duration timeout, int maxRetries) {
        this.endpoint = endpoint;
        this.apiKey = apiKey;
        this.timeout = timeout == null ? Duration.ofSeconds(30) : timeout;
        this.maxRetries = maxRetries;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.stubDelegate =
                endpoint != null && endpoint.startsWith("stub://") ? new StubLlmClient() : null;
    }

    @Override
    public String name() {
        return stubDelegate != null ? "http(stub)" : "http";
    }

    @Override
    public String complete(String prompt, String modelName, int maxTokens) throws LlmException {
        if (stubDelegate != null) {
            return stubDelegate.complete(prompt, modelName, maxTokens);
        }
        if (endpoint == null || endpoint.isEmpty()) {
            throw new LlmException("HttpLlmClient endpoint not configured");
        }
        int attempts = 0;
        LlmException last = null;
        while (attempts <= maxRetries) {
            try {
                return doHttpCall(prompt, modelName, maxTokens);
            } catch (LlmException e) {
                last = e;
                attempts++;
                log.debug("HttpLlmClient attempt {} failed: {}", attempts, e.getMessage());
            }
        }
        throw last == null ? new LlmException("HttpLlmClient failed without exception") : last;
    }

    private String doHttpCall(String prompt, String modelName, int maxTokens) throws LlmException {
        try {
            ObjectNode requestBody = MAPPER.createObjectNode();
            requestBody.put("model", modelName);
            ArrayNode messages = requestBody.putArray("messages");
            messages.add(MAPPER.createObjectNode().put("role", "user").put("content", prompt));
            requestBody.put("max_tokens", maxTokens);

            HttpRequest httpRequest =
                    HttpRequest.newBuilder()
                            .uri(URI.create(endpoint))
                            .header("Content-Type", "application/json")
                            .header("Authorization", "Bearer " + apiKey)
                            .POST(
                                    HttpRequest.BodyPublishers.ofString(
                                            MAPPER.writeValueAsString(requestBody)))
                            .timeout(timeout)
                            .build();

            HttpResponse<String> response =
                    httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new LlmException(
                        "LLM API returned status "
                                + response.statusCode()
                                + ": "
                                + response.body());
            }
            JsonNode root = MAPPER.readTree(response.body());
            String content =
                    root.path("choices").path(0).path("message").path("content").asText("");
            if (content.isEmpty()) {
                throw new LlmException("LLM API returned empty content");
            }
            return content;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LlmException("LLM API call interrupted", e);
        } catch (IOException e) {
            throw new LlmException("LLM API call failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            httpClient.close();
        } catch (Exception e) {
            log.warn("Failed to close HttpClient", e);
        }
    }
}
