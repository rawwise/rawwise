package io.github.rawwise.benchmark.vbench.feedback;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public final class HttpAxiomClient implements AxiomClient {
    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public HttpAxiomClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String ingest(AxiomIngest ingest) {
        try {
            String body =
                    objectMapper.writeValueAsString(
                            Map.of(
                                    "tableId", ingest.tableId(),
                                    "key", ingest.key(),
                                    "body", ingest.body(),
                                    "immutable", ingest.immutable(),
                                    "properties", ingest.properties()));
            HttpResponse<String> response = post("/api/axiom/ingest", body);
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(
                        "Axiom ingest failed: HTTP "
                                + response.statusCode()
                                + " "
                                + response.body());
            }
            return objectMapper.readTree(response.body()).path("recordKey").asText(ingest.key());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Axiom ingest failed: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IllegalStateException("Axiom ingest failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void annotate(AxiomAnnotation annotation) {
        try {
            String viewId = annotation.viewId();
            if (!viewId.startsWith("rec:")) {
                viewId = "rec:" + viewId;
            }
            String body =
                    objectMapper.writeValueAsString(
                            Map.of(
                                    "viewId", viewId,
                                    "type", toCoreAnnotationType(annotation.type()),
                                    "value", toCoreAnnotationValue(annotation),
                                    "annotatorId", annotation.annotatorId(),
                                    "confidence", annotation.confidence()));
            HttpResponse<String> response = post("/api/axiom/annotate", body);
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(
                        "Axiom annotate failed: HTTP "
                                + response.statusCode()
                                + " "
                                + response.body());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Axiom annotate failed: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new IllegalStateException("Axiom annotate failed: " + e.getMessage(), e);
        }
    }

    private String toCoreAnnotationType(String type) {
        return switch (type) {
            case "POSITIVE", "NEGATIVE" -> "PREFERENCE";
            default -> type;
        };
    }

    private String toCoreAnnotationValue(AxiomAnnotation annotation) {
        return switch (annotation.type()) {
            case "POSITIVE" -> "1.0";
            case "NEGATIVE" -> "-1.0";
            default -> annotation.value();
        };
    }

    private HttpResponse<String> post(String path, String body)
            throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + path))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
}
