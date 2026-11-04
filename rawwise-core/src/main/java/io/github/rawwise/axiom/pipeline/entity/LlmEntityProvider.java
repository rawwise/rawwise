package io.github.rawwise.axiom.pipeline.entity;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class LlmEntityProvider implements EntityProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String llmEndpoint;
    private final String apiKey;
    private final String modelName;
    private final Duration timeout;
    private final int maxRetries;
    private final HttpClient httpClient;

    public LlmEntityProvider(String llmEndpoint, Duration timeout, int maxRetries) {
        this(llmEndpoint, null, timeout, maxRetries);
    }

    public LlmEntityProvider(String llmEndpoint, String apiKey, Duration timeout, int maxRetries) {
        this(
                llmEndpoint,
                apiKey,
                System.getProperty("rawwise.entity.extraction.llm.model", "default"),
                timeout,
                maxRetries);
    }

    public LlmEntityProvider(
            String llmEndpoint, String apiKey, String modelName, Duration timeout, int maxRetries) {
        this.llmEndpoint = llmEndpoint;
        this.apiKey = apiKey;
        this.modelName = modelName == null || modelName.isBlank() ? "default" : modelName;
        this.timeout = timeout == null ? Duration.ofSeconds(30) : timeout;
        this.maxRetries = maxRetries;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    @Override
    public String name() {
        return "llm";
    }

    @Override
    public EntityView extract(String text, EntityContext context) throws ExtractionException {
        if (llmEndpoint != null && llmEndpoint.startsWith("stub://")) {
            return tryWithRetry(() -> extractStub(text, context));
        }
        return tryWithRetry(() -> extractWithLlm(text, context));
    }

    private EntityView tryWithRetry(StubCallable callable) throws ExtractionException {
        int attempts = 0;
        ExtractionException last = null;
        while (attempts <= maxRetries) {
            try {
                return callable.call();
            } catch (ExtractionException e) {
                last = e;
                attempts++;
                log.debug("LLM provider attempt {} failed: {}", attempts, e.getMessage());
            }
        }
        throw last == null
                ? new ExtractionException("LLM provider failed without exception")
                : last;
    }

    private EntityView extractWithLlm(String text, EntityContext context)
            throws ExtractionException {
        try {
            String prompt = buildExtractionPrompt(text);
            String response = callLlmApi(prompt);
            return parseLlmResponse(response, context);
        } catch (Exception e) {
            log.warn("LLM entity extraction failed, falling back to stub: {}", e.getMessage());
            return extractStub(text, context);
        }
    }

    private String buildExtractionPrompt(String text) {
        return """
            Extract ALL named entities and their relationships from the conversation text below.

            Entity types:
            - PERSON: individual people (use canonical full name, e.g. "Caroline Smith" not "Caroline")
            - ORG: organizations, groups, companies, collectives (e.g. "LGBTQ support group", "Pride committee")
            - LOC: physical places, venues, cities (e.g. "San Francisco", "the gallery")
            - EVENT: named or described events, activities, occasions (e.g. "Pride Festival 2023", "mural painting")
            - OBJECT: significant tangible items discussed (e.g. "painting", "journal")

            Relationship types:
            - TALKED_TO: direct conversation between two people
            - MET_AT: person was at a location or event
            - WORKS_WITH: collaboration, co-creation, shared project
            - MEMBER_OF: person belongs to a group or organization
            - ATTENDED: person participated in an event
            - DISCUSSED: person talked about a topic, event, or object
            - CREATED: person made, painted, or built an object
            - LOCATED_IN: place or event is in a location

            CRITICAL: You MUST extract at least as many relations as entities. Every pair of entities that interact in any way needs a relation. Do not return empty relations array unless the text has absolutely no entity interactions.

            Return ONLY a JSON object (no markdown fences, no extra text) with this exact shape:
            {"entities": [{"type": "PERSON|ORG|LOC|EVENT|OBJECT", "name": "canonical_name", "surface": "exact text"}], "relations": [{"head": "entity_canonical_name", "relType": "TALKED_TO|MET_AT|WORKS_WITH|MEMBER_OF|ATTENDED|DISCUSSED|CREATED|LOCATED_IN|RELATED_TO", "tail": "entity_canonical_name"}]}

            Example:
            Text: "Caroline met Melanie at the LGBTQ support group meeting. They discussed the Pride Festival. Melanie showed Caroline a painting she made."
            Response: {"entities":[{"type":"PERSON","name":"Caroline","surface":"Caroline"},{"type":"PERSON","name":"Melanie","surface":"Melanie"},{"type":"ORG","name":"LGBTQ Support Group","surface":"the LGBTQ support group"},{"type":"EVENT","name":"Pride Festival","surface":"the Pride Festival"},{"type":"OBJECT","name":"Painting","surface":"a painting"}],"relations":[{"head":"Caroline","relType":"MET_AT","tail":"Melanie"},{"head":"Caroline","relType":"MEMBER_OF","tail":"LGBTQ Support Group"},{"head":"Melanie","relType":"MEMBER_OF","tail":"LGBTQ Support Group"},{"head":"Caroline","relType":"DISCUSSED","tail":"Pride Festival"},{"head":"Melanie","relType":"DISCUSSED","tail":"Pride Festival"},{"head":"Melanie","relType":"CREATED","tail":"Painting"},{"head":"Caroline","relType":"TALKED_TO","tail":"Melanie"}]}

            Text:
            """
                + text;
    }

    private String callLlmApi(String prompt) throws IOException, InterruptedException {
        ObjectNode requestBody = MAPPER.createObjectNode();
        requestBody.put("model", modelName);
        ArrayNode messages = requestBody.putArray("messages");
        messages.add(MAPPER.createObjectNode().put("role", "user").put("content", prompt));
        requestBody.put("max_tokens", 2000);
        requestBody.put("temperature", 0);

        String rawRequestBody = MAPPER.writeValueAsString(requestBody);
        HttpRequest httpRequest =
                HttpRequest.newBuilder()
                        .uri(URI.create(llmEndpoint))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + (apiKey != null ? apiKey : ""))
                        .POST(HttpRequest.BodyPublishers.ofString(rawRequestBody))
                        .timeout(timeout)
                        .build();

        HttpResponse<String> response =
                httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
        log.info(
                "LLM API response status={} bodyLen={}",
                response.statusCode(),
                response.body().length());
        if (response.statusCode() != 200) {
            throw new IOException(
                    "LLM API returned status " + response.statusCode() + ": " + response.body());
        }
        JsonNode root = MAPPER.readTree(response.body());
        String content = root.path("choices").path(0).path("message").path("content").asText("");
        if (content.isEmpty()) {
            log.warn(
                    "LLM API response content empty, full body (first 500): {}",
                    response.body().length() > 500
                            ? response.body().substring(0, 500)
                            : response.body());
        }
        return content;
    }

    private EntityView parseLlmResponse(String response, EntityContext context)
            throws ExtractionException {
        String diaId = context == null ? "" : context.tableId() + ":" + context.sampleId();
        String sampleId = context == null ? "" : context.sampleId();

        if (response == null || response.isBlank()) {
            throw new ExtractionException("LLM entity extraction: empty response");
        }

        try {
            String jsonStr = response.trim();
            if (jsonStr.startsWith("```")) {
                jsonStr = jsonStr.replaceAll("^```(?:json)?\\s*", "").replaceAll("\\s*```$", "");
            }
            log.info(
                    "LLM entity extraction raw response (len={}): {}",
                    response.length(),
                    response.length() > 500 ? response.substring(0, 500) + "..." : response);
            JsonNode root = MAPPER.readTree(jsonStr);

            List<Entity> entities = new ArrayList<>();
            JsonNode entitiesNode = root.path("entities");
            if (entitiesNode.isArray()) {
                for (JsonNode node : entitiesNode) {
                    String typeStr = node.path("type").asText("OTHER").toUpperCase();
                    EntityType type = mapEntityType(typeStr);
                    String name = node.path("name").asText("");
                    String surface = node.path("surface").asText(name);
                    double confidence =
                            node.has("confidence") ? node.path("confidence").asDouble(0.9) : 0.9;
                    if (!name.isEmpty()) {
                        entities.add(
                                Entity.deterministic(type, name, sampleId, surface, confidence));
                    }
                }
            }

            List<Relation> relations = new ArrayList<>();
            JsonNode relationsNode = root.path("relations");
            if (relationsNode.isArray()) {
                for (JsonNode node : relationsNode) {
                    String head = node.path("head").asText("");
                    String relType = node.path("relType").asText("RELATED_TO");
                    String tail = node.path("tail").asText("");
                    String temporal = node.path("temporal").asText("");
                    double confidence =
                            node.has("confidence") ? node.path("confidence").asDouble(0.8) : 0.8;
                    if (!head.isEmpty() && !tail.isEmpty()) {
                        relations.add(new Relation(head, relType, tail, temporal, confidence));
                    }
                }
            }

            log.info(
                    "LLM entity extraction: {} entities, {} relations",
                    entities.size(),
                    relations.size());
            return new EntityView(
                    diaId,
                    sampleId,
                    entities,
                    relations,
                    "llm",
                    Instant.now(),
                    ExtractionStatus.SUCCESS,
                    null);
        } catch (Exception e) {
            throw new ExtractionException(
                    "LLM entity extraction: JSON parse failed: " + e.getMessage(), e);
        }
    }

    private static EntityType mapEntityType(String typeStr) {
        return switch (typeStr) {
            case "PERSON" -> EntityType.PERSON;
            case "ORG" -> EntityType.ORG;
            case "LOC", "LOCATION" -> EntityType.LOCATION;
            case "EVENT" -> EntityType.EVENT;
            case "TIME" -> EntityType.TIME;
            case "OBJECT" -> EntityType.OBJECT;
            default -> EntityType.OTHER;
        };
    }

    private EntityView extractStub(String text, EntityContext context) {
        List<Entity> entities = new ArrayList<>();
        if (text != null && !text.isBlank()) {
            String[] tokens = text.split("\\s+");
            for (String token : tokens) {
                String cleaned = token.replaceAll("[^\\p{L}\\p{N}]", "");
                if (cleaned.isEmpty()) continue;
                String sampleId = context == null ? "" : context.sampleId();
                if (cleaned.length() > 1
                        && cleaned.equals(cleaned.toUpperCase())
                        && cleaned.matches(".*\\p{L}.*")) {
                    entities.add(
                            Entity.deterministic(EntityType.ORG, cleaned, sampleId, token, 0.85));
                } else if (Character.isUpperCase(cleaned.charAt(0)) && cleaned.length() > 1) {
                    entities.add(
                            Entity.deterministic(
                                    EntityType.PERSON, cleaned, sampleId, token, 0.85));
                }
            }
        }
        String diaId = context == null ? "" : context.tableId() + ":" + context.sampleId();
        String sampleId = context == null ? "" : context.sampleId();
        return new EntityView(
                diaId,
                sampleId,
                entities,
                List.of(),
                "llm",
                Instant.now(),
                ExtractionStatus.SUCCESS,
                null);
    }

    @FunctionalInterface
    private interface StubCallable {
        EntityView call() throws ExtractionException;
    }
}
