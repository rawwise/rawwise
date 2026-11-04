package io.github.rawwise.benchmark.tpcds;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TpcdsQuerySet implements io.github.rawwise.benchmark.QuerySet {

    private static final String QUERY_RESOURCE_PATH = "tpcds/queries/";

    private final Map<String, String> templates;

    public TpcdsQuerySet() {
        this.templates = loadTemplates();
    }

    private static Map<String, String> loadTemplates() {
        Map<String, String> map = new HashMap<>();
        String[] queryNames = {
            "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
            "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20",
            "q21", "q22", "q23", "q24", "q25"
        };
        for (String name : queryNames) {
            String resourcePath = QUERY_RESOURCE_PATH + name + ".sql";
            try (InputStream is =
                    TpcdsQuerySet.class.getClassLoader().getResourceAsStream(resourcePath)) {
                if (is == null) {
                    throw new IllegalStateException("Query template not found: " + resourcePath);
                }
                String content =
                        new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                                .lines()
                                .collect(Collectors.joining("\n"));
                map.put(name, content);
            } catch (IOException e) {
                throw new RuntimeException("Failed to load query template: " + resourcePath, e);
            }
        }
        return map;
    }

    @Override
    public String getQuery(String queryId, String catalogPrefix) {
        String template = templates.get(queryId);
        if (template == null) {
            throw new IllegalArgumentException("Unknown query ID: " + queryId);
        }
        return template;
    }

    @Override
    public Set<String> queryIds() {
        return templates.keySet();
    }
}
