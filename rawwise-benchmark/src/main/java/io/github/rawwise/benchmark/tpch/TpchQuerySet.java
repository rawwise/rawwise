package io.github.rawwise.benchmark.tpch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TpchQuerySet implements io.github.rawwise.benchmark.QuerySet {

    private static final String QUERY_RESOURCE_PATH = "tpch/queries/";
    private static final String[] TABLE_NAMES = {
        "lineitem", "orders", "customer", "nation", "region", "part", "partsupp", "supplier"
    };

    private final Map<String, String> templates;

    public TpchQuerySet() {
        this.templates = loadTemplates();
    }

    private static Map<String, String> loadTemplates() {
        Map<String, String> map = new HashMap<>();
        String[] queryNames = {
            "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
            "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20",
            "q21", "q22"
        };
        for (String name : queryNames) {
            String resourcePath = QUERY_RESOURCE_PATH + name + ".sql";
            try (InputStream is =
                    TpchQuerySet.class.getClassLoader().getResourceAsStream(resourcePath)) {
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
        if (catalogPrefix != null && !catalogPrefix.isEmpty()) {
            String qualified = catalogPrefix + ".tpch.";
            for (String table : TABLE_NAMES) {
                template = template.replaceAll("\\b" + table + "\\b", qualified + table);
            }
        }
        return template;
    }

    @Override
    public Set<String> queryIds() {
        return templates.keySet();
    }

    public static TpchQuerySet forCatalog(String catalogPrefix) {
        return new TpchQuerySet();
    }
}
