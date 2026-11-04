package io.github.rawwise.axiom.memory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface AxiomClient {

    String ingest(String tableId, String key, String body, Map<String, String> properties);

    List<Map<String, Object>> search(String tableId, String query, int topK, String namespace);

    default List<Map<String, Object>> search(
            String tableId, String query, int topK, String namespace, Map<String, String> filter) {
        return search(tableId, query, topK, namespace);
    }

    Optional<Map<String, Object>> getEntry(String tableId, String key);

    List<Map<String, Object>> scan(String tableId, String namespace);
}
