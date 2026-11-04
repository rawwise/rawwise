package io.github.rawwise.axiom.memory;

import java.util.*;

/**
 * In-memory AxiomClient implementation for unit tests. Stores entries in a HashMap keyed by entry
 * key.
 */
public class InMemoryAxiomClient implements AxiomClient {

    private final Map<String, Map<String, Object>> store = new HashMap<>();

    @Override
    public String ingest(String tableId, String key, String body, Map<String, String> properties) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("body", body);
        entry.put("properties", properties);
        entry.put("tableId", tableId);
        entry.put("entryKey", key);
        store.put(key, entry);
        return key;
    }

    @Override
    public List<Map<String, Object>> search(
            String tableId, String query, int topK, String namespace) {
        return List.of();
    }

    @Override
    public Optional<Map<String, Object>> getEntry(String tableId, String key) {
        return Optional.ofNullable(store.get(key));
    }

    @Override
    public List<Map<String, Object>> scan(String tableId, String namespace) {
        return new ArrayList<>(store.values());
    }

    public boolean contains(String key) {
        return store.containsKey(key);
    }

    public void clear() {
        store.clear();
    }
}
