package io.github.rawwise.engine.vector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryVectorIndex implements VectorIndex {

    private final Map<String, Map<String, float[]>> index = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> l0Store = new ConcurrentHashMap<>();
    private final EmbeddingProvider embeddingProvider;
    private volatile VectorColdStore coldStore;

    public InMemoryVectorIndex(EmbeddingProvider embeddingProvider) {
        this.embeddingProvider = embeddingProvider;
    }

    @Override
    public void setColdStore(VectorColdStore coldStore) {
        this.coldStore = coldStore;
    }

    /**
     * Recover previously persisted embeddings from the cold store into memory. Must be called after
     * setColdStore().
     */
    public void recoverFromColdStore() {
        if (coldStore == null) return;
        for (String namespace : ((FlatVectorColdStore) coldStore).getAllNamespaces()) {
            List<ColdVectorEntry> entries =
                    ((FlatVectorColdStore) coldStore).getAllEntries(namespace);
            if (entries == null) continue;
            Map<String, float[]> nsIndex =
                    index.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>());
            Map<String, String> nsL0 =
                    l0Store.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>());
            for (ColdVectorEntry entry : entries) {
                nsIndex.put(entry.entryKey(), entry.embedding());
                if (entry.l0Abstract() != null && !entry.l0Abstract().isEmpty()) {
                    nsL0.put(entry.entryKey(), entry.l0Abstract());
                }
            }
        }
    }

    @Override
    public void index(String namespace, String entryKey, String l0Abstract) {
        if (l0Abstract == null || l0Abstract.isEmpty()) return;
        float[] embedding = embeddingProvider.embed(l0Abstract);
        indexWithEmbedding(namespace, entryKey, embedding, l0Abstract);
    }

    @Override
    public void indexWithEmbedding(String namespace, String entryKey, float[] embedding) {
        indexWithEmbedding(namespace, entryKey, embedding, null);
    }

    @Override
    public void indexWithEmbedding(
            String namespace, String entryKey, float[] embedding, String l0Abstract) {
        if (embedding == null || embedding.length == 0) return;
        index.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>()).put(entryKey, embedding);
        if (l0Abstract != null && !l0Abstract.isEmpty()) {
            l0Store.computeIfAbsent(namespace, k -> new ConcurrentHashMap<>())
                    .put(entryKey, l0Abstract);
        }
        // Persist to cold store for durability across restarts
        if (coldStore != null) {
            coldStore.write(
                    namespace,
                    entryKey,
                    List.of(new ColdVectorEntry(entryKey, embedding.clone(), l0Abstract)));
        }
    }

    @Override
    public List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK) {
        Map<String, float[]> namespaceIndex = index.get(namespace);
        Map<String, String> nsL0 = l0Store.get(namespace);

        // Dedup key across hot+cold: keep track of seen entryKeys
        java.util.Set<String> seen = new java.util.HashSet<>();
        List<VectorSearchResult> allResults = new ArrayList<>();

        // 1. Search in-memory (hot)
        if (namespaceIndex != null && !namespaceIndex.isEmpty()) {
            for (Map.Entry<String, float[]> entry : namespaceIndex.entrySet()) {
                float score = cosineSimilarity(queryEmbedding, entry.getValue());
                String summary = nsL0 != null ? nsL0.getOrDefault(entry.getKey(), "") : "";
                allResults.add(new VectorSearchResult(entry.getKey(), summary, score));
                seen.add(entry.getKey());
            }
        }

        // 2. Search cold store, dedup against hot
        if (coldStore != null) {
            List<VectorSearchResult> coldResults =
                    coldStore.search(namespace, queryEmbedding, topK * 2);
            for (VectorSearchResult cr : coldResults) {
                if (!seen.contains(cr.entryKey())) {
                    allResults.add(cr);
                }
            }
        }

        allResults.sort(Comparator.comparingDouble(VectorSearchResult::score).reversed());
        return allResults.stream().limit(topK).toList();
    }

    @Override
    public void delete(String namespace, String entryKey) {
        Map<String, float[]> namespaceIndex = index.get(namespace);
        if (namespaceIndex != null) namespaceIndex.remove(entryKey);
        Map<String, String> nsL0 = l0Store.get(namespace);
        if (nsL0 != null) nsL0.remove(entryKey);
        // Propagate deletion to cold store
        if (coldStore != null) {
            coldStore.delete(namespace, List.of(entryKey));
        }
    }

    private float cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) return 0f;
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        float denom = (float) (Math.sqrt(normA) * Math.sqrt(normB));
        return denom == 0 ? 0f : dot / denom;
    }

    @Override
    public void flushToCold(String namespace, Collection<String> entryKeys) {
        if (coldStore == null) {
            throw new IllegalStateException("No cold store configured for flushToCold");
        }
        Map<String, float[]> nsIndex = index.get(namespace);
        Map<String, String> nsL0 = l0Store.get(namespace);
        if (nsIndex == null) return;

        List<ColdVectorEntry> batch = new ArrayList<>();
        for (String key : entryKeys) {
            float[] embedding = nsIndex.get(key);
            if (embedding == null) continue;
            String l0 = nsL0 != null ? nsL0.get(key) : null;
            batch.add(new ColdVectorEntry(key, embedding.clone(), l0));
        }

        if (!batch.isEmpty()) {
            coldStore.write(namespace, "flush-" + System.currentTimeMillis(), batch);
            for (ColdVectorEntry e : batch) {
                nsIndex.remove(e.entryKey());
                if (nsL0 != null) nsL0.remove(e.entryKey());
            }
        }
    }
}
