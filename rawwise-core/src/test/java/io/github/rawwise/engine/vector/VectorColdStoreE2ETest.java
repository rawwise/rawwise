package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class VectorColdStoreE2ETest {

    @TempDir Path tempDir;

    private float[] vec(float... values) {
        return values;
    }

    @Test
    void inMemoryIndex_searchMergesHotAndCold() {
        EmbeddingProvider provider = text -> vec(1f, 0f, 0f);
        try (InMemoryVectorIndex index = new InMemoryVectorIndex(provider);
                FlatVectorColdStore coldStore =
                        new FlatVectorColdStore(tempDir.resolve("cold"), 3)) {

            index.setColdStore(coldStore);

            index.indexWithEmbedding("ns1", "hot1", vec(1f, 0f, 0f));
            index.indexWithEmbedding("ns1", "hot2", vec(0.9f, 0.1f, 0f));

            coldStore.write(
                    "ns1",
                    "block1",
                    List.of(
                            new ColdVectorEntry("cold1", vec(0f, 1f, 0f), "cold abstract"),
                            new ColdVectorEntry("cold2", vec(0f, 0f, 1f), "cold abstract 2")));

            List<VectorSearchResult> results = index.search("ns1", vec(1f, 0f, 0f), 10);

            assertFalse(results.isEmpty());
            assertTrue(results.stream().anyMatch(r -> r.entryKey().equals("hot1")));
            assertTrue(results.stream().anyMatch(r -> r.entryKey().equals("cold1")));
        }
    }

    @Test
    void flushToCold_movesEntriesFromHotToCold() {
        EmbeddingProvider provider = text -> vec(1f, 0f, 0f);
        try (InMemoryVectorIndex index = new InMemoryVectorIndex(provider);
                FlatVectorColdStore coldStore =
                        new FlatVectorColdStore(tempDir.resolve("cold"), 3)) {

            index.setColdStore(coldStore);

            index.indexWithEmbedding("ns1", "key1", vec(1f, 0f, 0f));
            index.indexWithEmbedding("ns1", "key2", vec(0f, 1f, 0f));

            index.flushToCold("ns1", List.of("key1"));

            List<VectorSearchResult> results = index.search("ns1", vec(1f, 0f, 0f), 10);
            assertTrue(results.stream().anyMatch(r -> r.entryKey().equals("key1")));
            assertTrue(coldStore.hasColdData("ns1"));
        }
    }

    @Test
    void delete_propagatesToColdStore() {
        EmbeddingProvider provider = text -> vec(1f, 0f, 0f);
        try (InMemoryVectorIndex index = new InMemoryVectorIndex(provider);
                FlatVectorColdStore coldStore =
                        new FlatVectorColdStore(tempDir.resolve("cold"), 3)) {

            index.setColdStore(coldStore);

            coldStore.write(
                    "ns1", "block1", List.of(new ColdVectorEntry("cold1", vec(1f, 0f, 0f), "a1")));

            index.delete("ns1", "cold1");

            List<VectorSearchResult> results = coldStore.search("ns1", vec(1f, 0f, 0f), 10);
            assertTrue(results.isEmpty());
        }
    }

    @Test
    void flushToCold_noDataLossOnWriteFailure() {
        EmbeddingProvider provider = text -> vec(1f, 0f, 0f);
        try (InMemoryVectorIndex index = new InMemoryVectorIndex(provider)) {

            index.indexWithEmbedding("ns1", "key1", vec(1f, 0f, 0f));

            assertThrows(
                    IllegalStateException.class, () -> index.flushToCold("ns1", List.of("key1")));

            List<VectorSearchResult> results = index.search("ns1", vec(1f, 0f, 0f), 10);
            assertTrue(results.stream().anyMatch(r -> r.entryKey().equals("key1")));
        }
    }

    @Test
    void jVectorIndex_searchMergesHotAndCold() {
        JVectorConfig config =
                new JVectorConfig(
                        "memory", 3, 16, 10, 50, tempDir.toString(), 10000, 10000, 500000);
        EmbeddingProvider provider = text -> vec(1f, 0f, 0f);
        try (JVectorVectorIndex index = new JVectorVectorIndex(config, provider);
                FlatVectorColdStore coldStore =
                        new FlatVectorColdStore(tempDir.resolve("cold"), 3)) {

            index.setColdStore(coldStore);

            index.indexWithEmbedding("ns1", "hot1", vec(1f, 0f, 0f));
            index.indexWithEmbedding("ns1", "hot2", vec(0.9f, 0.1f, 0f));

            coldStore.write(
                    "ns1",
                    "block1",
                    List.of(
                            new ColdVectorEntry("cold1", vec(0f, 1f, 0f), "cold abstract"),
                            new ColdVectorEntry("cold2", vec(0f, 0f, 1f), "cold abstract 2")));

            List<VectorSearchResult> results = index.search("ns1", vec(1f, 0f, 0f), 10);

            assertFalse(results.isEmpty());
            assertTrue(results.stream().anyMatch(r -> r.entryKey().equals("hot1")));
            assertTrue(results.stream().anyMatch(r -> r.entryKey().equals("cold1")));
        }
    }

    @Test
    void jVectorIndex_flushToCold_movesEntriesFromHotToCold() {
        JVectorConfig config =
                new JVectorConfig(
                        "memory", 3, 16, 10, 50, tempDir.toString(), 10000, 10000, 500000);
        EmbeddingProvider provider = text -> vec(1f, 0f, 0f);
        try (JVectorVectorIndex index = new JVectorVectorIndex(config, provider);
                FlatVectorColdStore coldStore =
                        new FlatVectorColdStore(tempDir.resolve("cold"), 3)) {

            index.setColdStore(coldStore);

            index.indexWithEmbedding("ns1", "key1", vec(1f, 0f, 0f));
            index.indexWithEmbedding("ns1", "key2", vec(0f, 1f, 0f));

            index.flushToCold("ns1", List.of("key1"));

            List<VectorSearchResult> results = index.search("ns1", vec(1f, 0f, 0f), 10);
            assertTrue(results.stream().anyMatch(r -> r.entryKey().equals("key1")));
            assertTrue(coldStore.hasColdData("ns1"));
        }
    }
}
