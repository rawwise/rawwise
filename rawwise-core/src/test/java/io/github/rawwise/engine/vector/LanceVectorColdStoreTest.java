package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LanceVectorColdStoreTest {

    @TempDir Path tempDir;

    private float[] vec(float... values) {
        return values;
    }

    @Test
    void writeAndSearchReturnsResults() {
        LanceVectorColdStore store = new LanceVectorColdStore(tempDir, 3);
        List<ColdVectorEntry> entries =
                List.of(
                        new ColdVectorEntry("key1", vec(1f, 0f, 0f), "abstract1"),
                        new ColdVectorEntry("key2", vec(0f, 1f, 0f), "abstract2"),
                        new ColdVectorEntry("key3", vec(0f, 0f, 1f), "abstract3"));
        store.write("ns1", "block1", entries);

        assertTrue(store.hasColdData("ns1"));
        assertFalse(store.hasColdData("ns2"));

        List<VectorSearchResult> results = store.search("ns1", vec(1f, 0f, 0f), 2);
        assertEquals(2, results.size());
        assertEquals("key1", results.get(0).entryKey());
        assertTrue(results.get(0).score() > 0.9f);
        store.close();
    }

    @Test
    void deleteRemovesEntriesAndPersistsAcrossRestart() {
        LanceVectorColdStore store1 = new LanceVectorColdStore(tempDir, 3);
        store1.write(
                "ns1",
                "block1",
                List.of(
                        new ColdVectorEntry("key1", vec(1f, 0f, 0f), "a1"),
                        new ColdVectorEntry("key2", vec(0f, 1f, 0f), "a2")));
        store1.delete("ns1", List.of("key1"));
        store1.close();

        LanceVectorColdStore store2 = new LanceVectorColdStore(tempDir, 3);
        List<VectorSearchResult> results = store2.search("ns1", vec(1f, 0.5f, 0f), 10);
        assertEquals(1, results.size());
        assertEquals("key2", results.get(0).entryKey());
        store2.close();
    }

    @Test
    void recoverFromDiskAfterRestart() {
        LanceVectorColdStore store1 = new LanceVectorColdStore(tempDir, 3);
        store1.write("ns1", "block1", List.of(new ColdVectorEntry("key1", vec(1f, 0f, 0f), "a1")));
        store1.close();

        LanceVectorColdStore store2 = new LanceVectorColdStore(tempDir, 3);
        assertTrue(store2.hasColdData("ns1"));
        List<VectorSearchResult> results = store2.search("ns1", vec(1f, 0f, 0f), 10);
        assertEquals(1, results.size());
        assertEquals("key1", results.get(0).entryKey());
        store2.close();
    }

    @Test
    void maxEntriesLimitDropsExcess() {
        LanceVectorColdStore store = new LanceVectorColdStore(tempDir, 3, 2);
        store.write(
                "ns1",
                "block1",
                List.of(
                        new ColdVectorEntry("key1", vec(1f, 0f, 0f), "a1"),
                        new ColdVectorEntry("key2", vec(0f, 1f, 0f), "a2")));
        store.write("ns1", "block2", List.of(new ColdVectorEntry("key3", vec(0f, 0f, 1f), "a3")));
        List<VectorSearchResult> results = store.search("ns1", vec(1f, 1f, 1f), 10);
        assertEquals(2, results.size());
        store.close();
    }

    @Test
    void writeEmptyEntriesIsNoop() {
        LanceVectorColdStore store = new LanceVectorColdStore(tempDir, 3);
        store.write("ns1", "block1", List.of());
        assertFalse(store.hasColdData("ns1"));
        store.close();
    }

    @Test
    void searchEmptyNamespaceReturnsEmpty() {
        LanceVectorColdStore store = new LanceVectorColdStore(tempDir, 3);
        List<VectorSearchResult> results = store.search("nonexistent", vec(1f, 0f, 0f), 10);
        assertTrue(results.isEmpty());
        store.close();
    }

    @Test
    void longL0AbstractDoesNotCrash() {
        LanceVectorColdStore store = new LanceVectorColdStore(tempDir, 3);
        String longAbstract = "x".repeat(100_000);
        store.write(
                "ns1",
                "block1",
                List.of(new ColdVectorEntry("key1", vec(1f, 0f, 0f), longAbstract)));
        List<VectorSearchResult> results = store.search("ns1", vec(1f, 0f, 0f), 10);
        assertEquals(1, results.size());
        store.close();
    }

    @Test
    void multipleFragmentsSearchedTogether() {
        LanceVectorColdStore store = new LanceVectorColdStore(tempDir, 3);
        store.write("ns1", "block1", List.of(new ColdVectorEntry("key1", vec(1f, 0f, 0f), "a1")));
        store.write("ns1", "block2", List.of(new ColdVectorEntry("key2", vec(0f, 1f, 0f), "a2")));

        List<VectorSearchResult> results = store.search("ns1", vec(0.5f, 0.5f, 0f), 10);
        assertEquals(2, results.size());
        store.close();
    }

    @Test
    void deletePersistsAcrossMultipleRestarts() {
        LanceVectorColdStore store1 = new LanceVectorColdStore(tempDir, 3);
        store1.write(
                "ns1",
                "block1",
                List.of(
                        new ColdVectorEntry("key1", vec(1f, 0f, 0f), "a1"),
                        new ColdVectorEntry("key2", vec(0f, 1f, 0f), "a2"),
                        new ColdVectorEntry("key3", vec(0f, 0f, 1f), "a3")));
        store1.delete("ns1", List.of("key1", "key3"));
        store1.close();

        LanceVectorColdStore store2 = new LanceVectorColdStore(tempDir, 3);
        List<VectorSearchResult> results2 = store2.search("ns1", vec(1f, 1f, 1f), 10);
        assertEquals(1, results2.size());
        assertEquals("key2", results2.get(0).entryKey());
        store2.close();

        LanceVectorColdStore store3 = new LanceVectorColdStore(tempDir, 3);
        List<VectorSearchResult> results3 = store3.search("ns1", vec(1f, 1f, 1f), 10);
        assertEquals(1, results3.size());
        assertEquals("key2", results3.get(0).entryKey());
        store3.close();
    }
}
