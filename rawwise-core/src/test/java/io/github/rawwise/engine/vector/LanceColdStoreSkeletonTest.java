package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LanceColdStoreSkeletonTest {

    @TempDir Path tempDir;

    private LanceVectorColdStore store;

    @BeforeEach
    void setUp() {
        store = new LanceVectorColdStore(tempDir.resolve("lance"), 3);
    }

    @AfterEach
    void tearDown() {
        if (store != null) store.close();
    }

    private float[] vec(float... values) {
        return values;
    }

    @Test
    void lanceColdStore_writeReadScanSearch_endToEnd() {
        List<ColdVectorEntry> entries =
                List.of(
                        new ColdVectorEntry("key1", vec(1f, 0f, 0f), "abstract1"),
                        new ColdVectorEntry("key2", vec(0f, 1f, 0f), "abstract2"),
                        new ColdVectorEntry("key3", vec(0f, 0f, 1f), "abstract3"));
        store.write("ns1", "block1", entries);

        assertTrue(store.hasColdData("ns1"));
        assertTrue(store.getAllNamespaces().contains("ns1"));

        float[] readVec = store.readVector("ns1", "key2");
        assertNotNull(readVec);
        assertArrayEquals(vec(0f, 1f, 0f), readVec, 1e-5f);

        assertEquals("abstract3", store.readL0Abstract("ns1", "key3"));

        List<ColdVectorEntry> scanned = store.scan("ns1");
        assertEquals(3, scanned.size());
        assertTrue(scanned.stream().anyMatch(e -> e.entryKey().equals("key1")));
        assertTrue(scanned.stream().anyMatch(e -> e.entryKey().equals("key3")));

        List<VectorSearchResult> results = store.search("ns1", vec(1f, 0f, 0f), 2);
        assertEquals(2, results.size());
        assertEquals("key1", results.get(0).entryKey());
        assertTrue(results.get(0).score() > 0.99f);

        LanceManifest manifest = store.getManifest("ns1");
        assertNotNull(manifest);
        assertEquals(3, manifest.getDimension());
        assertEquals(3, manifest.getTotalRowCount());
        assertEquals(List.of("entryKey", "embedding", "l0Abstract"), manifest.getColumnNames());
        assertTrue(manifest.getFragmentIds().contains("block1"));
    }

    @Test
    void lanceColdStore_deleteExcludesFromScanAndSearch() {
        store.write(
                "ns1",
                "block1",
                List.of(
                        new ColdVectorEntry("key1", vec(1f, 0f, 0f), "a1"),
                        new ColdVectorEntry("key2", vec(0f, 1f, 0f), "a2")));
        store.delete("ns1", List.of("key1"));

        assertNull(store.readVector("ns1", "key1"));
        assertEquals(1, store.scan("ns1").size());
        List<VectorSearchResult> results = store.search("ns1", vec(1f, 0f, 0f), 10);
        assertEquals(1, results.size());
        assertEquals("key2", results.get(0).entryKey());
    }

    @Test
    void lanceManifest_persistsSchemaAndRowCountAcrossRestart() {
        store.write(
                "ns1",
                "block1",
                List.of(
                        new ColdVectorEntry("key1", vec(1f, 0f, 0f), "a1"),
                        new ColdVectorEntry("key2", vec(0f, 1f, 0f), "a2")));
        store.write("ns1", "block2", List.of(new ColdVectorEntry("key3", vec(0f, 0f, 1f), "a3")));
        store.close();
        store = null;

        LanceVectorColdStore recovered = new LanceVectorColdStore(tempDir.resolve("lance"), 3);
        try {
            assertTrue(recovered.hasColdData("ns1"));
            LanceManifest manifest = recovered.getManifest("ns1");
            assertNotNull(manifest);
            assertEquals(3, manifest.getDimension());
            assertEquals(3, manifest.getTotalRowCount());
            assertEquals(List.of("entryKey", "embedding", "l0Abstract"), manifest.getColumnNames());
            assertTrue(manifest.getFragmentIds().contains("block1"));
            assertTrue(manifest.getFragmentIds().contains("block2"));

            List<VectorSearchResult> results = recovered.search("ns1", vec(0f, 0f, 1f), 1);
            assertEquals(1, results.size());
            assertEquals("key3", results.get(0).entryKey());
        } finally {
            recovered.close();
        }
    }

    @Test
    void lanceFragment_metadataAndReadAccessors() {
        Path fragPath = tempDir.resolve("frag.lance");
        LanceFragment frag =
                LanceFragment.write(
                        fragPath,
                        3,
                        List.of(
                                new ColdVectorEntry("k1", vec(1f, 0f, 0f), "abs1"),
                                new ColdVectorEntry("k2", vec(0f, 1f, 0f), "abs2")));
        try {
            assertEquals(2, frag.getNumRows());
            assertEquals(3, frag.getDimension());
            assertEquals(fragPath, frag.getFilePath());
            assertTrue(frag.fileLength() > 0);
            assertTrue(frag.vectorDataOffset() > 0);

            LanceFragment.FragmentMetadata meta = frag.metadata();
            assertEquals(fragPath, meta.filePath());
            assertEquals(3, meta.dimension());
            assertEquals(2, meta.numRows());
            assertEquals(frag.fileLength(), meta.fileLength());
            assertEquals(frag.vectorDataOffset(), meta.vectorDataOffset());

            assertEquals("k1", frag.getEntryKey(0));
            assertArrayEquals(vec(1f, 0f, 0f), frag.getVector(0), 1e-5f);
            assertEquals("abs2", frag.getL0Abstract(1));
        } finally {
            frag.close();
        }
    }

    @Test
    void vectorIndexManager_createStatsRebuildDrop_lifecycle() {
        IvfPqConfig config = new IvfPqConfig(3, 16, 8, 4, 10, 20, 1000, 42L, 3);
        VectorIndexManager manager = new VectorIndexManager(config, true, tempDir.resolve("vim"));
        try {
            assertTrue(manager.isTableFormatMode());

            ShardInternalIndex idx = manager.createIndex("t1", "emb", 3);
            assertNotNull(idx);
            assertSame(idx, manager.getIndex("t1", "emb"));

            manager.recordSearch("t1", "emb", 5);
            manager.recordSearch("t1", "emb", 3);

            VectorIndexManager.IndexStats stats = manager.getStats("t1", "emb");
            assertEquals(3, stats.dimension());
            assertEquals(2L, stats.searchCount());
            assertEquals(8L, stats.totalResultsReturned());
            assertTrue(stats.lastSearchedMs() > 0);

            assertEquals(1, manager.getAllStats().size());

            manager.rebuildIndex(
                    "t1", "emb", List.of(new VectorSchemaDiff.DimensionChanged("emb", 3, 8)));
            ShardInternalIndex rebuilt = manager.getIndex("t1", "emb");
            assertNotNull(rebuilt);
            assertNotSame(idx, rebuilt);
            VectorIndexManager.IndexStats statsAfterRebuild = manager.getStats("t1", "emb");
            assertEquals(8, statsAfterRebuild.dimension());

            manager.dropIndex("t1", "emb");
            assertNull(manager.getIndex("t1", "emb"));
            VectorIndexManager.IndexStats statsAfterDrop = manager.getStats("t1", "emb");
            assertEquals(0L, statsAfterDrop.searchCount());
            assertTrue(manager.getAllStats().isEmpty());
        } finally {
            manager.close();
        }
    }

    @Test
    void remoteShardIndex_routesSearchAndCachesMetadata() {
        EmbeddingProvider provider = new StubEmbeddingProvider(3);
        try (InMemoryVectorIndex localIndex = new InMemoryVectorIndex(provider)) {
            localIndex.indexWithEmbedding("ns", "rk1", vec(1f, 0f, 0f));
            localIndex.indexWithEmbedding("ns", "rk2", vec(0f, 1f, 0f));

            ShardClient client =
                    (tableId, shardId, query, topK) ->
                            localIndex.search("ns", query, topK).stream()
                                    .map(r -> new SearchCandidate(r.entryKey(), r.score()))
                                    .toList();

            RemoteShardIndex remote = new RemoteShardIndex("t1", "shard-1", client);
            try {
                assertTrue(remote.isAvailable());
                assertEquals("t1", remote.getTableId());
                assertEquals("shard-1", remote.getShardId());

                remote.updateLocation("rk1", VectorLocation.WARM, "lance://shard-1/rk1");
                assertEquals(VectorLocation.WARM, remote.getLocation("rk1"));
                assertEquals("lance://shard-1/rk1", remote.getStorageRef("rk1"));
                assertEquals(1, remote.vectorCount());
                assertTrue(remote.entryKeys().contains("rk1"));

                remote.cacheVector("rk1", vec(1f, 0f, 0f));
                float[] cached = remote.getVector("rk1");
                assertNotNull(cached);
                assertArrayEquals(vec(1f, 0f, 0f), cached, 1e-5f);

                List<SearchCandidate> results = remote.search(vec(1f, 0f, 0f), 2);
                assertFalse(results.isEmpty());
                assertTrue(results.stream().anyMatch(c -> c.entryKey().equals("rk1")));

                remote.markTombstone("rk1");
                assertNull(remote.getLocation("rk1"));
                assertNull(remote.getVector("rk1"));
                assertFalse(remote.entryKeys().contains("rk1"));
                assertEquals(0, remote.vectorCount());
            } finally {
                remote.close();
            }
        }
    }

    @Test
    void remoteShardIndex_unavailableReturnsEmptySearch() {
        ShardClient unavailableClient =
                new ShardClient() {
                    @Override
                    public List<SearchCandidate> search(
                            String tableId, String shardId, float[] queryEmbedding, int topK) {
                        return List.of();
                    }

                    @Override
                    public boolean isAvailable(String shardId) {
                        return false;
                    }
                };
        RemoteShardIndex remote = new RemoteShardIndex("t1", "shard-down", unavailableClient);
        try {
            assertFalse(remote.isAvailable());
            List<SearchCandidate> results = remote.search(vec(1f, 0f, 0f), 5);
            assertTrue(results.isEmpty());
        } finally {
            remote.close();
        }
    }

    @Test
    void asyncEmbeddingProvider_completableFutureReturnsEmbedding() {
        TestableHttpEmbeddingProvider delegate = new TestableHttpEmbeddingProvider(4);
        try (AsyncEmbeddingProvider async = new AsyncEmbeddingProvider(delegate, 2, 8, 5000)) {
            CompletableFuture<float[]> future = async.embedAsyncFuture("hello world");
            float[] embedding = future.get(3, TimeUnit.SECONDS);
            assertNotNull(embedding);
            assertEquals(4, embedding.length);

            CompletableFuture<float[][]> batchFuture =
                    async.embedBatchAsyncFuture(List.of("a", "b", "c"));
            float[][] batch = batchFuture.get(3, TimeUnit.SECONDS);
            assertEquals(3, batch.length);
            for (float[] v : batch) assertEquals(4, v.length);
            assertEquals("test-model", async.modelIdentifier());
        } catch (TimeoutException e) {
            fail("Embedding future timed out", e);
        } catch (Exception e) {
            fail("Unexpected exception", e);
        }
    }

    static class TestableHttpEmbeddingProvider extends HttpEmbeddingProvider {
        private final int dimension;

        TestableHttpEmbeddingProvider(int dimension) {
            super(null, null, null, null, dimension, 0, 32, new CircuitBreaker(5, 30_000L), 1000);
            this.dimension = dimension;
        }

        @Override
        public float[] embed(String text) {
            float[] v = new float[dimension];
            int seed = text == null ? 0 : text.hashCode();
            for (int i = 0; i < dimension; i++) {
                v[i] = ((seed >> i) & 0xFF) / 255.0f;
            }
            return v;
        }

        @Override
        public float[][] embedBatch(List<String> texts) {
            return texts.stream().map(this::embed).toArray(float[][]::new);
        }

        @Override
        CircuitBreaker.State getCircuitBreakerState() {
            return CircuitBreaker.State.CLOSED;
        }

        @Override
        public String modelIdentifier() {
            return "test-model";
        }

        @Override
        public void close() {}
    }
}
