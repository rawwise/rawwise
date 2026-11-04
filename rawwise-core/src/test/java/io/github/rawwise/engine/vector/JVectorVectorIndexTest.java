package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("JVectorVectorIndex - LMDBafter vectorindexfull chaintest")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class JVectorVectorIndexTest {

    private static final int DIMENSION = 8;

    @TempDir Path tempDir;

    private StubEmbeddingProvider embeddingProvider;
    private JVectorConfig config;
    private JVectorVectorIndex index;

    private JVectorConfig createConfig() {
        return new JVectorConfig(
                "lmdb", DIMENSION, 16, 10, 50, tempDir.toString(), 10000, 10000, 500000);
    }

    @BeforeEach
    void setUp() {
        embeddingProvider = new StubEmbeddingProvider(DIMENSION);
        config = createConfig();
        index = new JVectorVectorIndex(config, embeddingProvider);
    }

    @AfterEach
    void tearDown() {
        if (index != null) {
            try {
                index.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Nested
    @DisplayName("S1-HappyPath - indexwrite delete")
    class HappyPath {

        @Test
        @DisplayName("index + search: after writingcan to")
        void indexAndSearch() {
            index.index("agent-1", "key1", "hello world");
            index.index("agent-1", "key2", "hello there");

            float[] query = embeddingProvider.embed("hello");
            List<VectorSearchResult> results = index.search("agent-1", query, 10);

            assertThat(results).hasSize(2);
            assertThat(results.get(0).score()).isGreaterThan(0);
        }

        @Test
        @DisplayName("indexWithEmbedding: directly vector")
        void indexWithEmbedding() {
            float[] emb = embeddingProvider.embed("test embedding");
            index.indexWithEmbedding("agent-1", "key1", emb);

            List<VectorSearchResult> results = index.search("agent-1", emb, 10);
            assertThat(results).hasSize(1);
            assertThat(results.get(0).entryKey()).isEqualTo("key1");
        }

        @Test
        @DisplayName("delete: deleteafter to")
        void deleteRemovesEntry() {
            index.index("agent-1", "key1", "hello");
            index.delete("agent-1", "key1");

            float[] query = embeddingProvider.embed("hello");
            List<VectorSearchResult> results = index.search("agent-1", query, 10);
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("multiAgentisolation: differentagentofdata can")
        void multiAgentIsolation() {
            index.index("agent-1", "key1", "hello from agent 1");
            index.index("agent-2", "key2", "hello from agent 2");

            float[] query = embeddingProvider.embed("hello");
            List<VectorSearchResult> results1 = index.search("agent-1", query, 10);
            List<VectorSearchResult> results2 = index.search("agent-2", query, 10);

            assertThat(results1).hasSize(1);
            assertThat(results1.get(0).entryKey()).isEqualTo("key1");
            assertThat(results2).hasSize(1);
            assertThat(results2.get(0).entryKey()).isEqualTo("key2");
        }

        @Test
        @DisplayName("agent returns empty results")
        void emptyAgentReturnsEmpty() {
            float[] query = embeddingProvider.embed("hello");
            List<VectorSearchResult> results = index.search("unknown-agent", query, 10);
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("topKlimit")
        void topKLimit() {
            for (int i = 0; i < 10; i++) {
                index.index("agent-1", "key" + i, "document " + i);
            }

            float[] query = embeddingProvider.embed("document");
            List<VectorSearchResult> results = index.search("agent-1", query, 3);
            assertThat(results).hasSize(3);
        }
    }

    @Nested
    @DisplayName("S2-Concurrency - concurrentoperations")
    class Concurrency {

        @Test
        @DisplayName("4 concurrentindex+search: allwrite succeeds can")
        void concurrentIndexAndSearch() throws Exception {
            int threadCount = 4;
            int writesPerThread = 20;
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger failureCount = new AtomicInteger(0);

            Runnable writer =
                    () -> {
                        try {
                            start.await();
                            for (int i = 0; i < writesPerThread; i++) {
                                index.index(
                                        "agent-1",
                                        "key-" + Thread.currentThread().getId() + "-" + i,
                                        "document " + i);
                            }
                            successCount.incrementAndGet();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            failureCount.incrementAndGet();
                        }
                    };

            var executor = Executors.newFixedThreadPool(threadCount);
            for (int i = 0; i < threadCount; i++) {
                executor.submit(writer);
            }
            start.countDown();
            executor.shutdown();
            boolean terminated = executor.awaitTermination(30, TimeUnit.SECONDS);
            assertThat(terminated).isTrue();

            assertThat(successCount.get()).isEqualTo(threadCount);
            assertThat(failureCount.get()).isEqualTo(0);

            float[] query = embeddingProvider.embed("document");
            List<VectorSearchResult> results =
                    index.search("agent-1", query, threadCount * writesPerThread);
            assertThat(results.size())
                    .isGreaterThanOrEqualTo((threadCount * writesPerThread) * 9 / 10);
        }

        @Test
        @DisplayName("concurrentsearch+close")
        void concurrentSearchAndClose() throws Exception {
            for (int i = 0; i < 10; i++) {
                index.index("agent-1", "key" + i, "document " + i);
            }

            AtomicInteger searchSuccess = new AtomicInteger(0);
            AtomicInteger searchFail = new AtomicInteger(0);
            CountDownLatch start = new CountDownLatch(1);

            Runnable searcher =
                    () -> {
                        try {
                            start.await();
                            float[] query = embeddingProvider.embed("document");
                            index.search("agent-1", query, 10);
                            searchSuccess.incrementAndGet();
                        } catch (IllegalStateException e) {
                            searchFail.incrementAndGet();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    };

            var executor = Executors.newFixedThreadPool(4);
            for (int i = 0; i < 3; i++) {
                executor.submit(searcher);
            }
            executor.submit(
                    () -> {
                        try {
                            start.await();
                            Thread.sleep(10);
                            index.close();
                        } catch (Exception ignored) {
                        }
                    });
            start.countDown();
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);

            assertThat(searchSuccess.get() + searchFail.get()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("S3-Boundary - boundary conditions")
    class Boundary {

        @Test
        @DisplayName("index index")
        void indexEmptyAbstractSkipped() {
            index.index("agent-1", "key1", "");
            index.index("agent-1", "key2", null);

            float[] query = embeddingProvider.embed("hello");
            List<VectorSearchResult> results = index.search("agent-1", query, 10);
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("indexWithEmbedding vector index")
        void indexWithEmptyEmbeddingSkipped() {
            index.indexWithEmbedding("agent-1", "key1", new float[0]);
            index.indexWithEmbedding("agent-1", "key2", null);

            float[] query = embeddingProvider.embed("hello");
            List<VectorSearchResult> results = index.search("agent-1", query, 10);
            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("deletenon-existent keydoes not throw exception")
        void deleteNonExistentNoThrow() {
            assertThatCode(() -> index.delete("agent-1", "nonexistent")).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("closeafteroperations IllegalStateException")
        void operationsAfterCloseThrow() {
            index.index("agent-1", "key1", "hello");
            index.close();

            assertThatThrownBy(() -> index.index("agent-1", "key2", "world"))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> index.search("agent-1", new float[DIMENSION], 10))
                    .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> index.delete("agent-1", "key1"))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        @DisplayName("singleitemvectorindex+search")
        void singleVectorIndexAndSearch() {
            index.index("agent-1", "key1", "hello world");

            float[] query = embeddingProvider.embed("hello world");
            List<VectorSearchResult> results = index.search("agent-1", query, 10);
            assertThat(results).hasSize(1);
            assertThat(results.get(0).entryKey()).isEqualTo("key1");
        }

        @Test
        @DisplayName("indexWithEmbedding do not match IllegalArgumentException")
        void dimensionMismatchOnIndexThrows() {
            assertThatThrownBy(
                            () ->
                                    index.indexWithEmbedding(
                                            "agent-1", "key1", new float[DIMENSION + 1]))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Dimension mismatch");
        }

        @Test
        @DisplayName("search do not match IllegalArgumentException")
        void dimensionMismatchOnSearchThrows() {
            assertThatThrownBy(() -> index.search("agent-1", new float[DIMENSION + 1], 10))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("dimension mismatch");
        }

        @Test
        @DisplayName("closeidempotent: duplicateclosedoes not throw exception")
        void closeIdempotent() {
            index.index("agent-1", "key1", "hello");
            index.close();
            index.close();
        }
    }

    @Nested
    @DisplayName("S4-LmdbPersistence - LMDBpersistedandOnDiskGraphIndex")
    class LmdbPersistence {

        @Test
        @DisplayName("writeexceedsbuildThresholdafterLMDBpersistedsuccess consistent")
        void searchConsistentAfterLmdbPersist() {
            JVectorConfig smallThresholdConfig =
                    new JVectorConfig(
                            "lmdb", DIMENSION, 16, 10, 50, tempDir.toString(), 5, 10000, 500000);
            try (JVectorVectorIndex smallIndex =
                    new JVectorVectorIndex(smallThresholdConfig, embeddingProvider)) {
                for (int i = 0; i < 8; i++) {
                    smallIndex.index("agent-1", "key" + i, "document " + i);
                }

                float[] query = embeddingProvider.embed("document 0");
                List<VectorSearchResult> results = smallIndex.search("agent-1", query, 10);
                assertThat(results).isNotEmpty();
                assertThat(results.stream().anyMatch(r -> r.entryKey().equals("key0"))).isTrue();
                assertThat(results.get(0).score()).isGreaterThan(0);
            }
        }

        @Test
        @DisplayName("overflow in main+overflowmerge")
        void overflowMergeSearch() {
            JVectorConfig smallThresholdConfig =
                    new JVectorConfig(
                            "lmdb", DIMENSION, 16, 10, 50, tempDir.toString(), 3, 10000, 500000);
            try (JVectorVectorIndex smallIndex =
                    new JVectorVectorIndex(smallThresholdConfig, embeddingProvider)) {
                for (int i = 0; i < 3; i++) {
                    smallIndex.index("agent-1", "main" + i, "main document " + i);
                }
                for (int i = 0; i < 3; i++) {
                    smallIndex.index("agent-1", "overflow" + i, "overflow document " + i);
                }

                float[] query = embeddingProvider.embed("document");
                List<VectorSearchResult> results = smallIndex.search("agent-1", query, 6);
                assertThat(results).hasSize(6);
            }
        }

        @Test
        @DisplayName("overflow in topKtruncatecorrect")
        void overflowTopKTruncation() {
            JVectorConfig smallThresholdConfig =
                    new JVectorConfig(
                            "lmdb", DIMENSION, 16, 10, 50, tempDir.toString(), 3, 10000, 500000);
            try (JVectorVectorIndex smallIndex =
                    new JVectorVectorIndex(smallThresholdConfig, embeddingProvider)) {
                for (int i = 0; i < 3; i++) {
                    smallIndex.index("agent-1", "main" + i, "main document " + i);
                }
                for (int i = 0; i < 3; i++) {
                    smallIndex.index("agent-1", "overflow" + i, "overflow document " + i);
                }

                float[] query = embeddingProvider.embed("document");
                List<VectorSearchResult> results = smallIndex.search("agent-1", query, 3);
                assertThat(results).hasSize(3);
                assertThat(results)
                        .isSortedAccordingTo(
                                java.util.Comparator.comparingDouble(VectorSearchResult::score)
                                        .reversed());
            }
        }

        @Test
        @DisplayName("deleteconsistency(LMDBpersistedafter)")
        void deleteConsistentAfterLmdbPersist() {
            JVectorConfig smallThresholdConfig =
                    new JVectorConfig(
                            "lmdb", DIMENSION, 16, 10, 50, tempDir.toString(), 3, 10000, 500000);
            try (JVectorVectorIndex smallIndex =
                    new JVectorVectorIndex(smallThresholdConfig, embeddingProvider)) {
                for (int i = 0; i < 5; i++) {
                    smallIndex.index("agent-1", "key" + i, "document " + i);
                }
                smallIndex.delete("agent-1", "key0");

                float[] query = embeddingProvider.embed("document 0");
                List<VectorSearchResult> results = smallIndex.search("agent-1", query, 10);
                assertThat(results.stream().noneMatch(r -> r.entryKey().equals("key0"))).isTrue();
            }
        }

        @Test
        @DisplayName("writecan : OnDiskpersistedafter writeofdataforsubsequent can")
        void writeVisibilityAfterOnDiskPersist() throws Exception {
            JVectorConfig smallConfig =
                    new JVectorConfig(
                            "lmdb", DIMENSION, 16, 10, 50, tempDir.toString(), 3, 10000, 500000);
            try (JVectorVectorIndex smallIndex =
                    new JVectorVectorIndex(smallConfig, embeddingProvider)) {
                for (int i = 0; i < 5; i++) {
                    smallIndex.index("agent-1", "key" + i, "document " + i);
                }

                float[] query = embeddingProvider.embed("document");
                List<VectorSearchResult> beforeWrite = smallIndex.search("agent-1", query, 10);

                smallIndex.index("agent-1", "extra1", "extra document 1");
                smallIndex.index("agent-1", "extra2", "extra document 2");

                List<VectorSearchResult> afterWrite = smallIndex.search("agent-1", query, 10);
                assertThat(afterWrite.size()).isGreaterThan(beforeWrite.size());
                assertThat(afterWrite.stream().anyMatch(r -> r.entryKey().equals("extra1")))
                        .isTrue();
                assertThat(afterWrite.stream().anyMatch(r -> r.entryKey().equals("extra2")))
                        .isTrue();
            }
        }
    }

    @Nested
    @DisplayName("S5-RestartRecovery - recovery")
    class RestartRecovery {

        @Test
        @DisplayName("afterOnDisk can , datacan index")
        void newIndexAfterRestartWorks() throws Exception {
            Path restartDir = tempDir.resolve("restart-test");
            JVectorConfig smallConfig =
                    new JVectorConfig(
                            "lmdb", DIMENSION, 16, 3, 50, restartDir.toString(), 3, 3, 500000);
            try (JVectorVectorIndex idx = new JVectorVectorIndex(smallConfig, embeddingProvider)) {
                for (int i = 0; i < 5; i++) {
                    idx.index("agent-1", "key" + i, "document " + i);
                }
            }

            try (JVectorVectorIndex idx2 = new JVectorVectorIndex(smallConfig, embeddingProvider)) {
                float[] queryOld = embeddingProvider.embed("document 0");
                List<VectorSearchResult> oldResults = idx2.search("agent-1", queryOld, 10);
                assertThat(oldResults).isNotEmpty();

                idx2.index("agent-1", "newkey", "new document");
                float[] queryNew = embeddingProvider.embed("new document");
                List<VectorSearchResult> newResults = idx2.search("agent-1", queryNew, 10);
                assertThat(newResults.stream().anyMatch(r -> r.entryKey().equals("newkey")))
                        .isTrue();
            }
        }
    }

    @Nested
    @DisplayName("S6-FaultyEmbedding - embedding")
    class FaultyEmbedding {

        @Test
        @DisplayName("FaultyEmbeddingProvider: beforeN failafterindexrecovery")
        void faultyProviderRecoversAfterFailures() {
            FaultyEmbeddingProvider faulty = new FaultyEmbeddingProvider(embeddingProvider, 2);
            try (JVectorVectorIndex faultyIndex = new JVectorVectorIndex(config, faulty)) {
                assertThatThrownBy(() -> faultyIndex.index("agent-1", "key1", "first"))
                        .isInstanceOf(EmbeddingUnavailableException.class);
                assertThatThrownBy(() -> faultyIndex.index("agent-1", "key2", "second"))
                        .isInstanceOf(EmbeddingUnavailableException.class);

                faultyIndex.index("agent-1", "key3", "third");

                float[] query = embeddingProvider.embed("third");
                List<VectorSearchResult> results = faultyIndex.search("agent-1", query, 10);
                assertThat(results).hasSize(1);
                assertThat(results.get(0).entryKey()).isEqualTo("key3");
            }
        }

        @Test
        @DisplayName("FaultyEmbeddingProvider: alwaysFail allindexfail search")
        void alwaysFailProviderBlocksIndexButSearchWorks() {
            FaultyEmbeddingProvider faulty = new FaultyEmbeddingProvider(embeddingProvider, 0);
            faulty.setAlwaysFail(true);
            try (JVectorVectorIndex faultyIndex = new JVectorVectorIndex(config, faulty)) {
                assertThatThrownBy(() -> faultyIndex.index("agent-1", "key1", "test"))
                        .isInstanceOf(EmbeddingUnavailableException.class);

                float[] query = embeddingProvider.embed("test");
                List<VectorSearchResult> results = faultyIndex.search("agent-1", query, 10);
                assertThat(results).isEmpty();

                faulty.reset();
                faultyIndex.index("agent-1", "key2", "after reset");
                List<VectorSearchResult> results2 = faultyIndex.search("agent-1", query, 10);
                assertThat(results2).hasSize(1);
            }
        }

        @Test
        @DisplayName("FaultyEmbeddingProvider: indexWithEmbedding directlywrite")
        void indexWithEmbeddingBypassesFaultyProvider() {
            FaultyEmbeddingProvider faulty = new FaultyEmbeddingProvider(embeddingProvider, 0);
            faulty.setAlwaysFail(true);
            try (JVectorVectorIndex faultyIndex = new JVectorVectorIndex(config, faulty)) {
                float[] emb = embeddingProvider.embed("direct embedding");
                faultyIndex.indexWithEmbedding("agent-1", "key1", emb);

                List<VectorSearchResult> results = faultyIndex.search("agent-1", emb, 10);
                assertThat(results).hasSize(1);
                assertThat(results.get(0).entryKey()).isEqualTo("key1");
            }
        }
    }

    @Nested
    @DisplayName("S7-ConcurrentReadWrite - concurrentreadwrite")
    class ConcurrentReadWrite {

        @Test
        @DisplayName("concurrentreadwrite: write and same row")
        void concurrentReadAndWrite() throws Exception {
            int writerCount = 2;
            int readerCount = 2;
            int writesPerWriter = 20;
            CyclicBarrier barrier = new CyclicBarrier(writerCount + readerCount);
            AtomicInteger readSuccess = new AtomicInteger(0);
            AtomicInteger readFail = new AtomicInteger(0);
            AtomicInteger writeSuccess = new AtomicInteger(0);
            AtomicInteger writeFail = new AtomicInteger(0);

            Runnable writerTask =
                    () -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < writesPerWriter; i++) {
                                index.index(
                                        "agent-1",
                                        "key-" + Thread.currentThread().getId() + "-" + i,
                                        "document " + i);
                            }
                            writeSuccess.incrementAndGet();
                        } catch (Exception e) {
                            writeFail.incrementAndGet();
                        }
                    };

            Runnable readerTask =
                    () -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < 10; i++) {
                                float[] query = embeddingProvider.embed("document");
                                index.search("agent-1", query, 10);
                                Thread.sleep(1);
                            }
                            readSuccess.incrementAndGet();
                        } catch (Exception e) {
                            readFail.incrementAndGet();
                        }
                    };

            var executor = Executors.newFixedThreadPool(writerCount + readerCount);
            for (int i = 0; i < writerCount; i++) executor.submit(writerTask);
            for (int i = 0; i < readerCount; i++) executor.submit(readerTask);
            executor.shutdown();
            boolean terminated = executor.awaitTermination(30, TimeUnit.SECONDS);
            assertThat(terminated).isTrue();

            assertThat(writeSuccess.get()).isEqualTo(writerCount);
            assertThat(readSuccess.get()).isEqualTo(readerCount);
            assertThat(writeFail.get()).isEqualTo(0);
        }

        @Test
        @DisplayName("concurrent writedifferentagent")
        void concurrentWriteDifferentAgents() throws Exception {
            int agentCount = 4;
            int writesPerAgent = 10;
            CyclicBarrier barrier = new CyclicBarrier(agentCount);
            AtomicInteger successCount = new AtomicInteger(0);

            var executor = Executors.newFixedThreadPool(agentCount);
            for (int a = 0; a < agentCount; a++) {
                final String agentId = "agent-" + a;
                executor.submit(
                        () -> {
                            try {
                                barrier.await();
                                for (int i = 0; i < writesPerAgent; i++) {
                                    index.index(agentId, "key" + i, "document " + i);
                                }
                                successCount.incrementAndGet();
                            } catch (Exception e) {
                                // ignore
                            }
                        });
            }
            executor.shutdown();
            boolean terminated = executor.awaitTermination(30, TimeUnit.SECONDS);
            assertThat(terminated).isTrue();

            assertThat(successCount.get()).isEqualTo(agentCount);

            float[] query = embeddingProvider.embed("document");
            for (int a = 0; a < agentCount; a++) {
                List<VectorSearchResult> results =
                        index.search("agent-" + a, query, writesPerAgent);
                assertThat(results).hasSize(writesPerAgent);
            }
        }
    }
}
