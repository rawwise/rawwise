package io.github.rawwise.axiom.pipeline.derivation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.View;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("DerivationEngine E2E - derivation full-chain business scenario test")
class DerivationEngineE2ETest {

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
    }

    private static String rawKeyId(byte[] keyBytes) {
        return "raw:" + java.util.Base64.getEncoder().encodeToString(keyBytes);
    }

    private void putChunk(String chunkId, String content) {
        try {
            metastore.put(
                    ("chunk:" + chunkId).getBytes(StandardCharsets.UTF_8),
                    content.getBytes(StandardCharsets.UTF_8));
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    @DisplayName("S1-DeriveView - chunks to views ofderivationfull chain")
    class DeriveView {

        @Test
        @DisplayName("from chunk IDs derivation RAW_IDENTITY view, correct")
        void deriveRawIdentityViewWithContentConcatenation() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            putChunk("chunk:raw1:0", "Hello world");
            putChunk("chunk:raw1:1", "Machine learning");
            putChunk("chunk:raw1:2", "Deep neural networks");
            List<String> chunkIds = List.of("chunk:raw1:0", "chunk:raw1:1", "chunk:raw1:2");

            View view = engine.deriveView(chunkIds, View.ViewType.RAW_IDENTITY, "v1", Map.of());

            assertThat(view.viewId()).startsWith("view:");
            assertThat(view.viewType()).isEqualTo(View.ViewType.RAW_IDENTITY);
            assertThat(view.sourceChunkIds()).containsExactlyElementsOf(chunkIds);
            assertThat(view.content()).isNotNull();

            String contentStr = new String(view.content(), StandardCharsets.UTF_8);
            assertThat(contentStr).contains("Hello world");
            assertThat(contentStr).contains("Machine learning");
            assertThat(contentStr).contains("chunkCount");
        }

        @Test
        @DisplayName("derivationafter getDerivedFrom can query")
        void reverseQueryAfterDeriveView() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            putChunk("chunk:a:0", "data A");
            putChunk("chunk:a:1", "data B");
            List<String> chunkIds = List.of("chunk:a:0", "chunk:a:1");
            View view = engine.deriveView(chunkIds, View.ViewType.EMBEDDING_INDEX, "v2", Map.of());

            List<DerivationChain> chains = engine.getDerivedFrom(view.viewId(), Layer.VIEW);

            assertThat(chains).isNotEmpty();
            assertThat(chains).allMatch(c -> c.derivedId().equals(view.viewId()));
            assertThat(chains).allMatch(c -> c.derivedLayer() == Layer.VIEW);
        }

        @Test
        @DisplayName("chunk IDs canderivation view")
        void deriveViewWithEmptyChunkIds() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            View view = engine.deriveView(List.of(), View.ViewType.RAW_IDENTITY, "v0", Map.of());

            assertThat(view.viewId()).startsWith("view:");
            assertThat(view.sourceChunkIds()).isEmpty();
        }

        @Test
        @DisplayName("SUMMARY type view derivation params")
        void deriveSummaryView() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            View view =
                    engine.deriveView(
                            List.of(),
                            View.ViewType.SUMMARY,
                            "v1",
                            Map.of("maxLength", 200, "language", "en"));

            assertThat(view.viewType()).isEqualTo(View.ViewType.SUMMARY);
            assertThat(view.content()).isNotNull();
        }

        @Test
        @DisplayName("same chunk derivation multiple view, derivation independent")
        void sameChunksMultipleViews() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            putChunk("chunk:shared:0", "shared content");
            List<String> chunkIds = List.of("chunk:shared:0");

            View v1 = engine.deriveView(chunkIds, View.ViewType.EMBEDDING_INDEX, "v1", Map.of());
            View v2 = engine.deriveView(chunkIds, View.ViewType.SUMMARY, "v1", Map.of());

            List<DerivationChain> chainsV1 = engine.getDerivedFrom(v1.viewId(), Layer.VIEW);
            List<DerivationChain> chainsV2 = engine.getDerivedFrom(v2.viewId(), Layer.VIEW);

            assertThat(chainsV1).isNotEmpty();
            assertThat(chainsV2).isNotEmpty();
            assertThat(v1.viewId()).isNotEqualTo(v2.viewId());
        }
    }

    @Nested
    @DisplayName("S2-DerivationChain - derivation storageandquery")
    class DerivationChainOps {

        @Test
        @DisplayName("derivation : chunks -> views derivation can")
        void fullDerivationChainTraceable() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            putChunk("chunk:trace:0", "trace data 0");
            putChunk("chunk:trace:1", "trace data 1");
            List<String> chunkIds = List.of("chunk:trace:0", "chunk:trace:1");
            View view = engine.deriveView(chunkIds, View.ViewType.EMBEDDING_INDEX, "v3", Map.of());

            for (String chunkId : chunkIds) {
                List<DerivationChain> chunkToView = engine.getChain(chunkId, Layer.CHUNK);
                assertThat(chunkToView).isNotEmpty();
                assertThat(chunkToView.get(0).derivedId()).isEqualTo(view.viewId());
            }

            List<DerivationChain> fromView = engine.getDerivedFrom(view.viewId(), Layer.VIEW);
            assertThat(fromView).hasSize(chunkIds.size());
        }

        @Test
        @DisplayName("getChain(entityId) entity ID queryderivation")
        void getChainByEntityId() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            putChunk("chunk:entity:0", "entity data");
            List<String> chunkIds = List.of("chunk:entity:0");
            View view = engine.deriveView(chunkIds, View.ViewType.RAW_IDENTITY, "v1", Map.of());

            List<DerivationChain> chains = engine.getChain(view.viewId());
            assertThat(chains).isNotEmpty();
            assertThat(chains.get(0).derivedId()).isEqualTo(view.viewId());
        }

        @Test
        @DisplayName("getChain(sourceId, sourceLayer) entity and query")
        void getChainBySourceAndLayer() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            putChunk("chunk:source:0", "source data");
            List<String> chunkIds = List.of("chunk:source:0");
            View view = engine.deriveView(chunkIds, View.ViewType.CLASSIFICATION, "v1", Map.of());

            List<DerivationChain> chains = engine.getChain("chunk:source:0", Layer.CHUNK);
            assertThat(chains).isNotEmpty();
            assertThat(chains.get(0).sourceId()).isEqualTo("chunk:source:0");
            assertThat(chains.get(0).derivedLayer()).isEqualTo(Layer.VIEW);
        }

        @Test
        @DisplayName("query non-existent entity returns empty list")
        void queryNonExistentEntityReturnsEmpty() {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);

            assertThat(engine.getChain("nonexistent")).isEmpty();
            assertThat(engine.getChain("nonexistent", Layer.RAW)).isEmpty();
            assertThat(engine.getDerivedFrom("nonexistent", Layer.VIEW)).isEmpty();
        }
    }

    @Nested
    @DisplayName("S3-Concurrent - concurrentderivation")
    class Concurrent {

        @Test
        @DisplayName(
                "multi-threaded concurrentderivationdifferent chunks, derivation independent conflict")
        void concurrentDerivationIndependent() throws Exception {
            DerivationEngine engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
            int threadCount = 4;
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            var executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                int threadId = t;
                putChunk("chunk:concurrent:" + threadId, "concurrent data " + threadId);
                executor.submit(
                        () -> {
                            try {
                                List<String> chunkIds = List.of("chunk:concurrent:" + threadId);
                                View view =
                                        engine.deriveView(
                                                chunkIds,
                                                View.ViewType.RAW_IDENTITY,
                                                "v" + threadId,
                                                Map.of());
                                List<DerivationChain> chains = engine.getChain(view.viewId());
                                if (!chains.isEmpty()) {
                                    successCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            assertThat(errorCount.get()).isEqualTo(0);
            assertThat(successCount.get()).isEqualTo(threadCount);
        }
    }

    @Nested
    @DisplayName("S4-ChunkingStrategies - shardstrategy")
    class ChunkingStrategies {

        @Test
        @DisplayName("paragraph chunking: aggregateis , ")
        void paragraphChunkingAggregatesShortParagraphs() {
            ChunkingStrategy strategy = new ParagraphChunkingStrategy();
            String text =
                    "Machine learning is a subset of AI.\n\n"
                            + "Deep learning uses neural networks.";
            List<byte[]> chunks = strategy.split(text.getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks).hasSize(1);
            String first = new String(chunks.get(0), StandardCharsets.UTF_8);
            assertThat(first).contains("Machine learning");

            List<byte[]> split =
                    strategy.split(
                            text.getBytes(StandardCharsets.UTF_8), Map.of("maxChunkSize", 30));
            assertThat(split.size()).isGreaterThanOrEqualTo(2);
        }

        @Test
        @DisplayName("paragraph chunking: in aggregatebehaviorconsistent")
        void paragraphChunkingChineseText() {
            ChunkingStrategy strategy = new ParagraphChunkingStrategy();
            String text = "人工智能正在改变世界。\n\n深度学习是机器学习的一个子集。";
            List<byte[]> chunks = strategy.split(text.getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks).hasSize(1);
            String first = new String(chunks.get(0), StandardCharsets.UTF_8);
            assertThat(first).contains("人工智能");
        }

        @Test
        @DisplayName("paragraph chunking: exceeds maxChunkSize")
        void paragraphChunkingRespectsMaxSize() {
            ChunkingStrategy strategy = new ParagraphChunkingStrategy();
            String text = "A".repeat(100) + "\n\n" + "B".repeat(100);
            List<byte[]> chunks =
                    strategy.split(
                            text.getBytes(StandardCharsets.UTF_8), Map.of("maxChunkSize", 50));

            assertThat(chunks.size()).isGreaterThan(1);
        }

        @Test
        @DisplayName("sentence chunking: sentenceboundary")
        void sentenceChunkingSplitsEnglishBySentence() {
            String text = "Hello world. This is a test. Another sentence here.";
            ChunkingStrategy strategy = new SentenceChunkingStrategy();
            List<byte[]> chunks = strategy.split(text.getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks).isNotEmpty();
            String first = new String(chunks.get(0), StandardCharsets.UTF_8);
            assertThat(first).contains("Hello world");
        }

        @Test
        @DisplayName("sentence chunking: maxSentences shard")
        void sentenceChunkingRespectsMaxSentences() {
            String text = "A. B. C. D. E. F. G. H.";
            ChunkingStrategy strategy = new SentenceChunkingStrategy();
            List<byte[]> chunks =
                    strategy.split(
                            text.getBytes(StandardCharsets.UTF_8), Map.of("maxSentences", 2));

            assertThat(chunks.size()).isGreaterThan(1);
        }

        @Test
        @DisplayName("sentence chunking: single shard")
        void sentenceChunkingSingleSentence() {
            ChunkingStrategy strategy = new SentenceChunkingStrategy();
            List<byte[]> chunks =
                    strategy.split(
                            "Just one sentence here".getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks).hasSize(1);
        }

        @Test
        @DisplayName("shard: / boundary Java")
        void codeBlockChunkingSplitsJavaClasses() {
            ChunkingStrategy strategy = new CodeBlockChunkingStrategy();
            String code =
                    "public class Foo {\n    public void bar() {}\n}\n\npublic class Baz {\n    private int x;\n}";
            List<byte[]> chunks = strategy.split(code.getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks.size()).isGreaterThanOrEqualTo(1);
        }

        @Test
        @DisplayName("shard: non- maintain")
        void codeBlockChunkingFallbackForNonCode() {
            ChunkingStrategy strategy = new CodeBlockChunkingStrategy();
            List<byte[]> chunks =
                    strategy.split("Just plain text".getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks).hasSize(1);
        }

        @Test
        @DisplayName("strategycanregisterand : registerStrategy coverdefaultstrategy")
        void strategyRegistryReplacesDefault() {
            DefaultDerivationEngine engine =
                    (DefaultDerivationEngine)
                            new DefaultDerivationEngine(null, metastore, knowledgeStore);
            engine.registerStrategy(new SentenceChunkingStrategy());

            assertThat(engine.selectStrategy("sentence")).isNotNull();
            assertThat(engine.selectStrategy("sentence").supportedType())
                    .isEqualTo(Chunk.ChunkType.TEXT_SEGMENT);
        }

        @Test
        @DisplayName("not yetregisterstrategyof ChunkType original")
        void unregisteredTypeReturnsOriginalContent() {
            DefaultDerivationEngine engine =
                    (DefaultDerivationEngine)
                            new DefaultDerivationEngine(null, metastore, knowledgeStore);
            engine.registerStrategy(
                    new ChunkingStrategy() {
                        @Override
                        public String name() {
                            return "image-noop";
                        }

                        @Override
                        public Chunk.ChunkType supportedType() {
                            return Chunk.ChunkType.IMAGE_REGION;
                        }

                        @Override
                        public List<byte[]> split(byte[] content, Map<String, Object> params) {
                            return List.of();
                        }
                    });

            assertThat(engine.selectStrategy("image-noop")).isNotNull();
            assertThat(engine.selectStrategy("image-noop").supportedType())
                    .isEqualTo(Chunk.ChunkType.IMAGE_REGION);
        }
    }

    @Nested
    @DisplayName("S5-StrategyAwareDerivation - strategyroutingand pattern derivation")
    class StrategyAwareDerivation {

        @Test
        @DisplayName("resolveChunkType alreadyregisterstrategy strategy of ChunkType")
        void resolveChunkTypeForRegisteredStrategy() {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);
            engine.registerStrategy(new ParagraphChunkingStrategy());

            Chunk.ChunkType resolved =
                    engine.resolveChunkType("paragraph", Chunk.ChunkType.TABLE_ROW);

            assertThat(resolved).isEqualTo(Chunk.ChunkType.TEXT_SEGMENT);
        }

        @Test
        @DisplayName("resolveChunkType not yetregisterstrategy fallback")
        void resolveChunkTypeForUnregisteredStrategyReturnsFallback() {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);

            Chunk.ChunkType resolved =
                    engine.resolveChunkType("nonexistent-strategy", Chunk.ChunkType.TEXT_SEGMENT);

            assertThat(resolved).isEqualTo(Chunk.ChunkType.TEXT_SEGMENT);
        }

        @Test
        @DisplayName("resolveChunkType null strategyName fallback")
        void resolveChunkTypeNullStrategyNameReturnsFallback() {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);

            Chunk.ChunkType resolved = engine.resolveChunkType(null, Chunk.ChunkType.CODE_BLOCK);

            assertThat(resolved).isEqualTo(Chunk.ChunkType.CODE_BLOCK);
        }

        @Test
        @DisplayName("deriveChunks alreadyregister strategyName + renderedContent thestrategy")
        void deriveChunksWithRegisteredStrategyNameAndRenderedContent() {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);
            engine.registerStrategy(new ParagraphChunkingStrategy());

            String renderedText = "第一段内容。\n\n第二段内容。";
            Record.Key rawKey = Records.keyOf("strategy-test-0".getBytes(StandardCharsets.UTF_8));

            Map<String, Object> params = new java.util.HashMap<>();
            params.put("renderedContent", renderedText.getBytes(StandardCharsets.UTF_8));

            List<Chunk> chunks =
                    engine.deriveChunks(rawKey, Chunk.ChunkType.TEXT_SEGMENT, "paragraph", params);

            assertThat(chunks).isNotEmpty();
            assertThat(chunks.get(0).chunkId()).startsWith("chunk:");
            String firstChunk = new String(chunks.get(0).content(), StandardCharsets.UTF_8);
            assertThat(firstChunk).contains("第一段内容");
        }

        @Test
        @DisplayName("deriveChunks not yetregister strategyName + renderedContent degradationto")
        void deriveChunksWithUnregisteredStrategyNameFallsBack() {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);

            String renderedText = "简单文本内容";
            Record.Key rawKey = Records.keyOf("fallback-test-0".getBytes(StandardCharsets.UTF_8));

            Map<String, Object> params = new java.util.HashMap<>();
            params.put("renderedContent", renderedText.getBytes(StandardCharsets.UTF_8));

            List<Chunk> chunks =
                    engine.deriveChunks(
                            rawKey, Chunk.ChunkType.TEXT_SEGMENT, "nonexistent", params);

            assertThat(chunks).isNotEmpty();
            String firstChunk = new String(chunks.get(0).content(), StandardCharsets.UTF_8);
            assertThat(firstChunk).contains("简单文本内容");
        }

        @Test
        @DisplayName("deriveChunks renderedContent is chunk (pattern in AutoDerivationHook test)")
        void deriveChunksUsesRenderedContentAsChunkContent() {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);
            engine.registerStrategy(new ParagraphChunkingStrategy());

            String renderedText = "这是一个用户的信息，id是1，名字是Alice，年龄是30";
            Record.Key rawKey = Records.keyOf("pattern-test-0".getBytes(StandardCharsets.UTF_8));

            Map<String, Object> params = new java.util.HashMap<>();
            params.put("renderedContent", renderedText.getBytes(StandardCharsets.UTF_8));

            List<Chunk> chunks =
                    engine.deriveChunks(rawKey, Chunk.ChunkType.TEXT_SEGMENT, "paragraph", params);

            assertThat(chunks).isNotEmpty();
            String chunkContent = new String(chunks.get(0).content(), StandardCharsets.UTF_8);
            assertThat(chunkContent).contains("Alice");
            assertThat(chunkContent).contains("id是1");
            assertThat(chunkContent).contains("年龄是30");
        }

        @Test
        @DisplayName("deriveChunks after getDerivedFrom(chunkId, CHUNK) can to raw")
        void derivationChainFromRawToChunkAfterStrategyAwareDerive() {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);
            engine.registerStrategy(new ParagraphChunkingStrategy());

            String renderedText = "段落一\n\n段落二";
            Record.Key rawKey = Records.keyOf("chain-test-0".getBytes(StandardCharsets.UTF_8));

            Map<String, Object> params = new java.util.HashMap<>();
            params.put("renderedContent", renderedText.getBytes(StandardCharsets.UTF_8));

            List<Chunk> chunks =
                    engine.deriveChunks(rawKey, Chunk.ChunkType.TEXT_SEGMENT, "paragraph", params);

            assertThat(chunks).isNotEmpty();
            for (Chunk chunk : chunks) {
                List<DerivationChain> chains = engine.getDerivedFrom(chunk.chunkId(), Layer.CHUNK);
                assertThat(chains).isNotEmpty();
                assertThat(chains.get(0).derivedId()).isEqualTo(chunk.chunkId());
                assertThat(chains.get(0).derivedLayer()).isEqualTo(Layer.CHUNK);
            }
        }

        @Test
        @DisplayName(
                "deriveChunks idempotent: same rawKey No.secondarycallreturns empty list(CAS intent already in)")
        void deriveChunksIdempotentOnRepeatedCall() {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);
            engine.registerStrategy(new ParagraphChunkingStrategy());

            String renderedText = "幂等测试内容";
            Record.Key rawKey = Records.keyOf("idempotent-test-0".getBytes(StandardCharsets.UTF_8));

            Map<String, Object> params = new java.util.HashMap<>();
            params.put("renderedContent", renderedText.getBytes(StandardCharsets.UTF_8));

            List<Chunk> firstCall =
                    engine.deriveChunks(rawKey, Chunk.ChunkType.TEXT_SEGMENT, "paragraph", params);
            assertThat(firstCall).isNotEmpty();

            List<Chunk> secondCall =
                    engine.deriveChunks(rawKey, Chunk.ChunkType.TEXT_SEGMENT, "paragraph", params);
            assertThat(secondCall).isEmpty();
        }

        @Test
        @DisplayName(
                "concurrentsame rawKey deriveChunks: CAS guarantee item success, its returns empty")
        void concurrentSameKeyDeriveChunksCasIsolation() throws Exception {
            DefaultDerivationEngine engine =
                    new DefaultDerivationEngine(null, metastore, knowledgeStore);
            engine.registerStrategy(new ParagraphChunkingStrategy());

            Record.Key rawKey =
                    Records.keyOf("concurrent-cas-test".getBytes(StandardCharsets.UTF_8));
            int threadCount = 8;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            AtomicInteger emptyCount = new AtomicInteger(0);
            AtomicInteger errorCount = new AtomicInteger(0);
            var executor = Executors.newFixedThreadPool(threadCount);

            for (int i = 0; i < threadCount; i++) {
                executor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                Map<String, Object> params = new java.util.HashMap<>();
                                params.put(
                                        "renderedContent",
                                        "并发CAS测试".getBytes(StandardCharsets.UTF_8));
                                List<Chunk> chunks =
                                        engine.deriveChunks(
                                                rawKey,
                                                Chunk.ChunkType.TEXT_SEGMENT,
                                                "paragraph",
                                                params);
                                if (!chunks.isEmpty()) {
                                    successCount.incrementAndGet();
                                } else {
                                    emptyCount.incrementAndGet();
                                }
                            } catch (Exception e) {
                                errorCount.incrementAndGet();
                            } finally {
                                doneLatch.countDown();
                            }
                        });
            }

            startLatch.countDown();
            assertThat(doneLatch.await(30, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            assertThat(errorCount.get()).isEqualTo(0);
            assertThat(successCount.get()).isEqualTo(1);
            assertThat(emptyCount.get()).isEqualTo(threadCount - 1);
        }
    }
}
