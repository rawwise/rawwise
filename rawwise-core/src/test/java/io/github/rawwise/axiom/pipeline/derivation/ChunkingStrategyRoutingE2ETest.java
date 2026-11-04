package io.github.rawwise.axiom.pipeline.derivation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ChunkingStrategy routing E2E - multistrategyrouting scenariotest")
class ChunkingStrategyRoutingE2ETest {

    private DefaultDerivationEngine engine;
    private Metastore metastore;
    private KnowledgeStore knowledgeStore;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        engine = new DefaultDerivationEngine(null, metastore, knowledgeStore);
    }

    @Nested
    @DisplayName("R1-SpiLoading - SPI load strategy")
    class SpiLoading {

        @Test
        @DisplayName("all 6 item strategy via SPI load")
        void allBuiltinStrategiesLoaded() {
            assertThat(engine.selectStrategy("paragraph")).isNotNull();
            assertThat(engine.selectStrategy("sentence")).isNotNull();
            assertThat(engine.selectStrategy("code-block")).isNotNull();
            assertThat(engine.selectStrategy("pattern")).isNotNull();
            assertThat(engine.selectStrategy("table-row")).isNotNull();
            assertThat(engine.selectStrategy("sliding-window")).isNotNull();
        }

        @Test
        @DisplayName("unknown strategy name null")
        void unknownStrategyReturnsNull() {
            assertThat(engine.selectStrategy("nonexistent")).isNull();
        }

        @Test
        @DisplayName("null/blank name null")
        void nullNameReturnsNull() {
            assertThat(engine.selectStrategy(null)).isNull();
            assertThat(engine.selectStrategy("")).isNull();
            assertThat(engine.selectStrategy("  ")).isNull();
        }
    }

    @Nested
    @DisplayName("R2-TableBinding - table strategybindrouting")
    class TableBinding {

        @Test
        @DisplayName("bind table-row after selectStrategy table-row")
        void bindTableRowStrategy() {
            engine.bindTableStrategy("default.users", "table-row");

            ChunkingStrategy selected =
                    engine.selectStrategy(null, "default.users", Chunk.ChunkType.TEXT_SEGMENT);

            assertThat(selected).isNotNull();
            assertThat(selected.name()).isEqualTo("table-row");
        }

        @Test
        @DisplayName("bind pattern after selectStrategy pattern")
        void bindPatternStrategy() {
            engine.bindTableStrategy("default.docs", "pattern");

            ChunkingStrategy selected =
                    engine.selectStrategy(null, "default.docs", Chunk.ChunkType.TEXT_SEGMENT);

            assertThat(selected).isNotNull();
            assertThat(selected.name()).isEqualTo("pattern");
        }

        @Test
        @DisplayName("bind sliding-window after selectStrategy sliding-window")
        void bindSlidingWindowStrategy() {
            engine.bindTableStrategy("default.articles", "sliding-window");

            ChunkingStrategy selected =
                    engine.selectStrategy(null, "default.articles", Chunk.ChunkType.TEXT_SEGMENT);

            assertThat(selected).isNotNull();
            assertThat(selected.name()).isEqualTo("sliding-window");
        }

        @Test
        @DisplayName("strategyName bind")
        void unbindWithBlankName() {
            engine.bindTableStrategy("default.users", "table-row");
            engine.bindTableStrategy("default.users", "");

            ChunkingStrategy selected =
                    engine.selectStrategy(null, "default.users", Chunk.ChunkType.TEXT_SEGMENT);

            assertThat(selected.name()).isEqualTo("paragraph");
        }

        @Test
        @DisplayName("bindnon-existent strategy name fallback todefault")
        void bindNonexistentStrategyFallback() {
            engine.bindTableStrategy("default.users", "nonexistent");

            ChunkingStrategy selected =
                    engine.selectStrategy(null, "default.users", Chunk.ChunkType.TEXT_SEGMENT);

            assertThat(selected).isNotNull();
            assertThat(selected.name()).isEqualTo("paragraph");
        }
    }

    @Nested
    @DisplayName("R3-FallbackRouting - nobind type fallback")
    class FallbackRouting {

        @Test
        @DisplayName("nobind TEXT_SEGMENT fallback to paragraph")
        void fallbackTextSegment() {
            ChunkingStrategy selected =
                    engine.selectStrategy(null, "unknown.table", Chunk.ChunkType.TEXT_SEGMENT);

            assertThat(selected).isNotNull();
            assertThat(selected.name()).isEqualTo("paragraph");
        }

        @Test
        @DisplayName("nobind TABLE_ROW fallback to table-row")
        void fallbackTableRow() {
            ChunkingStrategy selected =
                    engine.selectStrategy(null, "unknown.table", Chunk.ChunkType.TABLE_ROW);

            assertThat(selected).isNotNull();
            assertThat(selected.name()).isEqualTo("table-row");
        }

        @Test
        @DisplayName("nobind CODE_BLOCK fallback to code-block")
        void fallbackCodeBlock() {
            ChunkingStrategy selected =
                    engine.selectStrategy(null, "unknown.table", Chunk.ChunkType.CODE_BLOCK);

            assertThat(selected).isNotNull();
            assertThat(selected.name()).isEqualTo("code-block");
        }

        @Test
        @DisplayName("tablebind beforetype fallback")
        void tableBindingOverridesTypeFallback() {
            engine.bindTableStrategy("default.special", "sentence");

            ChunkingStrategy selected =
                    engine.selectStrategy(null, "default.special", Chunk.ChunkType.TEXT_SEGMENT);

            assertThat(selected.name()).isEqualTo("sentence");
        }
    }

    @Nested
    @DisplayName("R4-ClearDeriveIntent - derivation derivation")
    class ClearDeriveIntent {

        @Test
        @DisplayName("clearDeriveIntent aftercan derivation chunks")
        void clearDeriveIntentAllowsReDerive() {
            Record.Key rawKey = Records.keyOf("rederive-key".getBytes(StandardCharsets.UTF_8));
            Map<String, Object> params = new HashMap<>();
            params.put("renderedContent", "测试渲染内容".getBytes(StandardCharsets.UTF_8));

            List<Chunk> firstDerive =
                    engine.deriveChunks(rawKey, Chunk.ChunkType.TEXT_SEGMENT, params);
            assertThat(firstDerive).isNotEmpty();

            List<Chunk> secondDerive =
                    engine.deriveChunks(rawKey, Chunk.ChunkType.TEXT_SEGMENT, params);
            assertThat(secondDerive).isEmpty();

            engine.clearDeriveIntent(rawKey);

            List<Chunk> thirdDerive =
                    engine.deriveChunks(rawKey, Chunk.ChunkType.TEXT_SEGMENT, params);
            assertThat(thirdDerive).isNotEmpty();
        }

        @Test
        @DisplayName("different keys intents do not affect each other")
        void clearIntentDoesNotAffectOtherKeys() {
            Record.Key key1 = Records.keyOf("key-1".getBytes(StandardCharsets.UTF_8));
            Record.Key key2 = Records.keyOf("key-2".getBytes(StandardCharsets.UTF_8));
            Map<String, Object> params = new HashMap<>();
            params.put("renderedContent", "内容".getBytes(StandardCharsets.UTF_8));

            engine.deriveChunks(key1, Chunk.ChunkType.TEXT_SEGMENT, params);
            engine.deriveChunks(key2, Chunk.ChunkType.TEXT_SEGMENT, params);

            engine.clearDeriveIntent(key1);

            List<Chunk> key1Rederive =
                    engine.deriveChunks(key1, Chunk.ChunkType.TEXT_SEGMENT, params);
            assertThat(key1Rederive).isNotEmpty();

            List<Chunk> key2Rederive =
                    engine.deriveChunks(key2, Chunk.ChunkType.TEXT_SEGMENT, params);
            assertThat(key2Rederive).isEmpty();
        }
    }

    @Nested
    @DisplayName("R5-NewStrategySplit - strategy behavior")
    class NewStrategySplit {

        @Test
        @DisplayName("TableRowChunkingStrategy JSON ismultirow")
        void tableRowSplitsJsonArray() {
            TableRowChunkingStrategy strategy = new TableRowChunkingStrategy();
            String json = "[{\"id\":1,\"name\":\"Alice\"},{\"id\":2,\"name\":\"Bob\"}]";

            List<byte[]> chunks = strategy.split(json.getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks).hasSize(2);
            String row1 = new String(chunks.get(0), StandardCharsets.UTF_8);
            String row2 = new String(chunks.get(1), StandardCharsets.UTF_8);
            assertThat(row1).contains("Alice");
            assertThat(row2).contains("Bob");
        }

        @Test
        @DisplayName("TableRowChunkingStrategy single JSON for single chunk")
        void tableRowSingleJsonObject() {
            TableRowChunkingStrategy strategy = new TableRowChunkingStrategy();
            String json = "{\"id\":1,\"name\":\"Alice\"}";

            List<byte[]> chunks = strategy.split(json.getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks).hasSize(1);
        }

        @Test
        @DisplayName("SlidingWindowChunkingStrategy single chunk")
        void slidingWindowShortText() {
            SlidingWindowChunkingStrategy strategy = new SlidingWindowChunkingStrategy();
            String text = "短文本";

            List<byte[]> chunks = strategy.split(text.getBytes(StandardCharsets.UTF_8), Map.of());

            assertThat(chunks).hasSize(1);
        }

        @Test
        @DisplayName("SlidingWindowChunkingStrategy has overlap")
        void slidingWindowLongTextWithOverlap() {
            SlidingWindowChunkingStrategy strategy = new SlidingWindowChunkingStrategy();
            String text = "a".repeat(2500);

            Map<String, Object> params = new HashMap<>();
            params.put("maxChunkSize", 1024);
            params.put("chunkOverlap", 128);

            List<byte[]> chunks = strategy.split(text.getBytes(StandardCharsets.UTF_8), params);

            assertThat(chunks.size()).isGreaterThan(1);
            String first = new String(chunks.get(0), StandardCharsets.UTF_8);
            assertThat(first.length()).isEqualTo(1024);
            String second = new String(chunks.get(1), StandardCharsets.UTF_8);
            assertThat(second.length()).isEqualTo(1024);
            int step = 1024 - 128;
            String overlapFromFirst = first.substring(step);
            String overlapFromSecond = second.substring(0, 128);
            assertThat(overlapFromFirst).isEqualTo(overlapFromSecond);
        }

        @Test
        @DisplayName("SlidingWindowChunkingStrategy strategy name and type correct")
        void slidingWindowNameAndType() {
            SlidingWindowChunkingStrategy strategy = new SlidingWindowChunkingStrategy();
            assertThat(strategy.name()).isEqualTo("sliding-window");
            assertThat(strategy.supportedType()).isEqualTo(Chunk.ChunkType.TEXT_SEGMENT);
        }

        @Test
        @DisplayName("TableRowChunkingStrategy strategy name and type correct")
        void tableRowNameAndType() {
            TableRowChunkingStrategy strategy = new TableRowChunkingStrategy();
            assertThat(strategy.name()).isEqualTo("table-row");
            assertThat(strategy.supportedType()).isEqualTo(Chunk.ChunkType.TABLE_ROW);
        }
    }

    @Nested
    @DisplayName("R6-AppliesToDefault - all strategy default appliesTo")
    class AppliesToDefault {

        @Test
        @DisplayName("all strategy of appliesTo true")
        void allBuiltinStrategiesApplyToAnyRecord() {
            List<ChunkingStrategy> allStrategies =
                    List.of(
                            new ParagraphChunkingStrategy(),
                            new SentenceChunkingStrategy(),
                            new CodeBlockChunkingStrategy(),
                            new PatternBasedChunkingStrategy(),
                            new TableRowChunkingStrategy(),
                            new SlidingWindowChunkingStrategy());

            for (ChunkingStrategy s : allStrategies) {
                assertThat(s.appliesTo(null, "any.table"))
                        .as("strategy %s should apply to any table", s.name())
                        .isTrue();
            }
        }
    }
}
