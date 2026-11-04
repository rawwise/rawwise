package io.github.rawwise.axiom.pipeline.feedback;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.DefaultActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.DefaultCognitionEngine;
import io.github.rawwise.axiom.pipeline.derivation.Layer;
import io.github.rawwise.axiom.pipeline.evaluation.DefaultEvaluationEngine;
import io.github.rawwise.axiom.pipeline.evaluation.EvaluationEngine;
import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.RocksdbMetastore;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Chunk F_self Closure E2E — N1/N2/N3 verify")
class ChunkFselfClosureE2ETest {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());

    @TempDir Path tempDir;

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private LmdbInstance lmdbInstance;
    private FeedbackLoopCloser feedbackLoopCloser;
    private ExecutorService cascadeExecutor;

    @BeforeEach
    void setUp() throws IOException {
        metastore = new RocksdbMetastore(tempDir.resolve("metastore").toString());
        knowledgeStore = new InMemoryKnowledgeStore();
        lmdbInstance = createLmdbInstance("chunk-fself");
        cascadeExecutor = Executors.newFixedThreadPool(2);

        EvaluationEngine evaluationEngine = new DefaultEvaluationEngine(metastore, knowledgeStore);
        DefaultActionExecutor actionExecutor =
                new DefaultActionExecutor(metastore, knowledgeStore, null);
        CognitionEngine cognitionEngine =
                new DefaultCognitionEngine(
                        metastore, knowledgeStore, evaluationEngine, actionExecutor);

        feedbackLoopCloser =
                new FeedbackLoopCloser(
                        evaluationEngine,
                        cognitionEngine,
                        metastore,
                        knowledgeStore,
                        cascadeExecutor);
    }

    @AfterEach
    void tearDown() throws IOException {
        cascadeExecutor.shutdownNow();
        if (lmdbInstance != null) {
            lmdbInstance.close();
        }
        if (metastore instanceof AutoCloseable c) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    private LmdbInstance createLmdbInstance(String name) throws IOException {
        Path base = tempDir.resolve("lmdb-" + name + "-" + System.currentTimeMillis());
        Files.createDirectories(base);
        LmdbInstance inst = new LmdbInstance(name, base, 10 * 1024 * 1024L);
        inst.tryAcquireLease("e2e-owner", 300_000);
        inst.ensureRoutingOwnerLease("e2e-exec", 1);
        return inst;
    }

    private byte[] ksGet(String key) throws IOException {
        return knowledgeStore.get(key.getBytes(StandardCharsets.UTF_8)).orElse(null);
    }

    @Nested
    @DisplayName("S1-N1: cover — each Chunk independent F_self")
    class N1FullCoverage {

        @Test
        @DisplayName("singleChunk_onDeriveCreatesEvalScore")
        void singleChunkOnDeriveCreatesEvalScore() throws IOException {
            feedbackLoopCloser.onDerive("chunk:test:0", Layer.CHUNK, 1.0);

            byte[] evalValue = ksGet("chunk_score:chunk:test:0");
            assertThat(evalValue).as("eval_score entry for chunk:test:0").isNotNull();

            byte[] factValue = ksGet("feedback_fact:self:chunk:test:0");
            assertThat(factValue).as("feedback_fact entry for chunk:test:0").isNotNull();
        }

        @Test
        @DisplayName("multipleChunks_eachGetsOwnSignal")
        void multipleChunksEachGetsOwnSignal() throws IOException {
            List<String> chunkIds = List.of("chunk:doc1:0", "chunk:doc1:1", "chunk:doc1:2");

            for (String chunkId : chunkIds) {
                feedbackLoopCloser.onDerive(chunkId, Layer.CHUNK, 1.0);
            }

            for (String chunkId : chunkIds) {
                byte[] eval = ksGet("chunk_score:" + chunkId + "");
                assertThat(eval).as("eval_score for " + chunkId).isNotNull();

                byte[] fact = ksGet("feedback_fact:self:" + chunkId);
                assertThat(fact).as("feedback_fact for " + chunkId).isNotNull();
            }
        }

        @Test
        @DisplayName("chunkCountMatchesSignalCount")
        void chunkCountMatchesSignalCount() throws IOException {
            int chunkCount = 5;
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < chunkCount; i++) {
                String id = "chunk:batch:" + i;
                ids.add(id);
                feedbackLoopCloser.onDerive(id, Layer.CHUNK, 1.0);
            }

            int signalCount = 0;
            for (String id : ids) {
                if (ksGet("chunk_score:" + id + "") != null) {
                    signalCount++;
                }
            }
            assertThat(signalCount).as("every chunk has F_self signal").isEqualTo(chunkCount);
        }
    }

    @Nested
    @DisplayName("S2-N2: — F_self can ofstatus")
    class N2CausalClosure {

        @Test
        @DisplayName("onDerive_createsStateChangeInKnowledgeStore")
        void onDeriveCreatesStateChange() throws IOException {
            String chunkId = "chunk:causal:0";
            String key = "chunk_score:" + chunkId + "";

            assertThat(ksGet(key)).as("no eval_score before onDerive").isNull();

            feedbackLoopCloser.onDerive(chunkId, Layer.CHUNK, 0.85);

            assertThat(ksGet(key)).as("eval_score exists after onDerive").isNotNull();
        }

        @Test
        @DisplayName("onDerive_factPersistedWithCorrectFields")
        void onDeriveFactPersisted() throws IOException {
            String chunkId = "chunk:causal:1";

            assertThat(ksGet("feedback_fact:self:" + chunkId))
                    .as("no fact before onDerive")
                    .isNull();

            feedbackLoopCloser.onDerive(chunkId, Layer.CHUNK, 1.0);

            byte[] factBytes = ksGet("feedback_fact:self:" + chunkId);
            assertThat(factBytes).as("fact persisted after onDerive").isNotNull();

            var factNode = MAPPER.readTree(factBytes);
            assertThat(factNode.get("source").asText()).isEqualTo("SELF");
            assertThat(factNode.get("targetLayer").asText()).isEqualTo("CHUNK");
            assertThat(factNode.get("score").asDouble()).isEqualTo(1.0);
        }

        @Test
        @DisplayName("multipleRoundsEachProduceStateChange")
        void multipleRoundsEachProduceStateChange() throws IOException {
            for (int round = 0; round < 3; round++) {
                String chunkId = "chunk:round:" + round;
                feedbackLoopCloser.onDerive(chunkId, Layer.CHUNK, 1.0);

                assertThat(ksGet("chunk_score:" + chunkId + ""))
                        .as("round " + round + " eval_score")
                        .isNotNull();
                assertThat(ksGet("feedback_fact:self:" + chunkId))
                        .as("round " + round + " fact")
                        .isNotNull();
            }
        }
    }

    @Nested
    @DisplayName("S3-N3: singlecall — record")
    class N3MonotonicAccumulation {

        @Test
        @DisplayName("repeatedCallsDontDeleteExistingSignals")
        void repeatedCallsDontDeleteExistingSignals() throws IOException {
            feedbackLoopCloser.onDerive("chunk:acc:0", Layer.CHUNK, 1.0);
            assertThat(ksGet("chunk_score:chunk:acc:0"))
                    .as("first chunk after round 1")
                    .isNotNull();

            feedbackLoopCloser.onDerive("chunk:acc:1", Layer.CHUNK, 1.0);
            assertThat(ksGet("chunk_score:chunk:acc:0"))
                    .as("first chunk still exists after round 2")
                    .isNotNull();
        }

        @Test
        @DisplayName("sameChunkMultiRound_oldSignalNotDeleted")
        void sameChunkMultiRoundOldSignalNotDeleted() throws IOException {
            String key = "chunk_score:chunk:acc:same";

            feedbackLoopCloser.onDerive("chunk:acc:same", Layer.CHUNK, 1.0);
            assertThat(ksGet(key)).isNotNull();

            feedbackLoopCloser.onDerive("chunk:acc:same", Layer.CHUNK, 1.0);
            assertThat(ksGet(key)).as("after second call").isNotNull();

            feedbackLoopCloser.onDerive("chunk:acc:same", Layer.CHUNK, 1.0);
            assertThat(ksGet(key)).as("after third call").isNotNull();
        }

        @Test
        @DisplayName("feedbackSignalsAccumulateAcrossBatches")
        void feedbackSignalsAccumulateAcrossBatches() throws IOException {
            int batchSize = 4;
            int rounds = 3;
            List<String> allIds = new ArrayList<>();

            for (int r = 0; r < rounds; r++) {
                for (int i = 0; i < batchSize; i++) {
                    String id = "chunk:batch" + r + ":" + i;
                    allIds.add(id);
                    feedbackLoopCloser.onDerive(id, Layer.CHUNK, 1.0);
                }
            }

            for (String id : allIds) {
                assertThat(ksGet("chunk_score:" + id + "")).as("eval_score for " + id).isNotNull();
            }
        }
    }

    @Nested
    @DisplayName(
            "S4: full chain — onDerive -> emitFeedback -> persistEvaluationScore + ingestAsFact")
    class FullChain {

        @Test
        @DisplayName("fullChain_perChunkFself_allStorageTargetsPopulated")
        void fullChainAllStorageTargetsPopulated() throws IOException {
            feedbackLoopCloser.onDerive("chunk:full:0", Layer.CHUNK, 1.0);

            byte[] evalBytes = ksGet("chunk_score:chunk:full:0");
            assertThat(evalBytes).as("eval_score").isNotNull();
            var evalNode = MAPPER.readTree(evalBytes);
            assertThat(evalNode.has("score")).isTrue();
            assertThat(evalNode.get("score").asDouble()).isEqualTo(1.0);

            byte[] factBytes = ksGet("feedback_fact:self:chunk:full:0");
            assertThat(factBytes).as("feedback_fact").isNotNull();
            var factNode = MAPPER.readTree(factBytes);
            assertThat(factNode.get("source").asText()).isEqualTo("SELF");
            assertThat(factNode.get("targetEntityId").asText()).isEqualTo("chunk:full:0");
            assertThat(factNode.get("targetLayer").asText()).isEqualTo("CHUNK");
            assertThat(factNode.get("path").asText()).isEqualTo("EVAL_LAYER");
            assertThat(factNode.has("timestamp")).isTrue();
            assertThat(factNode.has("feedbackId")).isTrue();
        }

        @Test
        @DisplayName("signalFieldsCorrectAfterEmitFeedback")
        void signalFieldsCorrectAfterEmitFeedback() throws IOException {
            feedbackLoopCloser.onDerive("chunk:fields:test", Layer.CHUNK, 0.92);

            byte[] evalBytes = ksGet("chunk_score:chunk:fields:test");
            var evalNode = MAPPER.readTree(evalBytes);
            assertThat(evalNode.has("score")).isTrue();
            assertThat(evalNode.get("score").asDouble()).isEqualTo(0.92);
            assertThat(evalNode.get("source").asText()).isEqualTo("SELF");
            assertThat(evalNode.has("signalId")).isTrue();
            assertThat(evalNode.has("timestamp")).isTrue();
        }
    }
}
