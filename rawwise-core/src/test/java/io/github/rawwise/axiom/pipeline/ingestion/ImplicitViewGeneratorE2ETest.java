package io.github.rawwise.axiom.pipeline.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.InMemoryKnowledgeStore;
import io.github.rawwise.engine.storage.KnowledgeStore;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.View;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ImplicitViewGenerator E2E - implicit viewgeneratefull-chain business scenario test")
class ImplicitViewGeneratorE2ETest {

    private static final long TABLE_ID = 100L;
    private static final long SNAPSHOT_ID = 1L;

    private Metastore metastore;
    private KnowledgeStore knowledgeStore;
    private DefaultImplicitViewGenerator generator;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        knowledgeStore = new InMemoryKnowledgeStore();
        generator = new DefaultImplicitViewGenerator(metastore, knowledgeStore);
    }

    @Nested
    @DisplayName("S1-HappyPath - implicit viewgenerateand lifecycle")
    class HappyPath {

        @Test
        @DisplayName("generateimplicit viewafter isViewGenerated true")
        void generateThenCheckExistence() {
            Record.Key key = Records.keyOf("s1-key".getBytes());

            assertThat(generator.isViewGenerated(key, TABLE_ID, SNAPSHOT_ID)).isFalse();

            generator.generateImplicitView(key, TABLE_ID, SNAPSHOT_ID);

            assertThat(generator.isViewGenerated(key, TABLE_ID, SNAPSHOT_ID)).isTrue();
        }

        @Test
        @DisplayName("generateimplicit viewafter getImplicitView View for")
        void generateThenRetrieve() {
            Record.Key key = Records.keyOf("s1-retrieve-key".getBytes());

            generator.generateImplicitView(key, TABLE_ID, SNAPSHOT_ID);

            Optional<View> result = generator.getImplicitView(key, TABLE_ID, SNAPSHOT_ID);
            assertThat(result).isPresent();
            View view = result.get();
            assertThat(view.viewId()).contains("v0:" + TABLE_ID);
            assertThat(view.viewType()).isEqualTo(View.ViewType.RAW_IDENTITY);
            assertThat(view.version()).isEqualTo(0);
            assertThat(view.content()).isEqualTo(key.encoded());
            assertThat(view.qualityScore()).isPresent();
            assertThat(view.qualityScore().get()).isEqualTo(0.0);
        }

        @Test
        @DisplayName("duplicatecall generateImplicitView idempotent, does not throw exception")
        void idempotentGeneration() {
            Record.Key key = Records.keyOf("s1-idempotent-key".getBytes());

            generator.generateImplicitView(key, TABLE_ID, SNAPSHOT_ID);
            generator.generateImplicitView(key, TABLE_ID, SNAPSHOT_ID);
            generator.generateImplicitView(key, TABLE_ID, SNAPSHOT_ID);

            assertThat(generator.isViewGenerated(key, TABLE_ID, SNAPSHOT_ID)).isTrue();

            Optional<View> result = generator.getImplicitView(key, TABLE_ID, SNAPSHOT_ID);
            assertThat(result).isPresent();
        }
    }

    @Nested
    @DisplayName("S2-MultiView - multiviewindependent")
    class MultiView {

        @Test
        @DisplayName("different keys generate implicit views with different viewId")
        void differentKeysGenerateDifferentViews() {
            Record.Key key1 = Records.keyOf("multi-key-1".getBytes());
            Record.Key key2 = Records.keyOf("multi-key-2".getBytes());

            generator.generateImplicitView(key1, TABLE_ID, SNAPSHOT_ID);
            generator.generateImplicitView(key2, TABLE_ID, SNAPSHOT_ID);

            Optional<View> v1 = generator.getImplicitView(key1, TABLE_ID, SNAPSHOT_ID);
            Optional<View> v2 = generator.getImplicitView(key2, TABLE_ID, SNAPSHOT_ID);

            assertThat(v1).isPresent();
            assertThat(v2).isPresent();
            assertThat(v1.get().viewId()).isNotEqualTo(v2.get().viewId());
        }

        @Test
        @DisplayName("different tableId generates independent implicit views")
        void differentTableIdsAreIndependent() {
            long table1 = 100L;
            long table2 = 200L;
            Record.Key key = Records.keyOf("same-key".getBytes());

            generator.generateImplicitView(key, table1, SNAPSHOT_ID);
            generator.generateImplicitView(key, table2, SNAPSHOT_ID);

            Optional<View> v1 = generator.getImplicitView(key, table1, SNAPSHOT_ID);
            Optional<View> v2 = generator.getImplicitView(key, table2, SNAPSHOT_ID);

            assertThat(v1).isPresent();
            assertThat(v2).isPresent();
            assertThat(v1.get().viewId()).contains("100");
            assertThat(v2.get().viewId()).contains("200");
        }

        @Test
        @DisplayName("different snapshotId generates different implicit views")
        void differentSnapshotIdsAreIndependent() {
            long snap1 = 1L;
            long snap2 = 2L;
            Record.Key key = Records.keyOf("snap-key".getBytes());

            generator.generateImplicitView(key, TABLE_ID, snap1);
            generator.generateImplicitView(key, TABLE_ID, snap2);

            Optional<View> v1 = generator.getImplicitView(key, TABLE_ID, snap1);
            Optional<View> v2 = generator.getImplicitView(key, TABLE_ID, snap2);

            assertThat(v1).isPresent();
            assertThat(v2).isPresent();
        }
    }

    @Nested
    @DisplayName("S3-NotGenerated - not yetgenerateviewofboundary conditions")
    class NotGenerated {

        @Test
        @DisplayName("not yetgenerate isViewGenerated false")
        void notGeneratedReturnsFalse() {
            Record.Key key = Records.keyOf("never-generated".getBytes());

            assertThat(generator.isViewGenerated(key, TABLE_ID, SNAPSHOT_ID)).isFalse();
        }

        @Test
        @DisplayName("not yetgenerate getImplicitView empty")
        void notGeneratedReturnsEmpty() {
            Record.Key key = Records.keyOf("never-generated-2".getBytes());

            Optional<View> result = generator.getImplicitView(key, TABLE_ID, SNAPSHOT_ID);

            assertThat(result).isEmpty();
        }
    }

    @Nested
    @DisplayName("S4-Concurrent - concurrentgenerateimplicit view")
    class Concurrent {

        @Test
        @DisplayName("multi-threaded concurrentgenerates different key ofview, all succeed")
        void concurrentGenerationAllSucceed() throws Exception {
            int threadCount = 8;
            int keysPerThread = 10;
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger successCount = new AtomicInteger(0);
            var executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                int threadId = t;
                executor.submit(
                        () -> {
                            try {
                                for (int i = 0; i < keysPerThread; i++) {
                                    Record.Key key =
                                            Records.keyOf(
                                                    ("concurrent-t" + threadId + "-k" + i)
                                                            .getBytes());
                                    generator.generateImplicitView(key, TABLE_ID, SNAPSHOT_ID);
                                    if (generator.isViewGenerated(key, TABLE_ID, SNAPSHOT_ID)) {
                                        successCount.incrementAndGet();
                                    }
                                }
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
            executor.shutdown();

            assertThat(successCount.get()).isEqualTo(threadCount * keysPerThread);
        }

        @Test
        @DisplayName("multi-threaded concurrentgeneratesame key ofview, idempotentnoerror")
        void concurrentSameKeyIdempotent() throws Exception {
            int threadCount = 8;
            Record.Key key = Records.keyOf("same-concurrent-key".getBytes());
            CountDownLatch latch = new CountDownLatch(threadCount);
            AtomicInteger errorCount = new AtomicInteger(0);
            var executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                executor.submit(
                        () -> {
                            try {
                                generator.generateImplicitView(key, TABLE_ID, SNAPSHOT_ID);
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
            assertThat(generator.isViewGenerated(key, TABLE_ID, SNAPSHOT_ID)).isTrue();
        }
    }
}
