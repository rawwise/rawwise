package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Recovery: PublishStage replay + WAL COMMITTED")
class RecoveryPublishStageReplayTest {

    private static boolean lmdbAvailable = true;

    @TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbInstance;
    private DynamicMmapStore store;
    private ShardVersionTracker tracker;
    private TxnOrchestrator orchestrator;

    private static final String CATALOG = "rec_cat";
    private static final String DATABASE = "rec_db";
    private static final String TABLE = "rec_table";
    private static final String TABLE_ID = CATALOG + "." + DATABASE + "." + TABLE;

    @BeforeAll
    static void checkLmdbAvailability() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }
        Path probeDir = null;
        try {
            probeDir = Files.createTempDirectory("rec-probe-" + UUID.randomUUID());
            new LmdbInstance("probe-" + UUID.randomUUID(), probeDir, 1024L * 1024, 1).close();
        } catch (Throwable t) {
            lmdbAvailable = false;
        } finally {
            if (probeDir != null) {
                try (var stream = Files.walk(probeDir)) {
                    stream.sorted(Comparator.reverseOrder())
                            .forEach(
                                    p -> {
                                        try {
                                            Files.deleteIfExists(p);
                                        } catch (Exception ignored) {
                                        }
                                    });
                } catch (Exception ignored) {
                }
            }
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        assumeTrue(lmdbAvailable);

        metastore = new InMemoryMetastore();
        String clockKey = String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE);
        metastore.put(
                clockKey.getBytes(StandardCharsets.UTF_8),
                ByteBuffer.allocate(8).putLong(0L).array());

        lmdbInstance =
                new LmdbInstance("rec-inst-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);
        store = DynamicMmapStore.getInstance(tempDir.toString(), "rec-store");
        tracker = store.getOrCreateVersionTracker(TABLE_ID);

        orchestrator =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(),
                        metastore,
                        lmdbInstance,
                        CATALOG,
                        DATABASE,
                        TABLE,
                        null,
                        tracker);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (orchestrator instanceof TxnOrchestratorImpl impl) impl.close();
        if (store != null) store.close();
        if (lmdbInstance != null) lmdbInstance.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Tx commit -> tracker version equals clock commitTs")
    void testTrackerVersionCorrectAfterCommit() {
        long ts1 = commit("ver-k1", "v1");
        assertThat(tracker.getCommitTs(bytes("ver-k1"))).isEqualTo(ts1);

        long ts2 = commit("ver-k2", "v2");
        assertThat(tracker.getCommitTs(bytes("ver-k2"))).isEqualTo(ts2);
        assertThat(tracker.getCommitTs(bytes("ver-k1"))).isEqualTo(ts1);
    }

    @Test
    @DisplayName("Two transactions same key -> tracker versions monotonic")
    void testTrackerVersionsMonotonic() {
        long ts1 = commit("k1", "v1");
        long ts2 = commit("k1", "v2");

        assertThat(ts2).isGreaterThan(ts1);
        assertThat(tracker.getCommitTs(bytes("k1"))).isEqualTo(ts2);
    }

    private long commit(String key, String value) {
        TxnContext ctx = orchestrator.begin(Set.of(TABLE_ID), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(ctx, TABLE_ID, bytes(key), bytes(value), 0);
        TxnResult r = orchestrator.commit(ctx);
        assertThat(r).isInstanceOf(TxnResult.CommitOk.class);
        return ((TxnResult.CommitOk) r).commitTs();
    }
}
