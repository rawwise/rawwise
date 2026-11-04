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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName(
        "Per-key OCC Verification via DynamicMmapStore -> ShardVersionTracker -> ValidateStage")
class PerKeyOCCVerificationTest {

    private static boolean lmdbAvailable = true;

    @TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbInstance;
    private DynamicMmapStore store;
    private TxnOrchestrator perKeyOrchestrator;

    private static final String CATALOG = "pkocc_catalog";
    private static final String DATABASE = "pkocc_db";
    private static final String TABLE = "pkocc_table";
    private static final String TABLE_ID = CATALOG + "." + DATABASE + "." + TABLE;

    @BeforeAll
    static void checkLmdbAvailability() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception ignored) {
        }

        Path probeDir = null;
        try {
            probeDir = Files.createTempDirectory("pkocc-probe-" + UUID.randomUUID());
            LmdbInstance probe =
                    new LmdbInstance("probe-" + UUID.randomUUID(), probeDir, 1024L * 1024, 1);
            probe.close();
        } catch (Throwable t) {
            lmdbAvailable = false;
            System.err.println(
                    "[PerKeyOCCVerificationTest] LMDB not available, skipping: " + t.getMessage());
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
        assumeTrue(lmdbAvailable, "LMDB native library not available");

        metastore = new InMemoryMetastore();
        String clockKey = String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE);
        byte[] zeroBytes = ByteBuffer.allocate(Long.BYTES).putLong(0L).array();
        metastore.put(clockKey.getBytes(StandardCharsets.UTF_8), zeroBytes);

        lmdbInstance =
                new LmdbInstance("pkocc-inst-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024, 5);

        store = DynamicMmapStore.getInstance(tempDir.toString(), "pkocc-store");

        ShardVersionTracker tracker = store.getOrCreateVersionTracker(TABLE_ID);

        perKeyOrchestrator =
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
        if (perKeyOrchestrator instanceof TxnOrchestratorImpl impl) {
            impl.close();
        }
        if (store != null) {
            store.close();
        }
        if (lmdbInstance != null) {
            lmdbInstance.close();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private long writeAndCommit(TxnOrchestrator orch, String key, String value, long baseVersion) {
        TxnContext ctx = orch.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orch.recordWrite(ctx, TABLE, bytes(key), bytes(value), baseVersion);
        TxnResult r = orch.commit(ctx);
        assertThat(r).as("commit(%s=%s)", key, value).isInstanceOf(TxnResult.CommitOk.class);
        return ((TxnResult.CommitOk) r).commitTs();
    }

    @Nested
    @DisplayName("Per-key vs Table-level contrast")
    class PerKeyVsTableLevel {

        @Test
        @DisplayName("Per-key OCC: same-table DIFFERENT keys -> NO conflict (: per-key)")
        void testDifferentKeysSameTableNoConflict() throws Exception {
            long cTs1 = writeAndCommit(perKeyOrchestrator, "K1", "V1", 0);
            long cTs2 = writeAndCommit(perKeyOrchestrator, "K2", "V2", 0);

            assertThat(cTs1).isPositive();
            assertThat(cTs2).isPositive();
            assertThat(cTs2).isGreaterThan(cTs1);
        }

        @Test
        @DisplayName("Per-key OCC: same-table SAME key -> DETECTS conflict (: per-key not yet)")
        void testSameKeyDetectsConflict() {
            byte[] key = bytes("shared-key");
            long baseTs = writeAndCommit(perKeyOrchestrator, "shared-key", "initial", 0);

            TxnContext t1 = perKeyOrchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            perKeyOrchestrator.pointRead(TABLE, key, t1);
            perKeyOrchestrator.recordWrite(t1, TABLE, key, bytes("v1"), baseTs);

            TxnContext t2 = perKeyOrchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            perKeyOrchestrator.pointRead(TABLE, key, t2);
            perKeyOrchestrator.recordWrite(t2, TABLE, key, bytes("v2"), baseTs);

            assertThat(perKeyOrchestrator.commit(t2)).isInstanceOf(TxnResult.CommitOk.class);

            TxnResult t1Result = perKeyOrchestrator.commit(t1);
            assertThat(t1Result).isInstanceOf(TxnResult.CommitConflict.class);
            assertThat(((TxnResult.CommitConflict) t1Result).reason()).contains("OCC conflict");
        }

        @Test
        @DisplayName(
                "Per-key OCC: version updated after commit -> subsequent read sees correct baseVersion")
        void testVersionUpdateAfterCommit() {
            long ts1 = writeAndCommit(perKeyOrchestrator, "ver-k1", "v1", 0);

            TxnContext t2 = perKeyOrchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> read = perKeyOrchestrator.pointRead(TABLE, bytes("ver-k1"), t2);
            assertThat(read).isPresent();
            assertThat(read.get()).isEqualTo(bytes("v1"));

            perKeyOrchestrator.recordWrite(t2, TABLE, bytes("ver-k1"), bytes("v2"), ts1);
            assertThat(perKeyOrchestrator.commit(t2)).isInstanceOf(TxnResult.CommitOk.class);

            TxnContext t3 = perKeyOrchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            Optional<byte[]> read3 = perKeyOrchestrator.pointRead(TABLE, bytes("ver-k1"), t3);
            assertThat(read3).isPresent();
            assertThat(read3.get()).isEqualTo(bytes("v2"));
            perKeyOrchestrator.commit(t3);
        }
    }

    @Nested
    @DisplayName("Phantom detection via ShardVersionTracker.skipListMap")
    class PhantomDetection {

        @Test
        @DisplayName("SERIALIZABLE: other txn's insert in range -> detected via skipListMap")
        void testCrossTxnPhantomDetected() {
            byte[] k1 = bytes("p-k1");
            byte[] k5 = bytes("p-k5");
            byte[] k3 = bytes("p-k3");

            writeAndCommit(perKeyOrchestrator, "p-k1", "v1", 0);
            writeAndCommit(perKeyOrchestrator, "p-k5", "v5", 0);

            TxnContext txnA = perKeyOrchestrator.begin(Set.of(TABLE), IsolationLevel.SERIALIZABLE);
            assertThat(perKeyOrchestrator.pointRead(TABLE, k1, txnA)).isPresent();
            perKeyOrchestrator.rangeRead(TABLE, k1, k5, txnA);

            writeAndCommit(perKeyOrchestrator, "p-k3", "v3", 0);

            perKeyOrchestrator.recordWrite(txnA, TABLE, k1, bytes("updated-v1"), 0);
            TxnResult result = perKeyOrchestrator.commit(txnA);

            assertThat(result)
                    .as("Phantom: k3 inserted in [k1, k5] by other txn → SERIALIZABLE conflict")
                    .isInstanceOf(TxnResult.CommitConflict.class);
        }
    }
}
