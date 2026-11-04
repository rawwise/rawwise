package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.txn.clock.AtomicLongClock;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorFactory;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestratorImpl;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.governance.config.TxnEngineConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.*;

/**
 * E2E test verifying that commitTs embedded in the LMDB record header equals the transaction-level
 * commitTs returned by the orchestrator.
 */
@DisplayName("CommitTs embedded in LMDB record header")
class CommitTsHeaderE2ETest {

    @org.junit.jupiter.api.io.TempDir Path tempDir;

    private InMemoryMetastore metastore;
    private LmdbInstance lmdbInstance;
    private TxnOrchestratorImpl orchestrator;

    private static final String CATALOG = "committs";
    private static final String DATABASE = "default";
    private static final String TABLE = "hdr_table";

    @BeforeEach
    void setUp() throws Exception {
        metastore = new InMemoryMetastore();
        byte[] clockKey =
                String.format("/logical_clock/%s/%s/%s", CATALOG, DATABASE, TABLE)
                        .getBytes(StandardCharsets.UTF_8);
        metastore.put(clockKey, new byte[] {0, 0, 0, 0, 0, 0, 0, 1});

        lmdbInstance =
                new LmdbInstance("committs-" + UUID.randomUUID(), tempDir, 128L * 1024 * 1024);

        ShardVersionTracker tracker =
                new ShardVersionTracker(0, new AtomicLongClock(0), RecordHeaderProbe.NOOP);

        TxnOrchestrator orch =
                TxnOrchestratorFactory.create(
                        TxnEngineConfig.planA(),
                        metastore,
                        lmdbInstance,
                        CATALOG,
                        DATABASE,
                        TABLE,
                        null,
                        tracker);
        orchestrator = (TxnOrchestratorImpl) orch;
    }

    @AfterEach
    void tearDown() {
        if (orchestrator != null) orchestrator.close();
        if (lmdbInstance != null) lmdbInstance.close();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Write then verify LMDB header.commitTimestamp equals coordinator commitTs")
    void testHeaderCommitTsMatchesCoordinatorCommitTs() {
        byte[] keyA = bytes("keyA");
        byte[] value = bytes("hello-committs");

        TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(ctx, TABLE, keyA, value, 0);
        TxnResult result = orchestrator.commit(ctx);

        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
        long commitTs = ((TxnResult.CommitOk) result).commitTs();
        assertThat(commitTs).isGreaterThan(0);

        byte[] raw = lmdbInstance.get(keyA);
        assertThat(raw).as("Data must exist in LMDB after commit").isNotNull();

        byte[] decoded = NullableValueCodec.decode(raw);
        assertThat(decoded).as("LMDB value must be NullableValueCodec-wrapped record").isNotNull();

        long headerCommitTs = DefaultRecordCodec.peekCommitTimestamp(decoded);

        assertThat(headerCommitTs)
                .as("Record header commitTimestamp must equal coordinator commitTs")
                .isEqualTo(commitTs);
    }

    @Test
    @DisplayName("Multiple transactions: each row has correct monotonic commitTs")
    void testMultipleTransactionsMonotonicCommitTs() {
        long maxCommitTs = 0;

        for (int i = 0; i < 3; i++) {
            byte[] key = bytes("key" + i);
            byte[] val = bytes("val" + i);

            TxnContext ctx = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
            orchestrator.recordWrite(ctx, TABLE, key, val, 0);
            TxnResult result = orchestrator.commit(ctx);

            assertThat(result).isInstanceOf(TxnResult.CommitOk.class);
            long commitTs = ((TxnResult.CommitOk) result).commitTs();

            assertThat(commitTs).as("CommitTs must be monotonic").isGreaterThan(maxCommitTs);
            maxCommitTs = commitTs;

            byte[] raw = lmdbInstance.get(key);
            byte[] decoded = NullableValueCodec.decode(raw);
            long headerTs = DefaultRecordCodec.peekCommitTimestamp(decoded);

            assertThat(headerTs)
                    .as("Row %d header commitTs must equal coordinator commitTs", i)
                    .isEqualTo(commitTs);
        }
    }

    @Test
    @DisplayName("Concurrent writes to different keys: all headers have correct commitTs")
    void testConcurrentWritesDifferentKeysCorrectCommitTs() {
        TxnContext ctx1 = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(ctx1, TABLE, bytes("concurrentA"), bytes("a"), 0);

        TxnContext ctx2 = orchestrator.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        orchestrator.recordWrite(ctx2, TABLE, bytes("concurrentB"), bytes("b"), 0);

        TxnResult r2 = orchestrator.commit(ctx2);
        TxnResult r1 = orchestrator.commit(ctx1);

        assertThat(r1).isInstanceOf(TxnResult.CommitOk.class);
        assertThat(r2).isInstanceOf(TxnResult.CommitOk.class);

        long ts1 = ((TxnResult.CommitOk) r1).commitTs();
        long ts2 = ((TxnResult.CommitOk) r2).commitTs();

        assertThat(headerCommitTs(bytes("concurrentA"))).isEqualTo(ts1);
        assertThat(headerCommitTs(bytes("concurrentB"))).isEqualTo(ts2);
    }

    private long headerCommitTs(byte[] key) {
        byte[] raw = lmdbInstance.get(key);
        byte[] decoded = NullableValueCodec.decode(raw);
        return DefaultRecordCodec.peekCommitTimestamp(decoded);
    }
}
