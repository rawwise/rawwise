package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.orchestrate.TestOrchestratorBuilder;
import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnResult;
import io.github.rawwise.engine.txn.wal.InMemoryTxnWALWriter;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("Transaction Crash Recovery via WAL Replay")
class TransactionCrashRecoveryTest {

    private InMemoryMetastore metastore;
    private InMemoryTxnWALWriter walWriter;

    @Mock private LmdbInstance lmdbInstance;

    private final Map<ByteBuffer, byte[]> lmdbData = new ConcurrentHashMap<>();

    private static final String CATALOG = "test_catalog";
    private static final String DATABASE = "test_db";
    private static final String TABLE = "test_table";

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        walWriter = new InMemoryTxnWALWriter();
        lmdbData.clear();

        lenient().when(lmdbInstance.readOnly()).thenReturn(false);
        lenient()
                .when(lmdbInstance.put(any(), any()))
                .thenAnswer(
                        invocation -> {
                            byte[] key = invocation.getArgument(0);
                            byte[] value = invocation.getArgument(1);
                            lmdbData.put(ByteBuffer.wrap(key), value);
                            return LmdbInstance.PutStatus.SUCCESS;
                        });
        lenient()
                .when(lmdbInstance.batchPut(any(), any()))
                .thenAnswer(
                        invocation -> {
                            List<byte[]> keys = invocation.getArgument(0);
                            List<byte[]> values = invocation.getArgument(1);
                            for (int i = 0; i < keys.size(); i++) {
                                lmdbData.put(ByteBuffer.wrap(keys.get(i)), values.get(i));
                            }
                            return new LmdbInstance.BatchPutResult(
                                    keys.size(), LmdbInstance.PutStatus.SUCCESS);
                        });
        lenient()
                .when(lmdbInstance.batchPutWithMarker(any(), any(), any(), any()))
                .thenAnswer(
                        invocation -> {
                            List<byte[]> keys = invocation.getArgument(0);
                            List<byte[]> values = invocation.getArgument(1);
                            for (int i = 0; i < keys.size(); i++) {
                                lmdbData.put(ByteBuffer.wrap(keys.get(i)), values.get(i));
                            }
                            byte[] markerKey = invocation.getArgument(2);
                            byte[] markerValue = invocation.getArgument(3);
                            lmdbData.put(ByteBuffer.wrap(markerKey), markerValue);
                            return new LmdbInstance.BatchPutResult(
                                    keys.size(), LmdbInstance.PutStatus.SUCCESS);
                        });
        lenient()
                .when(lmdbInstance.get(any()))
                .thenAnswer(
                        invocation -> {
                            byte[] key = invocation.getArgument(0);
                            return lmdbData.get(ByteBuffer.wrap(key));
                        });
    }

    private TxnOrchestrator createOrchestrator() {
        return TestOrchestratorBuilder.createWithSharedWal(
                metastore, lmdbInstance, walWriter, CATALOG, DATABASE);
    }

    private byte[] markerKeyFor(long txnId) {
        return ("__commit_marker__:" + txnId).getBytes(StandardCharsets.UTF_8);
    }

    private boolean isMarkerPresent(long txnId) {
        byte[] value = lmdbData.get(ByteBuffer.wrap(markerKeyFor(txnId)));
        return value != null && value.length > 0 && value[0] == 1;
    }

    @Test
    @DisplayName("S1: Recovery is no-op when no WAL records exist for the transaction")
    void testRecoverNoOpWhenNoWalRecords() {
        TxnOrchestrator orchestrator = createOrchestrator();

        String result = orchestrator.recover(99999L);

        assertThat(result).isEqualTo("no-state-found:99999");
    }

    @Test
    @DisplayName(
            "S3: Committed transaction recovery returns already-committed and data remains visible")
    void testRecoverCommittedTransactionDataVisible() {
        TxnOrchestrator orchestrator1 = createOrchestrator();

        TxnContext ctx = orchestrator1.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        long txnId = ctx.txnId();
        orchestrator1.recordWrite(ctx, TABLE, "key-1".getBytes(), "value-1".getBytes(), 0);
        TxnResult result = orchestrator1.commit(ctx);
        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);

        assertThat(isMarkerPresent(txnId)).isTrue();

        TxnOrchestrator orchestrator2 = createOrchestrator();
        String recoverResult = orchestrator2.recover(txnId);

        assertThat(recoverResult).isEqualTo("already-committed:" + txnId);
        assertThat(isMarkerPresent(txnId)).isTrue();
    }

    @Test
    @DisplayName("S3: Uncommitted transaction recovery aborts and data is not visible")
    void testRecoverUncommittedTransactionDataNotVisible() {
        TxnOrchestrator orchestrator1 = createOrchestrator();

        TxnContext ctx = orchestrator1.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        long txnId = ctx.txnId();
        orchestrator1.recordWrite(
                ctx, TABLE, "key-uncommitted".getBytes(), "value-uncommitted".getBytes(), 0);

        assertThat(isMarkerPresent(txnId)).isFalse();

        TxnOrchestrator orchestrator2 = createOrchestrator();
        String recoverResult = orchestrator2.recover(txnId);

        assertThat(recoverResult).isEqualTo("recovered-aborted:" + txnId);
        assertThat(isMarkerPresent(txnId)).isFalse();
    }

    @Test
    @DisplayName(
            "S3: Recovery from WAL with incomplete transaction (begin + write, no commit) aborts")
    void testRecoverFromWalWithIncompleteTransaction() {
        TxnOrchestrator orchestrator1 = createOrchestrator();

        TxnContext ctx = orchestrator1.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        long txnId = ctx.txnId();
        orchestrator1.recordWrite(
                ctx, TABLE, "key-incomplete".getBytes(), "value-incomplete".getBytes(), 0);

        List<Object> walRecords = walWriter.scan();
        assertThat(walRecords).isNotEmpty();
        boolean hasBegin =
                walRecords.stream().anyMatch(r -> r instanceof TxnWALRecord.TxnBeginRecord);
        boolean hasCommitted =
                walRecords.stream().anyMatch(r -> r instanceof TxnWALRecord.TxnCommitted);
        assertThat(hasBegin).isTrue();
        assertThat(hasCommitted).isFalse();

        TxnOrchestrator orchestrator2 = createOrchestrator();
        String recoverResult = orchestrator2.recover(txnId);

        assertThat(recoverResult).isEqualTo("recovered-aborted:" + txnId);

        List<Object> walRecordsAfterRecovery = walWriter.scan();
        boolean hasAborted =
                walRecordsAfterRecovery.stream().anyMatch(r -> r instanceof TxnWALRecord.TxnAbort);
        assertThat(hasAborted).isTrue();
    }

    @Test
    @DisplayName("S3: Recovery from WAL with committed transaction returns already-committed")
    void testRecoverFromWalWithCommittedTransaction() {
        TxnOrchestrator orchestrator1 = createOrchestrator();

        TxnContext ctx = orchestrator1.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        long txnId = ctx.txnId();
        orchestrator1.recordWrite(
                ctx, TABLE, "key-committed".getBytes(), "value-committed".getBytes(), 0);
        TxnResult result = orchestrator1.commit(ctx);
        assertThat(result).isInstanceOf(TxnResult.CommitOk.class);

        List<Object> walRecords = walWriter.scan();
        assertThat(walRecords).isNotEmpty();
        boolean hasBegin =
                walRecords.stream().anyMatch(r -> r instanceof TxnWALRecord.TxnBeginRecord);
        boolean hasWriteIntent =
                walRecords.stream().anyMatch(r -> r instanceof TxnWALRecord.TxnWriteIntent);
        boolean hasPrepareAndDurable =
                walRecords.stream().anyMatch(r -> r instanceof TxnWALRecord.TxnPrepareAndDurable);
        boolean hasCommitted =
                walRecords.stream().anyMatch(r -> r instanceof TxnWALRecord.TxnCommitted);
        assertThat(hasBegin).isTrue();
        assertThat(hasWriteIntent).isTrue();
        assertThat(hasPrepareAndDurable).isTrue();
        assertThat(hasCommitted).isTrue();

        TxnOrchestrator orchestrator2 = createOrchestrator();
        String recoverResult = orchestrator2.recover(txnId);

        assertThat(recoverResult).isEqualTo("already-committed:" + txnId);
    }

    @Test
    @DisplayName("S4: Multiple transactions in mixed states recover with correct decisions")
    void testRecoverMultipleTransactionsMixedStates() {
        TxnOrchestrator orchestrator1 = createOrchestrator();

        TxnContext ctx1 = orchestrator1.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        long txnId1 = ctx1.txnId();
        orchestrator1.recordWrite(ctx1, TABLE, "key-1".getBytes(), "value-1".getBytes(), 0);
        TxnResult result1 = orchestrator1.commit(ctx1);
        assertThat(result1).isInstanceOf(TxnResult.CommitOk.class);

        TxnContext ctx2 = orchestrator1.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        long txnId2 = ctx2.txnId();
        orchestrator1.recordWrite(ctx2, TABLE, "key-2".getBytes(), "value-2".getBytes(), 0);

        TxnContext ctx3 = orchestrator1.begin(Set.of(TABLE), IsolationLevel.READ_COMMITTED);
        long txnId3 = ctx3.txnId();

        assertThat(txnId1).isLessThan(txnId2);
        assertThat(txnId2).isLessThan(txnId3);

        TxnOrchestrator orchestrator2 = createOrchestrator();

        String recoverResult1 = orchestrator2.recover(txnId1);
        assertThat(recoverResult1).isEqualTo("already-committed:" + txnId1);

        String recoverResult2 = orchestrator2.recover(txnId2);
        assertThat(recoverResult2).isEqualTo("recovered-aborted:" + txnId2);

        String recoverResult3 = orchestrator2.recover(txnId3);
        assertThat(recoverResult3).isEqualTo("recovered-aborted:" + txnId3);

        assertThat(isMarkerPresent(txnId1)).isTrue();
        assertThat(isMarkerPresent(txnId2)).isFalse();
        assertThat(isMarkerPresent(txnId3)).isFalse();
    }
}
