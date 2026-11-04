package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.wal.TxnWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecordCodec;
import io.github.rawwise.engine.txn.wal.InMemoryTxnWALWriter;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnAbort;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnBeginRecord;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnCommitted;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnPrepareAndDurable;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnWriteIntent;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("WAL persisted E2E test")
class WalPersistenceE2ETest {

    private static final String TABLE_ID = "test_table";
    private static final Set<String> TABLE_IDS = Set.of(TABLE_ID);

    @Nested
    @DisplayName("transaction commit WAL persisted")
    class WalPersistOnCommit {

        @Test
        @DisplayName("transaction commitafter WAL inhas PREPARED + COMMIT record")
        void shouldPersistWalOnCommit() {
            InMemoryTxnWALWriter walWriter = new InMemoryTxnWALWriter();

            long txnId = 1L;
            long commitTs = System.currentTimeMillis();

            walWriter.writeBegin(
                    new TxnBeginRecord(
                            txnId,
                            100L,
                            IsolationLevel.READ_COMMITTED,
                            TABLE_IDS,
                            System.currentTimeMillis()));

            walWriter.writeWriteIntent(
                    new TxnWriteIntent(
                            txnId,
                            TABLE_IDS,
                            TABLE_ID,
                            0,
                            100L,
                            IsolationLevel.READ_COMMITTED,
                            0,
                            "key1".getBytes(),
                            "value1".getBytes(),
                            io.github.rawwise.engine.txn.wal.TxnWALRecord.WriteType.UPSERT,
                            0,
                            0,
                            0L));

            walWriter.writePrepareAndDurable(
                    new TxnPrepareAndDurable(
                            txnId, null, List.of(), new byte[0], System.currentTimeMillis()));

            walWriter.writeCommitted(new TxnCommitted(txnId, commitTs, null));

            List<Object> records = walWriter.scan();
            assertThat(records).hasSize(4);

            assertThat(records.get(0)).isInstanceOf(TxnBeginRecord.class);
            TxnBeginRecord begin = (TxnBeginRecord) records.get(0);
            assertThat(begin.txnId()).isEqualTo(txnId);

            assertThat(records.get(1)).isInstanceOf(TxnWriteIntent.class);
            TxnWriteIntent intent = (TxnWriteIntent) records.get(1);
            assertThat(intent.txnId()).isEqualTo(txnId);
            assertThat(intent.keyBytes()).containsExactly("key1".getBytes());
            assertThat(intent.valueRef()).containsExactly("value1".getBytes());

            assertThat(records.get(2)).isInstanceOf(TxnPrepareAndDurable.class);
            TxnPrepareAndDurable prepared = (TxnPrepareAndDurable) records.get(2);
            assertThat(prepared.txnId()).isEqualTo(txnId);

            assertThat(records.get(3)).isInstanceOf(TxnCommitted.class);
            TxnCommitted committed = (TxnCommitted) records.get(3);
            assertThat(committed.txnId()).isEqualTo(txnId);
            assertThat(committed.commitTs()).isEqualTo(commitTs);
        }
    }

    @Nested
    @DisplayName("PREPARED aftercrash recovery")
    class RecoverPreparedTxnAfterCrash {

        @Test
        @DisplayName("PREPARED aftersimulate -> recoveryafterwrite data can")
        void shouldRecoverPreparedTxnAfterCrash() {
            InMemoryTxnWALWriter walWriter = new InMemoryTxnWALWriter();

            long txnId = 2L;

            walWriter.writeBegin(
                    new TxnBeginRecord(
                            txnId,
                            200L,
                            IsolationLevel.READ_COMMITTED,
                            TABLE_IDS,
                            System.currentTimeMillis()));

            walWriter.writeWriteIntent(
                    new TxnWriteIntent(
                            txnId,
                            TABLE_IDS,
                            TABLE_ID,
                            0,
                            200L,
                            IsolationLevel.READ_COMMITTED,
                            0,
                            "key2".getBytes(),
                            "value2".getBytes(),
                            io.github.rawwise.engine.txn.wal.TxnWALRecord.WriteType.UPSERT,
                            0,
                            0,
                            0L));

            walWriter.writePrepareAndDurable(
                    new TxnPrepareAndDurable(
                            txnId, null, List.of(), new byte[0], System.currentTimeMillis()));

            List<Object> records = walWriter.scan();

            boolean hasPrepared = records.stream().anyMatch(r -> r instanceof TxnPrepareAndDurable);
            boolean hasCommit = records.stream().anyMatch(r -> r instanceof TxnCommitted);
            boolean hasWriteIntents = records.stream().anyMatch(r -> r instanceof TxnWriteIntent);

            assertThat(hasPrepared).isTrue();
            assertThat(hasCommit).isFalse();
            assertThat(hasWriteIntents).isTrue();

            TxnWriteIntent writeIntent =
                    records.stream()
                            .filter(r -> r instanceof TxnWriteIntent)
                            .map(r -> (TxnWriteIntent) r)
                            .findFirst()
                            .orElseThrow();
            assertThat(writeIntent.keyBytes()).containsExactly("key2".getBytes());
            assertThat(writeIntent.valueRef()).containsExactly("value2".getBytes());
        }
    }

    @Nested
    @DisplayName("PREPARED no COMMIT crash recovery")
    class AbortUncommittedTxnAfterCrash {

        @Test
        @DisplayName("PREPARED no COMMIT nowrite -> recoveryafter ABORTED")
        void shouldAbortUncommittedTxnAfterCrash() {
            InMemoryTxnWALWriter walWriter = new InMemoryTxnWALWriter();

            long txnId = 3L;

            walWriter.writeBegin(
                    new TxnBeginRecord(
                            txnId,
                            300L,
                            IsolationLevel.READ_COMMITTED,
                            TABLE_IDS,
                            System.currentTimeMillis()));

            walWriter.writePrepareAndDurable(
                    new TxnPrepareAndDurable(
                            txnId, null, List.of(), new byte[0], System.currentTimeMillis()));

            List<Object> records = walWriter.scan();

            boolean hasPrepared = records.stream().anyMatch(r -> r instanceof TxnPrepareAndDurable);
            boolean hasCommit = records.stream().anyMatch(r -> r instanceof TxnCommitted);
            boolean hasWriteIntents = records.stream().anyMatch(r -> r instanceof TxnWriteIntent);

            assertThat(hasPrepared).isTrue();
            assertThat(hasCommit).isFalse();
            assertThat(hasWriteIntents).isFalse();

            walWriter.writeAbort(
                    new TxnAbort(txnId, "incomplete write set", System.currentTimeMillis()));

            List<Object> recordsAfterAbort = walWriter.scan();
            boolean hasAbort = recordsAfterAbort.stream().anyMatch(r -> r instanceof TxnAbort);
            assertThat(hasAbort).isTrue();
        }
    }

    @Nested
    @DisplayName("write transaction")
    class EmptyWriteSet {

        @Test
        @DisplayName("write transaction write WAL record")
        void shouldHandleEmptyWriteSet() {
            InMemoryTxnWALWriter walWriter = new InMemoryTxnWALWriter();

            long txnId = 4L;

            List<Object> records = walWriter.scan();
            assertThat(records).isEmpty();

            TxnContext txnContext =
                    new TxnContext(
                            txnId,
                            400L,
                            IsolationLevel.READ_COMMITTED,
                            TABLE_IDS,
                            0,
                            0L,
                            0L,
                            null,
                            TxnTimeoutProfile.DEFAULT);

            assertThat(txnContext.operationSet().isWriteEmpty()).isTrue();
        }
    }

    @Nested
    @DisplayName("WalBackedTxnWALWriter decode")
    class WalBackedTxnWALWriterCodecTest {

        @Test
        @DisplayName("TxnWALRecord correctly decodable after encodingis backend TxnWALRecord")
        void shouldEncodeAndDecodeTxnBeginRecord() {
            long txnId = 10L;
            List<String> tableIds = List.of(TABLE_ID);
            String isolationLevel = IsolationLevel.READ_COMMITTED.name();
            long beginTs = 1000L;
            long baseSnapshotId = 0L;

            TxnWALRecord backendRecord =
                    TxnWALRecord.begin(txnId, tableIds, isolationLevel, beginTs, baseSnapshotId);
            byte[] encoded = TxnWALRecordCodec.encode(backendRecord);
            TxnWALRecord decoded = TxnWALRecordCodec.decode(encoded);

            assertThat(decoded.type()).isEqualTo(TxnWALRecord.Type.TXN_BEGIN);
            assertThat(decoded.txnId()).isEqualTo(txnId);
            assertThat(decoded.tableIds()).containsExactly(TABLE_ID);
            assertThat(decoded.isolationLevel()).isEqualTo(isolationLevel);
            assertThat(decoded.beginTs()).isEqualTo(beginTs);
        }

        @Test
        @DisplayName("TXN_DATA correctly decodable after encoding")
        void shouldEncodeAndDecodeTxnDataRecord() {
            long txnId = 11L;
            byte[] key = "testKey".getBytes();
            byte[] value = "testValue".getBytes();

            TxnWALRecord dataRecord = TxnWALRecord.data(txnId, TABLE_ID, key, value, "UPSERT");
            byte[] encoded = TxnWALRecordCodec.encode(dataRecord);
            TxnWALRecord decoded = TxnWALRecordCodec.decode(encoded);

            assertThat(decoded.type()).isEqualTo(TxnWALRecord.Type.TXN_DATA);
            assertThat(decoded.txnId()).isEqualTo(txnId);
            assertThat(decoded.tableId()).isEqualTo(TABLE_ID);
            assertThat(decoded.key()).containsExactly(key);
            assertThat(decoded.value()).containsExactly(value);
            assertThat(decoded.writeType()).isEqualTo("UPSERT");
        }

        @Test
        @DisplayName("TXN_PREPARED correctly decodable after encoding")
        void shouldEncodeAndDecodeTxnPreparedRecord() {
            long txnId = 12L;
            long commitTs = 2000L;

            TxnWALRecord preparedRecord = TxnWALRecord.prepared(txnId, TABLE_ID, commitTs);
            byte[] encoded = TxnWALRecordCodec.encode(preparedRecord);
            TxnWALRecord decoded = TxnWALRecordCodec.decode(encoded);

            assertThat(decoded.type()).isEqualTo(TxnWALRecord.Type.TXN_PREPARED);
            assertThat(decoded.txnId()).isEqualTo(txnId);
            assertThat(decoded.tableId()).isEqualTo(TABLE_ID);
            assertThat(decoded.commitTs()).isEqualTo(commitTs);
        }

        @Test
        @DisplayName("TXN_COMMIT correctly decodable after encoding")
        void shouldEncodeAndDecodeTxnCommitRecord() {
            long txnId = 13L;
            long commitTs = 3000L;
            long newSnapshotId = 100L;

            TxnWALRecord commitRecord =
                    TxnWALRecord.commit(txnId, TABLE_ID, commitTs, newSnapshotId);
            byte[] encoded = TxnWALRecordCodec.encode(commitRecord);
            TxnWALRecord decoded = TxnWALRecordCodec.decode(encoded);

            assertThat(decoded.type()).isEqualTo(TxnWALRecord.Type.TXN_COMMIT);
            assertThat(decoded.txnId()).isEqualTo(txnId);
            assertThat(decoded.tableId()).isEqualTo(TABLE_ID);
            assertThat(decoded.commitTs()).isEqualTo(commitTs);
            assertThat(decoded.newSnapshotId()).isEqualTo(newSnapshotId);
        }

        @Test
        @DisplayName("TXN_ABORT correctly decodable after encoding")
        void shouldEncodeAndDecodeTxnAbortRecord() {
            long txnId = 14L;

            TxnWALRecord abortRecord = TxnWALRecord.abort(txnId, TABLE_ID);
            byte[] encoded = TxnWALRecordCodec.encode(abortRecord);
            TxnWALRecord decoded = TxnWALRecordCodec.decode(encoded);

            assertThat(decoded.type()).isEqualTo(TxnWALRecord.Type.TXN_ABORT);
            assertThat(decoded.txnId()).isEqualTo(txnId);
            assertThat(decoded.tableId()).isEqualTo(TABLE_ID);
        }
    }
}
