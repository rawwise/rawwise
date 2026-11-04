package io.github.rawwise.engine.txn.wal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnAbort;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnBeginRecord;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnCommitted;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnPrepareAndDurable;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnWriteIntent;
import io.github.rawwise.engine.txn.wal.TxnWALRecord.WriteType;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("TxnWALReader recovery E2E test")
class TxnWALReaderRecoveryTest {

    private static final String TABLE_ID = "test_db.test_table";
    private static final Set<String> TABLE_IDS = Set.of(TABLE_ID);

    private InMemoryTxnWALWriter walWriter;
    private ScanBackedTxnWALReader reader;

    @BeforeEach
    void setUp() {
        walWriter = new InMemoryTxnWALWriter();
        reader = new ScanBackedTxnWALReader(walWriter);
    }

    private TxnBeginRecord beginRecord(long txnId, long readSnapshotTs) {
        return new TxnBeginRecord(
                txnId,
                readSnapshotTs,
                IsolationLevel.READ_COMMITTED,
                TABLE_IDS,
                System.currentTimeMillis());
    }

    private TxnWriteIntent writeIntent(long txnId, String key, String value) {
        return new TxnWriteIntent(
                txnId,
                TABLE_IDS,
                TABLE_ID,
                0,
                100L,
                IsolationLevel.READ_COMMITTED,
                0,
                key.getBytes(),
                value.getBytes(),
                WriteType.UPSERT,
                0,
                0,
                0L);
    }

    private TxnPrepareAndDurable prepareRecord(long txnId) {
        return new TxnPrepareAndDurable(
                txnId, null, List.of(), new byte[] {0x01}, System.currentTimeMillis());
    }

    private TxnCommitted commitRecord(long txnId, long commitTs) {
        return new TxnCommitted(txnId, commitTs, null);
    }

    private TxnAbort abortRecord(long txnId, String reason) {
        return new TxnAbort(txnId, reason, System.currentTimeMillis());
    }

    private void writeFullTxn(long txnId, long commitTs, String key, String value) {
        walWriter.writeBegin(beginRecord(txnId, 100L));
        walWriter.writeWriteIntent(writeIntent(txnId, key, value));
        walWriter.writePrepareAndDurable(prepareRecord(txnId));
        walWriter.writeCommitted(commitRecord(txnId, commitTs));
    }

    @Nested
    @DisplayName("S1: Happy Path")
    class HappyPath {

        @Test
        @DisplayName("writemultiitem WAL recordafter readAll record")
        void testWalWriteAndReadAll() {
            writeFullTxn(1L, 1000L, "key1", "value1");
            walWriter.writeBegin(beginRecord(2L, 200L));
            walWriter.writeWriteIntent(writeIntent(2L, "key2", "value2"));

            List<Object> records = reader.readAll();

            assertThat(records).hasSize(6);
            assertThat(records.get(0)).isInstanceOf(TxnBeginRecord.class);
            assertThat(records.get(1)).isInstanceOf(TxnWriteIntent.class);
            assertThat(records.get(2)).isInstanceOf(TxnPrepareAndDurable.class);
            assertThat(records.get(3)).isInstanceOf(TxnCommitted.class);
            assertThat(records.get(4)).isInstanceOf(TxnBeginRecord.class);
            assertThat(records.get(5)).isInstanceOf(TxnWriteIntent.class);

            TxnBeginRecord firstBegin = (TxnBeginRecord) records.get(0);
            assertThat(firstBegin.txnId()).isEqualTo(1L);
            TxnCommitted commit = (TxnCommitted) records.get(3);
            assertThat(commit.txnId()).isEqualTo(1L);
            assertThat(commit.commitTs()).isEqualTo(1000L);
        }

        @Test
        @DisplayName("readByTxnId txnId ofrecord")
        void testWalReadByTxnId() {
            writeFullTxn(1L, 1000L, "key1", "value1");
            writeFullTxn(2L, 2000L, "key2", "value2");
            writeFullTxn(3L, 3000L, "key3", "value3");

            List<Object> txn2Records = reader.readByTxnId(2L);

            assertThat(txn2Records).hasSize(4);
            assertThat(txn2Records)
                    .allSatisfy(r -> assertThat(TxnWALReader.extractTxnId(r)).isEqualTo(2L));
            assertThat(txn2Records.get(0)).isInstanceOf(TxnBeginRecord.class);
            assertThat(txn2Records.get(1)).isInstanceOf(TxnWriteIntent.class);
            assertThat(txn2Records.get(2)).isInstanceOf(TxnPrepareAndDurable.class);
            assertThat(txn2Records.get(3)).isInstanceOf(TxnCommitted.class);

            List<Object> txn1Records = reader.readByTxnId(1L);
            assertThat(txn1Records).hasSize(4);

            List<Object> txn3Records = reader.readByTxnId(3L);
            assertThat(txn3Records).hasSize(4);
        }
    }

    @Nested
    @DisplayName("S3: Fault Recovery")
    class FaultRecovery {

        @Test
        @DisplayName("readUncommitted no COMMIT ofnot yetcompletetransaction")
        void testWalReadUncommittedIdentifiesIncompleteTxn() {
            walWriter.writeBegin(beginRecord(10L, 100L));
            walWriter.writeWriteIntent(writeIntent(10L, "key10", "value10"));
            walWriter.writePrepareAndDurable(prepareRecord(10L));

            writeFullTxn(11L, 1100L, "key11", "value11");

            List<Object> uncommitted = reader.readUncommitted();

            Set<Long> uncommittedTxnIds =
                    uncommitted.stream()
                            .map(TxnWALReader::extractTxnId)
                            .collect(Collectors.toSet());

            assertThat(uncommittedTxnIds).as("未提交事务 txnId=10 应在未提交集合中").contains(10L);
            assertThat(uncommittedTxnIds).as("已提交事务 txnId=11 不应在未提交集合中").doesNotContain(11L);

            assertThat(uncommitted)
                    .as("未提交记录应全部属于 txnId=10")
                    .allSatisfy(r -> assertThat(TxnWALReader.extractTxnId(r)).isEqualTo(10L));
            assertThat(uncommitted).hasSize(3);
        }

        @Test
        @DisplayName("readUncommitted foralreadycommittransactionreturns empty")
        void testWalReadCommittedTxn() {
            writeFullTxn(20L, 2000L, "key20", "value20");
            writeFullTxn(21L, 2100L, "key21", "value21");

            List<Object> uncommitted = reader.readUncommitted();

            assertThat(uncommitted).as("所有事务均已提交，未提交列表应为空").isEmpty();
        }

        @Test
        @DisplayName("readUncommitted foralready ABORT oftransaction isnot yetcommit")
        void testWalReadAbortedTxnNotInUncommitted() {
            walWriter.writeBegin(beginRecord(30L, 100L));
            walWriter.writeWriteIntent(writeIntent(30L, "key30", "value30"));
            walWriter.writeAbort(abortRecord(30L, "user rollback"));

            writeFullTxn(31L, 3100L, "key31", "value31");

            List<Object> uncommitted = reader.readUncommitted();

            assertThat(uncommitted).as("已 ABORT 的事务不应出现在未提交列表中").isEmpty();
        }
    }

    @Nested
    @DisplayName("S4: Boundary")
    class Boundary {

        @Test
        @DisplayName("WAL of readAll returns empty list")
        void testWalReadEmpty() {
            List<Object> records = reader.readAll();

            assertThat(records).isEmpty();
            assertThat(reader.readByTxnId(1L)).isEmpty();
            assertThat(reader.readUncommitted()).isEmpty();
            assertThat(reader.readLatest(1L)).isEmpty();
        }

        @Test
        @DisplayName("readLatest txnId of item WAL record")
        void testWalReadLatestTxnId() {
            walWriter.writeBegin(beginRecord(40L, 100L));
            walWriter.writeWriteIntent(writeIntent(40L, "key40", "value40"));
            walWriter.writePrepareAndDurable(prepareRecord(40L));
            walWriter.writeCommitted(commitRecord(40L, 4000L));

            Optional<Object> latest = reader.readLatest(40L);

            assertThat(latest).isPresent();
            assertThat(latest.get()).as("最新记录应为 TxnCommitted").isInstanceOf(TxnCommitted.class);
            TxnCommitted committed = (TxnCommitted) latest.get();
            assertThat(committed.txnId()).isEqualTo(40L);
            assertThat(committed.commitTs()).isEqualTo(4000L);

            Optional<Object> latestForMissing = reader.readLatest(999L);
            assertThat(latestForMissing).as("不存在的事务 readLatest 应返回 empty").isEmpty();
        }
    }
}
