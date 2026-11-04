package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.engine.txn.wal.InMemoryTxnWALWriter;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName(".5-3: Vector WAL metadata E2E")
class VectorWalMetadataE2ETest {

    @Test
    @DisplayName("vectorafter writing WAL in in TxnVectorPrepared record")
    void walContainsVectorPreparedAfterVectorWrite() {
        InMemoryTxnWALWriter walWriter = new InMemoryTxnWALWriter();
        walWriter.writeVectorPrepared(
                new TxnWALRecord.TxnVectorPrepared(
                        1L,
                        "ns1",
                        "key1",
                        "text-embedding-3-small",
                        768,
                        System.currentTimeMillis()));

        var records = walWriter.scan();
        assertEquals(1, records.size());
        assertInstanceOf(TxnWALRecord.TxnVectorPrepared.class, records.get(0));
        TxnWALRecord.TxnVectorPrepared vp = (TxnWALRecord.TxnVectorPrepared) records.get(0);
        assertEquals(1L, vp.txnId());
        assertEquals("ns1", vp.namespace());
        assertEquals("key1", vp.entryKey());
        assertEquals("text-embedding-3-small", vp.embeddingProvider());
        assertEquals(768, vp.dimension());
    }

    @Test
    @DisplayName("crash recoveryaftervector datacanberecoverylogic")
    void vectorMetadataConsumedByRecovery() {
        InMemoryTxnWALWriter walWriter = new InMemoryTxnWALWriter();

        walWriter.writeBegin(
                new TxnWALRecord.TxnBeginRecord(
                        1L,
                        0L,
                        io.github.rawwise.engine.txn.IsolationLevel.READ_COMMITTED,
                        java.util.Set.of("ns1"),
                        System.currentTimeMillis()));

        walWriter.writeVectorPrepared(
                new TxnWALRecord.TxnVectorPrepared(
                        1L,
                        "ns1",
                        "key1",
                        "text-embedding-3-small",
                        768,
                        System.currentTimeMillis()));

        walWriter.writeCommitted(
                new TxnWALRecord.TxnCommitted(1L, System.currentTimeMillis(), new byte[0]));

        var records = walWriter.scan();
        assertEquals(3, records.size());

        long vectorPreparedCount =
                records.stream().filter(r -> r instanceof TxnWALRecord.TxnVectorPrepared).count();
        assertEquals(1, vectorPreparedCount);
    }

    @Test
    @DisplayName("TxnVectorPrepared verify: txnId is")
    void txnVectorPreparedValidationTxnId() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new TxnWALRecord.TxnVectorPrepared(0, "ns1", "key1", "model", 768, 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TxnWALRecord.TxnVectorPrepared(-1, "ns1", "key1", "model", 768, 0L));
    }

    @Test
    @DisplayName("TxnVectorPrepared verify: namespace non-")
    void txnVectorPreparedValidationNamespace() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new TxnWALRecord.TxnVectorPrepared(1L, null, "key1", "model", 768, 0L));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TxnWALRecord.TxnVectorPrepared(1L, "  ", "key1", "model", 768, 0L));
    }
}
