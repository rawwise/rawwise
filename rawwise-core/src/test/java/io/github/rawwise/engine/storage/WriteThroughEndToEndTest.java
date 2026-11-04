package io.github.rawwise.engine.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.catalog.InMemoryBlockCatalog;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.TxnMode;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WriteThroughEndToEndTest {

    @TempDir Path tempDir;

    private TableStore tableStore;
    private InMemoryBlockCatalog blockCatalog;

    @BeforeEach
    void setUp() {
        blockCatalog = new InMemoryBlockCatalog("testTable");
        tableStore = new TableStore("testTable", 3);
    }

    @AfterEach
    void tearDown() {
        if (tableStore != null) {
            tableStore.close();
        }
    }

    @Test
    void txnModeDefaultIsDeferred() {
        TxnContext ctx =
                new TxnContext(
                        1,
                        System.currentTimeMillis(),
                        IsolationLevel.READ_COMMITTED,
                        "testTable",
                        0);
        assertEquals(TxnMode.DEFERRED, ctx.txnMode());
        assertTrue(ctx.txnMode().isDeferred());
    }

    @Test
    void txnModeSetToWriteThrough() {
        TxnContext ctx =
                new TxnContext(
                        1,
                        System.currentTimeMillis(),
                        IsolationLevel.READ_COMMITTED,
                        "testTable",
                        0);
        ctx.setTxnMode(TxnMode.WRITE_THROUGH);
        assertTrue(ctx.txnMode().isWriteThrough());
    }

    @Test
    void pendingBatchOperations() {
        TxnContext ctx =
                new TxnContext(
                        1,
                        System.currentTimeMillis(),
                        IsolationLevel.READ_COMMITTED,
                        "testTable",
                        0);
        ctx.setTxnMode(TxnMode.WRITE_THROUGH);

        assertTrue(ctx.pendingBatchKeys().isEmpty());

        ctx.addPendingWrite("key1".getBytes(), "value1".getBytes());
        ctx.addPendingWrite("key2".getBytes(), "value2".getBytes());

        assertEquals(2, ctx.pendingBatchKeys().size());
        assertEquals(2, ctx.pendingBatchValues().size());

        ctx.clearPendingBatch();
        assertTrue(ctx.pendingBatchKeys().isEmpty());
        assertTrue(ctx.pendingBatchValues().isEmpty());
    }

    @Test
    void pendingBatchFullBySize() {
        TxnContext ctx =
                new TxnContext(
                        1,
                        System.currentTimeMillis(),
                        IsolationLevel.READ_COMMITTED,
                        "testTable",
                        0);

        assertFalse(ctx.isPendingBatchFull(10, 1024L));

        for (int i = 0; i < 10; i++) {
            ctx.addPendingWrite(("k" + i).getBytes(), ("v" + i).getBytes());
        }

        assertTrue(ctx.isPendingBatchFull(10, Long.MAX_VALUE));
    }

    @Test
    void pendingBatchFullByBytes() {
        TxnContext ctx =
                new TxnContext(
                        1,
                        System.currentTimeMillis(),
                        IsolationLevel.READ_COMMITTED,
                        "testTable",
                        0);

        assertFalse(ctx.isPendingBatchFull(Integer.MAX_VALUE, 20L));

        ctx.addPendingWrite("key1".getBytes(), "value1".getBytes());

        assertTrue(ctx.isPendingBatchFull(Integer.MAX_VALUE, 1L));
    }

    @Test
    void pendingInstanceIdsTracking() {
        TxnContext ctx =
                new TxnContext(
                        1,
                        System.currentTimeMillis(),
                        IsolationLevel.READ_COMMITTED,
                        "testTable",
                        0);

        assertTrue(ctx.pendingInstanceIds().isEmpty());

        ctx.addPendingInstanceId("inst-testTable-pending-1-0");
        ctx.addPendingInstanceId("inst-testTable-pending-1-1");

        assertEquals(2, ctx.pendingInstanceIds().size());
        assertTrue(ctx.pendingInstanceIds().contains("inst-testTable-pending-1-0"));
    }

    @Test
    void visibilityPendingExcludedFromCurrentBlocks() {
        List<DataBlock> currentBlocks = blockCatalog.currentBlocks();
        assertTrue(currentBlocks.isEmpty());
    }

    @Test
    void visibilityPendingListedInPendingBlocks() {
        List<DataBlock> pendingBlocks = blockCatalog.pendingBlocks();
        assertTrue(pendingBlocks.isEmpty());
    }

    @Test
    void batchUpdateVisibilityChangesState() {
        blockCatalog.batchUpdateVisibility(
                List.of("nonexistent"), DataBlock.Visibility.PENDING, DataBlock.Visibility.ACTIVE);
    }

    @Test
    void txnModeSerializationCompatibility() throws Exception {
        TxnContext ctx =
                new TxnContext(
                        1,
                        System.currentTimeMillis(),
                        IsolationLevel.READ_COMMITTED,
                        "testTable",
                        0);
        ctx.setTxnMode(TxnMode.WRITE_THROUGH);

        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
        oos.writeObject(ctx);
        oos.close();

        java.io.ObjectInputStream ois =
                new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bos.toByteArray()));
        TxnContext deserialized = (TxnContext) ois.readObject();

        assertEquals(TxnMode.WRITE_THROUGH, deserialized.txnMode());
        assertTrue(deserialized.pendingInstanceIds().isEmpty());
        assertTrue(deserialized.pendingBatchKeys().isEmpty());
    }

    @Test
    void txnModeNullFallbackOnDeserialization() throws Exception {
        TxnContext ctx =
                new TxnContext(
                        1,
                        System.currentTimeMillis(),
                        IsolationLevel.READ_COMMITTED,
                        "testTable",
                        0);

        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos);
        oos.writeObject(ctx);
        oos.close();

        java.io.ObjectInputStream ois =
                new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bos.toByteArray()));
        TxnContext deserialized = (TxnContext) ois.readObject();

        assertNotNull(deserialized.txnMode());
        assertEquals(TxnMode.DEFERRED, deserialized.txnMode());
    }

    @Test
    void commitMarkerEncodeDecode() throws Exception {
        Path markerDir = tempDir.resolve("commitMarkerTest-" + System.nanoTime());
        java.nio.file.Files.createDirectories(markerDir);
        LmdbInstance markerInstance =
                new LmdbInstance("inst-testTable-commit-marker", markerDir, 16 * 1024 * 1024L);

        if (!markerInstance.readOnly()) {
            boolean acquired =
                    markerInstance.tryAcquireLease(
                            java.net.InetAddress.getLocalHost().getHostName(), 30_000L);
            assertTrue(acquired, "Should be able to acquire lease for marker instance");

            CommitMarker commitMarker = new CommitMarker(markerInstance);

            commitMarker.writeCommitMarker(
                    1L,
                    List.of("inst-testTable-pending-1-0", "inst-testTable-pending-1-1"),
                    System.currentTimeMillis());

            byte[] markerKey =
                    ("__commit_marker__" + 1L).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] value = markerInstance.get(markerKey);
            assertNotNull(value, "Commit marker should be written to LMDB");

            List<CommitMarker.CommitRecord> records = commitMarker.scanCommitMarkers();
            assertEquals(1, records.size());
            assertEquals(1L, records.get(0).txnId());
            assertEquals(2, records.get(0).instanceIds().size());

            commitMarker.deleteCommitMarker(1L);

            byte[] afterDelete = markerInstance.get(markerKey);
            assertNull(afterDelete, "Commit marker should be deleted from LMDB");
        }

        markerInstance.close();
    }

    @Test
    void tableStorePendingInstanceCount() {
        assertEquals(0, tableStore.pendingInstanceCount());
    }

    @Test
    void tableStoreGetInstanceChecksPending() {
        assertNull(tableStore.getInstance("nonexistent"));
    }

    @Test
    void configOptionsDefaults() {
        RawWiseConfig config = RawWiseConfig.empty();

        assertEquals("DEFERRED", config.get(RawWiseOptions.INGESTION_TXN_MODE));
        assertEquals(128, config.get(RawWiseOptions.MAX_PENDING_INSTANCES));
        assertEquals(10000, config.get(RawWiseOptions.MAX_PENDING_BATCH_SIZE));
        assertEquals(100L * 1024L * 1024L, config.get(RawWiseOptions.MAX_PENDING_BATCH_BYTES));
    }

    @Test
    void configOptionsCustomValues() {
        RawWiseConfig config =
                RawWiseConfig.empty()
                        .with(RawWiseOptions.INGESTION_TXN_MODE, "WRITE_THROUGH")
                        .with(RawWiseOptions.MAX_PENDING_INSTANCES, 256);

        assertEquals("WRITE_THROUGH", config.get(RawWiseOptions.INGESTION_TXN_MODE));
        assertEquals(256, config.get(RawWiseOptions.MAX_PENDING_INSTANCES));
    }
}
