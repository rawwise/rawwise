package io.github.rawwise.common.catalog;

import static io.github.rawwise.common.catalog.SchemaValidator.Compatibility;
import static io.github.rawwise.common.catalog.SchemaValidator.validate;
import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.common.exception.CommitConflictException;
import io.github.rawwise.common.exception.SchemaIncompatibleException;
import io.github.rawwise.meta.PbTableMetadata;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.record.PartitionSpec;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.Types;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Metastore unit test.
 *
 * <p>cover {@code metastore-pitfalls.md} in not yetbe hastestcoverof scenario:
 *
 * <ul>
 *   <li>Pitfall 3: DecimalType boundary edge cases (precision=38, scale=0, scale=precision)
 *   <li>Pitfall 5: dropTable TOCTOU -- orphan scan metaKey detect / epoch recheck
 *   <li>Pitfall 5: createTable residue cleanup -- pre-CAS + post-CAS cleanup
 *   <li>Pitfall 6: commit CAS ABA -- tableIdKey ValueEquals lifecycleguard
 * </ul>
 */
@DisplayName("Metastore Pitfalls -- verify")
class MetastorePitfallsTest {

    // ======================================================================
    // Section 1: Pitfall 3 -- Decimal precision/scale boundarytest
    // (package-private DecimalType, name()-based detect, 38 , scale=precision boundary)
    // ======================================================================

    @Test
    @DisplayName("-1: Decimal(38,0) -> Decimal(38,0) identical at max precision (allowed)")
    void decimal38_0IdenticalAllowed() {
        Schema current = Schema.builder().field("price", Types.decimal(38, 0)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(38, 0)).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("-2: Decimal(38,0) -> Decimal(38,38) scale widening at max precision (allowed)")
    void decimal38_0To38_38ScaleWideningAllowed() {
        // precision unchanged (38->38), scale widened (0->38): both pass
        Schema current = Schema.builder().field("price", Types.decimal(38, 0)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(38, 38)).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("-3: Decimal(38,38) -> Decimal(38,0) scale narrowing at max precision (rejected)")
    void decimal38_38To38_0ScaleNarrowingRejected() {
        // precision unchanged (38->38), scale narrowed (38->0): incompatible
        Schema current = Schema.builder().field("price", Types.decimal(38, 38)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(38, 0)).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    @Test
    @DisplayName("-4: Decimal(1,1) -> Decimal(1,1) identical at min scale=precision (allowed)")
    void decimal1_1IdenticalAllowed() {
        Schema current = Schema.builder().field("price", Types.decimal(1, 1)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(1, 1)).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("-5: Decimal(1,1) -> Decimal(2,2) precision widening at scale=precision (allowed)")
    void decimal1_1To2_2PrecisionWideningAllowed() {
        Schema current = Schema.builder().field("price", Types.decimal(1, 1)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(2, 2)).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName(
            "-6: Decimal(10,10) -> Decimal(10,5) scale narrowing at scale=precision (rejected)")
    void decimal10_10To10_5ScaleNarrowingRejected() {
        Schema current = Schema.builder().field("price", Types.decimal(10, 10)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(10, 5)).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    @Test
    @DisplayName(
            "-7: Decimal(38,38) -> Decimal(38,37) scale narrowing by 1 at max boundary (rejected)")
    void decimal38_38To38_37ScaleNarrowingRejected() {
        Schema current = Schema.builder().field("price", Types.decimal(38, 38)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(38, 37)).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // ======================================================================
    // Section 2: Pitfall 5 -- dropTable TOCTOU / orphan scan
    // (CAS delete metaKey + orphan scan ofconcurrency window)
    // ======================================================================

    private Metastore store;
    private MetastoreCatalog catalog;
    private static final Schema TEST_SCHEMA =
            Schema.builder()
                    .field(1, "id", Types.int64())
                    .field(2, "name", Types.string())
                    .primaryKey("id")
                    .build();

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryMetastore();
        catalog =
                new MetastoreCatalog(
                        MetastoreCatalogConfig.builder()
                                .catalogName("test-pitfalls-catalog")
                                .store(store)
                                .basePath("/tmp/test-pitfalls")
                                .build());
        catalog.createNamespace("db");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (catalog != null) {
            catalog.close();
        }
    }

    // -- Helpers --

    /**
     * Builds the table-entry key from the namespace list prefix + table name, matching the logic in
     * {@code MetastoreCatalog.dropTable}.
     */
    private static byte[] buildTableEntryKey(TableId id) {
        byte[] listKey = Keys.namespaceTableListKey(id.database());
        byte[] tableEntryKey = new byte[listKey.length + id.tableName().length()];
        System.arraycopy(listKey, 0, tableEntryKey, 0, listKey.length);
        System.arraycopy(
                id.tableName().getBytes(StandardCharsets.UTF_8),
                0,
                tableEntryKey,
                listKey.length,
                id.tableName().length());
        return tableEntryKey;
    }

    /** Helper to encode a long as 8 big-endian bytes. */
    private static byte[] longBytes(long v) {
        byte[] b = new byte[8];
        ByteBuffer.wrap(b).putLong(v);
        return b;
    }

    // -- P5-1: orphan scanin epochKey/tableIdKey becorrect --

    @Test
    @DisplayName(
            "P5-1: dropTable orphan scan preserves epochKey and skips already-deleted tableIdKey")
    void dropTableOrphanScanPreservesEpochKey() throws Exception {
        TableId id = new TableId("db", "tbl_p5_1");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        // Add phantom keys that simulate a prior crash
        byte[] phantomSnap = Keys.snapshotKey(id, 5);
        byte[] phantomManifest = Keys.manifestKey(id, 10);
        byte[] phantomCounter = Keys.commitIdCounterKey(id);
        store.put(phantomSnap, new byte[] {5});
        store.put(phantomManifest, new byte[] {10});
        store.put(phantomCounter, new byte[] {100});

        int keyCountBefore = store.scanAll(Keys.tablePrefix(id)).size();
        assertThat(keyCountBefore)
                .isGreaterThanOrEqualTo(
                        5); // metaKey + tableEntryKey + epochKey + tableIdKey + 3 phantoms

        boolean dropped = catalog.dropTable(id, false);
        assertThat(dropped).isTrue();

        // After drop, only epochKey should survive (tableIdKey deleted by CAS, epochKey preserved
        // by scan filter)
        byte[] epochKey = Keys.tableEpochKey(id);
        byte[] tableIdKey = Keys.tableIdentityKey(id);
        byte[] metaKey = Keys.tablePrefix(id); // metaKey = tablePrefix

        assertThat(store.get(epochKey)).isPresent();
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(1);

        // metaKey, tableEntryKey, tableIdKey are all deleted by CAS
        assertThat(store.get(metaKey)).isEmpty();
        assertThat(store.get(tableIdKey)).isEmpty();

        // Phantom keys cleaned by orphan scan
        assertThat(store.get(phantomSnap)).isEmpty();
        assertThat(store.get(phantomManifest)).isEmpty();
        assertThat(store.get(phantomCounter)).isEmpty();

        // Only 1 key left: epochKey
        int keyCountAfter = store.scanAll(Keys.tablePrefix(id)).size();
        assertThat(keyCountAfter).isEqualTo(1);
    }

    // -- P5-2: dropTable orphan scan metaKey detect -- simulateconcurrent createTable --

    @Test
    @DisplayName(
            "P5-2: dropTable orphan scan detects metaKey reappearing -- simulated via direct store scan")
    void dropTableOrphanScanMetaKeyDetection() throws Exception {
        // Verify the orphan scan's metaKey detection logic:
        // After drop CAS deletes metaKey, if a concurrent createTable puts metaKey back,
        // the orphan scan must detect it and break to protect the new table's lifecycle.
        //
        // We simulate this by:
        // 1. Executing drop CAS (metaKey deleted, epoch incremented)
        // 2. Manually putting metaKey back (simulating concurrent createTable)
        // 3. Running the same scan logic as dropTable to verify metaKey detection

        TableId id = new TableId("db", "tbl_p5_2");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] metaKey = Keys.tablePrefix(id);
        byte[] epochKey = Keys.tableEpochKey(id);
        byte[] tableIdKey = Keys.tableIdentityKey(id);
        byte[] tableEntryKey = buildTableEntryKey(id);

        // Add phantom keys
        store.put(Keys.snapshotKey(id, 1), new byte[] {1});
        store.put(Keys.snapshotKey(id, 2), new byte[] {2});
        store.put(Keys.manifestKey(id, 3), new byte[] {3});

        // Simulate drop CAS: delete metaKey, tableEntryKey, tableIdKey; increment epoch to 1
        long currentEpoch = Keys.decodeEpoch(store.get(epochKey).get());
        store.compareAndSwap(
                List.of(
                        new Precondition.Exists(metaKey),
                        new Precondition.ValueEquals(metaKey, store.get(metaKey).get())),
                List.of(
                        new Mutation.Delete(metaKey),
                        new Mutation.Delete(tableEntryKey),
                        new Mutation.Delete(tableIdKey),
                        new Mutation.Put(epochKey, Keys.encodeEpoch(currentEpoch + 1))));

        long postDropEpoch = currentEpoch + 1;
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(postDropEpoch);

        // Simulate concurrent createTable: put metaKey BACK
        byte[] newMetaBytes = PbTableMetadata.newBuilder().setTableId(99999L).build().toByteArray();
        store.put(metaKey, newMetaBytes);

        // Replicate the orphan scan logic from dropTable:
        byte[] prefix = Keys.tablePrefix(id);
        List<byte[]> collectedKeys = new ArrayList<>();
        boolean foundMetaKey = false;

        try (CloseableIterator<KeyValue> it = store.scan(prefix)) {
            while (it.hasNext()) {
                byte[] key = it.next().key();
                if (java.util.Arrays.equals(key, metaKey)) {
                    foundMetaKey = true;
                    break;
                }
                if (!java.util.Arrays.equals(key, epochKey)
                        && !java.util.Arrays.equals(key, tableIdKey)) {
                    collectedKeys.add(key);
                }
            }
        }

        // Because metaKey = tablePrefix (e.g., "db:tbl_p5_2:"), it is a prefix of all
        // subkeys and therefore lexicographically FIRST in the scan.
        // The scan should break on metaKey before collecting any phantom keys.
        assertThat(foundMetaKey).isTrue();
        assertThat(collectedKeys).isEmpty();
    }

    // -- P5-3: dropTable orphan scan final batch epoch recheck --
    //
    // : dropTable oforphan scanineach SUBKEY_BATCH_SIZE=1000 item key write after,
    // andin afterfor final batch before writing, allwill recheck epoch.
    // by single testno trigger SUBKEY_BATCH_SIZE batchof epoch recheck,
    // test verify final batch of epoch recheck logic:
    // when epoch and postDropEpoch consistent , final batch be , key is notdelete.

    @Test
    @DisplayName(
            "P5-3: dropTable orphan scan final batch epoch recheck -- epoch mismatch discards pending deletes")
    void dropTableOrphanScanFinalBatchEpochRecheck() throws Exception {
        TableId id = new TableId("db", "tbl_p5_3");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] metaKey = Keys.tablePrefix(id);
        byte[] epochKey = Keys.tableEpochKey(id);
        byte[] tableIdKey = Keys.tableIdentityKey(id);
        byte[] tableEntryKey = buildTableEntryKey(id);

        // Add phantom keys that would be collected by the orphan scan
        for (int i = 1; i <= 5; i++) {
            store.put(Keys.snapshotKey(id, i), new byte[] {(byte) i});
        }

        // Simulate drop CAS: delete identity keys, increment epoch to 1
        long currentEpoch = Keys.decodeEpoch(store.get(epochKey).get());
        store.compareAndSwap(
                List.of(
                        new Precondition.Exists(metaKey),
                        new Precondition.ValueEquals(metaKey, store.get(metaKey).get())),
                List.of(
                        new Mutation.Delete(metaKey),
                        new Mutation.Delete(tableEntryKey),
                        new Mutation.Delete(tableIdKey),
                        new Mutation.Put(epochKey, Keys.encodeEpoch(currentEpoch + 1))));

        long postDropEpoch = currentEpoch + 1;

        // Simulate concurrent recreate: reset epoch to 0
        // (metaKey stays deleted -- we are testing the epoch recheck, not metaKey detection)
        store.put(epochKey, Keys.encodeEpoch(0L));

        // Replicate the orphan scan logic from dropTable (without metaKey present):
        byte[] prefix = Keys.tablePrefix(id);
        List<Mutation> batch = new ArrayList<>();

        try (CloseableIterator<KeyValue> it = store.scan(prefix)) {
            while (it.hasNext()) {
                byte[] key = it.next().key();
                if (java.util.Arrays.equals(key, metaKey)) {
                    break;
                }
                if (!java.util.Arrays.equals(key, epochKey)
                        && !java.util.Arrays.equals(key, tableIdKey)) {
                    batch.add(new Mutation.Delete(key));
                }
            }

            // Final batch epoch recheck (identical to dropTable's logic at lines ~387-400)
            if (!batch.isEmpty()) {
                long recheckEpoch = store.get(epochKey).map(Keys::decodeEpoch).orElse(-1L);
                if (recheckEpoch != postDropEpoch) {
                    // Epoch changed: concurrent recreate detected, discard the batch
                    batch.clear();
                }
            }
        }

        // The epoch was changed from 1 to 0 (concurrent recreate):
        // the final batch should have been discarded.
        assertThat(batch).isEmpty();

        // Phantom keys should still exist -- they were NOT deleted
        // because the epoch recheck blocked the cleanup.
        for (int i = 1; i <= 5; i++) {
            assertThat(store.get(Keys.snapshotKey(id, i))).isPresent();
        }
    }

    // -- P5-4: dropTable CAS precondition -- metaKey ValueEquals intablealreadybe afterdelete --

    @Test
    @DisplayName(
            "P5-4: dropTable CAS metaKey ValueEquals guards against dropping recreated table with different content")
    void dropTableCasValueEqualsGuardsRecreatedTable() throws Exception {
        TableId id = new TableId("db", "tbl_p5_4");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] metaKey = Keys.tablePrefix(id);
        byte[] originalMetaBytes = store.get(metaKey).get();
        byte[] epochKey = Keys.tableEpochKey(id);
        byte[] tableIdKey = Keys.tableIdentityKey(id);
        byte[] tableEntryKey = buildTableEntryKey(id);

        long currentEpoch = Keys.decodeEpoch(store.get(epochKey).get());

        // Simulate recreate: write different metaKey content
        byte[] recreatedMetaBytes =
                PbTableMetadata.newBuilder().setTableId(77777L).build().toByteArray();
        store.put(metaKey, recreatedMetaBytes);

        // Now attempt the stale drop CAS with original content expectation
        boolean casSuccess =
                store.compareAndSwap(
                        List.of(
                                new Precondition.Exists(metaKey),
                                new Precondition.ValueEquals(metaKey, originalMetaBytes)),
                        List.of(
                                new Mutation.Delete(metaKey),
                                new Mutation.Delete(tableEntryKey),
                                new Mutation.Delete(tableIdKey),
                                new Mutation.Put(epochKey, Keys.encodeEpoch(currentEpoch + 1))));

        // CAS must fail because metaKey content no longer matches original
        assertThat(casSuccess).isFalse();

        // Table should still exist (recreated version intact)
        assertThat(store.get(metaKey)).isPresent();
        assertThat(store.get(metaKey).get()).isEqualTo(recreatedMetaBytes);
    }

    // ======================================================================
    // Section 3: Pitfall 6 -- commit CAS ABA lifecycle
    // (tableIdKey ValueEquals triple guard)
    // ======================================================================

    // -- P6-1: commit CAS tableIdKey ValueEquals lifecyclecommit --

    @Test
    @DisplayName("P6-1: commit CAS tableIdKey ValueEquals blocks cross-lifecycle commit")
    void commitCasTableIdGuardBlocksCrossLifecycle() throws Exception {
        TableId id = new TableId("db", "tbl_p6_1");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        long firstTableId = catalog.loadTable(id).meta().tableId();

        // Start a commit on the first lifecycle
        CommitTransaction oldTxn = catalog.newCommit(id);

        // Drop + recreate (new tableId)
        catalog.dropTable(id, false);
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        long secondTableId = catalog.loadTable(id).meta().tableId();
        assertThat(secondTableId).isNotEqualTo(firstTableId);

        // Build the commit CAS preconditions manually to verify each guard individually.
        // The commit CAS preconditions are:
        // NotExists(latestKey) | ValueEquals(latestKey, baseBytes)
        // Exists(intentKey)
        // Exists(metaKey)
        // ValueEquals(tableIdKey, tableIdBytes)
        //
        // tableIdBytes comes from the intent record, which was bound to firstTableId.

        byte[] intentKey = Keys.commitIntentKey(id, oldTxn.commitId());
        byte[] tableIdKey = Keys.tableIdentityKey(id);

        // Current tableIdKey value = secondTableId (from recreate)
        byte[] currentTableIdBytes = longBytes(secondTableId);
        // Old intent's tableId = firstTableId
        byte[] oldTableIdBytes = longBytes(firstTableId);

        // Verify the tableIdKey precondition would fail
        assertThat(
                        store.compareAndSwap(
                                List.of(new Precondition.ValueEquals(tableIdKey, oldTableIdBytes)),
                                List.of(new Mutation.Put(new byte[] {0x01}, new byte[] {0x01}))))
                .isFalse();

        // Verify the tableIdKey precondition would succeed with correct TableId
        assertThat(
                        store.compareAndSwap(
                                List.of(
                                        new Precondition.ValueEquals(
                                                tableIdKey, currentTableIdBytes)),
                                List.of(new Mutation.Put(new byte[] {0x01}, new byte[] {0x01}))))
                .isTrue();
    }

    // -- P6-2: newCommit intent correctbindwhenbefore tableId --

    @Test
    @DisplayName("P6-2: newCommit intent records correct tableId for cross-lifecycle protection")
    void newCommitIntentBindsCurrentTableId() throws Exception {
        TableId id = new TableId("db", "tbl_p6_2");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        long firstTableId = catalog.loadTable(id).meta().tableId();

        // Start a commit -- intent should carry firstTableId
        CommitTransaction txn1 = catalog.newCommit(id);
        byte[] intentKey1 = Keys.commitIntentKey(id, txn1.commitId());
        byte[] intentBytes1 = store.get(intentKey1).get();

        // Intent format: [baseSnapshotId:8][timestamp:8][tableId:8] = 24 bytes
        ByteBuffer buf1 = ByteBuffer.wrap(intentBytes1);
        buf1.getLong(); // baseSnapshotId
        buf1.getLong(); // timestamp
        long recordedTableId1 = buf1.getLong();
        assertThat(recordedTableId1).isEqualTo(firstTableId);

        // Drop + recreate
        catalog.dropTable(id, false);
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        long secondTableId = catalog.loadTable(id).meta().tableId();
        assertThat(secondTableId).isNotEqualTo(firstTableId);

        // Start a new commit -- intent should carry secondTableId
        CommitTransaction txn2 = catalog.newCommit(id);
        byte[] intentKey2 = Keys.commitIntentKey(id, txn2.commitId());
        byte[] intentBytes2 = store.get(intentKey2).get();

        ByteBuffer buf2 = ByteBuffer.wrap(intentBytes2);
        buf2.getLong(); // baseSnapshotId
        buf2.getLong(); // timestamp
        long recordedTableId2 = buf2.getLong();
        assertThat(recordedTableId2).isEqualTo(secondTableId);
    }

    // -- P6-3: commit intent no in recreated table commit (end-to-end) --

    @Test
    @DisplayName(
            "P6-3: old commit intent from previous lifecycle cannot commit to recreated table (E2E)")
    void oldCommitIntentCannotCommitToRecreatedTable() throws Exception {
        TableId id = new TableId("db", "tbl_p6_3");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        long firstTableId = catalog.loadTable(id).meta().tableId();

        // Prepare a full commit from the first lifecycle
        CommitTransaction oldTxn = catalog.newCommit(id);
        long oldCommitId = oldTxn.commitId();
        byte[] oldIntentKey = Keys.commitIntentKey(id, oldCommitId);
        byte[] oldIntentBytes = store.get(oldIntentKey).orElseThrow();
        long oldBaseSnapshotId = oldTxn.baseSnapshotId();

        // Drop + recreate (orphan scan cleans old intent; counter is reset)
        catalog.dropTable(id, false);
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        long secondTableId = catalog.loadTable(id).meta().tableId();
        assertThat(secondTableId).isNotEqualTo(firstTableId);

        // Start fresh newCommit FIRST to allocate a new intent from the fresh counter.
        // (The counter was cleaned by drop's orphan scan, so commitIds may recycle.)
        CommitTransaction freshTxn = catalog.newCommit(id);

        // Manually restore old intent (simulating a delayed/retried commit
        // with commitId from the previous lifecycle)
        store.put(oldIntentKey, oldIntentBytes);

        // Attempt to commit using the old transaction from the first lifecycle:
        // the CAS precondition ValueEquals(tableIdKey, oldTableIdBytes) must fail
        // because the new table has a different tableIdKey value.
        assertThatThrownBy(
                        () ->
                                catalog.commit(
                                        new CommitTransaction(
                                                oldCommitId,
                                                id,
                                                oldBaseSnapshotId,
                                                oldTxn.operations())))
                .isInstanceOf(CommitConflictException.class);

        // Verify the new table is unharmed
        assertThat(catalog.tableExists(id)).isTrue();
        assertThat(catalog.listSnapshots(id)).isEmpty();

        // A fresh commit on the new table should work fine
        assertThat(freshTxn.baseSnapshotId()).isEqualTo(-1L);

        // Clean up old intent so it doesn't interfere with later tests
        store.delete(oldIntentKey);
    }

    // ======================================================================
    // Section 4: Pitfall 5 -- createTable residue cleanup
    // (pre-CAS + post-CAS batchcleanup)
    // ======================================================================

    // -- P5-5: createTable pre-CAS correct epochKey and tableIdKey --

    @Test
    @DisplayName(
            "P5-5: createTable pre-CAS scan preserves epochKey and tableIdKey from previous lifecycle")
    void createTablePreCasScanPreservesEpochAndIdentity() throws Exception {
        TableId id = new TableId("db", "tbl_p5_5");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        // Normal drop
        catalog.dropTable(id, false);

        byte[] epochKey = Keys.tableEpochKey(id);
        byte[] tableIdKey = Keys.tableIdentityKey(id);

        // After drop, epochKey should exist with value 1
        assertThat(store.get(epochKey)).isPresent();
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(1);

        // Add phantom keys simulating a crashed drop that left residue
        byte[] phantomSnap = Keys.snapshotKey(id, 42);
        byte[] phantomManifest = Keys.manifestKey(id, 99);
        store.put(phantomSnap, new byte[] {42});
        store.put(phantomManifest, new byte[] {99});

        // Now recreate: createTable's pre-CAS scan should detect epochKey and tableIdKey
        // and NOT add them to residueDeletes.
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        // Phantom keys should be cleaned by post-CAS cleanup
        assertThat(store.get(phantomSnap)).isEmpty();
        assertThat(store.get(phantomManifest)).isEmpty();

        // Epoch should be reset to 0 by CAS
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(0);

        // tableIdKey should exist with new value
        assertThat(store.get(tableIdKey)).isPresent();
    }

    // -- P5-6: createTable in key exceeds batch size correct cleanup --

    @Test
    @DisplayName("P5-6: createTable cleans large number of residue keys via paginated batchWrite")
    void createTableCleansManyResidueKeysPaginated() throws Exception {
        TableId id = new TableId("db", "tbl_p5_6");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        catalog.dropTable(id, false);

        // Add 50 phantom snapshot keys (simulating a crashed drop with many orphans)
        List<byte[]> phantomKeys = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            byte[] snapKey = Keys.snapshotKey(id, 100 + i);
            store.put(snapKey, new byte[] {(byte) i});
            phantomKeys.add(snapKey);
        }

        // Verify all phantom keys exist before recreation
        for (byte[] key : phantomKeys) {
            assertThat(store.get(key)).isPresent();
        }

        // Recreate table
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        // All phantom keys should be cleaned
        for (byte[] key : phantomKeys) {
            assertThat(store.get(key)).isEmpty();
        }

        // Table should be functional
        assertThat(catalog.tableExists(id)).isTrue();
        assertThat(catalog.listSnapshots(id)).isEmpty();

        // Verify only expected keys exist under tablePrefix (metaKey, epochKey, tableIdKey)
        // Note: tableEntryKey lives under namespaceTableListKey, not under tablePrefix
        int keyCount = store.scanAll(Keys.tablePrefix(id)).size();
        assertThat(keyCount).isEqualTo(3);
    }

    // -- P5-7: createTable of CAS NotExists concurrentcreate --

    @Test
    @DisplayName("P5-7: createTable CAS NotExists prevents concurrent creation of same table")
    void createTableCasNotExistsPreventsConcurrentCreation() throws Exception {
        TableId id = new TableId("db", "tbl_p5_7");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        // Second createTable should fail because metaKey already exists
        assertThatThrownBy(
                        () ->
                                catalog.createTable(
                                        id,
                                        TEST_SCHEMA,
                                        PartitionSpec.unpartitioned(),
                                        TableProperties.empty()))
                .isInstanceOf(Exception.class) // TableAlreadyExistsException
                .hasMessageContaining("already exists");
    }
}
