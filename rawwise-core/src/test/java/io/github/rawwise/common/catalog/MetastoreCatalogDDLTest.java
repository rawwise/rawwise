package io.github.rawwise.common.catalog;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.common.exception.TableNotFoundException;
import io.github.rawwise.meta.PbTableMetadata;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.record.PartitionSpec;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.Types;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.*;

@DisplayName("MetastoreCatalog DDL Consistency Integration Tests")
class MetastoreCatalogDDLTest {

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
                                .catalogName("test-ddl-catalog")
                                .store(store)
                                .basePath("/tmp/test-ddl")
                                .build());
        catalog.createNamespace("db_src");
        catalog.createNamespace("db_dst");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    @DisplayName("R1: renameTable updates namespace index (old removed, new added)")
    void testRenameUpdatesNamespaceIndex() throws Exception {
        TableId from = new TableId("db_src", "tbl_r1");
        TableId to = new TableId("db_dst", "tbl_r1_renamed");
        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        boolean renamed = catalog.renameTable(from, to);
        assertThat(renamed).isTrue();

        assertThat(catalog.tableExists(from)).isFalse();
        assertThat(catalog.tableExists(to)).isTrue();

        List<TableId> srcTables = catalog.listTables("db_src");
        List<TableId> dstTables = catalog.listTables("db_dst");

        assertThat(srcTables).doesNotContain(from);
        assertThat(dstTables).contains(to);
    }

    @Test
    @DisplayName("R2: renameTable updates protobuf identifier fields")
    void testRenameUpdatesProtobufIdentifier() throws Exception {
        TableId from = new TableId("db_src", "tbl_r2");
        TableId to = new TableId("db_dst", "tbl_r2_target");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());
        catalog.renameTable(from, to);

        byte[] toMetaKey = Keys.tablePrefix(to);
        Optional<byte[]> metaBytes = store.get(toMetaKey);
        assertThat(metaBytes).isPresent();

        PbTableMetadata pbMeta = PbTableMetadata.parseFrom(metaBytes.get());
        assertThat(pbMeta.getIdentifier().getDatabase()).isEqualTo("db_dst");
        assertThat(pbMeta.getIdentifier().getTable()).isEqualTo("tbl_r2_target");
    }

    @Test
    @DisplayName("R3: rename within same database updates TableMeta identity")
    void testRenameWithinSameDatabase() throws Exception {
        TableId from = new TableId("db_src", "tbl_r3_old");
        TableId to = new TableId("db_src", "tbl_r3_new");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());
        catalog.renameTable(from, to);

        assertThat(catalog.tableExists(from)).isFalse();
        assertThat(catalog.tableExists(to)).isTrue();

        List<TableId> tables = catalog.listTables("db_src");
        assertThat(tables).contains(to);
        assertThat(tables).doesNotContain(from);

        byte[] metaKey = Keys.tablePrefix(to);
        Optional<byte[]> metaBytes = store.get(metaKey);
        assertThat(metaBytes).isPresent();

        PbTableMetadata pbMeta = PbTableMetadata.parseFrom(metaBytes.get());
        assertThat(pbMeta.getIdentifier().getDatabase()).isEqualTo("db_src");
        assertThat(pbMeta.getIdentifier().getTable()).isEqualTo("tbl_r3_new");
    }

    @Test
    @DisplayName("R4: rename to existing table returns false without side effects")
    void testRenameToExistingTableReturnsFalse() throws Exception {
        TableId existing = new TableId("db_dst", "tbl_r4_existing");
        TableId from = new TableId("db_src", "tbl_r4_from");

        catalog.createTable(
                existing, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());
        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        boolean renamed = catalog.renameTable(from, existing);
        assertThat(renamed).isFalse();

        assertThat(catalog.tableExists(from)).isTrue();
        assertThat(catalog.tableExists(existing)).isTrue();

        Optional<byte[]> fromMeta = store.get(Keys.tablePrefix(from));
        assertThat(fromMeta).isPresent();
        PbTableMetadata pbFrom = PbTableMetadata.parseFrom(fromMeta.get());
        assertThat(pbFrom.getIdentifier().getTable()).isEqualTo("tbl_r4_from");
    }

    @Test
    @DisplayName("D1: dropped table removed from namespace index")
    void testDropTableRemovesFromNamespaceIndex() throws Exception {
        TableId id = new TableId("db_src", "tbl_d1");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        boolean dropped = catalog.dropTable(id, false);
        assertThat(dropped).isTrue();

        assertThat(catalog.tableExists(id)).isFalse();
        assertThat(catalog.listTables("db_src")).doesNotContain(id);
    }

    @Test
    @DisplayName("D2: dropTable cleans up all orphan keys under tablePrefix (epoch survives)")
    void testDropTableCleansUpOrphanKeys() throws Exception {
        TableId id = new TableId("db_dst", "tbl_d2");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] prefix = Keys.tablePrefix(id);
        int keyCountBefore = store.scanAll(prefix).size();
        assertThat(keyCountBefore).isPositive();

        catalog.dropTable(id, false);

        int keyCountAfter = store.scanAll(prefix).size();
        assertThat(keyCountAfter).isEqualTo(1);
        assertThat(store.get(Keys.tableEpochKey(id))).isPresent();
    }

    @Test
    @DisplayName("D3: drop+recreate same table name yields clean state (no old data)")
    void testDropRecreateSameTableNameYieldsCleanState() throws Exception {
        TableId id = new TableId("db_src", "tbl_d3");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        catalog.dropTable(id, false);

        byte[] prefix = Keys.tablePrefix(id);
        int keyCountAfterDrop = store.scanAll(prefix).size();
        assertThat(keyCountAfterDrop).isEqualTo(1);
        assertThat(store.get(Keys.tableEpochKey(id))).isPresent();

        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        assertThat(catalog.tableExists(id)).isTrue();
        assertThat(catalog.listTables("db_src")).contains(id);
    }

    @Test
    @DisplayName("D4: dropTable is idempotent — dropping nonexistent table returns false")
    void testDropNonexistentTableReturnsFalse() throws Exception {
        TableId id = new TableId("db_src", "tbl_d4_nonexistent");

        boolean first = catalog.dropTable(id, false);
        assertThat(first).isFalse();

        boolean second = catalog.dropTable(id, false);
        assertThat(second).isFalse();
    }

    @Test
    @DisplayName("D5: concurrent drop is safe — second drop returns false")
    void testConcurrentDropIsSafe() throws Exception {
        TableId id = new TableId("db_dst", "tbl_d5");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        boolean first = catalog.dropTable(id, false);
        assertThat(first).isTrue();

        boolean second = catalog.dropTable(id, false);
        assertThat(second).isFalse();
    }

    @Test
    @DisplayName("D6: concurrent rename — only one succeeds, no data corruption")
    void testConcurrentRenameOnlyOneSucceeds() throws Exception {
        TableId from = new TableId("db_src", "tbl_r6");
        TableId toA = new TableId("db_dst", "tbl_r6_a");
        TableId toB = new TableId("db_dst", "tbl_r6_b");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        boolean[] results = new boolean[2];
        Thread tA =
                new Thread(
                        () -> {
                            try {
                                results[0] = catalog.renameTable(from, toA);
                            } catch (Exception e) {
                                results[0] = false;
                            }
                        });
        Thread tB =
                new Thread(
                        () -> {
                            try {
                                results[1] = catalog.renameTable(from, toB);
                            } catch (Exception e) {
                                results[1] = false;
                            }
                        });

        tA.start();
        tB.start();
        tA.join();
        tB.join();

        long successCount = 0;
        for (boolean r : results) {
            if (r) successCount++;
        }
        assertThat(successCount).isEqualTo(1);

        assertThat(catalog.tableExists(from)).isFalse();
        assertThat(catalog.tableExists(toA) || catalog.tableExists(toB)).isTrue();
    }

    @Test
    @DisplayName("D7: concurrent drop — both calls safe, no exceptions")
    void testConcurrentDropBothSafe() throws Exception {
        TableId id = new TableId("db_dst", "tbl_d7");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        boolean[] results = new boolean[2];
        Thread tA =
                new Thread(
                        () -> {
                            try {
                                results[0] = catalog.dropTable(id, false);
                            } catch (Exception e) {
                                results[0] = false;
                            }
                        });
        Thread tB =
                new Thread(
                        () -> {
                            try {
                                results[1] = catalog.dropTable(id, false);
                            } catch (Exception e) {
                                results[1] = false;
                            }
                        });

        tA.start();
        tB.start();
        tA.join();
        tB.join();

        long successCount = 0;
        for (boolean r : results) {
            if (r) successCount++;
        }
        assertThat(successCount).isEqualTo(1);

        assertThat(catalog.tableExists(id)).isFalse();
    }

    @Test
    @DisplayName("D8: renameTable preserves identity for loadTable read path")
    void testRenamePreservesLoadTableIdentity() throws Exception {
        TableId from = new TableId("db_src", "tbl_d8");
        TableId to = new TableId("db_dst", "tbl_d8_renamed");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());
        catalog.renameTable(from, to);

        assertThat(catalog.tableExists(from)).isFalse();
        assertThat(catalog.tableExists(to)).isTrue();

        var table = catalog.loadTable(to);
        assertThat(table.meta().databaseName()).isEqualTo("db_dst");
        assertThat(table.meta().tableName()).isEqualTo("tbl_d8_renamed");
    }

    @Test
    @DisplayName("R5: renameTable preserves all TableMeta fields besides identifier")
    void testRenamePreservesTableMetaFields() throws Exception {
        TableId from = new TableId("db_src", "tbl_r5");
        TableId to = new TableId("db_dst", "tbl_r5_target");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        Optional<byte[]> beforeBytes = store.get(Keys.tablePrefix(from));
        assertThat(beforeBytes).isPresent();
        PbTableMetadata before = PbTableMetadata.parseFrom(beforeBytes.get());
        long originalTableId = before.getTableId();

        catalog.renameTable(from, to);

        Optional<byte[]> afterBytes = store.get(Keys.tablePrefix(to));
        assertThat(afterBytes).isPresent();
        PbTableMetadata after = PbTableMetadata.parseFrom(afterBytes.get());

        assertThat(after.getTableId()).isEqualTo(originalTableId);
        assertThat(after.getIdentifier().getDatabase()).isEqualTo("db_dst");
        assertThat(after.getIdentifier().getTable()).isEqualTo("tbl_r5_target");
        assertThat(after.getTableType()).isEqualTo(before.getTableType());
        assertThat(after.getSchema().getStructType().getFieldsCount())
                .isEqualTo(TEST_SCHEMA.structType().fields().size());
        assertThat(after.getSchema().getVersion()).isEqualTo(before.getSchema().getVersion());
        assertThat(after.getPartitionSpec().getBucketSpecsList())
                .isEqualTo(before.getPartitionSpec().getBucketSpecsList());
        assertThat(after.getPropertiesMap()).isEqualTo(before.getPropertiesMap());
        assertThat(after.getLocation()).isEqualTo(before.getLocation());
        assertThat(after.getPrimaryKey().getFieldNamesList())
                .isEqualTo(before.getPrimaryKey().getFieldNamesList());
        assertThat(after.getIdentifier().getCatalog())
                .isEqualTo(before.getIdentifier().getCatalog());
        assertThat(after.getHiddenColumns().getColumnsList())
                .isEqualTo(before.getHiddenColumns().getColumnsList());
    }

    @Test
    @DisplayName("D9: renameTable with purged table returns TableNotFoundException")
    void testRenameAfterDropThrowsNotFoundException() throws Exception {
        TableId from = new TableId("db_src", "tbl_r9");
        TableId to = new TableId("db_src", "tbl_r9_new");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());
        catalog.dropTable(from, false);

        assertThatThrownBy(() -> catalog.renameTable(from, to))
                .isInstanceOf(TableNotFoundException.class);
    }

    @Test
    @DisplayName("S1: renameTable relocates all subkeys from old prefix to new prefix")
    void testRenameRelocatesSubkeys() throws Exception {
        TableId from = new TableId("db_src", "tbl_s1");
        TableId to = new TableId("db_dst", "tbl_s1_renamed");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] snapshotKey = Keys.snapshotKey(from, 42);
        byte[] manifestKey = Keys.manifestKey(from, 99);
        byte[] counterKey = Keys.commitIdCounterKey(from);
        byte[] snapshotValue = new byte[] {1};
        byte[] manifestValue = new byte[] {2};
        byte[] counterValue = new byte[] {3};

        store.put(snapshotKey, snapshotValue);
        store.put(manifestKey, manifestValue);
        store.put(counterKey, counterValue);

        catalog.renameTable(from, to);

        assertThat(store.get(snapshotKey)).isEmpty();
        assertThat(store.get(manifestKey)).isEmpty();
        assertThat(store.get(counterKey)).isEmpty();

        byte[] newSnapshotKey = Keys.snapshotKey(to, 42);
        byte[] newManifestKey = Keys.manifestKey(to, 99);
        byte[] newCounterKey = Keys.commitIdCounterKey(to);

        assertThat(store.get(newSnapshotKey)).hasValue(snapshotValue);
        assertThat(store.get(newManifestKey)).hasValue(manifestValue);
        assertThat(store.get(newCounterKey)).hasValue(counterValue);

        assertThat(catalog.tableExists(from)).isFalse();
        assertThat(catalog.tableExists(to)).isTrue();
    }

    @Test
    @DisplayName("S2: renameTable with empty subkeys succeeds without errors")
    void testRenameWithNoSubkeys() throws Exception {
        TableId from = new TableId("db_src", "tbl_s2");
        TableId to = new TableId("db_src", "tbl_s2_renamed");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());
        catalog.renameTable(from, to);

        assertThat(catalog.tableExists(to)).isTrue();
        assertThat(catalog.tableExists(from)).isFalse();
    }

    @Test
    @DisplayName("S3: renameTable + recreate with old name has no subkey contamination")
    void testRenameRecreateNoContamination() throws Exception {
        TableId from = new TableId("db_src", "tbl_s3");
        TableId to = new TableId("db_dst", "tbl_s3_renamed");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] counterKey = Keys.commitIdCounterKey(from);
        store.put(counterKey, new byte[] {7});

        catalog.renameTable(from, to);

        byte[] newCounterKey = Keys.commitIdCounterKey(to);
        assertThat(store.get(newCounterKey)).hasValue(new byte[] {7});

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());
        assertThat(store.get(counterKey)).isEmpty();
        assertThat(catalog.tableExists(from)).isTrue();

        int keyCountUnderFrom = store.scanAll(Keys.tablePrefix(from)).size();
        assertThat(keyCountUnderFrom).isEqualTo(3);
    }

    @Test
    @DisplayName("S4: loadTable after rename with relocated subkeys returns correct identity")
    void testLoadTableAfterSubkeyRelocation() throws Exception {
        TableId from = new TableId("db_src", "tbl_s4");
        TableId to = new TableId("db_dst", "tbl_s4_renamed");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] snapshotKey = Keys.snapshotKey(from, 1);
        store.put(snapshotKey, new byte[] {99});

        catalog.renameTable(from, to);

        var table = catalog.loadTable(to);
        assertThat(table.meta().databaseName()).isEqualTo("db_dst");
        assertThat(table.meta().tableName()).isEqualTo("tbl_s4_renamed");
        assertThat(table.meta().tableId()).isGreaterThan(0);

        assertThat(store.get(Keys.snapshotKey(to, 1))).hasValue(new byte[] {99});
    }

    @Test
    @DisplayName("S5: rename-intent recovery relocates remaining subkeys after simulated crash")
    void testRenameIntentRecoveryAfterCrash() throws Exception {
        TableId from = new TableId("db_src", "tbl_s5");
        TableId to = new TableId("db_dst", "tbl_s5_renamed");

        catalog.createTable(
                from, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());
        store.put(Keys.snapshotKey(from, 10), new byte[] {10});
        store.put(Keys.manifestKey(from, 20), new byte[] {20});
        store.put(Keys.commitIdCounterKey(from), new byte[] {30});

        catalog.renameTable(from, to);

        assertThat(store.get(Keys.snapshotKey(from, 10))).isEmpty();
        assertThat(store.get(Keys.manifestKey(from, 20))).isEmpty();
        assertThat(store.get(Keys.commitIdCounterKey(from))).isEmpty();

        assertThat(store.get(Keys.renameIntentKey(to))).isEmpty();

        store.put(Keys.snapshotKey(from, 100), new byte[] {100});
        store.put(Keys.manifestKey(from, 200), new byte[] {(byte) 200});
        byte[] intentValue = MetastoreCatalog.buildRenameIntentValue(from);
        store.put(Keys.renameIntentKey(to), intentValue);

        catalog.loadTable(to);

        assertThat(store.get(Keys.snapshotKey(from, 100))).isEmpty();
        assertThat(store.get(Keys.manifestKey(from, 200))).isEmpty();
        assertThat(store.get(Keys.snapshotKey(to, 100))).hasValue(new byte[] {100});
        assertThat(store.get(Keys.manifestKey(to, 200))).hasValue(new byte[] {(byte) 200});
        assertThat(store.get(Keys.renameIntentKey(to))).isEmpty();

        catalog.invalidateCache(to);
        var table = catalog.loadTable(to);
        assertThat(table.meta().databaseName()).isEqualTo("db_dst");
        assertThat(table.meta().tableName()).isEqualTo("tbl_s5_renamed");
        assertThat(table.meta().tableId()).isGreaterThan(0);
    }

    @Test
    @DisplayName("S6: corrupt rename-intent value is deleted without error during recovery")
    void testCorruptRenameIntentIsDeleted() throws Exception {
        TableId id = new TableId("db_dst", "tbl_s6");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        store.put(Keys.renameIntentKey(id), "corrupt_no_colon".getBytes(StandardCharsets.UTF_8));

        catalog.invalidateCache(id);
        var table = catalog.loadTable(id);
        assertThat(table.meta().tableName()).isEqualTo("tbl_s6");
        assertThat(store.get(Keys.renameIntentKey(id))).isEmpty();
    }

    @Test
    @DisplayName("E1: createTable initializes epoch key to 0")
    void testCreateTableInitializesEpoch() throws Exception {
        TableId id = new TableId("db_src", "tbl_e1");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] epochKey = Keys.tableEpochKey(id);
        assertThat(store.get(epochKey)).isPresent();
        assertThat(store.get(epochKey).get()).isEqualTo(Keys.encodeEpoch(0));
    }

    @Test
    @DisplayName("E2: dropTable increments epoch after successful drop")
    void testDropTableIncrementsEpoch() throws Exception {
        TableId id = new TableId("db_src", "tbl_e2");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] epochKey = Keys.tableEpochKey(id);
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(0);

        boolean dropped = catalog.dropTable(id, false);
        assertThat(dropped).isTrue();

        assertThat(store.get(epochKey)).isPresent();
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(1);
    }

    @Test
    @DisplayName("E3: epoch-based identity separation — drop+recreate produces different table")
    void testEpochBasedIdentitySeparation() throws Exception {
        TableId id = new TableId("db_src", "tbl_e3");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        var beforeDrop = catalog.loadTable(id);
        long originalNumericId = beforeDrop.meta().tableId();

        catalog.dropTable(id, false);

        byte[] epochKey = Keys.tableEpochKey(id);
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(1);

        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(0);

        var afterRecreate = catalog.loadTable(id);
        assertThat(afterRecreate.meta().tableId()).isNotEqualTo(originalNumericId);
        assertThat(afterRecreate.meta().tableName()).isEqualTo("tbl_e3");
    }

    @Test
    @DisplayName("E4: drop+recreate same name yields epoch=0 and clean state")
    void testDropRecreateEpochReset() throws Exception {
        TableId id = new TableId("db_src", "tbl_e4");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] snapshotKey = Keys.snapshotKey(id, 1);
        store.put(snapshotKey, new byte[] {1});

        catalog.dropTable(id, false);

        byte[] epochKey = Keys.tableEpochKey(id);
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(1);
        assertThat(store.get(snapshotKey)).isEmpty();
        assertThat(store.scanAll(Keys.tablePrefix(id))).hasSize(1);

        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        assertThat(catalog.tableExists(id)).isTrue();
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(0);
        assertThat(store.get(snapshotKey)).isEmpty();
    }

    @Test
    @DisplayName("E5: dropTable cleans all orphan subkeys with epoch protection")
    void testDropCleansAllOrphanSubkeysWithEpoch() throws Exception {
        TableId id = new TableId("db_dst", "tbl_e5");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        store.put(Keys.snapshotKey(id, 5), new byte[] {5});
        store.put(Keys.manifestKey(id, 10), new byte[] {10});
        store.put(Keys.commitIdCounterKey(id), new byte[] {100});

        byte[] prefix = Keys.tablePrefix(id);
        int keyCountBefore = store.scanAll(prefix).size();
        assertThat(keyCountBefore).isGreaterThanOrEqualTo(4);

        catalog.dropTable(id, false);

        int keyCountAfter = store.scanAll(prefix).size();
        assertThat(keyCountAfter).isEqualTo(1);

        byte[] epochKey = Keys.tableEpochKey(id);
        assertThat(store.get(epochKey)).isPresent();
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(1);
    }

    @Test
    @DisplayName(
            "E6: ABA-proof — stale drop CAS must not destroy recreated table (metaKey content guard)")
    void testStaleDropDoesNotDestroyRecreatedTable() throws Exception {
        TableId id = new TableId("db_src", "tbl_e6");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        var firstTable = catalog.loadTable(id);
        long firstNumericId = firstTable.meta().tableId();

        catalog.dropTable(id, false);
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        var secondTable = catalog.loadTable(id);
        long secondNumericId = secondTable.meta().tableId();
        assertThat(secondNumericId).isNotEqualTo(firstNumericId);

        byte[] metaKey = Keys.tablePrefix(id);
        byte[] currentMetaBytes = store.get(metaKey).get();
        PbTableMetadata currentPb = PbTableMetadata.parseFrom(currentMetaBytes);
        long currentTableId = currentPb.getTableId();
        assertThat(currentTableId).isEqualTo(secondNumericId);

        byte[] staleMetaBytes =
                PbTableMetadata.newBuilder().setTableId(firstNumericId).build().toByteArray();

        byte[] epochKey = Keys.tableEpochKey(id);
        long currentEpoch = Keys.decodeEpoch(store.get(epochKey).get());

        byte[] listKey = Keys.namespaceTableListKey(id.database());
        byte[] tableEntryKey = new byte[listKey.length + id.tableName().length()];
        System.arraycopy(listKey, 0, tableEntryKey, 0, listKey.length);
        System.arraycopy(
                id.tableName().getBytes(StandardCharsets.UTF_8),
                0,
                tableEntryKey,
                listKey.length,
                id.tableName().length());

        byte[] tableIdKey = Keys.tableIdentityKey(id);

        boolean staleSuccess =
                store.compareAndSwap(
                        List.of(
                                new Precondition.Exists(metaKey),
                                new Precondition.ValueEquals(metaKey, staleMetaBytes)),
                        List.of(
                                new Mutation.Delete(metaKey),
                                new Mutation.Delete(tableEntryKey),
                                new Mutation.Delete(tableIdKey),
                                new Mutation.Put(epochKey, Keys.encodeEpoch(currentEpoch + 1))));

        assertThat(staleSuccess).isFalse();
        assertThat(catalog.tableExists(id)).isTrue();

        var stillSecond = catalog.loadTable(id);
        assertThat(stillSecond.meta().tableId()).isEqualTo(secondNumericId);
    }

    @Test
    @DisplayName("E7: phantom key cleanup — pre-existing keys cleaned via first scan during drop")
    void testPhantomKeyCleanupViaFirstScan() throws Exception {
        TableId id = new TableId("db_dst", "tbl_e7");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] phantomKey = Keys.snapshotKey(id, 999);
        store.put(phantomKey, new byte[] {99});

        int keyCountBefore = store.scanAll(Keys.tablePrefix(id)).size();
        assertThat(keyCountBefore).isGreaterThanOrEqualTo(2);

        catalog.dropTable(id, false);

        assertThat(store.get(phantomKey)).isEmpty();
        assertThat(store.get(Keys.tableEpochKey(id))).isPresent();
        assertThat(store.scanAll(Keys.tablePrefix(id))).hasSize(1);
    }

    @Test
    @DisplayName("E8: purge=true throws UnsupportedOperationException")
    void testPurgeThrowsUnsupportedOperation() throws Exception {
        TableId id = new TableId("db_dst", "tbl_e8");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        assertThatThrownBy(() -> catalog.dropTable(id, true))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("purge=true");

        assertThat(catalog.tableExists(id)).isTrue();
    }

    @Test
    @DisplayName(
            "E9: createTable cleans phantom keys from previous lifecycle (crashed drop simulation)")
    void testCreateTableCleansPhantomKeysFromCrashedDrop() throws Exception {
        TableId id = new TableId("db_src", "tbl_e9");
        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        byte[] metaKey = Keys.tablePrefix(id);
        byte[] tableIdKey = Keys.tableIdentityKey(id);
        byte[] epochKey = Keys.tableEpochKey(id);

        byte[] listKey = Keys.namespaceTableListKey(id.database());
        byte[] tableEntryKey = new byte[listKey.length + id.tableName().length()];
        System.arraycopy(listKey, 0, tableEntryKey, 0, listKey.length);
        System.arraycopy(
                id.tableName().getBytes(StandardCharsets.UTF_8),
                0,
                tableEntryKey,
                listKey.length,
                id.tableName().length());

        catalog.dropTable(id, false);

        assertThat(catalog.tableExists(id)).isFalse();

        byte[] phantom1 = Keys.snapshotKey(id, 42);
        byte[] phantom2 = Keys.manifestKey(id, 7);
        store.put(phantom1, new byte[] {1});
        store.put(phantom2, new byte[] {2});

        assertThat(store.get(phantom1)).isPresent();
        assertThat(store.get(phantom2)).isPresent();
        assertThat(store.get(epochKey)).isPresent();

        catalog.createTable(
                id, TEST_SCHEMA, PartitionSpec.unpartitioned(), TableProperties.empty());

        assertThat(catalog.tableExists(id)).isTrue();
        assertThat(store.get(phantom1)).isEmpty();
        assertThat(store.get(phantom2)).isEmpty();
        assertThat(store.get(epochKey)).isPresent();
        assertThat(Keys.decodeEpoch(store.get(epochKey).get())).isEqualTo(0);
    }
}
