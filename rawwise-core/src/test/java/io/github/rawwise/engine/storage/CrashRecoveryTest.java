package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("crash recovery test")
class CrashRecoveryTest {

    @BeforeAll
    static void registerStorageProvider() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
            // Already registered
        }
    }

    @TempDir Path tempDir;

    private String tableIdentifier;

    @BeforeEach
    void setUp() {
        tableIdentifier = "catalog.db.crash_test_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String readRawValue(DynamicMmapStore store, Map<String, Comparable> pk)
            throws Exception {
        TableStore ts = store.tableStore(tableIdentifier);
        KeyCodec keyCodec = ts.getKeyCodec();
        byte[] encodedKey = keyCodec.encodePrimaryKey(pk);
        byte[] rawValue = ts.getRawValue(encodedKey);
        if (rawValue == null) return null;
        return new String(NullableValueCodec.decode(rawValue), StandardCharsets.UTF_8);
    }

    @Nested
    @DisplayName("LMDB persistence and recovery")
    class LmdbPersistenceTests {

        @Test
        @DisplayName("after writing data LMDB files persisted to disk")
        void testWritePersistsLmdbFiles() throws Exception {
            String storeId = "persist-file-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                StorageTestHelper.writeKv(
                        store, tableIdentifier, Map.of("id", "persist-key"), "persist-value");
            } finally {
                store.close();
            }

            Path tablePath = tempDir.resolve(tableIdentifier);
            assertThat(java.nio.file.Files.exists(tablePath)).isTrue();

            boolean hasLmdbFiles =
                    java.nio.file.Files.walk(tablePath)
                            .anyMatch(
                                    p ->
                                            p.getFileName().toString().equals("data.mdb")
                                                    || p.getFileName().toString().endsWith(".mdb"));
            assertThat(hasLmdbFiles).isTrue();
        }

        @Test
        @DisplayName("after reopening Store in same directory committed data is readable")
        void testReopenSameDirectoryDataReadable() throws Exception {
            String storeId = "reopen-" + UUID.randomUUID();
            Map<String, Comparable> pk1 = Map.of("id", "reopen-key-1");
            Map<String, Comparable> pk2 = Map.of("id", "reopen-key-2");

            DynamicMmapStore store1 = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                StorageTestHelper.writeKv(store1, tableIdentifier, pk1, "value-1");
                StorageTestHelper.writeKv(store1, tableIdentifier, pk2, "value-2");

                assertThat(readRawValue(store1, pk1)).isEqualTo("value-1");
            } finally {
                store1.close();
            }

            DynamicMmapStore store2 =
                    DynamicMmapStore.getInstance(tempDir.toString(), storeId + "-reopen");
            try {
                assertThat(readRawValue(store2, pk1)).as("重新打开后key1的数据应可读").isNotNull();
                assertThat(readRawValue(store2, pk2)).as("重新打开后key2的数据应可读").isNotNull();
            } finally {
                store2.close();
            }
        }

        @Test
        @DisplayName("after batch write all data persisted")
        void testBatchWriteAllPersisted() throws Exception {
            String storeId = "batch-persist-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                for (int i = 0; i < 10; i++) {
                    StorageTestHelper.writeKv(
                            store,
                            tableIdentifier,
                            Map.of("id", "batch-key-" + i),
                            "batch-value-" + i);
                }

                for (int i = 0; i < 10; i++) {
                    assertThat(readRawValue(store, Map.of("id", "batch-key-" + i)))
                            .as("batch-key-%d 应持久化", i)
                            .isNotNull();
                }
            } finally {
                store.close();
            }
        }
    }

    @Nested
    @DisplayName("empty directory and empty WAL recovery")
    class EmptyRecoveryTests {

        @Test
        @DisplayName("empty directory create DynamicMmapStore does not throw exception")
        void testEmptyDirectoryRecovery() {
            String storeId = "empty-crash-" + UUID.randomUUID();

            assertThatCode(
                            () -> {
                                DynamicMmapStore store =
                                        DynamicMmapStore.getInstance(tempDir.toString(), storeId);
                                try {
                                    TableStore ts = store.tableStore("nonexistent.table");
                                    assertThat(ts.getAllInstancesDescending()).isEmpty();
                                } finally {
                                    store.close();
                                }
                            })
                    .doesNotThrowAnyException();
        }

        @Test
        @DisplayName("PROCESS WALisnulldoes not affect operations")
        void testProcessModeNoWalNormalOperation() throws Exception {
            String storeId = "no-wal-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                assertThat(store.getWal()).isNull();

                StorageTestHelper.writeKv(
                        store, tableIdentifier, Map.of("id", "no-wal-key"), "no-wal-value");
                assertThat(readRawValue(store, Map.of("id", "no-wal-key")))
                        .isEqualTo("no-wal-value");
            } finally {
                store.close();
            }
        }
    }

    @Nested
    @DisplayName("Instancelifecycleandrecovery")
    class InstanceLifecycleTests {

        @Test
        @DisplayName("readInstancedata canviarawread")
        void testReadOnlyInstanceDataReadableViaRaw() throws Exception {
            String storeId = "readonly-find-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                StorageTestHelper.writeKv(
                        store, tableIdentifier, Map.of("id", "ro-key"), "ro-value");

                TableStore ts = store.tableStore(tableIdentifier);
                LmdbInstance instance = ts.getAllInstancesDescending().getFirst();
                instance.markAsReadOnly();

                assertThat(readRawValue(store, Map.of("id", "ro-key"))).isEqualTo("ro-value");
            } finally {
                store.close();
            }
        }
    }

    @Nested
    @DisplayName("after writing readverify")
    class WriteReadConsistencyTests {

        @Test
        @DisplayName("after writingviaTableStoredirectlyreadverifydataalready")
        void testWriteThenDirectReadFromTableStore() throws Exception {
            String storeId = "direct-read-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                StorageTestHelper.writeKv(
                        store, tableIdentifier, Map.of("id", "direct-key"), "direct-value");
                assertThat(readRawValue(store, Map.of("id", "direct-key")))
                        .isEqualTo("direct-value");
            } finally {
                store.close();
            }
        }

        @Test
        @DisplayName("write-delete- after writing statuscorrect")
        void testWriteDeleteRewriteRecovery() throws Exception {
            String storeId = "wdw-recover-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                Map<String, Comparable> pk = Map.of("id", "wdw-key");
                StorageTestHelper.writeKv(store, tableIdentifier, pk, "v1");
                StorageTestHelper.deleteKey(store, tableIdentifier, pk);
                StorageTestHelper.writeKv(store, tableIdentifier, pk, "v3");

                assertThat(readRawValue(store, pk)).isEqualTo("v3");
            } finally {
                store.close();
            }
        }
    }

    @Nested
    @DisplayName("PROCESS LMDBpersistedrecovery")
    class LmdbCrashRecoveryTests {

        @Test
        @DisplayName("simulate : sameStore IDafter reopeningdatareadable")
        void testCrashRecoverySameStoreId() throws Exception {
            String storeId = "crash-recover-" + UUID.randomUUID();
            Map<String, Comparable> pk1 = Map.of("id", "crash-key-1");
            Map<String, Comparable> pk2 = Map.of("id", "crash-key-2");

            DynamicMmapStore store1 = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                StorageTestHelper.writeKv(store1, tableIdentifier, pk1, "crash-value-1");
                StorageTestHelper.writeKv(store1, tableIdentifier, pk2, "crash-value-2");
                assertThat(readRawValue(store1, pk1)).isEqualTo("crash-value-1");
            } finally {
                store1.close();
            }

            DynamicMmapStore store2 = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                String value1 = readRawValue(store2, pk1);
                String value2 = readRawValue(store2, pk2);
                assertThat(value1).as("崩溃恢复后key1数据应可读").isEqualTo("crash-value-1");
                assertThat(value2).as("崩溃恢复后key2数据应可读").isEqualTo("crash-value-2");
            } finally {
                store2.close();
            }
        }

        @Test
        @DisplayName("simulate : tombstonestatuscorrectly recovers")
        void testCrashRecoveryTombstonePreserved() throws Exception {
            String storeId = "crash-tomb-" + UUID.randomUUID();
            Map<String, Comparable> pk = Map.of("id", "crash-tomb-key");

            DynamicMmapStore store1 = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                StorageTestHelper.writeKv(store1, tableIdentifier, pk, "original");
                StorageTestHelper.deleteKey(store1, tableIdentifier, pk);

                TableStore ts1 = store1.tableStore(tableIdentifier);
                byte[] rawBefore = ts1.getRawValue(ts1.getKeyCodec().encodePrimaryKey(pk));
                assertThat(NullableValueCodec.isTombstone(rawBefore)).isTrue();
            } finally {
                store1.close();
            }

            DynamicMmapStore store2 = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                TableStore ts2 = store2.tableStore(tableIdentifier);
                byte[] rawAfter = ts2.getRawValue(ts2.getKeyCodec().encodePrimaryKey(pk));
                assertThat(rawAfter).as("崩溃恢复后tombstone应存在").isNotNull();
                assertThat(NullableValueCodec.isTombstone(rawAfter)).isTrue();
            } finally {
                store2.close();
            }
        }

        @Test
        @DisplayName("simulate : write-delete- write statuscorrectly recovers")
        void testCrashRecoveryWriteDeleteRewrite() throws Exception {
            String storeId = "crash-wdw-" + UUID.randomUUID();
            Map<String, Comparable> pk = Map.of("id", "crash-wdw-key");

            DynamicMmapStore store1 = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                StorageTestHelper.writeKv(store1, tableIdentifier, pk, "v1");
                StorageTestHelper.deleteKey(store1, tableIdentifier, pk);
                StorageTestHelper.writeKv(store1, tableIdentifier, pk, "v3");
            } finally {
                store1.close();
            }

            DynamicMmapStore store2 = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                assertThat(readRawValue(store2, pk)).isEqualTo("v3");
            } finally {
                store2.close();
            }
        }
    }
}
