package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

import io.github.rawwise.backend.wal.WalWriteAdapter;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.model.op.KvWriteBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("test")
class FaultInjectionTest {

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
        tableIdentifier = "catalog.db.fault_test_" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Nested
    @DisplayName("WALwritefail")
    class WalFailureTests {

        @Test
        @DisplayName("WAL append exception extractWalData can extractdata")
        void testWalExtractDataIndependentOfWalFailure() {
            WalWriteAdapter adapter = new WalWriteAdapter(null, DurabilityLevel.PROCESS, null);

            KvWriteBuilder builder =
                    KvWriteBuilder.builder()
                            .table(new StorageTestHelper.TestTable(tableIdentifier))
                            .key("test-key".getBytes(StandardCharsets.UTF_8))
                            .value("test-value".getBytes(StandardCharsets.UTF_8))
                            .partitionHint(0);

            byte[] walData = adapter.extractWalData(builder, tableIdentifier);
            assertThat(walData).isNotEmpty();
        }

        @Test
        @DisplayName("PROCESS WALisnull, write-read")
        void testProcessModeNoWalWriteReadFlow() throws Exception {
            String storeId = "process-no-wal-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                assertThat(store.getWal()).isNull();

                Map<String, Comparable> pk = Map.of("id", "no-wal-key");
                StorageTestHelper.writeKv(store, tableIdentifier, pk, "no-wal-value");

                TableStore ts = store.tableStore(tableIdentifier);
                KeyCodec keyCodec = ts.getKeyCodec();
                byte[] encodedKey = keyCodec.encodePrimaryKey(pk);
                byte[] rawValue = ts.getRawValue(encodedKey);
                assertThat(rawValue).isNotNull();
                assertThat(new String(NullableValueCodec.decode(rawValue), StandardCharsets.UTF_8))
                        .isEqualTo("no-wal-value");
            } finally {
                store.close();
            }
        }
    }

    @Nested
    @DisplayName("LMDB MAP_FULL")
    class LmdbMapFullTests {

        @Test
        @DisplayName("Instance readafter writeroutingto Instance, data readable")
        void testAutoSwitchOnReadOnlyBothDataReadable() throws Exception {
            String storeId = "mapfull-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                Map<String, Comparable> pk1 = Map.of("id", "before-readonly");
                StorageTestHelper.writeKv(store, tableIdentifier, pk1, "value-1");

                TableStore ts = store.tableStore(tableIdentifier);
                LmdbInstance firstInstance = ts.getAllInstancesDescending().getFirst();
                firstInstance.markAsReadOnly();

                Map<String, Comparable> pk2 = Map.of("id", "after-readonly");
                StorageTestHelper.writeKv(store, tableIdentifier, pk2, "value-2");

                List<LmdbInstance> instancesAfter = ts.getAllInstancesDescending();
                boolean hasNewInstance =
                        instancesAfter.stream()
                                .anyMatch(
                                        inst ->
                                                !inst.instanceId()
                                                        .equals(firstInstance.instanceId()));
                assertThat(hasNewInstance).isTrue();

                KeyCodec keyCodec = ts.getKeyCodec();
                byte[] raw1 = ts.getRawValue(keyCodec.encodePrimaryKey(pk1));
                byte[] raw2 = ts.getRawValue(keyCodec.encodePrimaryKey(pk2));
                assertThat(raw1).isNotNull();
                assertThat(raw2).isNotNull();
                assertThat(new String(NullableValueCodec.decode(raw1), StandardCharsets.UTF_8))
                        .isEqualTo("value-1");
                assertThat(new String(NullableValueCodec.decode(raw2), StandardCharsets.UTF_8))
                        .isEqualTo("value-2");
            } finally {
                store.close();
            }
        }
    }

    @Nested
    @DisplayName("DeletionManagerretry")
    class DeletionRetryTests {

        @Test
        @DisplayName("DeletionManagerindestroyfailafter waitretry")
        void testDeletionManagerRetriesOnFailure() throws Exception {
            AtomicInteger releaseCount = new AtomicInteger(0);
            long instanceMappedBytes = 1024 * 1024;

            DeletionManager dm =
                    new DeletionManager(
                            released -> releaseCount.incrementAndGet(), instanceMappedBytes);

            try {
                LmdbInstance mockInstance = mock(LmdbInstance.class);
                when(mockInstance.instanceId()).thenReturn("mock-inst");
                when(mockInstance.tryWriteLock()).thenReturn(true);
                doThrow(new RuntimeException("simulated destroy failure"))
                        .when(mockInstance)
                        .destroy();

                dm.scheduleDeletion(mockInstance, 0);

                await().atMost(2, TimeUnit.SECONDS)
                        .untilAsserted(() -> verify(mockInstance, atLeast(1)).destroy());
                assertThat(dm.pendingCount()).isGreaterThan(0);
            } finally {
                dm.close();
            }
        }
    }

    @Nested
    @DisplayName("batchwrite data")
    class BatchWriteIntegrityTests {

        @Test
        @DisplayName("100itemafter batch write data intact")
        void testBatchWriteDataIntegrity() throws Exception {
            String storeId = "batch-integrity-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                int totalWrites = 100;
                for (int i = 0; i < totalWrites; i++) {
                    Map<String, Comparable> pk = Map.of("id", "batch-key-" + i);
                    StorageTestHelper.writeKv(store, tableIdentifier, pk, "value-" + i);
                }

                TableStore ts = store.tableStore(tableIdentifier);
                KeyCodec keyCodec = ts.getKeyCodec();

                int verifiedCount = 0;
                for (int i = 0; i < totalWrites; i++) {
                    byte[] encodedKey = keyCodec.encodePrimaryKey(Map.of("id", "batch-key-" + i));
                    byte[] rawValue = ts.getRawValue(encodedKey);
                    if (rawValue != null) {
                        String decoded =
                                new String(
                                        NullableValueCodec.decode(rawValue),
                                        StandardCharsets.UTF_8);
                        assertThat(decoded).isEqualTo("value-" + i);
                        verifiedCount++;
                    }
                }
                assertThat(verifiedCount).isEqualTo(totalWrites);
            } finally {
                store.close();
            }
        }

        @Test
        @DisplayName("write-delete- write operationsdata")
        void testInterleavedWriteDeleteRewrite() throws Exception {
            String storeId = "interleaved-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                for (int i = 0; i < 20; i++) {
                    Map<String, Comparable> pk = Map.of("id", "interleave-key-" + i);
                    StorageTestHelper.writeKv(store, tableIdentifier, pk, "v1-" + i);
                    StorageTestHelper.deleteKey(store, tableIdentifier, pk);
                    StorageTestHelper.writeKv(store, tableIdentifier, pk, "v3-" + i);
                }

                TableStore ts = store.tableStore(tableIdentifier);
                KeyCodec keyCodec = ts.getKeyCodec();

                for (int i = 0; i < 20; i++) {
                    byte[] encodedKey =
                            keyCodec.encodePrimaryKey(Map.of("id", "interleave-key-" + i));
                    byte[] rawValue = ts.getRawValue(encodedKey);
                    assertThat(rawValue).as("interleave-key-%d 应存在", i).isNotNull();
                    assertThat(NullableValueCodec.isTombstone(rawValue)).isFalse();
                    String decoded =
                            new String(NullableValueCodec.decode(rawValue), StandardCharsets.UTF_8);
                    assertThat(decoded).isEqualTo("v3-" + i);
                }
            } finally {
                store.close();
            }
        }
    }

    @Nested
    @DisplayName("write-delete- writefailure recovery")
    class WriteDeleteRewriteRecoveryTests {

        @Test
        @DisplayName("after writingdelete write, statuscorrect")
        void testWriteDeleteRewriteFinalStateCorrect() throws Exception {
            String storeId = "wdw-fault-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                Map<String, Comparable> pk = Map.of("id", "wdw-key");
                StorageTestHelper.writeKv(store, tableIdentifier, pk, "v1");

                TableStore ts = store.tableStore(tableIdentifier);
                KeyCodec keyCodec = ts.getKeyCodec();
                byte[] encodedKey = keyCodec.encodePrimaryKey(pk);
                StorageTestHelper.deleteKey(store, tableIdentifier, pk);

                byte[] rawAfterDelete = ts.getRawValue(encodedKey);
                assertThat(rawAfterDelete).isNotNull();
                assertThat(NullableValueCodec.isTombstone(rawAfterDelete)).isTrue();

                StorageTestHelper.writeKv(store, tableIdentifier, pk, "v3");

                byte[] rawAfterRewrite = ts.getRawValue(encodedKey);
                assertThat(rawAfterRewrite).isNotNull();
                assertThat(NullableValueCodec.isTombstone(rawAfterRewrite)).isFalse();
                assertThat(
                                new String(
                                        NullableValueCodec.decode(rawAfterRewrite),
                                        StandardCharsets.UTF_8))
                        .isEqualTo("v3");
            } finally {
                store.close();
            }
        }
    }

    @Nested
    @DisplayName("Instancelifecycle")
    class InstanceLifecycleFaultTests {

        @Test
        @DisplayName("Instance readafterdatareadable")
        void testReadOnlyInstanceDataReadable() throws Exception {
            String storeId = "closed-inst-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                Map<String, Comparable> pk = Map.of("id", "close-key");
                StorageTestHelper.writeKv(store, tableIdentifier, pk, "close-value");

                TableStore ts = store.tableStore(tableIdentifier);
                LmdbInstance instance = ts.getAllInstancesDescending().getFirst();
                instance.markAsReadOnly();

                assertThat(instance.isReadOnly()).isTrue();

                KeyCodec keyCodec = ts.getKeyCodec();
                byte[] encodedKey = keyCodec.encodePrimaryKey(pk);
                byte[] rawValue = ts.getRawValue(encodedKey);
                assertThat(rawValue).isNotNull();
                assertThat(new String(NullableValueCodec.decode(rawValue), StandardCharsets.UTF_8))
                        .isEqualTo("close-value");
            } finally {
                store.close();
            }
        }

        @Test
        @DisplayName("multiple timesmarkAsReadOnlyidempotent")
        void testMarkAsReadOnlyIdempotent() throws Exception {
            String storeId = "idempotent-ro-" + UUID.randomUUID();
            DynamicMmapStore store = DynamicMmapStore.getInstance(tempDir.toString(), storeId);
            try {
                StorageTestHelper.writeKv(
                        store, tableIdentifier, Map.of("id", "idempotent-key"), "idempotent-value");

                TableStore ts = store.tableStore(tableIdentifier);
                LmdbInstance instance = ts.getAllInstancesDescending().getFirst();

                instance.markAsReadOnly();
                instance.markAsReadOnly();

                assertThat(instance.isReadOnly()).isTrue();
            } finally {
                store.close();
            }
        }
    }
}
