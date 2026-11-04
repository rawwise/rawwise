package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Write-Readintegration test")
class WriteReadIntegrationTest {

    @BeforeAll
    static void registerStorageProvider() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
            // Already registered
        }
    }

    @TempDir Path tempDir;

    private DynamicMmapStore store;
    private String tableIdentifier;
    private KeyCodec keyCodec;

    @BeforeEach
    void setUp() throws Exception {
        tableIdentifier = "catalog.db.wr_test_" + UUID.randomUUID().toString().substring(0, 8);
        store = DynamicMmapStore.getInstance(tempDir.toString(), "wr-store-" + UUID.randomUUID());
        keyCodec = store.tableStore(tableIdentifier).getKeyCodec();
    }

    @BeforeEach
    void cleanTxn() {
        try {
            io.github.rawwise.engine.txn.state.TxnContextHolder.clear();
        } catch (Exception e) {
            // ignore
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    private byte[] encodeKey(Map<String, Comparable> pkValues) throws IOException {
        return keyCodec.encodePrimaryKey(pkValues);
    }

    private void writeKv(Map<String, Comparable> pkValues, String value) throws IOException {
        StorageTestHelper.writeKv(store, tableIdentifier, pkValues, value);
    }

    private void deleteKey(Map<String, Comparable> pkValues) throws IOException {
        StorageTestHelper.deleteKey(store, tableIdentifier, pkValues);
    }

    @Nested
    @DisplayName("write-readhot path")
    class HotReadTests {

        @Test
        @DisplayName("after writingdirectlyfromLMDBreadreturns correct value")
        void testWriteThenHotRead() throws Exception {
            Map<String, Comparable> pk = Map.of("id", "hot-key-1");
            writeKv(pk, "hot-value-1");

            TableStore ts = store.tableStore(tableIdentifier);
            byte[] encodedKey = encodeKey(pk);
            byte[] rawValue = ts.getRawValue(encodedKey);
            assertThat(rawValue).isNotNull();
            assertThat(NullableValueCodec.isTombstone(rawValue)).isFalse();
            byte[] decoded = NullableValueCodec.decode(rawValue);
            assertThat(new String(decoded, StandardCharsets.UTF_8)).isEqualTo("hot-value-1");
        }

        @Test
        @DisplayName("multiple timeswritesamekey value")
        void testMultipleWritesSameKeyReturnsLatest() throws Exception {
            Map<String, Comparable> pk = Map.of("id", "hot-key-2");
            writeKv(pk, "value-v1");
            writeKv(pk, "value-v2");
            writeKv(pk, "value-v3");

            TableStore ts = store.tableStore(tableIdentifier);
            byte[] encodedKey = encodeKey(pk);
            byte[] rawValue = ts.getRawValue(encodedKey);
            assertThat(rawValue).isNotNull();
            byte[] decoded = NullableValueCodec.decode(rawValue);
            assertThat(new String(decoded, StandardCharsets.UTF_8)).isEqualTo("value-v3");
        }

        @Test
        @DisplayName("write multipledifferentkeyafter read correct")
        void testMultipleKeysWriteAndRead() throws Exception {
            Map<String, Comparable> pk1 = Map.of("id", "multi-key-1");
            Map<String, Comparable> pk2 = Map.of("id", "multi-key-2");
            Map<String, Comparable> pk3 = Map.of("id", "multi-key-3");

            writeKv(pk1, "value-1");
            writeKv(pk2, "value-2");
            writeKv(pk3, "value-3");

            TableStore ts = store.tableStore(tableIdentifier);

            byte[] raw1 = ts.getRawValue(encodeKey(pk1));
            byte[] raw2 = ts.getRawValue(encodeKey(pk2));
            byte[] raw3 = ts.getRawValue(encodeKey(pk3));

            assertThat(raw1).isNotNull();
            assertThat(raw2).isNotNull();
            assertThat(raw3).isNotNull();

            assertThat(new String(NullableValueCodec.decode(raw1), StandardCharsets.UTF_8))
                    .isEqualTo("value-1");
            assertThat(new String(NullableValueCodec.decode(raw2), StandardCharsets.UTF_8))
                    .isEqualTo("value-2");
            assertThat(new String(NullableValueCodec.decode(raw3), StandardCharsets.UTF_8))
                    .isEqualTo("value-3");
        }
    }

    @Nested
    @DisplayName("deletetombstone")
    class TombstoneTests {

        @Test
        @DisplayName("after writingdelete, raw can tombstone")
        void testWriteThenDeleteIdentifiableAsTombstone() throws Exception {
            Map<String, Comparable> pk = Map.of("id", "tomb-key-1");
            writeKv(pk, "tomb-value");
            deleteKey(pk);

            TableStore ts = store.tableStore(tableIdentifier);
            byte[] encodedKey = encodeKey(pk);
            byte[] rawValue = ts.getRawValue(encodedKey);
            assertThat(rawValue).isNotNull();
            assertThat(NullableValueCodec.isTombstone(rawValue)).isTrue();
        }

        @Test
        @DisplayName("deleteafter write, raw istombstone")
        void testDeleteThenRewriteNotTombstone() throws Exception {
            Map<String, Comparable> pk = Map.of("id", "tomb-key-2");
            writeKv(pk, "old-value");
            deleteKey(pk);
            writeKv(pk, "new-value");

            TableStore ts = store.tableStore(tableIdentifier);
            byte[] encodedKey = encodeKey(pk);
            byte[] rawValue = ts.getRawValue(encodedKey);
            assertThat(rawValue).isNotNull();
            assertThat(NullableValueCodec.isTombstone(rawValue)).isFalse();
            byte[] decoded = NullableValueCodec.decode(rawValue);
            assertThat(new String(decoded, StandardCharsets.UTF_8)).isEqualTo("new-value");
        }
    }

    @Nested
    @DisplayName("Flushisolation")
    class FlushIsolationTests {

        @Test
        @DisplayName("readInstancedata readable")
        void testReadOnlyInstanceStillReadable() throws Exception {
            Map<String, Comparable> pk = Map.of("id", "readonly-key");
            writeKv(pk, "readonly-value");

            TableStore ts = store.tableStore(tableIdentifier);
            LmdbInstance instance = ts.getAllInstancesDescending().getFirst();
            instance.markAsReadOnly();

            byte[] rawValue = ts.getRawValue(encodeKey(pk));
            assertThat(rawValue).isNotNull();
            assertThat(new String(NullableValueCodec.decode(rawValue), StandardCharsets.UTF_8))
                    .isEqualTo("readonly-value");
        }

        @Test
        @DisplayName("Flush : write-> read-> instancewrite-> data readable")
        void testFlushCycleBothInstancesReadable() throws Exception {
            Map<String, Comparable> pk1 = Map.of("id", "flush-key-1");
            writeKv(pk1, "before-flush-value");

            TableStore ts = store.tableStore(tableIdentifier);
            LmdbInstance firstInstance = ts.getAllInstancesDescending().getFirst();
            firstInstance.markAsReadOnly();

            Map<String, Comparable> pk2 = Map.of("id", "flush-key-2");
            writeKv(pk2, "after-flush-value");

            List<LmdbInstance> instances = ts.getAllInstancesDescending();
            boolean hasNewInstance =
                    instances.stream()
                            .anyMatch(
                                    inst -> !inst.instanceId().equals(firstInstance.instanceId()));
            assertThat(hasNewInstance).isTrue();

            byte[] raw1 = ts.getRawValue(encodeKey(pk1));
            byte[] raw2 = ts.getRawValue(encodeKey(pk2));
            assertThat(raw1).isNotNull();
            assertThat(raw2).isNotNull();
            assertThat(new String(NullableValueCodec.decode(raw1), StandardCharsets.UTF_8))
                    .isEqualTo("before-flush-value");
            assertThat(new String(NullableValueCodec.decode(raw2), StandardCharsets.UTF_8))
                    .isEqualTo("after-flush-value");
        }
    }

    @Nested
    @DisplayName("WAL-first-then-LMDB")
    class WalOrderingTests {

        @Test
        @DisplayName("PROCESS WALisnull, write success")
        void testProcessModeWriteSucceedsWithoutWAL() throws Exception {
            assertThat(store.getWal()).isNull();

            Map<String, Comparable> pk = Map.of("id", "no-wal-key");
            writeKv(pk, "no-wal-value");

            TableStore ts = store.tableStore(tableIdentifier);
            byte[] rawValue = ts.getRawValue(encodeKey(pk));
            assertThat(rawValue).isNotNull();
            assertThat(NullableValueCodec.isTombstone(rawValue)).isFalse();
        }
    }

    @Nested
    @DisplayName("non-existent keyread")
    class NotFoundTests {

        @Test
        @DisplayName("readnon-existent key null rawValue")
        void testReadNonExistentKeyReturnsNullRawValue() throws Exception {
            Map<String, Comparable> pk = Map.of("id", "nonexistent-key");
            TableStore ts = store.tableStore(tableIdentifier);
            byte[] rawValue = ts.getRawValue(encodeKey(pk));
            assertThat(rawValue).isNull();
        }
    }
}
