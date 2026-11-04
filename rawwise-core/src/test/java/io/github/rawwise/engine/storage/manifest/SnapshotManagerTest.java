package io.github.rawwise.engine.storage.manifest;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.protobuf.ByteString;
import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.catalog.Keys;
import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.ParquetDataFile;
import io.github.rawwise.engine.storage.RawWiseFile;
import io.github.rawwise.meta.PbManifestContent;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import io.github.rawwise.model.op.OpType;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableId;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SnapshotManagerTest {

    private static final TableId TABLE = new TableId("testdb", "testtable");
    private static final String TABLE_ID_STR = "testdb.testtable";

    private Metastore store;
    private Catalog catalog;
    private SnapshotManager manager;

    @BeforeEach
    void setUp() {
        InMemoryMetastore memStore = new InMemoryMetastore();
        store = memStore;
        catalog = mock(Catalog.class);
        manager = new SnapshotManager(catalog, store);
    }

    private DataFile createDataFile(String path, long size, long rowCount) {
        return new ParquetDataFile(
                path,
                size,
                System.currentTimeMillis(),
                TABLE_ID_STR,
                -1L,
                KeyRange.FULL_RANGE,
                rowCount,
                0,
                0,
                0,
                Map.of(),
                null,
                0);
    }

    @Nested
    @DisplayName("readManifestContent via reverse index")
    class ReadManifestContentViaIndex {

        @Test
        @DisplayName("returns data files when index and payload exist")
        void returnsDataFilesWhenIndexAndPayloadExist() throws Exception {
            String manifestId = UUID.randomUUID().toString();
            long commitId = 42L;
            long snapshotId = 100L;

            DataFile df = createDataFile("data.parquet", 1024L, 100L);
            ManifestFile manifest =
                    new ManifestFile(
                            manifestId,
                            RawWiseFile.FileFormat.JSON,
                            0,
                            System.currentTimeMillis(),
                            -1L,
                            List.of(df),
                            List.of(),
                            0L,
                            100L,
                            null);
            byte[] manifestBytes =
                    ManifestJsonCodecs.sharedObjectMapper().writeValueAsBytes(manifest);
            PbManifestContent pbContent =
                    PbManifestContent.newBuilder()
                            .setManifestId(manifestId)
                            .setContent(ByteString.copyFrom(manifestBytes))
                            .setContentSize(manifestBytes.length)
                            .setCreatedAtMillis(System.currentTimeMillis())
                            .build();

            byte[] idxKey = Keys.manifestIndexKey(TABLE, manifestId);
            byte[] commitIdBytes = ByteBuffer.allocate(8).putLong(commitId).array();
            store.put(idxKey, commitIdBytes);

            byte[] opKey =
                    Keys.operationPayloadKey(
                            TABLE, commitId, OpType.MANIFEST_WRITE.wireName(), manifestId);
            store.put(opKey, pbContent.toByteArray());

            Snapshot snap =
                    Snapshot.of(
                            snapshotId,
                            List.of(manifestId),
                            System.currentTimeMillis(),
                            "test",
                            "append",
                            null);
            when(catalog.getSnapshot(TABLE, snapshotId)).thenReturn(Optional.of(snap));

            List<DataFile> files = manager.listDataFiles(TABLE_ID_STR, snapshotId);
            assertEquals(1, files.size());
            assertEquals("data.parquet", files.get(0).filePath());
        }

        @Test
        @DisplayName("returns empty list when manifest index does not exist")
        void returnsEmptyWhenIndexMissing() throws Exception {
            String manifestId = UUID.randomUUID().toString();
            long snapshotId = 100L;

            Snapshot snap =
                    Snapshot.of(
                            snapshotId,
                            List.of(manifestId),
                            System.currentTimeMillis(),
                            "test",
                            "append",
                            null);
            when(catalog.getSnapshot(TABLE, snapshotId)).thenReturn(Optional.of(snap));

            List<DataFile> files = manager.listDataFiles(TABLE_ID_STR, snapshotId);
            assertTrue(files.isEmpty());
        }

        @Test
        @DisplayName("returns empty list when index exists but payload missing")
        void returnsEmptyWhenPayloadMissing() throws Exception {
            String manifestId = UUID.randomUUID().toString();
            long commitId = 42L;
            long snapshotId = 100L;

            byte[] idxKey = Keys.manifestIndexKey(TABLE, manifestId);
            byte[] commitIdBytes = ByteBuffer.allocate(8).putLong(commitId).array();
            store.put(idxKey, commitIdBytes);

            Snapshot snap =
                    Snapshot.of(
                            snapshotId,
                            List.of(manifestId),
                            System.currentTimeMillis(),
                            "test",
                            "append",
                            null);
            when(catalog.getSnapshot(TABLE, snapshotId)).thenReturn(Optional.of(snap));

            List<DataFile> files = manager.listDataFiles(TABLE_ID_STR, snapshotId);
            assertTrue(files.isEmpty());
        }

        @Test
        @DisplayName("skips corrupt payload and returns empty")
        void skipsCorruptPayload() throws Exception {
            String manifestId = UUID.randomUUID().toString();
            long commitId = 42L;
            long snapshotId = 100L;

            byte[] idxKey = Keys.manifestIndexKey(TABLE, manifestId);
            byte[] commitIdBytes = ByteBuffer.allocate(8).putLong(commitId).array();
            store.put(idxKey, commitIdBytes);

            byte[] opKey =
                    Keys.operationPayloadKey(TABLE, commitId, OpType.MANIFEST_WRITE.wireName());
            store.put(opKey, "not-valid-protobuf".getBytes(StandardCharsets.UTF_8));

            Snapshot snap =
                    Snapshot.of(
                            snapshotId,
                            List.of(manifestId),
                            System.currentTimeMillis(),
                            "test",
                            "append",
                            null);
            when(catalog.getSnapshot(TABLE, snapshotId)).thenReturn(Optional.of(snap));

            List<DataFile> files = manager.listDataFiles(TABLE_ID_STR, snapshotId);
            assertTrue(files.isEmpty());
        }
    }

    static class InMemoryMetastore implements Metastore {
        private final ConcurrentHashMap<ByteArrayKey, byte[]> data = new ConcurrentHashMap<>();
        private final ConcurrentNavigableMap<ByteArrayKey, byte[]> sorted =
                new ConcurrentSkipListMap<>();

        @Override
        public Optional<byte[]> get(byte[] key) {
            byte[] v = data.get(new ByteArrayKey(key));
            if (v == null) return Optional.empty();
            return Optional.of(Arrays.copyOf(v, v.length));
        }

        @Override
        public void put(byte[] key, byte[] value) {
            ByteArrayKey bk = new ByteArrayKey(key);
            data.put(bk, Arrays.copyOf(value, value.length));
            sorted.put(bk, Arrays.copyOf(value, value.length));
        }

        @Override
        public void delete(byte[] key) {
            ByteArrayKey bk = new ByteArrayKey(key);
            data.remove(bk);
            sorted.remove(bk);
        }

        @Override
        public boolean exists(byte[] key) {
            return data.containsKey(new ByteArrayKey(key));
        }

        @Override
        public CloseableIterator<KeyValue> scan(byte[] prefix) {
            ByteArrayKey start = new ByteArrayKey(prefix);
            List<KeyValue> results = new ArrayList<>();
            for (Map.Entry<ByteArrayKey, byte[]> entry : sorted.tailMap(start).entrySet()) {
                if (!entry.getKey().startsWith(prefix)) break;
                results.add(new KeyValue(entry.getKey().bytes, entry.getValue()));
            }
            return CloseableIteratorAdapter.of(results.iterator());
        }

        @Override
        public CloseableIterator<KeyValue> scan(byte[] prefix, int limit) {
            return scan(prefix);
        }

        @Override
        public CloseableIterator<KeyValue> scanRange(byte[] startInclusive, byte[] endExclusive) {
            return scan(startInclusive);
        }

        @Override
        public CloseableIterator<KeyValue> scanRange(
                byte[] startInclusive, byte[] endExclusive, int limit) {
            return scan(startInclusive);
        }

        @Override
        public boolean compareAndSwap(List<Precondition> preconditions, List<Mutation> mutations) {
            for (Precondition p : preconditions) {
                if (p instanceof Precondition.NotExists ne) {
                    if (data.containsKey(new ByteArrayKey(ne.key()))) return false;
                } else if (p instanceof Precondition.ValueEquals ve) {
                    byte[] existing = data.get(new ByteArrayKey(ve.key()));
                    if (existing == null || !Arrays.equals(existing, ve.expected())) return false;
                } else if (p instanceof Precondition.Exists ex) {
                    if (!data.containsKey(new ByteArrayKey(ex.key()))) return false;
                }
            }
            for (Mutation m : mutations) {
                if (m instanceof Mutation.Put put) {
                    put(put.key(), put.value());
                } else if (m instanceof Mutation.Delete del) {
                    delete(del.key());
                }
            }
            return true;
        }

        @Override
        public void batchWrite(List<Mutation> mutations) {
            for (Mutation m : mutations) {
                if (m instanceof Mutation.Put put) put(put.key(), put.value());
                else if (m instanceof Mutation.Delete del) delete(del.key());
            }
        }

        @Override
        public void close() {
            data.clear();
            sorted.clear();
        }

        record ByteArrayKey(byte[] bytes) implements Comparable<ByteArrayKey> {
            @Override
            public boolean equals(Object o) {
                return o instanceof ByteArrayKey that && Arrays.equals(bytes, that.bytes);
            }

            @Override
            public int hashCode() {
                return Arrays.hashCode(bytes);
            }

            @Override
            public int compareTo(ByteArrayKey other) {
                return Arrays.compare(bytes, other.bytes);
            }

            boolean startsWith(byte[] prefix) {
                if (bytes.length < prefix.length) return false;
                for (int i = 0; i < prefix.length; i++) if (bytes[i] != prefix[i]) return false;
                return true;
            }
        }
    }

    private static class CloseableIteratorAdapter implements CloseableIterator<KeyValue> {
        private final Iterator<KeyValue> delegate;

        static CloseableIteratorAdapter of(Iterator<KeyValue> it) {
            return new CloseableIteratorAdapter(it);
        }

        private CloseableIteratorAdapter(Iterator<KeyValue> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public KeyValue next() {
            return delegate.next();
        }

        @Override
        public void close() {}
    }
}
