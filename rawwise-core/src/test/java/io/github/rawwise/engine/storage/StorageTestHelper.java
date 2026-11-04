package io.github.rawwise.engine.storage;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.HotDataBlock;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.storage.table.read.LmdbTableReader;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.KvWriteBuilder;
import io.github.rawwise.model.record.DynamicTableInfo;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableMeta;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public final class StorageTestHelper {

    private StorageTestHelper() {}

    public static class TestTable implements RawWiseTable {
        private final String identifier;

        public TestTable(String identifier) {
            this.identifier = identifier;
        }

        @Override
        public String uniqId() {
            return identifier;
        }

        @Override
        public String name() {
            return identifier;
        }

        @Override
        public String getTableIdentifier() {
            return identifier;
        }

        @Override
        public void close() {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void refresh() {}

        @Override
        public TableMeta meta() {
            return null;
        }

        @Override
        public DynamicTableInfo dynamicInfo() {
            return null;
        }

        @Override
        public Snapshot currentSnapshot() {
            return null;
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            return null;
        }

        @Override
        public List<Snapshot> snapshots() {
            return List.of();
        }
    }

    public static final RoutingOwnerContext TEST_OWNER = new RoutingOwnerContext("exec-test", 1);

    public static void writeKv(
            DynamicMmapStore store,
            String tableIdentifier,
            Map<String, Comparable> pkValues,
            String value)
            throws IOException {
        TableStore ts = store.tableStore(tableIdentifier);
        KeyCodec keyCodec = ts.getKeyCodec();
        byte[] encodedKey = keyCodec.encodePrimaryKey(pkValues);
        KvWriteBuilder builder =
                KvWriteBuilder.builder()
                        .table(new TestTable(tableIdentifier))
                        .key(encodedKey)
                        .value(value.getBytes(StandardCharsets.UTF_8))
                        .partitionHint(0);
        store.write(builder, TEST_OWNER);
    }

    public static void deleteKey(
            DynamicMmapStore store, String tableIdentifier, Map<String, Comparable> pkValues)
            throws IOException {
        TableStore ts = store.tableStore(tableIdentifier);
        KeyCodec keyCodec = ts.getKeyCodec();
        byte[] encodedKey = keyCodec.encodePrimaryKey(pkValues);
        store.delete(tableIdentifier, encodedKey, TEST_OWNER);
    }

    public static Find.Result findKey(
            DynamicMmapStore store, String tableIdentifier, Map<String, Comparable> pkValues)
            throws IOException {
        TableStore ts = store.tableStore(tableIdentifier);
        LmdbTableReader reader = new LmdbTableReader(ts);
        List<LmdbInstance> instances = ts.getAllInstancesDescending();
        String blockId = instances.isEmpty() ? "inst-dummy" : instances.getFirst().instanceId();
        DataBlock dataBlock =
                new HotDataBlock(
                        blockId,
                        tableIdentifier,
                        DataBlock.Kind.MUTABLE_HOT,
                        DataBlock.Format.LMDB,
                        null,
                        null,
                        0,
                        0,
                        0,
                        DataBlock.Visibility.ACTIVE,
                        0,
                        0,
                        0,
                        0,
                        null);
        Find find =
                Find.builder()
                        .withTable(new TestTable(tableIdentifier))
                        .withPrimaryKey(pkValues)
                        .withDataBlocks(List.of(dataBlock))
                        .withReader(reader)
                        .build();
        return (Find.Result) store.read(find);
    }

    public static String decodeValue(Find.Result result) {
        byte[] raw = result.rawNullableValue();
        if (raw == null) return null;
        byte[] decoded = NullableValueCodec.decode(raw);
        if (decoded == null) return null;
        return new String(decoded, StandardCharsets.UTF_8);
    }
}
