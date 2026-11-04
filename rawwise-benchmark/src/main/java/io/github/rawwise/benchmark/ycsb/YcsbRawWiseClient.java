package io.github.rawwise.benchmark.ycsb;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.RoutingOwnerContext;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.HotDataBlock;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.engine.storage.table.read.LmdbTableReader;
import io.github.rawwise.engine.txn.SimpleTableRef;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.KvWriteBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class YcsbRawWiseClient implements YcsbClient {

    private static final String TABLE_NAME = "ycsb_usertable";
    private static final String COL_ID = "id";
    private static final int SCAN_LENGTH = 10;

    private final DynamicMmapStore store;
    private final RoutingOwnerContext ownerContext;

    public YcsbRawWiseClient(DynamicMmapStore store) {
        this.store = store;
        this.ownerContext = new RoutingOwnerContext("ycsb-exec", 1);
    }

    public void init(int recordCount) throws IOException {
        TableStore ts = store.tableStore(TABLE_NAME, List.of(COL_ID));
        KeyCodec keyCodec = ts.getKeyCodec();
        for (int i = 0; i < recordCount; i++) {
            String key = "user" + i;
            String value = generateValue(i);
            byte[] encodedKey = keyCodec.encodePrimaryKey(Map.of(COL_ID, (Comparable) (Integer) i));
            KvWriteBuilder builder =
                    KvWriteBuilder.builder()
                            .table(new SimpleTableRef(TABLE_NAME))
                            .key(encodedKey)
                            .value(value.getBytes(StandardCharsets.UTF_8))
                            .partitionHint(0);
            store.write(builder, ownerContext);
        }
    }

    public String read(int key) throws IOException {
        Find.Result result = doFind(key);
        if (!result.success() || !result.foundRecord().isPresent()) {
            return null;
        }
        return decodeResult(result);
    }

    public void write(int key) throws IOException {
        String value = generateValue(key);
        TableStore ts = store.tableStore(TABLE_NAME);
        KeyCodec keyCodec = ts.getKeyCodec();
        byte[] encodedKey = keyCodec.encodePrimaryKey(Map.of(COL_ID, (Comparable) (Integer) key));
        KvWriteBuilder builder =
                KvWriteBuilder.builder()
                        .table(new SimpleTableRef(TABLE_NAME))
                        .key(encodedKey)
                        .value(value.getBytes(StandardCharsets.UTF_8))
                        .partitionHint(0);
        store.write(builder, ownerContext);
    }

    public void insert(int key) throws IOException {
        write(key);
    }

    public void delete(int key) throws IOException {
        TableStore ts = store.tableStore(TABLE_NAME);
        KeyCodec keyCodec = ts.getKeyCodec();
        byte[] encodedKey = keyCodec.encodePrimaryKey(Map.of(COL_ID, (Comparable) (Integer) key));
        store.delete(TABLE_NAME, encodedKey, ownerContext);
    }

    public int scan(int startKey) throws IOException {
        int count = 0;
        for (int i = 0; i < SCAN_LENGTH; i++) {
            int k = startKey + i;
            Find.Result result = doFind(k);
            if (result.success() && result.foundRecord().isPresent()) {
                count++;
            }
        }
        return count;
    }

    public void close() throws Exception {
        store.close();
    }

    private Find.Result doFind(int key) throws IOException {
        TableStore ts = store.tableStore(TABLE_NAME);
        LmdbTableReader reader = new LmdbTableReader(ts);
        List<LmdbInstance> instances = ts.getAllInstancesDescending();
        String blockId = instances.isEmpty() ? "inst-dummy" : instances.getFirst().instanceId();
        DataBlock dataBlock =
                new HotDataBlock(
                        blockId,
                        TABLE_NAME,
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
                        .withTable(new SimpleTableRef(TABLE_NAME))
                        .withPrimaryKey(Map.of(COL_ID, (Comparable) (Integer) key))
                        .withDataBlocks(List.of(dataBlock))
                        .withReader(reader)
                        .build();
        return (Find.Result) store.read(find);
    }

    private static String decodeResult(Find.Result result) {
        byte[] raw = result.rawNullableValue();
        if (raw == null) return null;
        byte[] decoded = NullableValueCodec.decode(raw);
        if (decoded == null) return null;
        return new String(decoded, StandardCharsets.UTF_8);
    }

    private static String generateValue(int i) {
        return "field0-"
                + i
                + ",field1-"
                + (i * 2)
                + ",field2-"
                + (i * 3)
                + ",field3-"
                + (i * 4)
                + ",field4-"
                + (i * 5);
    }
}
