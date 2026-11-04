package io.github.rawwise.engine.storage.block.read;

import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.QueryCapability;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.codec.BodyCodec;
import io.github.rawwise.engine.storage.codec.RecordDecoder;
import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.engine.storage.table.RangeIndex;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Scan;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Records;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

public class LmdbDataBlockReader extends AbstractReader implements DataBlockReader {
    private final TableStore tableStore;
    private final KeyCodec keyCodec;
    private volatile BodyCodec bodyCodec;
    private volatile RecordCodec.SchemaResolver schemaResolver;

    public LmdbDataBlockReader(TableStore tableStore) {
        this(tableStore, tableStore.getKeyCodec());
    }

    public LmdbDataBlockReader(TableStore tableStore, KeyCodec keyCodec) {
        this.tableStore = tableStore;
        this.keyCodec = keyCodec;
    }

    /** Configure BodyCodec and SchemaResolver for projection-aware decode. */
    public void setBodyCodec(BodyCodec bodyCodec, RecordCodec.SchemaResolver schemaResolver) {
        this.bodyCodec = bodyCodec;
        this.schemaResolver = schemaResolver;
    }

    @Override
    public boolean supports(DataBlock block) {
        return block.layer() == DataBlock.Layer.HOT && block.format() == DataBlock.Format.LMDB;
    }

    @Override
    public Find.Result find(Find find, List<DataBlock> blocks) throws IOException {
        ensureOpen();
        byte[] encodedKey = keyCodec.encodePrimaryKey(find.primaryKey().values());
        RecordCodec recordCodec = tableStore.getRecordCodec();
        RecordCodec.SchemaResolver resolver = resolveSchemaResolver();

        List<LmdbInstance> instances;
        if (blocks == null || blocks.isEmpty()) {
            instances = resolveInstancesWithRangeIndex(encodedKey);
        } else {
            instances = resolveInstances(blocks);
        }

        for (LmdbInstance instance : instances) {
            byte[] raw = instance.get(encodedKey);
            if (raw != null) {
                RecordDecoder.DecodeResult decodeResult =
                        RecordDecoder.decodeNullableWrapped(raw, recordCodec, resolver);
                if (decodeResult instanceof RecordDecoder.DecodeResult.Found f) {
                    Record record = f.record();
                    try {
                        if (find.filter() != null && !find.filter().evaluate(record)) {
                            record.body().evict();
                            continue;
                        }
                        return Find.Result.found(record, raw);
                    } catch (Exception e) {
                        record.body().evict();
                        throw e;
                    }
                }
                continue;
            }
        }
        return Find.Result.notFound();
    }

    @Override
    public Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException {
        ensureOpen();
        List<LmdbInstance> instances = resolveInstances(blocks, scan.keyRange());
        LinkedHashMap<ByteBuffer, ScanEntry> latestEntries = new LinkedHashMap<>();
        RecordCodec recordCodec = tableStore.getRecordCodec();
        RecordCodec.SchemaResolver resolver = resolveSchemaResolver();

        for (LmdbInstance instance : instances) {
            try (var iterator = instance.closeableIterator()) {
                while (iterator.hasNext()) {
                    Pair<byte[], byte[]> pair = iterator.next();
                    byte[] key = pair.getLeft();
                    byte[] raw = pair.getRight();
                    ByteBuffer keyBuf = ByteBuffer.wrap(key).asReadOnlyBuffer();
                    if (!latestEntries.containsKey(keyBuf)) {
                        latestEntries.put(keyBuf, new ScanEntry(key, raw));
                    }
                }
            }
        }

        List<Record> rows = new ArrayList<>();
        List<String> projections = scan.projections();
        BodyCodec bc = this.bodyCodec;
        boolean useProjection = bc != null && projections != null && !projections.isEmpty();

        for (ScanEntry entry : latestEntries.values()) {
            RecordDecoder.DecodeResult decodeResult;
            if (useProjection) {
                decodeResult =
                        RecordDecoder.decodeNullableWrappedProjected(
                                entry.rawValue, recordCodec, resolver, bc, projections);
            } else {
                decodeResult =
                        RecordDecoder.decodeNullableWrapped(entry.rawValue, recordCodec, resolver);
            }
            if (!(decodeResult instanceof RecordDecoder.DecodeResult.Found f)) {
                continue;
            }
            Record record = f.record();
            try {
                if ((scan.keyRange() == null || scan.keyRange().contains(entry.key))
                        && matchesRecordPredicate(record, scan)) {
                    if (!useProjection && projections != null && !projections.isEmpty()) {
                        Record projected = Records.projectCopy(record, projections);
                        record.body().evict();
                        rows.add(projected);
                    } else {
                        rows.add(record);
                    }
                } else {
                    record.body().evict();
                }
            } catch (Exception e) {
                record.body().evict();
                throw e;
            }
        }
        return Scan.Result.ok(rows, false);
    }

    private RecordCodec.SchemaResolver resolveSchemaResolver() {
        RecordCodec.SchemaResolver sv = this.schemaResolver;
        if (sv != null) return sv;
        return tableStore.getSchemaResolver();
    }

    @Override
    public Set<QueryCapability> capabilities() {
        return Set.of(
                QueryCapability.POINT_LOOKUP,
                QueryCapability.RANGE_SCAN,
                QueryCapability.PREDICATE_PUSHDOWN,
                QueryCapability.LIMIT_PUSHDOWN,
                QueryCapability.PARTITION_PRUNING);
    }

    private record ScanEntry(byte[] key, byte[] rawValue) {}

    private List<LmdbInstance> resolveInstances(List<DataBlock> plannedBlocks) {
        return resolveInstances(plannedBlocks, null);
    }

    private List<LmdbInstance> resolveInstances(List<DataBlock> plannedBlocks, KeyRange keyRange) {
        if (plannedBlocks == null || plannedBlocks.isEmpty()) {
            if (keyRange == null) {
                return tableStore.getAllInstancesDescending();
            }
            return filterByKeyRange(tableStore.getAllInstancesDescending(), keyRange);
        }
        List<LmdbInstance> instances = new ArrayList<>();
        for (DataBlock block : plannedBlocks) {
            if (!supports(block)) {
                continue;
            }
            if (block.visibility() == DataBlock.Visibility.PENDING) {
                continue;
            }
            if (keyRange != null
                    && block.keyRange() != null
                    && !block.keyRange().overlaps(keyRange)) {
                continue;
            }
            LmdbInstance instance = tableStore.getInstance(block.blockId());
            if (instance != null) {
                instances.add(instance);
            }
        }
        instances.sort(Comparator.comparing(LmdbInstance::instanceId).reversed());
        return instances;
    }

    List<LmdbInstance> resolveInstancesWithRangeIndex(byte[] encodedKey) {
        RangeIndex rangeIndex = tableStore.getRangeIndex();
        List<LmdbInstance> writableInstances = tableStore.getWritableInstances();

        if (rangeIndex.isEmpty()) {
            List<LmdbInstance> all = tableStore.getAllInstancesDescending();
            return all;
        }

        List<String> candidateIds = rangeIndex.lookup(encodedKey);
        List<LmdbInstance> candidates = new ArrayList<>();

        for (LmdbInstance instance : writableInstances) {
            candidates.add(instance);
        }

        for (String instanceId : candidateIds) {
            LmdbInstance instance = tableStore.getInstance(instanceId);
            if (instance != null && !instance.readOnly()) {
                continue;
            }
            if (instance != null) {
                byte[] minKey = instance.minKey();
                byte[] maxKey = instance.maxKey();
                if (minKey != null && maxKey != null) {
                    KeyRange instanceRange = new KeyRange(minKey, true, maxKey, true);
                    if (instanceRange.contains(encodedKey)) {
                        candidates.add(instance);
                    }
                }
            }
        }

        candidates.sort(Comparator.comparing(LmdbInstance::instanceId).reversed());
        return candidates;
    }

    private List<LmdbInstance> filterByKeyRange(List<LmdbInstance> instances, KeyRange keyRange) {
        if (keyRange == null) {
            return instances;
        }
        return instances.stream()
                .filter(
                        instance -> {
                            KeyRange instanceRange =
                                    new KeyRange(instance.minKey(), true, instance.maxKey(), true);
                            return instanceRange.overlaps(keyRange);
                        })
                .toList();
    }

    private boolean matchesRecordPredicate(Record record, Scan scan) {
        if (scan.getPushedPredicate() == null) {
            return true;
        }
        return scan.getPushedPredicate().evaluate(record);
    }
}
