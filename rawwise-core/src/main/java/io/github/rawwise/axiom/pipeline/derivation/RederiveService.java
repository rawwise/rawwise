package io.github.rawwise.axiom.pipeline.derivation;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.PostWriteHook;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.DefaultRecord;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Types;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class RederiveService {

    private final DynamicMmapStore store;

    public RederiveService(DynamicMmapStore store) {
        this.store = store;
    }

    public RederiveResult rederive(
            String tableId,
            Scope scope,
            byte[] startKey,
            byte[] endKey,
            byte[] singleKey,
            boolean forceReindex) {
        if (scope == Scope.KEY_RANGE && startKey == null && endKey == null) {
            throw new IllegalArgumentException(
                    "KEY_RANGE scope requires at least one of startKey or endKey to be non-null");
        }
        if (scope == Scope.SINGLE_KEY && (singleKey == null || singleKey.length == 0)) {
            throw new IllegalArgumentException(
                    "SINGLE_KEY scope requires non-null non-empty singleKey");
        }

        if (!store.isTableAiNative(tableId)) {
            store.setTableAiNative(tableId, true);
        }
        store.applyTablePropertiesFromMetastore(tableId);
        store.registerAutoDeriveHook(tableId);

        DerivationEngine derivationEngine = store.getDerivationEngine();
        TableStore ts = store.tableStore(tableId);
        List<PostWriteHook> hooks = ts.getPostWriteHooks();

        RederiveContext ctx =
                new RederiveContext(forceReindex, derivationEngine, ts, hooks, tableId);
        forEachKey(tableId, scope, startKey, endKey, singleKey, ctx);

        String status = ctx.failedCount == 0 ? "OK" : "PARTIAL";
        log.info(
                "Rederive completed: table={} scope={} total={} rederived={} skipped={} failed={} status={}",
                tableId,
                scope,
                ctx.totalKeys,
                ctx.rederivedCount,
                ctx.skippedCount,
                ctx.failedCount,
                status);
        return new RederiveResult(
                ctx.totalKeys, ctx.rederivedCount, ctx.skippedCount, ctx.failedCount, status);
    }

    private void forEachKey(
            String tableId,
            Scope scope,
            byte[] startKey,
            byte[] endKey,
            byte[] singleKey,
            RederiveContext ctx) {
        switch (scope) {
            case SINGLE_KEY -> {
                if (singleKey != null && singleKey.length > 0) {
                    ctx.process(singleKey);
                }
            }
            case KEY_RANGE -> scanKeysInRange(tableId, startKey, endKey, ctx);
            case FULL_TABLE -> scanAllKeys(tableId, ctx);
        }
    }

    private void scanAllKeys(String tableId, RederiveContext ctx) {
        for (LmdbInstance instance : store.instancePool().getAllInstances()) {
            String instanceTable = store.instancePool().findTableIdentifier(instance.instanceId());
            if (!tableId.equals(instanceTable)) {
                continue;
            }
            List<byte[]> keys = collectInstanceKeys(instance, null, null);
            for (byte[] keyBytes : keys) {
                ctx.process(keyBytes);
            }
        }
    }

    private void scanKeysInRange(
            String tableId, byte[] startKey, byte[] endKey, RederiveContext ctx) {
        for (LmdbInstance instance : store.instancePool().getAllInstances()) {
            String instanceTable = store.instancePool().findTableIdentifier(instance.instanceId());
            if (!tableId.equals(instanceTable)) {
                continue;
            }
            List<byte[]> keys = collectInstanceKeys(instance, startKey, endKey);
            for (byte[] keyBytes : keys) {
                ctx.process(keyBytes);
            }
        }
    }

    private List<byte[]> collectInstanceKeys(
            LmdbInstance instance, byte[] startKey, byte[] endKey) {
        List<byte[]> keys = new ArrayList<>();
        try (CloseableIterator<Pair<byte[], byte[]>> iter = instance.closeableIterator()) {
            while (iter.hasNext()) {
                Pair<byte[], byte[]> kv = iter.next();
                byte[] key = kv.getKey();
                if (startKey != null && compareBytes(key, startKey) < 0) {
                    continue;
                }
                if (endKey != null && compareBytes(key, endKey) > 0) {
                    continue;
                }
                keys.add(key);
            }
        } catch (Exception e) {
            log.warn(
                    "Failed to iterate keys from instance {}: {}",
                    instance.instanceId(),
                    e.getMessage(),
                    e);
        }
        return keys;
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.compare(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(a.length, b.length);
    }

    private static final class RederiveContext {
        final boolean forceReindex;
        final DerivationEngine derivationEngine;
        final TableStore ts;
        final List<PostWriteHook> hooks;
        final String tableId;

        int totalKeys;
        int rederivedCount;
        int skippedCount;
        int failedCount;

        RederiveContext(
                boolean forceReindex,
                DerivationEngine derivationEngine,
                TableStore ts,
                List<PostWriteHook> hooks,
                String tableId) {
            this.forceReindex = forceReindex;
            this.derivationEngine = derivationEngine;
            this.ts = ts;
            this.hooks = hooks;
            this.tableId = tableId;
        }

        void process(byte[] keyBytes) {
            totalKeys++;
            try {
                byte[] rawValue = ts.getRawValue(keyBytes);
                if (rawValue == null || NullableValueCodec.isTombstone(rawValue)) {
                    log.debug("Rederive skipping tombstone/missing key for table={}", tableId);
                    failedCount++;
                    return;
                }
                byte[] decoded = NullableValueCodec.decode(rawValue);
                if (decoded == null) {
                    failedCount++;
                    return;
                }

                Record.Key key = Records.keyOf(keyBytes);

                if (!forceReindex
                        && derivationEngine != null
                        && derivationEngine.isChunkDerived(key)) {
                    skippedCount++;
                    return;
                }

                if (forceReindex && derivationEngine != null) {
                    derivationEngine.clearAllDeriveIntents(key);
                    derivationEngine.deleteChunks(key);
                }

                Schema schema = Schema.builder().field("body", Types.binary(true)).build();
                Record.Body body = new DefaultBody(schema, new Object[] {decoded});
                Record.Header header = Record.Header.of(key, 0, 0, 0, 0, Record.Kind.INSERT, true);
                Record record = new DefaultRecord(key, body, header);

                for (PostWriteHook hook : hooks) {
                    try {
                        hook.afterWrite(record, tableId);
                    } catch (Exception e) {
                        log.error(
                                "Rederive PostWriteHook failed for table={} hook={}: {}",
                                tableId,
                                hook.getClass().getSimpleName(),
                                e.getMessage(),
                                e);
                    }
                }
                rederivedCount++;
            } catch (Exception e) {
                log.error("Rederive failed for key in table={}: {}", tableId, e.getMessage(), e);
                failedCount++;
            }
        }
    }

    public enum Scope {
        FULL_TABLE,
        KEY_RANGE,
        SINGLE_KEY
    }

    public record RederiveResult(
            int totalKeys, int rederivedCount, int skippedCount, int failedCount, String status) {}
}
