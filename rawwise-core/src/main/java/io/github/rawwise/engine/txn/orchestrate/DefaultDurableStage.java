package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.spi.FaultInjector;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.txn.TxnMode;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.engine.txn.wal.TxnWALWriter;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.CRC32C;
import lombok.extern.slf4j.Slf4j;

/**
 * DurableStage default implementation.
 *
 * <p>can inject to support fault injection test.
 */
@Slf4j
public class DefaultDurableStage implements DurableStage {

    private final FaultInjector faultInjector;
    private final boolean encodeValues;

    public DefaultDurableStage() {
        this(null, false);
    }

    public DefaultDurableStage(boolean encodeValues) {
        this(null, encodeValues);
    }

    public DefaultDurableStage(FaultInjector faultInjector) {
        this(faultInjector, false);
    }

    public DefaultDurableStage(FaultInjector faultInjector, boolean encodeValues) {
        this.faultInjector = faultInjector != null ? faultInjector : FaultInjector.NOOP;
        this.encodeValues = encodeValues;
    }

    @Override
    public DurableStage.DurableResult execute(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            LmdbInstance lmdbInstance,
            TxnMode txnMode,
            TxnWALWriter walWriter) {
        if (writeIntents == null || writeIntents.isEmpty()) {
            byte[] emptyDigest = computeSealedDigest(List.of());
            return DurableStage.DurableResult.of(emptyDigest, 0, 0, false);
        }

        // Step 1: compute sealedDigest CRC32(compute digest before all data write)
        byte[] sealedDigest = computeSealedDigest(writeIntents);

        // Step 2: prepare batchPut data
        List<byte[]> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();

        Map<String, List<Integer>> tableIndices = new HashMap<>();
        for (int i = 0; i < writeIntents.size(); i++) {
            TxnWALRecord.TxnWriteIntent intent = writeIntents.get(i);
            if (intent.writeType() == TxnWALRecord.WriteType.DELETE) {
                keys.add(intent.keyBytes());
                values.add(NullableValueCodec.encodeTombstone(ctx.txnId()));
            } else {
                keys.add(intent.keyBytes());
                byte[] rawValue = intent.valueRef() != null ? intent.valueRef() : new byte[0];
                values.add(encodeValueForLmdb(intent.keyBytes(), rawValue, ctx.txnId()));
            }
            tableIndices.computeIfAbsent(intent.primaryTableId(), k -> new ArrayList<>()).add(i);
        }

        // Step 3: WAL TXN_PREPARE_AND_DURABLE first (WAL-ahead, INV-002/CCI-006)
        // Step 4: LMDB batchPut + CommitMarker (INV-006)
        // TODO: wrap batchPut + CommitMarker in single LMDB write transaction
        boolean commitMarkerWritten = false;
        try {
            if (lmdbInstance != null && !lmdbInstance.readOnly()) {
                // WAL-ahead: write PREPARE_AND_DURABLE before data hits disk
                if (walWriter != null) {
                    walWriter.writePrepareAndDurable(
                            new TxnWALRecord.TxnPrepareAndDurable(
                                    ctx.txnId(),
                                    sealedDigest, // writeSetDigest
                                    List.of(), // stagingRefs (placeholder)
                                    sealedDigest, // sealedDigest
                                    System.currentTimeMillis(),
                                    keys // writtenKeys
                                    ));
                    log.debug("DurableStage: txn={} WAL PREPARE_AND_DURABLE written", ctx.txnId());
                }

                faultInjector.inject(
                        FaultInjector.InjectionPoint.AFTER_WAL_WRITE_BEFORE_LMDB_WRITE);

                LmdbInstance.BatchPutResult batchResult =
                        lmdbInstance.batchPutWithMarker(
                                keys,
                                values,
                                ("__commit_marker__:" + ctx.txnId())
                                        .getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                new byte[] {1});
                if (batchResult != null) {
                    log.info(
                            "DurableStage: txn={} batchPutWithMarker {} records (atomic data+marker), status={}",
                            ctx.txnId(),
                            batchResult.writtenCount(),
                            batchResult.status());
                } else {
                    log.info(
                            "DurableStage: txn={} batchPutWithMarker returned null (mock or no-op)",
                            ctx.txnId());
                }

                faultInjector.inject(
                        FaultInjector.InjectionPoint.AFTER_LMDB_TXN_COMMIT_BEFORE_MARKER_PERSIST);

                commitMarkerWritten = true;
                log.info(
                        "DurableStage: txn={} CommitMarker written atomically with data",
                        ctx.txnId());
            } else {
                log.warn(
                        "DurableStage: txn={} LMDB instance is read-only or null, skip batchPut",
                        ctx.txnId());
            }
        } catch (Exception e) {
            log.error("DurableStage: txn={} failed", ctx.txnId(), e);
            // TODO: if batchPut fails after WAL write, recovery path handles via RecoverStage
            throw new RuntimeException("DurableStage failed for txn=" + ctx.txnId(), e);
        }

        long totalBytes =
                keys.stream().mapToLong(k -> k.length).sum()
                        + values.stream().mapToLong(v -> v.length).sum();

        return DurableStage.DurableResult.of(
                sealedDigest, keys.size(), totalBytes, commitMarkerWritten);
    }

    /**
     * multi-table persistence implementation.
     *
     * <p>by {@code intent.primaryTableId()} group, independently call batchPutWithMarker for each
     * table. anytablewritefail → throwexception, caller responsible for rollback of already written
     * tables.
     */
    @Override
    public DurableStage.DurableResult executeMultiTable(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            Function<String, LmdbInstance> tableLmdbResolver,
            TxnMode txnMode,
            TxnWALWriter walWriter) {
        if (writeIntents == null || writeIntents.isEmpty()) {
            byte[] emptyDigest = computeSealedDigest(List.of());
            return DurableStage.DurableResult.of(emptyDigest, 0, 0, false);
        }
        if (tableLmdbResolver == null) {
            throw new IllegalArgumentException("tableLmdbResolver must not be null");
        }

        // Step 1: group by primaryTableId(maintain intra-group order)
        Map<String, List<TxnWALRecord.TxnWriteIntent>> grouped = new LinkedHashMap<>();
        for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
            grouped.computeIfAbsent(intent.primaryTableId(), k -> new ArrayList<>()).add(intent);
        }

        // Step 2: global sealedDigest(cross-table, by key lexicographic)
        byte[] sealedDigest = computeSealedDigest(writeIntents);

        // Step 3: collect all keys for WAL PREPARE_AND_DURABLE
        List<byte[]> allKeys = new ArrayList<>();
        for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
            allKeys.add(intent.keyBytes());
        }

        // Step 4: WAL PREPARE_AND_DURABLE (single WAL, override all tables)
        if (walWriter != null) {
            walWriter.writePrepareAndDurable(
                    new TxnWALRecord.TxnPrepareAndDurable(
                            ctx.txnId(),
                            sealedDigest,
                            List.of(),
                            sealedDigest,
                            System.currentTimeMillis(),
                            allKeys));
            log.debug(
                    "DurableStage(multi-table): txn={} WAL PREPARE_AND_DURABLE written for {} tables",
                    ctx.txnId(),
                    grouped.size());
        }

        faultInjector.inject(FaultInjector.InjectionPoint.AFTER_WAL_WRITE_BEFORE_LMDB_WRITE);

        // Step 5: dispatch batchPutWithMarker by tableId
        // anytablefail → throwexception; successful tables' rollback handled by TxnOrchestratorImpl
        // in catch
        // (rollbackPendingInstances + recover stage reads Metastore TxnCommitRecord to decide
        // replay or rollback)
        long totalBytes = 0;
        int totalCount = 0;
        boolean allMarkersWritten = true;
        List<String> writtenTables = new ArrayList<>();

        try {
            for (Map.Entry<String, List<TxnWALRecord.TxnWriteIntent>> entry : grouped.entrySet()) {
                String tableId = entry.getKey();
                List<TxnWALRecord.TxnWriteIntent> tableIntents = entry.getValue();
                LmdbInstance lmdbInstance = tableLmdbResolver.apply(tableId);
                if (lmdbInstance == null) {
                    throw new IllegalStateException(
                            "tableLmdbResolver returned null for tableId=" + tableId);
                }
                if (lmdbInstance.readOnly()) {
                    log.warn(
                            "DurableStage(multi-table): txn={} table={} LMDB read-only, skip",
                            ctx.txnId(),
                            tableId);
                    allMarkersWritten = false;
                    continue;
                }

                List<byte[]> keys = new ArrayList<>();
                List<byte[]> values = new ArrayList<>();
                for (TxnWALRecord.TxnWriteIntent intent : tableIntents) {
                    keys.add(intent.keyBytes());
                    if (intent.writeType() == TxnWALRecord.WriteType.DELETE) {
                        values.add(NullableValueCodec.encodeTombstone(ctx.txnId()));
                    } else {
                        byte[] rawValue =
                                intent.valueRef() != null ? intent.valueRef() : new byte[0];
                        values.add(encodeValueForLmdb(intent.keyBytes(), rawValue, ctx.txnId()));
                    }
                }

                LmdbInstance.BatchPutResult batchResult =
                        lmdbInstance.batchPutWithMarker(
                                keys,
                                values,
                                ("__commit_marker__:" + ctx.txnId())
                                        .getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                new byte[] {1});
                writtenTables.add(tableId);
                if (batchResult != null) {
                    totalCount += batchResult.writtenCount();
                }
                totalBytes +=
                        keys.stream().mapToLong(k -> k.length).sum()
                                + values.stream().mapToLong(v -> v.length).sum();

                if (batchResult != null) {
                    log.info(
                            "DurableStage(multi-table): txn={} table={} marker+data written ({} records, status={})",
                            ctx.txnId(),
                            tableId,
                            batchResult.writtenCount(),
                            batchResult.status());
                } else {
                    log.info(
                            "DurableStage(multi-table): txn={} table={} batchPutWithMarker returned null",
                            ctx.txnId(),
                            tableId);
                }

                faultInjector.inject(
                        FaultInjector.InjectionPoint.AFTER_LMDB_TXN_COMMIT_BEFORE_MARKER_PERSIST);
            }
        } catch (Exception e) {
            log.error(
                    "DurableStage(multi-table): txn={} failed after writing tables={}",
                    ctx.txnId(),
                    writtenTables,
                    e);
            throw new RuntimeException(
                    "DurableStage(multi-table) failed for txn="
                            + ctx.txnId()
                            + ", writtenTables="
                            + writtenTables,
                    e);
        }

        return DurableStage.DurableResult.of(
                sealedDigest, totalCount, totalBytes, allMarkersWritten);
    }

    private static final byte[] REC_MAGIC = {0x52, 0x45, 0x43};
    private static final byte REC_VERSION = 0x10;
    private static final int REC_FLAG_HAS_KEY = 0x02;
    private static final int REC_FLAG_HAS_IMMUTABLE = 0x20;

    /**
     * Encode raw value bytes into RecordCodec format with commitTimestamp, then wrap with
     * NullableValueCodec for LMDB storage.
     */
    private byte[] encodeValueForLmdb(byte[] key, byte[] value, long commitTs) {
        if (!encodeValues) {
            return value != null ? value : new byte[0];
        }
        byte[] recordBytes = encodeRecordCodec(key, value, commitTs);
        return NullableValueCodec.encode(recordBytes);
    }

    /** Create minimal RecordCodec-encoded bytes with commitTimestamp in header. */
    private static byte[] encodeRecordCodec(byte[] key, byte[] value, long commitTs) {
        int flags = REC_FLAG_HAS_KEY | REC_FLAG_HAS_IMMUTABLE;
        int bodySize = (value != null) ? value.length : 0;
        int headerSize =
                DefaultRecordCodec.MAGIC_LEN
                        + DefaultRecordCodec.VERSION_LEN
                        + DefaultRecordCodec.FLAGS_LEN
                        + DefaultRecordCodec.KEY_LEN_FIELD_SIZE
                        + key.length
                        + 8
                        + 8
                        + 8
                        + 8
                        + 1
                        + 1
                        + 1
                        + 1;
        int total = headerSize + bodySize + 8;

        ByteBuffer buf = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        buf.put(REC_MAGIC);
        buf.put(REC_VERSION);
        buf.put((byte) flags);
        buf.putInt(key.length);
        buf.put(key);
        buf.putLong(0L); // tableId
        buf.putLong(0L); // schemaVersion
        buf.putLong(0L); // sequenceNumber
        buf.putLong(commitTs); // commitTimestamp
        buf.put((byte) Record.Kind.INSERT.ordinal());
        buf.put((byte) 1); // visible
        buf.put((byte) 0); // immutable
        buf.put((byte) 0); // immutableSource DEFAULT
        if (value != null) buf.put(value);

        long checksum =
                net.jpountz.xxhash.XXHashFactory.fastestInstance()
                        .hash64()
                        .hash(buf.array(), 0, buf.position(), 0x97C3BE15L);
        buf.putLong(checksum);

        return Arrays.copyOf(buf.array(), buf.position());
    }

    /**
     * compute sealedDigest: CRC32C digest of entire write set. sort by key to ensure
     * determinism(HashMap iteration order unpredictable).
     */
    private static byte[] computeSealedDigest(List<TxnWALRecord.TxnWriteIntent> intents) {
        List<TxnWALRecord.TxnWriteIntent> sorted = new ArrayList<>(intents);
        sorted.sort(Comparator.comparing(i -> ByteBuffer.wrap(i.keyBytes())));
        CRC32C crc = new CRC32C();
        for (TxnWALRecord.TxnWriteIntent intent : sorted) {
            crc.update(intent.keyBytes());
            // Always CRC value — DELETE uses empty array for consistency with LMDB tombstone
            crc.update(intent.valueRef() != null ? intent.valueRef() : new byte[0]);
        }
        long value = crc.getValue();
        return new byte[] {
            (byte) (value >> 56),
            (byte) (value >> 48),
            (byte) (value >> 40),
            (byte) (value >> 32),
            (byte) (value >> 24),
            (byte) (value >> 16),
            (byte) (value >> 8),
            (byte) value
        };
    }
}
