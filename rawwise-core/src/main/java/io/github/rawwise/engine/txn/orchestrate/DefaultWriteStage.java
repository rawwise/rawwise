package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.txn.TxnMode;
import io.github.rawwise.engine.txn.WriteBuffer;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * WriteStage default implementation.
 *
 * <p>can inject to support fault injection test.
 */
@Slf4j
public class DefaultWriteStage implements WriteStage {

    public DefaultWriteStage() {
        /* no dependencies for now */
    }

    @Override
    public WriteResult execute(
            TxnContext ctx, TxnMode txnMode, Map<String, Map<ByteBuffer, Long>> baseVersions) {
        WriteBuffer buffer = ctx.writeBuffer();
        Set<String> tableIds = ctx.tableIds();
        long readSnapshotTs = ctx.readSnapshot() != null ? ctx.readSnapshot().snapshotTs() : 0;

        List<TxnWALRecord.TxnWriteIntent> intents = new ArrayList<>();
        int operationSeq = 0;

        for (String tableId : tableIds) {
            Map<ByteBuffer, WriteBuffer.BufferEntry> entries = buffer.drain(tableId);
            if (entries.isEmpty()) {
                continue;
            }

            Map<ByteBuffer, Long> tableBaseVersions =
                    baseVersions != null ? baseVersions.getOrDefault(tableId, Map.of()) : Map.of();

            for (Map.Entry<ByteBuffer, WriteBuffer.BufferEntry> entry : entries.entrySet()) {
                byte[] keyBytes = toByteArray(entry.getKey());
                WriteBuffer.BufferEntry be = entry.getValue();

                TxnWALRecord.WriteType writeType =
                        switch (be.type()) {
                            case NEW_VALUE -> TxnWALRecord.WriteType.UPSERT;
                            case DELETED -> TxnWALRecord.WriteType.DELETE;
                        };

                byte[] valueRef = be.getValue();

                long crc32c = computeCRC32(keyBytes, valueRef);

                // Look up baseVersion from read cache (captured during pointRead for OCC)
                long baseVersion = tableBaseVersions.getOrDefault(entry.getKey(), 0L);

                TxnWALRecord.TxnWriteIntent intent =
                        new TxnWALRecord.TxnWriteIntent(
                                ctx.txnId(),
                                tableIds,
                                // use this iteration's tableId(buffer actual owned table),
                                // instead of ctx.primaryTableId().multi-table transaction different
                                // intents require their respective tableId
                                // so DurableStage.executeMultiTable dispatches to correct LMDB by
                                // primaryTableId.
                                tableId,
                                0, // shardId
                                readSnapshotTs,
                                ctx.isolationLevel(),
                                operationSeq++,
                                keyBytes,
                                valueRef,
                                writeType,
                                baseVersion,
                                extractSchemaVersion(valueRef), // schemaVersion
                                crc32c);
                intents.add(intent);

                log.debug(
                        "WriteStage: txn={} table={} keySize={} writeType={} baseVersion={}",
                        ctx.txnId(),
                        tableId,
                        keyBytes.length,
                        writeType,
                        baseVersion);
            }
        }

        if (intents.isEmpty()) {
            log.debug("WriteStage: txn={} no writes to persist", ctx.txnId());
        } else {
            log.info("WriteStage: txn={} intents={} mode={}", ctx.txnId(), intents.size(), txnMode);
        }

        return WriteResult.of(intents);
    }

    private static byte[] toByteArray(java.nio.ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    private static long computeCRC32(byte[] key, byte[] value) {
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(key);
        if (value != null) {
            crc.update(value);
        }
        return crc.getValue();
    }

    private static int extractSchemaVersion(byte[] valueRef) {
        if (valueRef == null || valueRef.length == 0) return 0;
        if (NullableValueCodec.isTombstone(valueRef)) return 0;
        try {
            byte[] candidate = NullableValueCodec.decode(valueRef);
            if (candidate == null || candidate.length == 0) return 0;
            var header = DefaultRecordCodec.extractHeaderStatic(candidate);
            return header != null ? (int) header.schemaVersion() : 0;
        } catch (Exception e) {
            return 0;
        }
    }
}
