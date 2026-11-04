package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.engine.storage.RawWiseFile;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.serializer.ProtobufExpressionSerializer;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record UpdateLogEntry(
        String entryId,
        String tableId,
        Expression predicate,
        Map<String, Comparable<?>> updates,
        long transactionId,
        long sequenceNumber,
        long createdAtMillis)
        implements Serializable {

    @Serial private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UpdateLogEntry.class);

    public UpdateLogEntry(
            String entryId,
            String tableId,
            Expression predicate,
            Map<String, Comparable<?>> updates,
            long transactionId,
            long sequenceNumber,
            long createdAtMillis) {
        this.entryId = entryId;
        this.tableId = tableId;
        this.predicate = predicate;
        this.updates = (updates != null) ? new HashMap<>(updates) : new HashMap<>();
        this.transactionId = transactionId;
        this.sequenceNumber = sequenceNumber;
        this.createdAtMillis = createdAtMillis;
    }

    public static UpdateLogEntry fromUpdateEntry(UpdateEntry entry, long txnId, long seqNum) {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Map<String, Comparable<?>> updates = (Map) entry.getUpdates();
        return new UpdateLogEntry(
                entry.getEntryId(),
                entry.getTableName(),
                entry.getPredicate(),
                updates,
                txnId,
                seqNum,
                System.currentTimeMillis());
    }

    public static UpdateLogEntry fromUpdateLog(
            UpdateLog log, ProtobufExpressionSerializer serializer) {
        Expression predicate = null;
        if (log.serializedPredicate() != null && log.serializedPredicate().length > 0) {
            predicate = serializer.deserialize(log.serializedPredicate());
        }
        Map<String, Comparable<?>> updates = new HashMap<>();
        if (log.serializedUpdates() != null && log.serializedUpdates().length > 0) {
            try {
                updates = deserializeUpdates(log.serializedUpdates());
            } catch (IOException e) {
                LOG.warn(
                        "Failed to deserialize updates for entry={}, table={}, updates lost",
                        log.entryId(),
                        log.tableId(),
                        e);
                updates = new HashMap<>();
            }
        }
        return new UpdateLogEntry(
                log.entryId(),
                log.tableId(),
                predicate,
                updates,
                log.transactionId(),
                log.fromSequence(),
                log.createdAtMillis());
    }

    public UpdateLog toUpdateLog(
            String filePath,
            long snapshotId,
            long fromSeq,
            long toSeq,
            ProtobufExpressionSerializer serializer) {
        byte[] predicateBytes = null;
        byte[] updatesBytes = null;
        if (predicate != null) {
            predicateBytes = serializer.serialize(predicate);
        }
        if (updates != null && !updates.isEmpty()) {
            updatesBytes = serializeUpdates(updates);
        }
        return new UpdateLog(
                filePath,
                RawWiseFile.FileFormat.NWL,
                estimateSize(predicateBytes, updatesBytes),
                createdAtMillis,
                tableId,
                snapshotId,
                fromSeq,
                toSeq,
                predicateBytes,
                updatesBytes,
                transactionId,
                entryId);
    }

    @Override
    public Map<String, Comparable<?>> updates() {
        return Collections.unmodifiableMap(updates);
    }

    @Override
    public String toString() {
        return String.format(
                "UpdateLogEntry[entryId=%s, tableId=%s, txn=%d, seq=%d, predicate=%s, updates=%s]",
                entryId, tableId, transactionId, sequenceNumber, predicate, updates);
    }

    private long estimateSize(byte[] predicateBytes, byte[] updatesBytes) {
        long size = 0;
        if (predicateBytes != null) size += predicateBytes.length;
        if (updatesBytes != null) size += updatesBytes.length;
        return size;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Comparable<?>> deserializeUpdates(byte[] data) throws IOException {
        return deserializeUpdatesMap(data);
    }

    private static byte[] serializeUpdates(Map<String, Comparable<?>> updates) {
        return serializeUpdatesMap(updates);
    }

    public static byte[] serializeUpdatesDirect(Map<String, Comparable<?>> updates) {
        if (updates == null || updates.isEmpty()) return new byte[0];
        return serializeUpdatesMap(updates);
    }

    static byte[] serializeUpdatesMap(Map<String, Comparable<?>> updates) {
        if (updates == null || updates.isEmpty()) return new byte[0];
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(updates.size());
            for (Map.Entry<String, Comparable<?>> e : updates.entrySet()) {
                byte[] colBytes = e.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(colBytes.length);
                dos.write(colBytes);
                byte[] valBytes = String.valueOf(e.getValue()).getBytes(StandardCharsets.UTF_8);
                dos.writeInt(valBytes.length);
                dos.write(valBytes);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize updates map", e);
        }
        return bos.toByteArray();
    }

    static Map<String, Comparable<?>> deserializeUpdatesMap(byte[] data) throws IOException {
        if (data == null || data.length == 0) return new HashMap<>();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int count = buffer.getInt();
        if (count < 0 || count > 10_000) {
            throw new IOException("Invalid updates map count: " + count);
        }
        Map<String, Comparable<?>> result = new HashMap<>();
        for (int i = 0; i < count; i++) {
            int colLen = buffer.getInt();
            if (colLen < 0 || colLen > 64 * 1024 || colLen > buffer.remaining()) {
                throw new IOException("Invalid column name length: " + colLen);
            }
            byte[] colBytes = new byte[colLen];
            buffer.get(colBytes);
            String colName = new String(colBytes, StandardCharsets.UTF_8);
            int valLen = buffer.getInt();
            if (valLen < 0 || valLen > 64 * 1024 || valLen > buffer.remaining()) {
                throw new IOException("Invalid value length: " + valLen);
            }
            byte[] valBytes = new byte[valLen];
            buffer.get(valBytes);
            result.put(colName, new String(valBytes, StandardCharsets.UTF_8));
        }
        return result;
    }
}
