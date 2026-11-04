package io.github.rawwise.common.catalog;

import io.github.rawwise.model.record.TableId;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Package-private utility class for encoding Metastore keys.
 *
 * <p>Key format: "{database}:{tableName}:" as table prefix, followed by a namespace segment (e.g.
 * ":snapshot-latest", ":manifest:{id}").
 *
 * <p>All string parts use UTF-8 encoding; long values use big-endian byte order.
 */
public final class Keys {

    private static final byte[] SNAPSHOT_LATEST =
            ":snapshot-latest".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SNAPSHOT_NS = ":snapshot:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MANIFEST_NS = ":manifest:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FILE_NS = ":file:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COMMIT_INTENT_NS =
            ":commit-intent:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COUNTER_COMMIT_ID =
            ":counter:commit-id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] COUNTER_SNAPSHOT_ID =
            ":counter:snapshot-id".getBytes(StandardCharsets.UTF_8);
    private static final byte[] MANIFEST_IDX_NS = ":manifest-idx:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] STATS_NS = ":stats:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] RENAME_INTENT_NS =
            ":rename-intent".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TABLE_EPOCH_NS = ":epoch".getBytes(StandardCharsets.UTF_8);
    private static final byte[] TABLE_IDENTITY_NS = ":identity".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SCHEMA_VERSION_NS = ":schema:".getBytes(StandardCharsets.UTF_8);

    private Keys() {}

    /** Builds the table prefix: "{database}:{tableName}:". */
    static byte[] tablePrefix(TableId id) {
        byte[] nsBytes = id.database().getBytes(StandardCharsets.UTF_8);
        byte[] tblBytes = id.tableName().getBytes(StandardCharsets.UTF_8);
        byte[] prefix = new byte[nsBytes.length + 1 + tblBytes.length + 1];
        System.arraycopy(nsBytes, 0, prefix, 0, nsBytes.length);
        prefix[nsBytes.length] = ':';
        System.arraycopy(tblBytes, 0, prefix, nsBytes.length + 1, tblBytes.length);
        prefix[prefix.length - 1] = ':';
        return prefix;
    }

    static byte[] snapshotLatestKey(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] key = new byte[pfx.length + SNAPSHOT_LATEST.length];
        System.arraycopy(pfx, 0, key, 0, pfx.length);
        System.arraycopy(SNAPSHOT_LATEST, 0, key, pfx.length, SNAPSHOT_LATEST.length);
        return key;
    }

    public static byte[] snapshotKey(TableId id, long snapshotId) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + SNAPSHOT_NS.length + 8];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(SNAPSHOT_NS, 0, result, pfx.length, SNAPSHOT_NS.length);
        ByteBuffer buf = ByteBuffer.wrap(result, pfx.length + SNAPSHOT_NS.length, 8);
        buf.putLong(snapshotId);
        return result;
    }

    public static byte[] gcEntryPrefix(String tableId) {
        return (tableId + ":gc:").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] gcEntryKey(String tableId, long snapshotId, String filePath) {
        String key = tableId + ":gc:" + snapshotId + ":" + filePath;
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] flushedInstanceKey(String tableId, String instanceId) {
        return (tableId + ":flushed-inst:" + instanceId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] flushedInstancePrefix(String tableId) {
        return (tableId + ":flushed-inst:").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] flushedInstanceScanPrefix() {
        return ":flushed-inst:".getBytes(StandardCharsets.UTF_8);
    }

    static byte[] snapshotPrefix(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + SNAPSHOT_NS.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(SNAPSHOT_NS, 0, result, pfx.length, SNAPSHOT_NS.length);
        return result;
    }

    public static byte[] schemaVersionKey(TableId id, long version) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + SCHEMA_VERSION_NS.length + 8];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(SCHEMA_VERSION_NS, 0, result, pfx.length, SCHEMA_VERSION_NS.length);
        ByteBuffer buf = ByteBuffer.wrap(result, pfx.length + SCHEMA_VERSION_NS.length, 8);
        buf.putLong(version);
        return result;
    }

    public static byte[] schemaVersionPrefix(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + SCHEMA_VERSION_NS.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(SCHEMA_VERSION_NS, 0, result, pfx.length, SCHEMA_VERSION_NS.length);
        return result;
    }

    static byte[] manifestKey(TableId id, long manifestId) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + MANIFEST_NS.length + 8];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(MANIFEST_NS, 0, result, pfx.length, MANIFEST_NS.length);
        ByteBuffer buf = ByteBuffer.wrap(result, pfx.length + MANIFEST_NS.length, 8);
        buf.putLong(manifestId);
        return result;
    }

    static byte[] fileEntryKey(TableId id, long snapshotId, byte[] dataFilePath) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + FILE_NS.length + 8 + 1 + dataFilePath.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(FILE_NS, 0, result, pfx.length, FILE_NS.length);
        ByteBuffer buf = ByteBuffer.wrap(result, pfx.length + FILE_NS.length, 8);
        buf.putLong(snapshotId);
        result[pfx.length + FILE_NS.length + 8] = ':';
        System.arraycopy(
                dataFilePath, 0, result, pfx.length + FILE_NS.length + 9, dataFilePath.length);
        return result;
    }

    static byte[] commitIntentKey(TableId id, long commitId) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + COMMIT_INTENT_NS.length + 8];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(COMMIT_INTENT_NS, 0, result, pfx.length, COMMIT_INTENT_NS.length);
        ByteBuffer buf = ByteBuffer.wrap(result, pfx.length + COMMIT_INTENT_NS.length, 8);
        buf.putLong(commitId);
        return result;
    }

    static byte[] renameIntentKey(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + RENAME_INTENT_NS.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(RENAME_INTENT_NS, 0, result, pfx.length, RENAME_INTENT_NS.length);
        return result;
    }

    static byte[] tableEpochKey(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + TABLE_EPOCH_NS.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(TABLE_EPOCH_NS, 0, result, pfx.length, TABLE_EPOCH_NS.length);
        return result;
    }

    static byte[] tableIdentityKey(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + TABLE_IDENTITY_NS.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(TABLE_IDENTITY_NS, 0, result, pfx.length, TABLE_IDENTITY_NS.length);
        return result;
    }

    static byte[] encodeEpoch(long epoch) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putLong(epoch);
        return bytes;
    }

    static long decodeEpoch(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    static byte[] commitIntentPrefix(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + COMMIT_INTENT_NS.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(COMMIT_INTENT_NS, 0, result, pfx.length, COMMIT_INTENT_NS.length);
        return result;
    }

    static byte[] commitIdCounterKey(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + COUNTER_COMMIT_ID.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(COUNTER_COMMIT_ID, 0, result, pfx.length, COUNTER_COMMIT_ID.length);
        return result;
    }

    static byte[] snapshotIdCounterKey(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + COUNTER_SNAPSHOT_ID.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(COUNTER_SNAPSHOT_ID, 0, result, pfx.length, COUNTER_SNAPSHOT_ID.length);
        return result;
    }

    public static byte[] manifestIndexKey(TableId id, String manifestId) {
        byte[] pfx = tablePrefix(id);
        byte[] midBytes = manifestId.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[pfx.length + MANIFEST_IDX_NS.length + midBytes.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(MANIFEST_IDX_NS, 0, result, pfx.length, MANIFEST_IDX_NS.length);
        System.arraycopy(midBytes, 0, result, pfx.length + MANIFEST_IDX_NS.length, midBytes.length);
        return result;
    }

    public static byte[] snapshotStatsKey(TableId id, long snapshotId) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + STATS_NS.length + 8];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(STATS_NS, 0, result, pfx.length, STATS_NS.length);
        ByteBuffer buf = ByteBuffer.wrap(result, pfx.length + STATS_NS.length, 8);
        buf.putLong(snapshotId);
        return result;
    }

    /**
     * Builds a scan prefix for all SnapshotStatistics entries of a table. Key format:
     * "{tablePrefix}:stats:"
     */
    public static byte[] snapshotStatsPrefix(TableId id) {
        byte[] pfx = tablePrefix(id);
        byte[] result = new byte[pfx.length + STATS_NS.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(STATS_NS, 0, result, pfx.length, STATS_NS.length);
        return result;
    }

    /**
     * Builds a scan prefix for all operation payloads of a given type. Key format:
     * "{tablePrefix}:op:{operationType}:"
     */
    public static byte[] operationPayloadPrefix(TableId id, String operationType) {
        byte[] pfx = tablePrefix(id);
        byte[] opTypeBytes = (":op:" + operationType + ":").getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[pfx.length + opTypeBytes.length];
        System.arraycopy(pfx, 0, result, 0, pfx.length);
        System.arraycopy(opTypeBytes, 0, result, pfx.length, opTypeBytes.length);
        return result;
    }

    /**
     * Builds a key for storing operation payload data within a commit. Key format:
     * "{tablePrefix}:op:{operationType}:{commitId}:{manifestId}"
     *
     * @param manifestId the manifest identifier (only meaningful for MANIFEST_WRITE ops)
     */
    public static byte[] operationPayloadKey(
            TableId id, long commitId, String operationType, String manifestId) {
        byte[] pfx = tablePrefix(id);
        byte[] opTypeBytes = (":op:" + operationType + ":").getBytes(StandardCharsets.UTF_8);
        byte[] midBytes = manifestId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[pfx.length + opTypeBytes.length + 8 + 1 + midBytes.length];
        System.arraycopy(pfx, 0, key, 0, pfx.length);
        System.arraycopy(opTypeBytes, 0, key, pfx.length, opTypeBytes.length);
        ByteBuffer buf = ByteBuffer.wrap(key, pfx.length + opTypeBytes.length, 8);
        buf.putLong(commitId);
        key[pfx.length + opTypeBytes.length + 8] = ':';
        System.arraycopy(midBytes, 0, key, pfx.length + opTypeBytes.length + 9, midBytes.length);
        return key;
    }

    /**
     * Builds a key for the commit-result record, used for idempotent replay detection. Key format:
     * "{tablePrefix}:commit-result:{commitId}"
     */
    public static byte[] commitResultKey(TableId id, long commitId) {
        byte[] pfx = tablePrefix(id);
        byte[] nsBytes = ":commit-result:".getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[pfx.length + nsBytes.length + 8];
        System.arraycopy(pfx, 0, key, 0, pfx.length);
        System.arraycopy(nsBytes, 0, key, pfx.length, nsBytes.length);
        ByteBuffer buf = ByteBuffer.wrap(key, pfx.length + nsBytes.length, 8);
        buf.putLong(commitId);
        return key;
    }

    static byte[] namespaceListKey() {
        return "namespace:list:".getBytes(StandardCharsets.UTF_8);
    }

    static byte[] namespaceTableListKey(String ns) {
        byte[] nsb = ns.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[nsb.length + 2];
        System.arraycopy(nsb, 0, key, 0, nsb.length);
        key[nsb.length] = ':';
        key[nsb.length + 1] = ':';
        return key;
    }

    static long extractLong(byte[] key, int offset) {
        return ByteBuffer.wrap(key, offset, 8).getLong();
    }
}
