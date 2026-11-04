package io.github.rawwise.engine.txn;

import io.github.rawwise.model.record.IndexMeta;
import java.nio.charset.StandardCharsets;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

/**
 * Secondary index post-write sync hook (cross-table consistency dependency).
 *
 * <p>Registered to {@code TableStore.postWriteHooks}, generates index entries after each write to
 * the main table. actual index shadow writes are performed by the caller within the transaction.
 *
 * <h3>Atomicity dependency</h3>
 *
 * <p><b>currently</b>: main table and index table writes are in two separate transactions. Main
 * table committed but index write failed will cause inconsistency. This is a known limitation.
 *
 * <p><b>After implementation</b>: complete main table + index table writes within the same {@code
 * CatalogLevelTxnOrchestrator} transaction, ensure atomicity through Metastore-based
 * GlobalCommitMarker.
 *
 * <h3>Encoding convention</h3>
 *
 * <ul>
 *   <li>index key: column values joined with U+001F (in index column definition order)
 *   <li>index value: STORING column values joined with U+001F
 * </ul>
 */
@Slf4j
public class SecondaryIndexWriteHook {

    public static final byte UNIT_SEPARATOR = 0x1F;

    private final IndexMeta indexMeta;
    private final String mainTableIdentifier;

    public SecondaryIndexWriteHook(IndexMeta indexMeta, String mainTableIdentifier) {
        this.indexMeta = Objects.requireNonNull(indexMeta, "indexMeta");
        this.mainTableIdentifier =
                Objects.requireNonNull(mainTableIdentifier, "mainTableIdentifier");
    }

    // ==== Column value extraction ====

    /**
     * Extract specified column value from a record row (for encoding index key and value).
     *
     * @param row rowdata: column name → value
     * @param column column name
     * @return column value (string form), returns null if not present
     */
    public static Object columnValue(Map<String, Object> row, String column) {
        return row.get(column);
    }

    /** Check whether the row matches this hook's table identifier. */
    public boolean matchesTable(String tableId) {
        return mainTableIdentifier.equals(tableId);
    }

    // ==== Encoding ====

    /**
     * Encode index KEY: {@code col1_val \x1F col2_val \x1F ...}
     *
     * @param row main table row data
     * @return index key byte array; missingindexcolumnwhenreturn null
     */
    public byte[] encodeIndexKey(Map<String, Object> row) {
        List<String> cols = indexMeta.columns();
        ByteBuffer buf = new ByteBuffer();
        for (String col : cols) {
            Object val = columnValue(row, col);
            if (val == null) return null;
            if (buf.size > 0) buf.add(UNIT_SEPARATOR);
            buf.add(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
        }
        return buf.toArray();
    }

    /**
     * Encode index VALUE: {@code storing1_val \x1F storing2_val \x1F ...}
     *
     * @param row main table row data
     * @return index value byte array
     */
    public byte[] encodeIndexValue(Map<String, Object> row) {
        if (indexMeta.storing().isEmpty()) return new byte[0];
        ByteBuffer buf = new ByteBuffer();
        boolean first = true;
        for (String col : indexMeta.storing()) {
            Object val = columnValue(row, col);
            if (!first) buf.add(UNIT_SEPARATOR);
            first = false;
            if (val != null) {
                buf.add(String.valueOf(val).getBytes(StandardCharsets.UTF_8));
            }
        }
        return buf.toArray();
    }

    /** Index metadata */
    public IndexMeta indexMeta() {
        return indexMeta;
    }

    /** Main table identifier */
    public String mainTableIdentifier() {
        return mainTableIdentifier;
    }

    // ==== ByteBuffer helper ====

    private static class ByteBuffer {
        private byte[] data = new byte[256];
        private int size;

        void add(byte b) {
            ensureCapacity(1);
            data[size++] = b;
        }

        void add(byte[] bytes) {
            ensureCapacity(bytes.length);
            System.arraycopy(bytes, 0, data, size, bytes.length);
            size += bytes.length;
        }

        private void ensureCapacity(int extra) {
            if (size + extra > data.length) {
                data = Arrays.copyOf(data, Math.max(data.length * 2, size + extra));
            }
        }

        byte[] toArray() {
            return Arrays.copyOf(data, size);
        }
    }
}
