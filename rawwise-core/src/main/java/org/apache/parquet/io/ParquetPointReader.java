package org.apache.parquet.io;

import static org.apache.parquet.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.Objects;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.impl.RowSkipColumnReader;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordMaterializer;

/**
 * A record reader that performs PK-based point lookups in Parquet files. Uses {@link
 * RowSkipColumnReader} on non-PK columns to skip rows without decoding values, achieving
 * significant speedup over full-scan for point queries.
 *
 * @param <T> the record type
 */
public class ParquetPointReader<T> extends RecordReaderImplementation<T> {
    private final ColumnReader pkColumnReader;
    private final RowSkipColumnReader[] nonPkColumnReaders;
    private final int[] maxDefinitionLevels;
    private final long totalRowCount;
    private long rowsConsumed;
    private int preSkipped;
    private boolean skipCurrentRecord;

    public ParquetPointReader(
            MessageColumnIO root,
            RecordMaterializer<T> recordMaterializer,
            boolean validating,
            ColumnReadStoreImpl columnStore,
            ColumnPath pkPath,
            long totalRowCount) {
        super(root, recordMaterializer, validating, columnStore);
        this.totalRowCount = totalRowCount;

        int stateCount = getStateCount();
        int nonPkCount = stateCount - 1;
        ColumnReader tempPkReader = null;
        RowSkipColumnReader[] tempNonPk = new RowSkipColumnReader[nonPkCount];
        int[] tempMaxDef = new int[nonPkCount];
        int idx = 0;

        for (int i = 0; i < stateCount; i++) {
            State state = getState(i);
            ColumnReader colReader = state.column;
            if (Arrays.equals(state.fieldPath, pkPath.toArray())) {
                tempPkReader = colReader;
            } else {
                checkArgument(
                        colReader instanceof RowSkipColumnReader,
                        "Non-PK column reader must be RowSkipColumnReader, got %s",
                        colReader.getClass().getName());
                tempNonPk[idx] = (RowSkipColumnReader) colReader;
                tempMaxDef[idx] = state.maxDefinitionLevel;
                idx++;
            }
        }
        this.pkColumnReader = Objects.requireNonNull(tempPkReader, "PK column reader not found");
        this.nonPkColumnReaders = tempNonPk;
        this.maxDefinitionLevels = tempMaxDef;
    }

    @Override
    public T read() {
        if (preSkipped > 0) {
            skipCurrentRecord = true;
            preSkipped--;
            return null;
        }
        return super.read();
    }

    @Override
    public boolean shouldSkipCurrentRecord() {
        if (!skipCurrentRecord) {
            return false;
        }
        skipCurrentRecord = false;
        return true;
    }

    /**
     * Seek to the row with the given primary key using linear scan on PK column while skipping
     * non-PK columns without decoding their values.
     *
     * @param targetKey the target primary key
     * @return 0 if found, 1 if target beyond all keys, -1 if target before current position
     */
    public int seekTo(Binary targetKey) {
        int skipped = 0;
        Binary current;
        int cmp;
        do {
            current = pkColumnReader.getBinary();
            cmp = compareUnsigned(current, targetKey);
            if (cmp < 0) {
                skipped++;
                rowsConsumed++;
                pkColumnReader.skip();
                pkColumnReader.consume();
            }
        } while (cmp < 0 && rowsConsumed < totalRowCount);

        if (skipped > 0) {
            skipNonPkColumns(skipped);
        }

        if (cmp == 0) {
            return 0;
        }
        return rowsConsumed >= totalRowCount ? 1 : -1;
    }

    private void skipNonPkColumns(int rowsToSkip) {
        for (int i = 0; i < nonPkColumnReaders.length; i++) {
            RowSkipColumnReader reader = nonPkColumnReaders[i];
            checkArgument(
                    reader.getCurrentRepetitionLevel() == 0,
                    "expected RL=0 at row boundary, got %d",
                    reader.getCurrentRepetitionLevel());
            if (reader.getCurrentDefinitionLevel() == maxDefinitionLevels[i]) {
                reader.skip();
            }
            reader.skipRows(rowsToSkip - 1);
            reader.consume();
            checkArgument(
                    reader.getCurrentRepetitionLevel() == 0,
                    "expected RL=0 after skipRows, got %d",
                    reader.getCurrentRepetitionLevel());
        }
    }

    private static int compareUnsigned(Binary a, Binary b) {
        byte[] ab = a.getBytes();
        byte[] bb = b.getBytes();
        int minLen = Math.min(ab.length, bb.length);
        for (int i = 0; i < minLen; i++) {
            int au = ab[i] & 0xFF;
            int bu = bb[i] & 0xFF;
            if (au != bu) {
                return au - bu;
            }
        }
        return ab.length - bb.length;
    }
}
