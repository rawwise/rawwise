package org.apache.parquet.column.impl;

import static org.apache.parquet.Preconditions.checkArgument;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * A column reader that supports row-level skipping without decoding column values. Tracks
 * repetition level transitions to skip entire records efficiently.
 */
public class RowSkipColumnReader extends ColumnReaderBase {
    private int skipCount;
    private boolean skipDone;
    private int valuesReadFromPage;

    public RowSkipColumnReader(
            ColumnDescriptor path,
            PageReader pageReader,
            PrimitiveConverter converter,
            VersionParser.ParsedVersion writerVersion) {
        super(path, pageReader, converter, writerVersion);
        skipDone = true;
        consume();
    }

    @Override
    boolean isPageFullyConsumed() {
        return getPageValueCount() <= valuesReadFromPage;
    }

    public void skipRows(int count) {
        checkArgument(skipDone, "cannot skip rows before previous skip finished");
        checkArgument(skipCount == 0, "cannot skip rows before previous skip finished");
        this.skipCount = count;
        this.skipDone = false;
    }

    @Override
    boolean skipRL(int rl) {
        ++valuesReadFromPage;
        if (skipDone) {
            return false;
        }
        if (rl == 0) {
            skipCount--;
            if (skipCount == 0) {
                skipDone = true;
            }
        }
        return true;
    }

    @Override
    void newPageInitialized(DataPage page) {
        valuesReadFromPage = 0;
    }

    public boolean isSkipDone() {
        return skipDone;
    }
}
