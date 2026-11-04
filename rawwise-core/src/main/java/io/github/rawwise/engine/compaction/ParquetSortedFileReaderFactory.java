package io.github.rawwise.engine.compaction;

import io.github.rawwise.engine.storage.reader.ReaderConfig;
import io.github.rawwise.engine.storage.reader.ReaderContext;
import io.github.rawwise.engine.storage.reader.ReaderFactory;
import io.github.rawwise.engine.storage.reader.ReaderType;
import java.io.IOException;

public class ParquetSortedFileReaderFactory implements ReaderFactory<ParquetSortedFileReader> {

    static final String COMMIT_TIMESTAMP_PROP = "commitTimestamp";

    @Override
    public ReaderType readerType() {
        return ReaderType.SORTED_FILE_PARQUET;
    }

    @Override
    public Class<ParquetSortedFileReader> readerClass() {
        return ParquetSortedFileReader.class;
    }

    @Override
    public ParquetSortedFileReader create(ReaderConfig config, ReaderContext context)
            throws IOException {
        String filePath = config.filePath();
        if (filePath == null) {
            throw new IllegalStateException(
                    "filePath is required in ReaderConfig for ParquetSortedFileReader");
        }
        Long commitTimestamp = null;
        String tsStr = config.extraProp(COMMIT_TIMESTAMP_PROP);
        if (tsStr != null) {
            try {
                commitTimestamp = Long.parseLong(tsStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid commitTimestamp in ReaderConfig extraProps: " + tsStr, e);
            }
        }
        return new ParquetSortedFileReader(filePath, commitTimestamp);
    }
}
