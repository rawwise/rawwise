package io.github.rawwise.engine.storage;

import java.io.Serializable;

public interface RawWiseFile extends Serializable {

    String filePath();

    FileFormat format();

    long fileSizeBytes();

    long createdAtMillis();

    enum FileFormat {
        PARQUET,
        ORC,
        NWL,
        JSON,
        UNKNOWN;

        public static FileFormat fromBlockFormat(
                io.github.rawwise.engine.storage.block.DataBlock.Format blockFormat) {
            return switch (blockFormat) {
                case PARQUET -> PARQUET;
                case ORC -> ORC;
                case LMDB, UNKNOWN -> UNKNOWN;
            };
        }
    }
}
