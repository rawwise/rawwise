package io.github.rawwise.frontend.ir;

import io.github.rawwise.engine.storage.KeyRange;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public sealed interface MetadataResult extends Serializable
        permits MetadataResult.ScalarResult, MetadataResult.TableInfoResult {

    record ScalarResult(String name, Comparable<?> value) implements MetadataResult {
        public ScalarResult {
            Objects.requireNonNull(name, "name");
        }
    }

    record TableInfoResult(long totalRows, long totalBytes, int fileCount, List<KeyRange> keyRanges)
            implements MetadataResult {
        public TableInfoResult {
            keyRanges = List.copyOf(keyRanges);
        }
    }
}
