package io.github.rawwise.engine.compaction.merger;

import java.util.List;

@FunctionalInterface
public interface MergeFunction {
    byte[] merge(byte[] key, List<VersionedValue> versionedValues);
}
