package io.github.rawwise.engine.compaction.merger;

import java.util.List;

public class PartialUpdateMergeFunction implements MergeFunction {

    @Override
    public byte[] merge(byte[] key, List<VersionedValue> versionedValues) {
        throw new UnsupportedOperationException(
                "PartialUpdateMergeFunction.merge not yet implemented");
    }
}
