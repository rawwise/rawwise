package io.github.rawwise.engine.compaction.merger;

import java.io.IOException;
import java.util.List;

public interface MultiWayMerger {

    MergeResult merge(List<SortedFileReader> sources, MergeFunction mergeFn, OutputWriter output)
            throws IOException;
}
