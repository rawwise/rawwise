package io.github.rawwise.engine.compaction.rewriter;

import io.github.rawwise.engine.compaction.CompactionManager;
import io.github.rawwise.engine.compaction.merger.MergeFunction;
import io.github.rawwise.engine.compaction.merger.TTLFilterMergeFunction;
import io.github.rawwise.engine.compaction.merger.TimestampCodec;
import io.github.rawwise.engine.compaction.merger.TimestampExtractor;
import java.io.IOException;
import java.util.List;

public class CompactionCleanDataFileRewriter implements CleanDataFileRewriter {

    private final CompactionManager compactionManager;

    public CompactionCleanDataFileRewriter(CompactionManager compactionManager) {
        this.compactionManager = compactionManager;
    }

    @Override
    public RewriteResult rewriteFiles(String tableId, MergeFunction mergeFn, List<String> filePaths)
            throws IOException {
        CompactionManager.RewriteFilesResult result =
                compactionManager.rewriteFiles(tableId, mergeFn, filePaths);
        return new RewriteResult(result.rewrittenFileCount(), result.rewrittenBytes());
    }

    @Override
    public RewriteResult rewriteWithTTLFilter(
            String tableId,
            String expireColumn,
            String logicalType,
            long expiryThresholdMs,
            List<String> filePaths)
            throws IOException {
        TimestampExtractor extractor =
                (key, value) ->
                        TimestampCodec.deserializeToMillis(
                                value, logicalType != null ? logicalType : "TIMESTAMP_MILLIS");

        TTLFilterMergeFunction mergeFn =
                new TTLFilterMergeFunction(null, expiryThresholdMs, extractor);

        CompactionManager.RewriteFilesResult result =
                compactionManager.rewriteFiles(tableId, mergeFn, filePaths);
        return new RewriteResult(result.rewrittenFileCount(), result.rewrittenBytes());
    }
}
