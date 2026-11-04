package io.github.rawwise.engine.compaction.rewriter;

import io.github.rawwise.engine.compaction.merger.MergeFunction;
import java.io.IOException;
import java.util.List;

public interface CleanDataFileRewriter {
    RewriteResult rewriteFiles(String tableId, MergeFunction mergeFn, List<String> filePaths)
            throws IOException;

    RewriteResult rewriteWithTTLFilter(
            String tableId,
            String expireColumn,
            String logicalType,
            long expiryThresholdMs,
            List<String> filePaths)
            throws IOException;

    record RewriteResult(int rewrittenFileCount, long rewrittenBytes) {
        public static RewriteResult empty() {
            return new RewriteResult(0, 0);
        }
    }
}
