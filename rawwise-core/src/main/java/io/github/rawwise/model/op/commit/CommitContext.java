package io.github.rawwise.model.op.commit;

import java.util.Map;

public record CommitContext(CommitSource source, String description, Map<String, String> tags) {
    public enum CommitSource {
        TXN_COMMIT,
        FLUSH,
        COMPACTION,
        CLEAN,
        LAZY_UPDATE_COMMIT,
        MANUAL
    }

    public static CommitContext of(CommitSource source, String description) {
        return new CommitContext(source, description, Map.of());
    }

    public static CommitContext of(
            CommitSource source, String description, Map<String, String> tags) {
        return new CommitContext(source, description, tags != null ? tags : Map.of());
    }
}
