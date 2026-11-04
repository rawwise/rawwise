package io.github.rawwise.frontend.ir.parser;

import javax.annotation.Nullable;

public interface QueryParser {

    ParseResult parse(String sql);

    default ParseResult parse(String sql, @Nullable Long requestedSnapshotId) {
        return parse(sql);
    }
}
