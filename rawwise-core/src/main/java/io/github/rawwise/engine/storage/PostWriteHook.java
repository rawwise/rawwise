package io.github.rawwise.engine.storage;

import io.github.rawwise.model.record.SemanticRecord;

@FunctionalInterface
public interface PostWriteHook {
    void afterWrite(SemanticRecord record, String tableIdentifier);
}
