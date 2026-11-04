package io.github.rawwise.engine.storage.write;

import io.github.rawwise.engine.storage.ingestion.IngestionDataSink;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SinkWritePathCoordinator implements WritePathCoordinator {
    private final IngestionDataSink ingestionDataSink;

    public SinkWritePathCoordinator(IngestionDataSink ingestionDataSink) {
        this.ingestionDataSink = ingestionDataSink;
    }

    @Override
    public boolean ensureWritable(String tableIdentifier, byte[] key) {
        if (ingestionDataSink == null) {
            log.warn(
                    "IngestionDataSink not configured, cannot ensure writable for table {}",
                    tableIdentifier);
            return false;
        }
        return true;
    }

    @Override
    public boolean isLmdbBacked() {
        return false;
    }
}
