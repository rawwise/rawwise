package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.model.record.WriteEntry;
import java.util.List;

public interface DeadLetterHandler {
    void save(List<WriteEntry> failedData, Exception cause);
}
