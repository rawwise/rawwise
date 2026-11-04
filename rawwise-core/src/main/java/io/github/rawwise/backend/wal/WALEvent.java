package io.github.rawwise.backend.wal;

import io.github.rawwise.common.event.Event;
import lombok.Getter;

public class WALEvent implements Event<WALRecord> {
    @Getter private WALRecord entry;
    private volatile boolean processed = false;

    public WALEvent() {}

    @Override
    public void set(WALRecord value) {
        this.entry = value;
    }

    public void markProcessed() {
        this.processed = true;
    }

    public boolean isProcessed() {
        return processed;
    }
}
