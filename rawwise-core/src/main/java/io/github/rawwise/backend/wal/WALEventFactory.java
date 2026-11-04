package io.github.rawwise.backend.wal;

import com.lmax.disruptor.EventFactory;

public class WALEventFactory implements EventFactory<WALEvent> {
    @Override
    public WALEvent newInstance() {
        return new WALEvent();
    }
}
