package io.github.rawwise.engine.storage.flush;

import java.util.Collection;

public interface FlushListener {

    void onFlushCommitted(String tableId, String instanceId, Collection<byte[]> flushedKeys);
}
