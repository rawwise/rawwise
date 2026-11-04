package io.github.rawwise.engine.txn.wal;

import java.util.List;

public interface FastPathWalReader {

    List<FastPathReplayEntry> scanReplayEntries();

    record FastPathReplayEntry(
            long commitTs, String tableId, byte[] keyBytes, byte[] valueBytes, String writeType) {}
}
