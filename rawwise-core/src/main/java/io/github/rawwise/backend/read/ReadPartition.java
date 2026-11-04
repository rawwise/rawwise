package io.github.rawwise.backend.read;

import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.model.record.TableMeta;
import java.util.List;

public record ReadPartition(
        String[] preferredHosts,
        List<DataBlock> blocks,
        TableMeta tableMeta,
        List<UpdateLogEntry> updateLogs) {
    public ReadPartition {
        preferredHosts = preferredHosts == null ? new String[0] : preferredHosts.clone();
        blocks = List.copyOf(blocks);
        updateLogs = updateLogs != null ? List.copyOf(updateLogs) : List.of();
    }

    public int blockCount() {
        return blocks.size();
    }

    public boolean hasPreferredHost() {
        return preferredHosts != null && preferredHosts.length > 0 && preferredHosts[0] != null;
    }

    public String primaryHost() {
        return hasPreferredHost() ? preferredHosts[0] : null;
    }
}
