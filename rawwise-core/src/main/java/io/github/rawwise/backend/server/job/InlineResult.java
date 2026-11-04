package io.github.rawwise.backend.server.job;

import io.github.rawwise.server.ColumnSchema;
import java.util.List;

public record InlineResult(
        String resultId,
        byte[] data,
        int totalRows,
        long completionTime,
        List<ColumnSchema> columns)
        implements JobResultHandle {
    public InlineResult {
        data = data.clone();
        columns = columns == null ? List.of() : List.copyOf(columns);
    }

    @Override
    public long estimatedBytes() {
        return data.length;
    }

    @Override
    public byte[] data() {
        return data.clone();
    }

    public boolean isExpired(long cutoffTime) {
        return completionTime < cutoffTime;
    }
}
