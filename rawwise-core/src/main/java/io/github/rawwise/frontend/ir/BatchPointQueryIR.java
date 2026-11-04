package io.github.rawwise.frontend.ir;

import io.github.rawwise.engine.txn.ReadSnapshot;
import java.io.Serializable;
import java.util.*;

public record BatchPointQueryIR(
        TableMetaIR tableMeta,
        DataVersionToken versionToken,
        long snapshotTs,
        ReadSnapshot snapshot,
        List<byte[]> encodedKeys,
        List<String> projection,
        String targetExecutorId,
        int shardId,
        long shardEpoch)
        implements QueryIR, Serializable {

    public BatchPointQueryIR {
        encodedKeys = encodedKeys.stream().map(b -> b.clone()).toList();
        projection = List.copyOf(projection);
        Objects.requireNonNull(tableMeta, "tableMeta");
        Objects.requireNonNull(targetExecutorId, "targetExecutorId");
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return IRType.BATCH_POINT_QUERY;
    }
}
