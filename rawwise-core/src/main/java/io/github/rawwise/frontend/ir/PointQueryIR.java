package io.github.rawwise.frontend.ir;

import io.github.rawwise.engine.txn.ReadSnapshot;
import java.io.Serializable;
import java.util.*;

public record PointQueryIR(
        TableMetaIR tableMeta,
        DataVersionToken versionToken,
        long snapshotTs,
        ReadSnapshot snapshot,
        Map<String, Comparable<?>> pkValues,
        byte[] encodedKey,
        List<String> projection,
        String targetExecutorId,
        int shardId,
        long shardEpoch)
        implements QueryIR, Serializable {

    public PointQueryIR {
        Objects.requireNonNull(tableMeta, "tableMeta");
        Objects.requireNonNull(targetExecutorId, "targetExecutorId");
        Objects.requireNonNull(pkValues, "pkValues");
        pkValues = Map.copyOf(pkValues);
        encodedKey = encodedKey.clone();
        projection = List.copyOf(projection);
    }

    @Override
    public Set<String> tableIdentifiers() {
        return Set.of(tableMeta.tableIdentifier());
    }

    @Override
    public IRType type() {
        return IRType.POINT_QUERY;
    }
}
