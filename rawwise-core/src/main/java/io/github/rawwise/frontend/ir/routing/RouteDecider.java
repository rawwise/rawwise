package io.github.rawwise.frontend.ir.routing;

import io.github.rawwise.frontend.ir.*;
import io.github.rawwise.model.record.RawWiseType;

public class RouteDecider {

    private final FastPathExecutor fastPathExecutor;

    public RouteDecider(FastPathExecutor fastPathExecutor) {
        this.fastPathExecutor = fastPathExecutor;
    }

    public ExecutionPath decide(QueryIR ir) {
        return switch (ir) {
            case MetadataQueryIR mq ->
                    canAnswerFromMetadata(mq)
                            ? ExecutionPath.METADATA_ONLY
                            : ExecutionPath.LOGICAL_PLAN;
            case PointQueryIR pq -> {
                if (fastPathExecutor != null && fastPathExecutor.hasAvailableExecutors()) {
                    yield ExecutionPath.FAST_PATH;
                }
                yield ExecutionPath.LOGICAL_PLAN;
            }
            case BatchPointQueryIR bpq -> {
                if (fastPathExecutor != null && fastPathExecutor.hasAvailableExecutors()) {
                    yield ExecutionPath.FAST_PATH;
                }
                yield ExecutionPath.LOGICAL_PLAN;
            }
            case ScanQueryIR sq -> ExecutionPath.LOGICAL_PLAN;
            case WriteQueryIR wq -> {
                // Only single-row INSERT goes through FastPath: single row = 1 stub RPC + 1 LMDB
                // write transaction, atomicity naturally holds.
                // Multi-row INSERT/BATCH_INSERT falls back to LOGICAL_PLAN (Spark columnar batch
                // write + single commit),
                // Preserves SQL multi-row INSERT transaction atomicity semantics (first N rows
                // succeed + (N+1)th row fails -> full rollback).
                // Review feedback: multi-row FastPath row-by-row RPC has neither atomicity nor
                // N×RTT amplification - technical debt should not enter production.
                boolean isInsert =
                        wq.writeType() == WriteType.INSERT
                                || wq.writeType() == WriteType.BATCH_INSERT;
                if (isInsert
                        && wq.rows() != null
                        && wq.rows().size() == 1
                        && fastPathExecutor != null
                        && fastPathExecutor.hasAvailableExecutors()) {
                    yield ExecutionPath.FAST_PATH;
                }
                yield ExecutionPath.LOGICAL_PLAN;
            }
            case UpdateQueryIR uq ->
                    switch (uq.mode()) {
                        case POINT ->
                                fastPathExecutor != null && fastPathExecutor.hasAvailableExecutors()
                                        ? ExecutionPath.FAST_PATH
                                        : ExecutionPath.LOGICAL_PLAN;
                        case LAZY -> ExecutionPath.LOGICAL_PLAN;
                    };
            case DeleteQueryIR dq ->
                    switch (dq.mode()) {
                        case POINT ->
                                fastPathExecutor != null && fastPathExecutor.hasAvailableExecutors()
                                        ? ExecutionPath.FAST_PATH
                                        : ExecutionPath.LOGICAL_PLAN;
                        case LAZY -> ExecutionPath.LOGICAL_PLAN;
                    };
            default -> ExecutionPath.LOGICAL_PLAN;
        };
    }

    private boolean canAnswerFromMetadata(MetadataQueryIR mq) {
        TableMetaIR meta = mq.tableMeta();
        if (meta.schemaVersion() == 0
                && meta.schema().fields().size() == 1
                && meta.schema().fields().get(0).name().equals("placeholder")) {
            return false;
        }
        return switch (mq) {
            case MetadataQueryIR.CountAll c -> true;
            case MetadataQueryIR.CountColumn c -> hasColumn(c.tableMeta(), c.column());
            case MetadataQueryIR.MinValue m -> hasBasicColumn(m.tableMeta(), m.column());
            case MetadataQueryIR.MaxValue m -> hasBasicColumn(m.tableMeta(), m.column());
            case MetadataQueryIR.TableInfo t -> true;
        };
    }

    private boolean hasBasicColumn(TableMetaIR meta, String col) {
        var field = meta.schema().field(col);
        if (field.isEmpty()) return false;
        var kind = field.get().dataType().category();
        return kind != RawWiseType.TypeCategory.COMPLEX
                && kind != RawWiseType.TypeCategory.STRUCT
                && kind != RawWiseType.TypeCategory.COLLECTION;
    }

    private boolean hasColumn(TableMetaIR meta, String col) {
        return meta.schema().hasField(col);
    }
}
