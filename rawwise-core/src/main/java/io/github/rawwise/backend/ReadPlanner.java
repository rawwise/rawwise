package io.github.rawwise.backend;

import io.github.rawwise.backend.lazyupdate.LazyUpdateReadEnhancer;
import io.github.rawwise.backend.lazyupdate.UpdateLogEntry;
import io.github.rawwise.backend.read.ReadPlan;
import io.github.rawwise.backend.read.ReadPruning;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableMeta;
import java.util.List;

public interface ReadPlanner {

    record ScanContext(
            String basePath,
            String storeId,
            String tableIdentifier,
            TableMeta tableMeta,
            Expression predicate,
            Snapshot snapshot,
            boolean primaryKeyLookup,
            ReadPruning.KeyPruningSpec pruningSpec,
            List<UpdateLogEntry> updateLogs,
            LazyUpdateReadEnhancer enhancer,
            List<String> projectionColumns) {
        public ScanContext {
            pruningSpec = pruningSpec != null ? pruningSpec : ReadPruning.KeyPruningSpec.NO_PRUNING;
            projectionColumns = projectionColumns != null ? projectionColumns : List.of();
        }

        public static ScanContext of(
                String basePath,
                String storeId,
                String tableIdentifier,
                TableMeta tableMeta,
                Expression predicate) {
            return new ScanContext(
                    basePath,
                    storeId,
                    tableIdentifier,
                    tableMeta,
                    predicate,
                    null,
                    false,
                    null,
                    null,
                    null,
                    List.of());
        }
    }

    ReadPlan planReads(ScanContext context);
}
