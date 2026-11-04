package io.github.rawwise.frontend.ir;

import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.frontend.ir.executor.MetadataQueryExecutor;
import io.github.rawwise.frontend.ir.parser.QueryParser;
import io.github.rawwise.frontend.ir.parser.SqlQueryParser;
import io.github.rawwise.frontend.ir.routing.FastPathExecutor;
import io.github.rawwise.frontend.ir.routing.IndexSelector;
import io.github.rawwise.frontend.ir.routing.RouteDecider;
import java.util.Objects;

public record IrRuntime(
        QueryParser queryParser,
        RouteDecider routeDecider,
        FastPathExecutor fastPathExecutor,
        MetadataQueryExecutor metadataExecutor,
        IndexSelector indexSelector)
        implements AutoCloseable {
    public IrRuntime {
        Objects.requireNonNull(queryParser, "queryParser");
        Objects.requireNonNull(routeDecider, "routeDecider");
        Objects.requireNonNull(fastPathExecutor, "fastPathExecutor");
        Objects.requireNonNull(metadataExecutor, "metadataExecutor");
    }

    public static IrRuntime create(Catalog catalog, SnapshotManager snapshotManager) {
        QueryParser queryParser = new SqlQueryParser(catalog, snapshotManager, true);
        FastPathExecutor fastPathExecutor = new FastPathExecutor();
        MetadataQueryExecutor metadataExecutor = new MetadataQueryExecutor(snapshotManager);
        IndexSelector indexSelector = new IndexSelector(catalog);
        RouteDecider routeDecider = new RouteDecider(fastPathExecutor);
        return new IrRuntime(
                queryParser, routeDecider, fastPathExecutor, metadataExecutor, indexSelector);
    }

    @Override
    public void close() {
        // parser is stateless, nothing to close
    }
}
