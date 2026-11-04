package io.github.rawwise.frontend.ir.routing;

import io.github.rawwise.frontend.backend.BackendEndpoint;
import java.util.List;

public record RoutingTable(
        String tableId, long version, List<RoutingEntry> entries, BackendEndpoint defaultBackend) {

    public BackendEndpoint lookup(String tableId, long key) {
        for (RoutingEntry entry : entries) {
            if (entry.keyRange().contains(key)) {
                return new BackendEndpoint(
                        entry.backendGroupId(), defaultBackend.host(), defaultBackend.port());
            }
        }
        return defaultBackend;
    }
}
