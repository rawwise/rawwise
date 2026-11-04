package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.frontend.api.dto.ErrorResponseDto;
import io.github.rawwise.frontend.backend.BackendEndpoint;
import io.github.rawwise.frontend.ir.routing.KeyRange;
import io.github.rawwise.frontend.ir.routing.RoutingEntry;
import io.github.rawwise.frontend.ir.routing.RoutingTable;
import io.github.rawwise.frontend.ir.routing.RoutingTableManager;
import io.github.rawwise.frontend.ir.routing.RoutingTableWriter;
import io.javalin.http.Handler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RoutingRoutes {

    private final RoutingTableWriter writer;
    private final RoutingTableManager manager;

    public RoutingRoutes(RoutingTableWriter writer, RoutingTableManager manager) {
        this.writer = writer;
        this.manager = manager;
    }

    public Handler initialize() {
        return ctx -> {
            String tableId = ctx.pathParam("tableId");
            var body = ctx.bodyAsClass(InitializeRequest.class);

            if (body.entries() == null || body.defaultBackend() == null) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("entries and defaultBackend are required")
                                        .build());
                return;
            }

            List<RoutingEntry> entries = new ArrayList<>();
            for (var e : body.entries()) {
                entries.add(
                        new RoutingEntry(
                                new KeyRange(e.keyStart(), e.keyEnd()), e.backendGroupId()));
            }

            try {
                boolean ok =
                        writer.initialize(
                                tableId,
                                entries,
                                new BackendEndpoint(
                                        body.defaultBackend().groupId(),
                                        body.defaultBackend().host(),
                                        body.defaultBackend().port()));
                if (ok) {
                    ctx.json(Map.of("status", "initialized", "tableId", tableId));
                } else {
                    ctx.status(409)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("ALREADY_EXISTS")
                                            .message("Routing table already exists for " + tableId)
                                            .build());
                }
            } catch (IOException e) {
                log.error("Failed to initialize routing table for {}", tableId, e);
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message(
                                                "Failed to initialize routing table: "
                                                        + e.getMessage())
                                        .build());
            }
        };
    }

    public Handler addEntry() {
        return ctx -> {
            String tableId = ctx.pathParam("tableId");
            var body = ctx.bodyAsClass(EntryRequest.class);

            if (body.keyStart() == null || body.keyEnd() == null || body.backendGroupId() == null) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message(
                                                "keyStart, keyEnd, and backendGroupId are required")
                                        .build());
                return;
            }

            RoutingEntry entry =
                    new RoutingEntry(
                            new KeyRange(body.keyStart(), body.keyEnd()), body.backendGroupId());

            try {
                if (writer.addEntry(tableId, entry)) {
                    ctx.json(Map.of("status", "added"));
                } else {
                    ctx.status(404)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("NOT_FOUND")
                                            .message("Routing table not found for " + tableId)
                                            .build());
                }
            } catch (IOException e) {
                log.error("Failed to add routing entry for {}", tableId, e);
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message("Failed to add routing entry: " + e.getMessage())
                                        .build());
            }
        };
    }

    public Handler updateEntry() {
        return ctx -> {
            String tableId = ctx.pathParam("tableId");
            int seq;
            try {
                seq = Integer.parseInt(ctx.pathParam("seq"));
            } catch (NumberFormatException e) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("seq must be an integer")
                                        .build());
                return;
            }

            var body = ctx.bodyAsClass(EntryRequest.class);

            RoutingEntry entry =
                    new RoutingEntry(
                            new KeyRange(body.keyStart(), body.keyEnd()), body.backendGroupId());

            try {
                if (writer.updateEntry(tableId, seq, entry)) {
                    ctx.json(Map.of("status", "updated"));
                } else {
                    ctx.status(404)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("NOT_FOUND")
                                            .message("Routing entry not found for seq " + seq)
                                            .build());
                }
            } catch (IOException e) {
                log.error("Failed to update routing entry for {}", tableId, e);
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message(
                                                "Failed to update routing entry: " + e.getMessage())
                                        .build());
            }
        };
    }

    public Handler removeEntry() {
        return ctx -> {
            String tableId = ctx.pathParam("tableId");
            int seq;
            try {
                seq = Integer.parseInt(ctx.pathParam("seq"));
            } catch (NumberFormatException e) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("seq must be an integer")
                                        .build());
                return;
            }

            try {
                if (writer.removeEntry(tableId, seq)) {
                    ctx.json(Map.of("status", "removed"));
                } else {
                    ctx.status(404)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("NOT_FOUND")
                                            .message("Routing entry not found for seq " + seq)
                                            .build());
                }
            } catch (IOException e) {
                log.error("Failed to remove routing entry for {}", tableId, e);
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message(
                                                "Failed to remove routing entry: " + e.getMessage())
                                        .build());
            }
        };
    }

    public Handler updateDefault() {
        return ctx -> {
            String tableId = ctx.pathParam("tableId");
            var body = ctx.bodyAsClass(BackendEndpointDto.class);

            try {
                if (writer.updateDefaultBackend(
                        tableId, new BackendEndpoint(body.groupId(), body.host(), body.port()))) {
                    ctx.json(Map.of("status", "updated"));
                } else {
                    ctx.status(404)
                            .json(
                                    ErrorResponseDto.builder()
                                            .code("NOT_FOUND")
                                            .message("Routing table not found for " + tableId)
                                            .build());
                }
            } catch (IOException e) {
                log.error("Failed to update default backend for {}", tableId, e);
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message(
                                                "Failed to update default backend: "
                                                        + e.getMessage())
                                        .build());
            }
        };
    }

    public Handler getRoutingTable() {
        return ctx -> {
            String tableId = ctx.pathParam("tableId");

            RoutingTable rt = manager.getRoutingTable(tableId);
            if (rt == null) {
                ctx.status(404)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("NOT_FOUND")
                                        .message("Routing table not found for " + tableId)
                                        .build());
                return;
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("tableId", rt.tableId());
            result.put("version", rt.version());

            List<Map<String, Object>> entryList = new ArrayList<>();
            for (RoutingEntry e : rt.entries()) {
                Map<String, Object> entryMap = new LinkedHashMap<>();
                entryMap.put("keyStart", e.keyRange().startInclusive());
                entryMap.put("keyEnd", e.keyRange().endExclusive());
                entryMap.put("backendGroupId", e.backendGroupId());
                entryList.add(entryMap);
            }
            result.put("entries", entryList);

            Map<String, Object> defaultMap = new LinkedHashMap<>();
            defaultMap.put("groupId", rt.defaultBackend().groupId());
            defaultMap.put("host", rt.defaultBackend().host());
            defaultMap.put("port", rt.defaultBackend().port());
            result.put("defaultBackend", defaultMap);

            ctx.json(result);
        };
    }

    private record InitializeRequest(
            List<EntryRequest> entries, BackendEndpointDto defaultBackend) {}

    private record EntryRequest(Long keyStart, Long keyEnd, String backendGroupId) {}

    private record BackendEndpointDto(String groupId, String host, int port) {}
}
