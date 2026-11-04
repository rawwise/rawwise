package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.frontend.api.dto.ErrorResponseDto;
import io.github.rawwise.frontend.api.dto.TableInfoDto;
import io.github.rawwise.frontend.backend.BackendClient;
import io.javalin.http.Handler;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableRoutes {

    private final BackendClient backendClient;

    public TableRoutes(BackendClient backendClient) {
        this.backendClient = backendClient;
    }

    public Handler listTables() {
        return ctx -> {
            var clusterStatus = backendClient.getClusterStatus();

            if (clusterStatus == null) {
                ctx.status(500)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INTERNAL_ERROR")
                                        .message("Failed to get cluster status")
                                        .build());
                return;
            }

            ctx.json(Map.of("tables", List.of()));
        };
    }

    public Handler getTable() {
        return ctx -> {
            String tableName = ctx.pathParam("table");

            if (tableName == null || tableName.isBlank()) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("Table name cannot be empty")
                                        .build());
                return;
            }

            ctx.json(
                    TableInfoDto.builder()
                            .name(tableName)
                            .database("default")
                            .type("MANAGED")
                            .build());
        };
    }

    public Handler getTablePartitions() {
        return ctx -> {
            String tableName = ctx.pathParam("table");

            if (tableName == null || tableName.isBlank()) {
                ctx.status(400)
                        .json(
                                ErrorResponseDto.builder()
                                        .code("INVALID_REQUEST")
                                        .message("Table name cannot be empty")
                                        .build());
                return;
            }

            ctx.json(Map.of("partitions", List.of()));
        };
    }
}
