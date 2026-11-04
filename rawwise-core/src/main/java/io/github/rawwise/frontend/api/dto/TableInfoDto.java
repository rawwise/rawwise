package io.github.rawwise.frontend.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TableInfoDto {

    private String name;
    private String database;
    private String type;
    private SchemaDto schema;
    private List<String> partitionColumns;
    private Map<String, String> properties;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaDto {
        private List<ColumnDto> columns;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ColumnDto {
        private String name;
        private String type;
        private boolean nullable;
    }
}
