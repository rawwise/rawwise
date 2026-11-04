package io.github.rawwise.frontend.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryResultDto {
    private List<ColumnInfoDto> columns;
    private List<List<Object>> rows;
    private Integer rowCount;
    private Long executionTimeMs;
    private Boolean truncated;
    private String nextPageToken;
    private String snapshotId;
    private String schemaVersion;
    private String snapshotSource;
}
