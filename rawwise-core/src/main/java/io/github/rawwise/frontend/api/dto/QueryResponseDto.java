package io.github.rawwise.frontend.api.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class QueryResponseDto {

    private String jobId;
    private String state;
    private Double progress;
    private ErrorResponseDto error;
    private QueryResultDto data;
}
