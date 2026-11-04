package io.github.rawwise.frontend.api.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryRequestDto {

    private String sql;

    private String type;

    @JsonProperty("options")
    private Map<String, String> options;
}
