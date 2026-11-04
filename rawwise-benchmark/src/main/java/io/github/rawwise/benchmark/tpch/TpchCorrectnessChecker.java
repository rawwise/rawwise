package io.github.rawwise.benchmark.tpch;

import io.github.rawwise.benchmark.CorrectnessChecker;
import io.github.rawwise.frontend.api.dto.QueryResponseDto;
import io.github.rawwise.frontend.api.dto.QueryResultDto;
import java.util.Map;

public class TpchCorrectnessChecker implements CorrectnessChecker {

    private final Map<String, Long> expectedRowCounts;

    public TpchCorrectnessChecker(Map<String, Long> expectedRowCounts) {
        this.expectedRowCounts = expectedRowCounts;
    }

    @Override
    public String check(String queryId, Object actual) {
        Long expected = expectedRowCounts.get(queryId);
        if (expected == null) {
            return "NO_EXPECTED_COUNT";
        }

        if (actual instanceof QueryResponseDto response) {
            if (!"SUCCEEDED".equals(response.getState())) {
                return "QUERY_FAILED: " + response.getState();
            }
            QueryResultDto data = response.getData();
            if (data == null || data.getRowCount() == null) {
                return "NO_RESULT_DATA";
            }
            long actualRows = data.getRowCount();
            if (actualRows == expected) {
                return "MATCH";
            }
            return "ROW_COUNT_MISMATCH: expected=" + expected + ", actual=" + actualRows;
        }

        if (actual instanceof Long actualRows) {
            if (actualRows == expected) {
                return "MATCH";
            }
            return "ROW_COUNT_MISMATCH: expected=" + expected + ", actual=" + actualRows;
        }

        return "UNSUPPORTED_RESULT_TYPE: " + actual.getClass().getName();
    }
}
