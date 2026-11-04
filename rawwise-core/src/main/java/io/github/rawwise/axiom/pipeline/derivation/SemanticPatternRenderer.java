package io.github.rawwise.axiom.pipeline.derivation;

import java.util.List;
import java.util.Map;

public interface SemanticPatternRenderer {

    String render(String pattern, Map<String, Object> fields);

    String inferDefaultPattern(String tableId, List<String> columns);
}
