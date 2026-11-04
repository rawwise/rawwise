package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Raw;
import io.github.rawwise.model.record.Semantic;
import io.github.rawwise.model.record.View;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface ExecutionContext {

    Raw raw();

    List<Chunk> chunks();

    List<View> views();

    Optional<Semantic> semantics();

    Map<String, Object> params();
}
