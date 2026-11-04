package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.model.record.Chunk;
import io.github.rawwise.model.record.Raw;
import io.github.rawwise.model.record.Semantic;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DefaultExecutionContext implements ExecutionContext {

    private final Raw raw;
    private final List<Chunk> chunks;
    private final List<io.github.rawwise.model.record.View> views;
    private final Optional<Semantic> semantics;
    private final Map<String, Object> params;

    public DefaultExecutionContext(
            Raw raw,
            List<Chunk> chunks,
            List<io.github.rawwise.model.record.View> views,
            Optional<Semantic> semantics,
            Map<String, Object> params) {
        this.raw = raw;
        this.chunks = chunks;
        this.views = views;
        this.semantics = semantics;
        this.params = params;
    }

    public static DefaultExecutionContext of(Raw raw) {
        return new DefaultExecutionContext(raw, List.of(), List.of(), Optional.empty(), Map.of());
    }

    public static DefaultExecutionContext of(
            Raw raw, List<Chunk> chunks, List<io.github.rawwise.model.record.View> views) {
        return new DefaultExecutionContext(raw, chunks, views, Optional.empty(), Map.of());
    }

    @Override
    public Raw raw() {
        return raw;
    }

    @Override
    public List<Chunk> chunks() {
        return chunks;
    }

    @Override
    public List<io.github.rawwise.model.record.View> views() {
        return views;
    }

    @Override
    public Optional<Semantic> semantics() {
        return semantics;
    }

    @Override
    public Map<String, Object> params() {
        return params;
    }
}
