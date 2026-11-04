package io.github.rawwise.axiom.pipeline.derivation;

import java.util.Map;

public record ChunkMeta(byte[] content, Map<String, Object> metadata) {}
