package io.github.rawwise.axiom.pipeline.entity;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import lombok.Builder;

@Builder
public record Entity(
        String id,
        EntityType type,
        String name,
        String surface,
        Map<String, String> attrs,
        double confidence) {

    public Entity {
        attrs = attrs == null ? Map.of() : Collections.unmodifiableMap(attrs);
    }

    public static Entity deterministic(
            EntityType type, String name, String sampleId, String surface, double confidence) {
        String canonical = canonicalize(name);
        String seed = type.name() + ":" + canonical + ":" + (sampleId == null ? "" : sampleId);
        String id = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
        return new Entity(id, type, name, surface, Map.of(), confidence);
    }

    public static String canonicalize(String name) {
        if (name == null) return "";
        return name.trim().toLowerCase();
    }
}
