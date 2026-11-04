package io.github.rawwise.axiom.memory;

import java.time.Duration;

public record MemorySharePolicy(Duration ttl, boolean revocable, String permission) {}
