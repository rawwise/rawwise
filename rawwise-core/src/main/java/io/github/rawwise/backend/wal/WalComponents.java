package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.DurabilityLevel;

public record WalComponents(WAL wal, WALConfig walConfig, DurabilityLevel durabilityLevel) {}
