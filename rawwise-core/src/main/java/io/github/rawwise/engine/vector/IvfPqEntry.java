package io.github.rawwise.engine.vector;

public record IvfPqEntry(
        String entryKey, byte[] pqCode, VectorLocation location, String storageRef) {}
