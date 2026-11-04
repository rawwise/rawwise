package io.github.rawwise.engine.compaction.merger;

@FunctionalInterface
public interface TimestampExtractor {
    long extractTimestampMillis(byte[] key, byte[] value);
}
