package io.github.rawwise.model.record.keygen;

public interface KeyGenerator {
    byte[] generateKey(int partitionId);
}
