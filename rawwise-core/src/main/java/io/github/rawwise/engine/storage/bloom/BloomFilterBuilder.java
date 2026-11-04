package io.github.rawwise.engine.storage.bloom;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public final class BloomFilterBuilder {
    private static final double FPP = 0.01;

    private BloomFilterBuilder() {}

    public static byte[] build(List<byte[]> encodedKeys, long expectedInsertions) {
        BloomFilter<byte[]> filter =
                BloomFilter.create(Funnels.byteArrayFunnel(), Math.max(expectedInsertions, 1), FPP);
        for (byte[] key : encodedKeys) {
            filter.put(key);
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            filter.writeTo(bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to serialize bloom filter for " + expectedInsertions + " keys", e);
        }
    }
}
