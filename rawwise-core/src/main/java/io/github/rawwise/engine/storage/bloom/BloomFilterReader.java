package io.github.rawwise.engine.storage.bloom;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class BloomFilterReader {

    private BloomFilterReader() {}

    public static BloomFilter<byte[]> read(byte[] serialized) {
        if (serialized == null || serialized.length == 0) {
            return null;
        }
        try {
            return BloomFilter.readFrom(
                    new ByteArrayInputStream(serialized), Funnels.byteArrayFunnel());
        } catch (IOException e) {
            log.warn("Failed to deserialize bloom filter ({} bytes)", serialized.length, e);
            return null;
        }
    }

    public static boolean mightContain(byte[] serialized, byte[] key) {
        BloomFilter<byte[]> filter = read(serialized);
        return filter != null && filter.mightContain(key);
    }
}
