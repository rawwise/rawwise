package io.github.rawwise.engine.txn;

import java.util.Random;
import net.jqwik.api.*;
import org.junit.jupiter.api.DisplayName;

@DisplayName("P1-7: sealedDigest PBT")
class SealedDigestPbtTest {

    @Property
    @Label("CRC32C detects random bit flips with high probability")
    boolean crc32cDetectsBitFlips(
            @ForAll("byteArrays") byte[] data, @ForAll("nonNegativeInts") int bitIndex) {
        if (data.length == 0) return true;
        var original = computeCrc32C(data);
        var flipped = data.clone();
        int byteIdx = bitIndex % data.length;
        flipped[byteIdx] ^= (1 << (bitIndex % 8));
        var modified = computeCrc32C(flipped);
        return original != modified;
    }

    @Provide
    Arbitrary<Integer> nonNegativeInts() {
        return Arbitraries.integers().between(0, Integer.MAX_VALUE - 1);
    }

    @Provide
    Arbitrary<byte[]> byteArrays() {
        return Arbitraries.integers()
                .between(1, 1000)
                .map(
                        size -> {
                            var bytes = new byte[size];
                            new Random().nextBytes(bytes);
                            return bytes;
                        });
    }

    private long computeCrc32C(byte[] data) {
        var crc = new java.util.zip.CRC32C();
        crc.update(data);
        return crc.getValue();
    }
}
