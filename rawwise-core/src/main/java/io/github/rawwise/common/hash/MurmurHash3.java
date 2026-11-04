package io.github.rawwise.common.hash;

import java.nio.charset.StandardCharsets;

public class MurmurHash3 {
    private MurmurHash3() {}

    // Constants for 128-bit hash
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;

    public static int hash32(byte[] data) {
        return hash32(data, 0, data.length, 0);
    }

    public static int hash32(byte[] data, int offset, int length, int seed) {
        int h1 = seed;
        int roundedEnd = offset + (length & 0xFFFFFFFC); // round down to multiple of 4 bytes

        for (int i = offset; i < roundedEnd; i += 4) {
            int k1 =
                    (data[i] & 0xFF)
                            | ((data[i + 1] & 0xFF) << 8)
                            | ((data[i + 2] & 0xFF) << 16)
                            | ((data[i + 3] & 0xFF) << 24);

            k1 *= 0xcc9e2d51;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= 0x1b873593;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // Process remaining bytes
        int k1 = 0;
        int switchVal = length & 0x03;
        switch (switchVal) {
            case 3:
                k1 ^= (data[roundedEnd + 2] & 0xFF) << 16;
            case 2:
                k1 ^= (data[roundedEnd + 1] & 0xFF) << 8;
            case 1:
                k1 ^= (data[roundedEnd] & 0xFF);
                k1 *= 0xcc9e2d51;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= 0x1b873593;
                h1 ^= k1;
        }

        // Final mixing
        h1 ^= length;
        h1 = fmix32(h1);

        return h1;
    }

    public static long hash64(byte[] data) {
        return hash64(data, 0, data.length, 0);
    }

    public static long hash64(byte[] data, int offset, int length, long seed) {
        long h1 = seed;
        long h2 = seed;

        int roundedEnd = offset + (length & 0xFFFFFFF0); // round down to multiple of 16 bytes

        for (int i = offset; i < roundedEnd; i += 16) {
            long k1 = getLongLittleEndian(data, i);
            long k2 = getLongLittleEndian(data, i + 8);

            // Process first block
            k1 *= C1;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= C2;
            h1 ^= k1;

            h1 = Long.rotateLeft(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            // Process second block
            k2 *= C2;
            k2 = Long.rotateLeft(k2, 33);
            k2 *= C1;
            h2 ^= k2;

            h2 = Long.rotateLeft(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }

        // Process remaining bytes
        long k1 = 0;
        long k2 = 0;

        int switchVal = length & 0x0F;
        switch (switchVal) {
            case 15:
                k2 ^= ((long) data[roundedEnd + 14] & 0xFF) << 48;
            case 14:
                k2 ^= ((long) data[roundedEnd + 13] & 0xFF) << 40;
            case 13:
                k2 ^= ((long) data[roundedEnd + 12] & 0xFF) << 32;
            case 12:
                k2 ^= ((long) data[roundedEnd + 11] & 0xFF) << 24;
            case 11:
                k2 ^= ((long) data[roundedEnd + 10] & 0xFF) << 16;
            case 10:
                k2 ^= ((long) data[roundedEnd + 9] & 0xFF) << 8;
            case 9:
                k2 ^= ((long) data[roundedEnd + 8] & 0xFF);
                k2 *= C2;
                k2 = Long.rotateLeft(k2, 33);
                k2 *= C1;
                h2 ^= k2;

            case 8:
                k1 ^= ((long) data[roundedEnd + 7] & 0xFF) << 56;
            case 7:
                k1 ^= ((long) data[roundedEnd + 6] & 0xFF) << 48;
            case 6:
                k1 ^= ((long) data[roundedEnd + 5] & 0xFF) << 40;
            case 5:
                k1 ^= ((long) data[roundedEnd + 4] & 0xFF) << 32;
            case 4:
                k1 ^= ((long) data[roundedEnd + 3] & 0xFF) << 24;
            case 3:
                k1 ^= ((long) data[roundedEnd + 2] & 0xFF) << 16;
            case 2:
                k1 ^= ((long) data[roundedEnd + 1] & 0xFF) << 8;
            case 1:
                k1 ^= ((long) data[roundedEnd] & 0xFF);
                k1 *= C1;
                k1 = Long.rotateLeft(k1, 31);
                k1 *= C2;
                h1 ^= k1;
        }

        // Final mixing
        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix64(h1);
        h2 = fmix64(h2);

        h1 += h2;
        h2 += h1;

        return h1;
    }

    private static long getLongLittleEndian(byte[] data, int offset) {
        return ((long) data[offset] & 0xFF)
                | (((long) data[offset + 1] & 0xFF) << 8)
                | (((long) data[offset + 2] & 0xFF) << 16)
                | (((long) data[offset + 3] & 0xFF) << 24)
                | (((long) data[offset + 4] & 0xFF) << 32)
                | (((long) data[offset + 5] & 0xFF) << 40)
                | (((long) data[offset + 6] & 0xFF) << 48)
                | (((long) data[offset + 7] & 0xFF) << 56);
    }

    private static int fmix32(int h) {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    private static long fmix64(long k) {
        k ^= k >>> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= k >>> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= k >>> 33;
        return k;
    }

    public static int hash32(String str) {
        return hash32(str.getBytes(StandardCharsets.UTF_8));
    }

    public static long hash64(String str) {
        return hash64(str.getBytes(StandardCharsets.UTF_8));
    }
}
