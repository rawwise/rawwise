package io.github.rawwise.engine.storage;

public final class ByteArrays {

    private ByteArrays() {}

    public static int compareUnsigned(byte[] left, byte[] right) {
        int min = Math.min(left.length, right.length);
        for (int i = 0; i < min; i++) {
            int cmp = Byte.compareUnsigned(left[i], right[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
