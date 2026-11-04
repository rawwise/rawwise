package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public record ComparableByteArray(byte[] data)
        implements Comparable<ComparableByteArray>, Serializable {
    private static final long serialVersionUID = 1L;

    public ComparableByteArray {
        Objects.requireNonNull(data, "data must not be null");
    }

    public static ComparableByteArray wrap(byte[] data) {
        return new ComparableByteArray(data);
    }

    @Override
    public int compareTo(ComparableByteArray o) {
        int len = Math.min(data.length, o.data.length);
        for (int i = 0; i < len; i++) {
            int cmp = Byte.compare(data[i], o.data[i]);
            if (cmp != 0) return cmp;
        }
        return Integer.compare(data.length, o.data.length);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ComparableByteArray that && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "ComparableByteArray" + Arrays.toString(data);
    }
}
