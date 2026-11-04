package io.github.rawwise.metastore;

import java.util.Arrays;
import java.util.HexFormat;

public record KeyValue(byte[] key, byte[] value) {

    public KeyValue {
        key = key.clone();
        value = value.clone();
    }

    public byte[] key() {
        return key.clone();
    }

    public byte[] value() {
        return value.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof KeyValue(byte[] k, byte[] v)
                && Arrays.equals(key, k)
                && Arrays.equals(value, v);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(key) + Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        HexFormat hf = HexFormat.of();
        return "KeyValue[key=" + hf.formatHex(key) + ", value=" + hf.formatHex(value) + "]";
    }
}
