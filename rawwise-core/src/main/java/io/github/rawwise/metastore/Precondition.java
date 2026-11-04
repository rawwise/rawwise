package io.github.rawwise.metastore;

import java.util.Arrays;
import java.util.HexFormat;

public sealed interface Precondition {

    byte[] key();

    record Exists(byte[] key) implements Precondition {
        public Exists {
            key = key.clone();
        }

        public byte[] key() {
            return key.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Exists(byte[] k) && Arrays.equals(key, k);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

        @Override
        public String toString() {
            return "Exists[key=" + HexFormat.of().formatHex(key) + "]";
        }
    }

    record NotExists(byte[] key) implements Precondition {
        public NotExists {
            key = key.clone();
        }

        public byte[] key() {
            return key.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof NotExists(byte[] k) && Arrays.equals(key, k);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

        @Override
        public String toString() {
            return "NotExists[key=" + HexFormat.of().formatHex(key) + "]";
        }
    }

    record ValueEquals(byte[] key, byte[] expected) implements Precondition {
        public ValueEquals {
            key = key.clone();
            expected = expected.clone();
        }

        public byte[] key() {
            return key.clone();
        }

        public byte[] expected() {
            return expected.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ValueEquals(byte[] k, byte[] e)
                    && Arrays.equals(key, k)
                    && Arrays.equals(expected, e);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(key) + Arrays.hashCode(expected);
        }

        @Override
        public String toString() {
            HexFormat hf = HexFormat.of();
            return "ValueEquals[key="
                    + hf.formatHex(key)
                    + ", expected="
                    + hf.formatHex(expected)
                    + "]";
        }
    }
}
