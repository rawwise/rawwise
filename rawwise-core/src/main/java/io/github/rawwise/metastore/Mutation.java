package io.github.rawwise.metastore;

import java.util.Arrays;
import java.util.HexFormat;

public sealed interface Mutation {

    byte[] key();

    record Put(byte[] key, byte[] value) implements Mutation {
        public Put {
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
            return o instanceof Put(byte[] k, byte[] v)
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
            return "Put[key=" + hf.formatHex(key) + ", value=" + hf.formatHex(value) + "]";
        }
    }

    record Delete(byte[] key) implements Mutation {
        public Delete {
            key = key.clone();
        }

        public byte[] key() {
            return key.clone();
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Delete(byte[] k) && Arrays.equals(key, k);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

        @Override
        public String toString() {
            return "Delete[key=" + HexFormat.of().formatHex(key) + "]";
        }
    }
}
