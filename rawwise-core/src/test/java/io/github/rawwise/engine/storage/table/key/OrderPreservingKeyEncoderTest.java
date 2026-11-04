package io.github.rawwise.engine.storage.table.key;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.engine.storage.KeyRange;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class OrderPreservingKeyEncoderTest {

    private static Map<String, Comparable<?>> pk(String col, Comparable<?> value) {
        Map<String, Comparable<?>> m = new LinkedHashMap<>();
        m.put(col, value);
        return m;
    }

    private static int unsignedCompare(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int ca = a[i] & 0xFF;
            int cb = b[i] & 0xFF;
            if (ca != cb) return Integer.compare(ca, cb);
        }
        return Integer.compare(a.length, b.length);
    }

    @Nested
    class IntegerEncoding {

        @Test
        void negativeEncodesBeforePositive() {
            byte[] neg = OrderPreservingKeyEncoder.encode(pk("id", -1));
            byte[] zero = OrderPreservingKeyEncoder.encode(pk("id", 0));
            byte[] pos = OrderPreservingKeyEncoder.encode(pk("id", 1));
            assertThat(unsignedCompare(neg, zero)).isNegative();
            assertThat(unsignedCompare(zero, pos)).isNegative();
            assertThat(unsignedCompare(neg, pos)).isNegative();
        }

        @Test
        void preservesFullSignedRange() {
            int[] values = {Integer.MIN_VALUE, -1000, -1, 0, 1, 1000, Integer.MAX_VALUE};
            byte[][] encoded = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                encoded[i] = OrderPreservingKeyEncoder.encode(pk("id", values[i]));
            }
            for (int i = 0; i < values.length - 1; i++) {
                assertThat(unsignedCompare(encoded[i], encoded[i + 1]))
                        .as(
                                "values[%d]=%d should encode before values[%d]=%d",
                                i, values[i], i + 1, values[i + 1])
                        .isNegative();
            }
        }

        @Test
        void minValueEncodesBeforeMaxValue() {
            byte[] minVal = OrderPreservingKeyEncoder.encode(pk("id", Integer.MIN_VALUE));
            byte[] maxVal = OrderPreservingKeyEncoder.encode(pk("id", Integer.MAX_VALUE));
            assertThat(unsignedCompare(minVal, maxVal)).isNegative();
        }

        @Test
        void encodesTo4Bytes() {
            assertThat(OrderPreservingKeyEncoder.encode(pk("id", 0))).hasSize(4);
        }
    }

    @Nested
    class LongEncoding {

        @Test
        void negativeEncodesBeforePositive() {
            byte[] neg = OrderPreservingKeyEncoder.encode(pk("id", -1L));
            byte[] zero = OrderPreservingKeyEncoder.encode(pk("id", 0L));
            byte[] pos = OrderPreservingKeyEncoder.encode(pk("id", 1L));
            assertThat(unsignedCompare(neg, zero)).isNegative();
            assertThat(unsignedCompare(zero, pos)).isNegative();
        }

        @Test
        void preservesFullSignedRange() {
            long[] values = {Long.MIN_VALUE, -1000L, -1L, 0L, 1L, 1000L, Long.MAX_VALUE};
            byte[][] encoded = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                encoded[i] = OrderPreservingKeyEncoder.encode(pk("id", values[i]));
            }
            for (int i = 0; i < values.length - 1; i++) {
                assertThat(unsignedCompare(encoded[i], encoded[i + 1]))
                        .as(
                                "values[%d]=%d should encode before values[%d]=%d",
                                i, values[i], i + 1, values[i + 1])
                        .isNegative();
            }
        }

        @Test
        void encodesTo8Bytes() {
            assertThat(OrderPreservingKeyEncoder.encode(pk("id", 0L))).hasSize(8);
        }
    }

    @Nested
    class ShortEncoding {

        @Test
        void negativeEncodesBeforePositive() {
            byte[] neg = OrderPreservingKeyEncoder.encode(pk("id", (short) -1));
            byte[] zero = OrderPreservingKeyEncoder.encode(pk("id", (short) 0));
            byte[] pos = OrderPreservingKeyEncoder.encode(pk("id", (short) 1));
            assertThat(unsignedCompare(neg, zero)).isNegative();
            assertThat(unsignedCompare(zero, pos)).isNegative();
        }

        @Test
        void preservesFullSignedRange() {
            short[] values = {
                Short.MIN_VALUE,
                (short) -100,
                (short) -1,
                (short) 0,
                (short) 1,
                (short) 100,
                Short.MAX_VALUE
            };
            byte[][] encoded = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                encoded[i] = OrderPreservingKeyEncoder.encode(pk("id", values[i]));
            }
            for (int i = 0; i < values.length - 1; i++) {
                assertThat(unsignedCompare(encoded[i], encoded[i + 1]))
                        .as(
                                "values[%d]=%d should encode before values[%d]=%d",
                                i, values[i], i + 1, values[i + 1])
                        .isNegative();
            }
        }

        @Test
        void encodesTo2Bytes() {
            assertThat(OrderPreservingKeyEncoder.encode(pk("id", (short) 0))).hasSize(2);
        }
    }

    @Nested
    class ByteEncoding {

        @Test
        void negativeEncodesBeforePositive() {
            byte[] neg = OrderPreservingKeyEncoder.encode(pk("id", (byte) -1));
            byte[] zero = OrderPreservingKeyEncoder.encode(pk("id", (byte) 0));
            byte[] pos = OrderPreservingKeyEncoder.encode(pk("id", (byte) 1));
            assertThat(unsignedCompare(neg, zero)).isNegative();
            assertThat(unsignedCompare(zero, pos)).isNegative();
        }

        @Test
        void preservesFullSignedRange() {
            byte[] values = {
                Byte.MIN_VALUE, (byte) -10, (byte) -1, (byte) 0, (byte) 1, (byte) 10, Byte.MAX_VALUE
            };
            byte[][] encoded = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                encoded[i] = OrderPreservingKeyEncoder.encode(pk("id", values[i]));
            }
            for (int i = 0; i < values.length - 1; i++) {
                assertThat(unsignedCompare(encoded[i], encoded[i + 1]))
                        .as(
                                "values[%d]=%d should encode before values[%d]=%d",
                                i, values[i], i + 1, values[i + 1])
                        .isNegative();
            }
        }

        @Test
        void encodesTo1Byte() {
            assertThat(OrderPreservingKeyEncoder.encode(pk("id", (byte) 0))).hasSize(1);
        }
    }

    @Nested
    class FloatEncoding {

        @Test
        void negativeEncodesBeforePositive() {
            byte[] neg = OrderPreservingKeyEncoder.encode(pk("id", -1.5f));
            byte[] zero = OrderPreservingKeyEncoder.encode(pk("id", 0.0f));
            byte[] pos = OrderPreservingKeyEncoder.encode(pk("id", 1.5f));
            assertThat(unsignedCompare(neg, zero)).isNegative();
            assertThat(unsignedCompare(zero, pos)).isNegative();
        }

        @Test
        void preservesOrderAcrossMagnitudes() {
            float[] values = {
                Float.NEGATIVE_INFINITY,
                -Float.MAX_VALUE,
                -1.5f,
                -0.0f,
                0.0f,
                1.5f,
                Float.MAX_VALUE,
                Float.POSITIVE_INFINITY
            };
            byte[][] encoded = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                encoded[i] = OrderPreservingKeyEncoder.encode(pk("id", values[i]));
            }
            for (int i = 0; i < values.length - 1; i++) {
                assertThat(unsignedCompare(encoded[i], encoded[i + 1]))
                        .as(
                                "values[%d]=%f should encode before values[%d]=%f",
                                i, values[i], i + 1, values[i + 1])
                        .isNegative();
            }
        }
    }

    @Nested
    class DoubleEncoding {

        @Test
        void negativeEncodesBeforePositive() {
            byte[] neg = OrderPreservingKeyEncoder.encode(pk("id", -1.5));
            byte[] zero = OrderPreservingKeyEncoder.encode(pk("id", 0.0));
            byte[] pos = OrderPreservingKeyEncoder.encode(pk("id", 1.5));
            assertThat(unsignedCompare(neg, zero)).isNegative();
            assertThat(unsignedCompare(zero, pos)).isNegative();
        }

        @Test
        void preservesOrderAcrossMagnitudes() {
            double[] values = {
                Double.NEGATIVE_INFINITY,
                -Double.MAX_VALUE,
                -1.5,
                -0.0,
                0.0,
                1.5,
                Double.MAX_VALUE,
                Double.POSITIVE_INFINITY
            };
            byte[][] encoded = new byte[values.length][];
            for (int i = 0; i < values.length; i++) {
                encoded[i] = OrderPreservingKeyEncoder.encode(pk("id", values[i]));
            }
            for (int i = 0; i < values.length - 1; i++) {
                assertThat(unsignedCompare(encoded[i], encoded[i + 1]))
                        .as(
                                "values[%d]=%f should encode before values[%d]=%f",
                                i, values[i], i + 1, values[i + 1])
                        .isNegative();
            }
        }
    }

    @Nested
    class StringEncoding {

        @Test
        void preservesLexicographicOrder() {
            byte[] a = OrderPreservingKeyEncoder.encode(pk("id", "apple"));
            byte[] b = OrderPreservingKeyEncoder.encode(pk("id", "banana"));
            byte[] c = OrderPreservingKeyEncoder.encode(pk("id", "cherry"));
            assertThat(unsignedCompare(a, b)).isNegative();
            assertThat(unsignedCompare(b, c)).isNegative();
        }

        @Test
        void nullTerminatorPreventsPrefixConflict() {
            byte[] prefix = OrderPreservingKeyEncoder.encode(pk("id", "ab"));
            byte[] longer = OrderPreservingKeyEncoder.encode(pk("id", "abcd"));
            assertThat(unsignedCompare(prefix, longer)).isNegative();
        }

        @Test
        void emptyStringEncodesBeforeNonEmpty() {
            byte[] empty = OrderPreservingKeyEncoder.encode(pk("id", ""));
            byte[] nonEmpty = OrderPreservingKeyEncoder.encode(pk("id", "a"));
            assertThat(unsignedCompare(empty, nonEmpty)).isNegative();
        }
    }

    @Nested
    class BooleanEncoding {

        @Test
        void falseEncodesBeforeTrue() {
            byte[] f = OrderPreservingKeyEncoder.encode(pk("id", false));
            byte[] t = OrderPreservingKeyEncoder.encode(pk("id", true));
            assertThat(unsignedCompare(f, t)).isNegative();
        }

        @Test
        void encodesTo1Byte() {
            assertThat(OrderPreservingKeyEncoder.encode(pk("id", false))).hasSize(1);
            assertThat(OrderPreservingKeyEncoder.encode(pk("id", true))).hasSize(1);
        }
    }

    @Nested
    class CompositePrimaryKey {

        @Test
        void preservesTupleOrder() {
            byte[] first = OrderPreservingKeyEncoder.encode(compositePk(1, "a"));
            byte[] second = OrderPreservingKeyEncoder.encode(compositePk(1, "b"));
            byte[] third = OrderPreservingKeyEncoder.encode(compositePk(2, "a"));
            assertThat(unsignedCompare(first, second)).isNegative();
            assertThat(unsignedCompare(second, third)).isNegative();
        }

        @Test
        void negativeFirstFieldOrdersCorrectly() {
            byte[] neg = OrderPreservingKeyEncoder.encode(compositePk(-1, "z"));
            byte[] zero = OrderPreservingKeyEncoder.encode(compositePk(0, "a"));
            assertThat(unsignedCompare(neg, zero)).isNegative();
        }

        private static Map<String, Comparable<?>> compositePk(int part1, String part2) {
            Map<String, Comparable<?>> m = new LinkedHashMap<>();
            m.put("part1", part1);
            m.put("part2", part2);
            return m;
        }
    }

    @Nested
    class KeyRangeIntegration {

        @Test
        void containsReturnsTrueForKeyInRange() {
            byte[] lo = OrderPreservingKeyEncoder.encode(pk("id", 100));
            byte[] mid = OrderPreservingKeyEncoder.encode(pk("id", 150));
            byte[] hi = OrderPreservingKeyEncoder.encode(pk("id", 200));
            KeyRange range = new KeyRange(lo, true, hi, true);
            assertThat(range.contains(mid)).isTrue();
            assertThat(range.contains(lo)).isTrue();
            assertThat(range.contains(hi)).isTrue();
        }

        @Test
        void containsReturnsFalseForKeyOutOfRange() {
            byte[] lo = OrderPreservingKeyEncoder.encode(pk("id", 100));
            byte[] mid = OrderPreservingKeyEncoder.encode(pk("id", 150));
            byte[] hi = OrderPreservingKeyEncoder.encode(pk("id", 200));
            KeyRange range = new KeyRange(lo, true, hi, true);
            byte[] below = OrderPreservingKeyEncoder.encode(pk("id", 50));
            byte[] above = OrderPreservingKeyEncoder.encode(pk("id", 250));
            assertThat(range.contains(below)).isFalse();
            assertThat(range.contains(above)).isFalse();
        }

        @Test
        void containsHandlesNegativeBounds() {
            byte[] lo = OrderPreservingKeyEncoder.encode(pk("id", -100));
            byte[] mid = OrderPreservingKeyEncoder.encode(pk("id", -50));
            byte[] hi = OrderPreservingKeyEncoder.encode(pk("id", 100));
            KeyRange range = new KeyRange(lo, true, hi, true);
            assertThat(range.contains(mid)).isTrue();
            byte[] below = OrderPreservingKeyEncoder.encode(pk("id", -200));
            byte[] above = OrderPreservingKeyEncoder.encode(pk("id", 200));
            assertThat(range.contains(below)).isFalse();
            assertThat(range.contains(above)).isFalse();
        }

        @Test
        void shardAssignmentCoversKeyWithMultipleRanges() {
            byte[] range1Lo = OrderPreservingKeyEncoder.encode(pk("id", -100));
            byte[] range1Hi = OrderPreservingKeyEncoder.encode(pk("id", -1));
            byte[] range2Lo = OrderPreservingKeyEncoder.encode(pk("id", 100));
            byte[] range2Hi = OrderPreservingKeyEncoder.encode(pk("id", 200));
            List<KeyRange> ranges =
                    List.of(
                            new KeyRange(range1Lo, true, range1Hi, true),
                            new KeyRange(range2Lo, true, range2Hi, true));
            byte[] keyInFirst = OrderPreservingKeyEncoder.encode(pk("id", -50));
            byte[] keyInSecond = OrderPreservingKeyEncoder.encode(pk("id", 150));
            byte[] keyInGap = OrderPreservingKeyEncoder.encode(pk("id", 50));
            assertThat(anyRangeContains(ranges, keyInFirst)).isTrue();
            assertThat(anyRangeContains(ranges, keyInSecond)).isTrue();
            assertThat(anyRangeContains(ranges, keyInGap)).isFalse();
        }

        private static boolean anyRangeContains(List<KeyRange> ranges, byte[] key) {
            for (KeyRange r : ranges) {
                if (r.contains(key)) return true;
            }
            return false;
        }
    }

    @Nested
    class ErrorHandling {

        @Test
        void emptyMapThrows() {
            assertThatThrownBy(() -> OrderPreservingKeyEncoder.encode(Map.of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void nullMapThrows() {
            assertThatThrownBy(() -> OrderPreservingKeyEncoder.encode(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void nullValueThrows() {
            Map<String, Comparable<?>> m = new LinkedHashMap<>();
            m.put("id", null);
            assertThatThrownBy(() -> OrderPreservingKeyEncoder.encode(m))
                    .isInstanceOf(RuntimeException.class)
                    .rootCause()
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void unsupportedTypeThrows() {
            assertThatThrownBy(
                            () ->
                                    OrderPreservingKeyEncoder.encode(
                                            pk("id", java.math.BigInteger.ZERO)))
                    .isInstanceOf(RuntimeException.class);
        }
    }
}
