package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.*;

import java.nio.ByteBuffer;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("WriteBuffer")
class WriteBufferTest {

    private WriteBuffer writeBuffer;

    @BeforeEach
    void setUp() {
        writeBuffer = new WriteBuffer();
    }

    @Nested
    @DisplayName("put/get basic functionality")
    class PutGetTests {

        @Test
        @DisplayName("put then get returns NEW_VALUE entry")
        void putThenGetReturnsNewValue() {
            byte[] key = "key1".getBytes();
            byte[] value = "value1".getBytes();

            writeBuffer.put("table1", key, value);

            WriteBuffer.BufferResult result = writeBuffer.get("table1", key);
            assertThat(result.exists()).isTrue();
            assertThat(result.entry().type()).isEqualTo(WriteBuffer.BufferEntryType.NEW_VALUE);
            assertThat(result.entry().getValue()).isEqualTo(value);
        }

        @Test
        @DisplayName("get on non-existent key returns exists=false")
        void getNonExistentKeyReturnsNotExists() {
            WriteBuffer.BufferResult result = writeBuffer.get("table1", "missing".getBytes());
            assertThat(result.exists()).isFalse();
            assertThat(result.entry()).isNull();
        }

        @Test
        @DisplayName("get on non-existent table returns exists=false")
        void getNonExistentTableReturnsNotExists() {
            WriteBuffer.BufferResult result = writeBuffer.get("nonexistent", "key".getBytes());
            assertThat(result.exists()).isFalse();
            assertThat(result.entry()).isNull();
        }

        @Test
        @DisplayName("put with different tables are isolated")
        void differentTablesAreIsolated() {
            byte[] key = "sameKey".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] value2 = "value2".getBytes();

            writeBuffer.put("table1", key, value1);
            writeBuffer.put("table2", key, value2);

            WriteBuffer.BufferResult result1 = writeBuffer.get("table1", key);
            WriteBuffer.BufferResult result2 = writeBuffer.get("table2", key);

            assertThat(result1.entry().getValue()).isEqualTo(value1);
            assertThat(result2.entry().getValue()).isEqualTo(value2);
        }
    }

    @Nested
    @DisplayName("delete creates tombstone")
    class DeleteTests {

        @Test
        @DisplayName("delete creates DELETED tombstone")
        void deleteCreatesTombstone() {
            byte[] key = "key1".getBytes();
            writeBuffer.put("table1", key, "value1".getBytes());
            writeBuffer.delete("table1", key);

            WriteBuffer.BufferResult result = writeBuffer.get("table1", key);
            assertThat(result.exists()).isTrue();
            assertThat(result.entry().type()).isEqualTo(WriteBuffer.BufferEntryType.DELETED);
            assertThat(result.entry().getValue()).isNull();
        }

        @Test
        @DisplayName("delete on non-existent key still creates tombstone")
        void deleteNonExistentKeyCreatesTombstone() {
            byte[] key = "ghost".getBytes();
            writeBuffer.delete("table1", key);

            WriteBuffer.BufferResult result = writeBuffer.get("table1", key);
            assertThat(result.exists()).isTrue();
            assertThat(result.entry().type()).isEqualTo(WriteBuffer.BufferEntryType.DELETED);
        }
    }

    @Nested
    @DisplayName("containsKey")
    class ContainsKeyTests {

        @Test
        @DisplayName("containsKey returns true for existing key")
        void containsKeyReturnsTrueForExistingKey() {
            byte[] key = "key1".getBytes();
            writeBuffer.put("table1", key, "value1".getBytes());

            assertThat(writeBuffer.containsKey("table1", key)).isTrue();
        }

        @Test
        @DisplayName("containsKey returns true for tombstone key")
        void containsKeyReturnsTrueForTombstone() {
            byte[] key = "key1".getBytes();
            writeBuffer.delete("table1", key);

            assertThat(writeBuffer.containsKey("table1", key)).isTrue();
        }

        @Test
        @DisplayName("containsKey returns false for missing key")
        void containsKeyReturnsFalseForMissingKey() {
            assertThat(writeBuffer.containsKey("table1", "missing".getBytes())).isFalse();
        }

        @Test
        @DisplayName("containsKey returns false for missing table")
        void containsKeyReturnsFalseForMissingTable() {
            assertThat(writeBuffer.containsKey("nonexistent", "key".getBytes())).isFalse();
        }
    }

    @Nested
    @DisplayName("put overwrites previous value")
    class OverwriteTests {

        @Test
        @DisplayName("put overwrites previous value")
        void putOverwritesPreviousValue() {
            byte[] key = "key1".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] value2 = "value2".getBytes();

            writeBuffer.put("table1", key, value1);
            writeBuffer.put("table1", key, value2);

            WriteBuffer.BufferResult result = writeBuffer.get("table1", key);
            assertThat(result.entry().getValue()).isEqualTo(value2);
        }

        @Test
        @DisplayName("put after delete restores value")
        void putAfterDeleteRestoresValue() {
            byte[] key = "key1".getBytes();
            writeBuffer.put("table1", key, "value1".getBytes());
            writeBuffer.delete("table1", key);
            writeBuffer.put("table1", key, "value2".getBytes());

            WriteBuffer.BufferResult result = writeBuffer.get("table1", key);
            assertThat(result.exists()).isTrue();
            assertThat(result.entry().type()).isEqualTo(WriteBuffer.BufferEntryType.NEW_VALUE);
            assertThat(result.entry().getValue()).isEqualTo("value2".getBytes());
        }

        @Test
        @DisplayName("overwrite adjusts byte tracking correctly")
        void overwriteAdjustsByteTracking() {
            byte[] key = "key1".getBytes();
            byte[] smallValue = new byte[10];
            byte[] largeValue = new byte[100];

            writeBuffer.put("table1", key, smallValue);
            long bytesAfterSmall = writeBuffer.currentBytes();

            writeBuffer.put("table1", key, largeValue);
            long bytesAfterLarge = writeBuffer.currentBytes();

            long expectedDelta = largeValue.length - smallValue.length;
            assertThat(bytesAfterLarge - bytesAfterSmall).isEqualTo(expectedDelta);
        }
    }

    @Nested
    @DisplayName("budget overflow")
    class BudgetOverflowTests {

        @Test
        @DisplayName("overflow throws WriteBufferOverflowException")
        void overflowThrowsException() {
            WriteBuffer smallBuffer = new WriteBuffer(20L);
            byte[] key = "key1".getBytes();
            byte[] value = "value1".getBytes();

            smallBuffer.put("table1", key, value);

            byte[] bigKey = "bigkey".getBytes();
            byte[] bigValue = new byte[100];

            assertThatThrownBy(() -> smallBuffer.put("table1", bigKey, bigValue))
                    .isInstanceOf(WriteBufferOverflowException.class);
        }

        @Test
        @DisplayName("overflow doesn't corrupt existing entries")
        void overflowDoesNotCorruptExistingEntries() {
            WriteBuffer smallBuffer = new WriteBuffer(30L);
            byte[] key = "k1".getBytes();
            byte[] value = "v1".getBytes();

            smallBuffer.put("table1", key, value);

            byte[] bigKey = "bigkey".getBytes();
            byte[] bigValue = new byte[100];

            assertThatThrownBy(() -> smallBuffer.put("table1", bigKey, bigValue))
                    .isInstanceOf(WriteBufferOverflowException.class);

            WriteBuffer.BufferResult result = smallBuffer.get("table1", key);
            assertThat(result.exists()).isTrue();
            assertThat(result.entry().getValue()).isEqualTo(value);
        }

        @Test
        @DisplayName("overflow doesn't corrupt on overwrite")
        void overflowDoesNotCorruptOnOverwrite() {
            WriteBuffer smallBuffer = new WriteBuffer(20L);
            byte[] key = "k1".getBytes();
            byte[] value = "v1".getBytes();

            smallBuffer.put("table1", key, value);

            byte[] bigValue = new byte[100];
            assertThatThrownBy(() -> smallBuffer.put("table1", key, bigValue))
                    .isInstanceOf(WriteBufferOverflowException.class);

            WriteBuffer.BufferResult result = smallBuffer.get("table1", key);
            assertThat(result.exists()).isTrue();
            assertThat(result.entry().getValue()).isEqualTo(value);
        }

        @Test
        @DisplayName("WriteBufferOverflowException contains correct fields")
        void overflowExceptionContainsCorrectFields() {
            WriteBuffer smallBuffer = new WriteBuffer(15L);
            byte[] key = "k1".getBytes();
            byte[] value = "v1".getBytes();

            smallBuffer.put("table1", key, value);

            byte[] bigKey = "big".getBytes();
            byte[] bigValue = new byte[50];

            assertThatThrownBy(() -> smallBuffer.put("table1", bigKey, bigValue))
                    .isInstanceOf(WriteBufferOverflowException.class)
                    .satisfies(
                            ex -> {
                                WriteBufferOverflowException overflow =
                                        (WriteBufferOverflowException) ex;
                                assertThat(overflow.maxBytes()).isEqualTo(15L);
                                assertThat(overflow.currentBytes()).isGreaterThan(0);
                                assertThat(overflow.attemptedBytes()).isGreaterThan(0);
                            });
        }

        @Test
        @DisplayName("overflow message format is correct")
        void overflowMessageFormat() {
            WriteBuffer smallBuffer = new WriteBuffer(10L);
            byte[] key = "k1".getBytes();
            byte[] value = "v1".getBytes();

            smallBuffer.put("table1", key, value);

            assertThatThrownBy(() -> smallBuffer.put("table1", "k2".getBytes(), new byte[50]))
                    .isInstanceOf(WriteBufferOverflowException.class)
                    .hasMessageMatching(
                            "WriteBuffer overflow: current=\\d+, max=10, attempted=\\d+");
        }
    }

    @Nested
    @DisplayName("drain")
    class DrainTests {

        @Test
        @DisplayName("drain returns all entries and clears table")
        void drainReturnsAllEntriesAndClearsTable() {
            byte[] key1 = "key1".getBytes();
            byte[] value1 = "value1".getBytes();
            byte[] key2 = "key2".getBytes();
            byte[] value2 = "value2".getBytes();

            writeBuffer.put("table1", key1, value1);
            writeBuffer.put("table1", key2, value2);

            Map<ByteBuffer, WriteBuffer.BufferEntry> drained = writeBuffer.drain("table1");

            assertThat(drained).hasSize(2);

            WriteBuffer.BufferResult afterDrain = writeBuffer.get("table1", key1);
            assertThat(afterDrain.exists()).isFalse();
        }

        @Test
        @DisplayName("drain resets byte tracking")
        void drainResetsByteTracking() {
            writeBuffer.put("table1", "key1".getBytes(), "value1".getBytes());
            writeBuffer.put("table1", "key2".getBytes(), "value2".getBytes());

            assertThat(writeBuffer.currentBytes()).isGreaterThan(0);

            writeBuffer.drain("table1");

            assertThat(writeBuffer.currentBytes()).isEqualTo(0);
        }

        @Test
        @DisplayName("drain on non-existent table returns empty map")
        void drainNonExistentTableReturnsEmptyMap() {
            Map<ByteBuffer, WriteBuffer.BufferEntry> drained = writeBuffer.drain("nonexistent");
            assertThat(drained).isEmpty();
        }

        @Test
        @DisplayName("drain only affects specified table")
        void drainOnlyAffectsSpecifiedTable() {
            byte[] key = "sameKey".getBytes();
            writeBuffer.put("table1", key, "v1".getBytes());
            writeBuffer.put("table2", key, "v2".getBytes());

            writeBuffer.drain("table1");

            assertThat(writeBuffer.get("table1", key).exists()).isFalse();
            assertThat(writeBuffer.get("table2", key).exists()).isTrue();
        }

        @Test
        @DisplayName("drain after delete keeps currentBytes non-negative")
        void drainAfterDeleteKeepsCurrentBytesNonNegative() {
            byte[] key = "key1".getBytes();
            byte[] value = "value1".getBytes();

            writeBuffer.put("table1", key, value);
            writeBuffer.delete("table1", key);

            writeBuffer.drain("table1");

            assertThat(writeBuffer.currentBytes()).isZero();
        }

        @Test
        @DisplayName("drain after delete non-existent key keeps currentBytes non-negative")
        void drainAfterDeleteNonExistentKeepsCurrentBytesNonNegative() {
            writeBuffer.delete("table1", "ghost".getBytes());

            writeBuffer.drain("table1");

            assertThat(writeBuffer.currentBytes()).isZero();
        }

        @Test
        @DisplayName("drain after mixed put and delete keeps currentBytes non-negative")
        void drainAfterMixedPutDeleteKeepsCurrentBytesNonNegative() {
            writeBuffer.put("table1", "key1".getBytes(), "value1".getBytes());
            writeBuffer.delete("table1", "key2".getBytes());
            writeBuffer.put("table1", "key3".getBytes(), "value3".getBytes());

            writeBuffer.drain("table1");

            assertThat(writeBuffer.currentBytes()).isZero();
        }
    }

    @Nested
    @DisplayName("clear")
    class ClearTests {

        @Test
        @DisplayName("clear removes all entries and resets bytes")
        void clearRemovesAllEntriesAndResetsBytes() {
            writeBuffer.put("table1", "key1".getBytes(), "value1".getBytes());
            writeBuffer.put("table2", "key2".getBytes(), "value2".getBytes());

            assertThat(writeBuffer.currentBytes()).isGreaterThan(0);

            writeBuffer.clear();

            assertThat(writeBuffer.currentBytes()).isEqualTo(0);
            assertThat(writeBuffer.get("table1", "key1".getBytes()).exists()).isFalse();
            assertThat(writeBuffer.get("table2", "key2".getBytes()).exists()).isFalse();
        }

        @Test
        @DisplayName("clear allows new writes after clear")
        void clearAllowsNewWritesAfterClear() {
            WriteBuffer smallBuffer = new WriteBuffer(30L);
            smallBuffer.put("table1", "k1".getBytes(), "v1".getBytes());
            smallBuffer.put("table1", "k2".getBytes(), "v2".getBytes());

            smallBuffer.clear();

            smallBuffer.put("table1", "k3".getBytes(), "v3".getBytes());
            assertThat(smallBuffer.get("table1", "k3".getBytes()).exists()).isTrue();
        }
    }

    @Nested
    @DisplayName("memory tracking")
    class MemoryTrackingTests {

        @Test
        @DisplayName("put increases currentBytes")
        void putIncreasesCurrentBytes() {
            long before = writeBuffer.currentBytes();
            byte[] key = "key1".getBytes();
            byte[] value = "value1".getBytes();

            writeBuffer.put("table1", key, value);

            assertThat(writeBuffer.currentBytes()).isEqualTo(before + key.length + value.length);
        }

        @Test
        @DisplayName(
                "delete decreases currentBytes by old value size, keeps key bytes for tombstone")
        void deleteDecreasesCurrentBytes() {
            byte[] key = "key1".getBytes();
            byte[] value = "value1".getBytes();

            writeBuffer.put("table1", key, value);
            long afterPut = writeBuffer.currentBytes();

            writeBuffer.delete("table1", key);

            assertThat(writeBuffer.currentBytes()).isEqualTo(afterPut - value.length);
        }

        @Test
        @DisplayName("delete on non-existent key adds key bytes for tombstone")
        void deleteNonExistentAddsKeyBytes() {
            long before = writeBuffer.currentBytes();
            byte[] key = "ghost".getBytes();
            writeBuffer.delete("table1", key);
            assertThat(writeBuffer.currentBytes()).isEqualTo(before + key.length);
        }

        @Test
        @DisplayName("overwrite adjusts bytes by delta")
        void overwriteAdjustsBytesByDelta() {
            byte[] key = "key1".getBytes();
            byte[] smallValue = new byte[10];
            byte[] largeValue = new byte[50];

            writeBuffer.put("table1", key, smallValue);
            long afterSmall = writeBuffer.currentBytes();

            writeBuffer.put("table1", key, largeValue);
            long afterLarge = writeBuffer.currentBytes();

            assertThat(afterLarge - afterSmall).isEqualTo(largeValue.length - smallValue.length);
        }
    }

    @Nested
    @DisplayName("constructor validation")
    class ConstructorTests {

        @Test
        @DisplayName("default constructor uses 64MB max")
        void defaultConstructorUses64MB() {
            WriteBuffer wb = new WriteBuffer();
            assertThat(wb.maxBytes()).isEqualTo(64L * 1024 * 1024);
        }

        @Test
        @DisplayName("custom maxBytes constructor works")
        void customMaxBytesConstructor() {
            WriteBuffer wb = new WriteBuffer(1024L);
            assertThat(wb.maxBytes()).isEqualTo(1024L);
        }

        @Test
        @DisplayName("zero maxBytes throws IllegalArgumentException")
        void zeroMaxBytesThrows() {
            assertThatThrownBy(() -> new WriteBuffer(0L))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("negative maxBytes throws IllegalArgumentException")
        void negativeMaxBytesThrows() {
            assertThatThrownBy(() -> new WriteBuffer(-1L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("input validation")
    class InputValidationTests {

        @Test
        @DisplayName("put with null tableId throws")
        void putNullTableIdThrows() {
            assertThatThrownBy(() -> writeBuffer.put(null, "k".getBytes(), "v".getBytes()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("put with blank tableId throws")
        void putBlankTableIdThrows() {
            assertThatThrownBy(() -> writeBuffer.put("  ", "k".getBytes(), "v".getBytes()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("put with null key throws")
        void putNullKeyThrows() {
            assertThatThrownBy(() -> writeBuffer.put("table1", null, "v".getBytes()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("put with null value throws")
        void putNullValueThrows() {
            assertThatThrownBy(() -> writeBuffer.put("table1", "k".getBytes(), null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("delete with null tableId throws")
        void deleteNullTableIdThrows() {
            assertThatThrownBy(() -> writeBuffer.delete(null, "k".getBytes()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("delete with null key throws")
        void deleteNullKeyThrows() {
            assertThatThrownBy(() -> writeBuffer.delete("table1", null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("get with null tableId returns not exists")
        void getNullTableIdReturnsNotExists() {
            WriteBuffer.BufferResult result = writeBuffer.get(null, "k".getBytes());
            assertThat(result.exists()).isFalse();
        }

        @Test
        @DisplayName("get with null key returns not exists")
        void getNullKeyReturnsNotExists() {
            WriteBuffer.BufferResult result = writeBuffer.get("table1", null);
            assertThat(result.exists()).isFalse();
        }
    }

    @Nested
    @DisplayName("BufferEntry defensive copy")
    class BufferEntryDefensiveCopyTests {

        @Test
        @DisplayName("BufferEntry getValue returns defensive copy")
        void bufferEntryGetValueReturnsDefensiveCopy() {
            byte[] originalValue = "value1".getBytes();
            WriteBuffer.BufferEntry entry =
                    new WriteBuffer.BufferEntry(
                            WriteBuffer.BufferEntryType.NEW_VALUE, originalValue);

            byte[] copy1 = entry.getValue();
            byte[] copy2 = entry.getValue();

            assertThat(copy1).isEqualTo(originalValue);
            assertThat(copy2).isEqualTo(originalValue);
            assertThat(copy1).isNotSameAs(copy2);
        }

        @Test
        @DisplayName("BufferEntry constructor clones value")
        void bufferEntryConstructorClonesValue() {
            byte[] originalValue = "value1".getBytes();
            WriteBuffer.BufferEntry entry =
                    new WriteBuffer.BufferEntry(
                            WriteBuffer.BufferEntryType.NEW_VALUE, originalValue);

            originalValue[0] = 'X';

            assertThat(entry.getValue()).isNotEqualTo(originalValue);
            assertThat(entry.getValue()).isEqualTo("value1".getBytes());
        }

        @Test
        @DisplayName("DELETED entry has null value")
        void deletedEntryHasNullValue() {
            WriteBuffer.BufferEntry entry =
                    new WriteBuffer.BufferEntry(WriteBuffer.BufferEntryType.DELETED, null);

            assertThat(entry.type()).isEqualTo(WriteBuffer.BufferEntryType.DELETED);
            assertThat(entry.getValue()).isNull();
        }
    }
}
