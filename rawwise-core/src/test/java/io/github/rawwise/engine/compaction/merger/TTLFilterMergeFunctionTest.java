package io.github.rawwise.engine.compaction.merger;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class TTLFilterMergeFunctionTest {

    private static final long EXPIRY_THRESHOLD_MS = 1000L;

    private final TimestampExtractor extractor =
            (key, value) -> {
                if (value == null || value.length < 8) return Long.MIN_VALUE;
                return ByteBuffer.wrap(value).getLong();
            };

    private VersionedValue vv(byte[] value, long seqNum, boolean tombstone) {
        return VersionedValue.mutable(value, seqNum, System.currentTimeMillis(), tombstone);
    }

    private byte[] key(String k) {
        return k.getBytes();
    }

    private byte[] tsValue(long timestampMs) {
        return ByteBuffer.allocate(8).putLong(timestampMs).array();
    }

    @Test
    void expiredRecordFilteredOut() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row1");
        byte[] expiredValue = tsValue(500L);
        List<VersionedValue> values = List.of(vv(expiredValue, 1L, false));

        byte[] result = fn.merge(key, values);
        assertThat(result).isNull();
    }

    @Test
    void unexpiredRecordPassesThrough() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row2");
        byte[] validValue = tsValue(2000L);
        List<VersionedValue> values = List.of(vv(validValue, 1L, false));

        byte[] result = fn.merge(key, values);
        assertThat(result).isEqualTo(validValue);
    }

    @Test
    void tombstoneRecordReturnsNull() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row3");
        byte[] value = tsValue(2000L);
        List<VersionedValue> values = List.of(vv(value, 1L, true));

        byte[] result = fn.merge(key, values);
        assertThat(result).isNull();
    }

    @Test
    void nullKeyReturnsNull() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] value = tsValue(2000L);
        List<VersionedValue> values = List.of(vv(value, 1L, false));

        byte[] result = fn.merge(null, values);
        assertThat(result).isNull();
    }

    @Test
    void delegateIsCalledForNonExpiredRecords() {
        byte[] delegateResult = "rewritten".getBytes();

        MergeFunction delegate = (key, versionedValues) -> delegateResult;

        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(delegate, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row4");
        byte[] validValue = tsValue(2000L);
        List<VersionedValue> values = List.of(vv(validValue, 1L, false));

        byte[] result = fn.merge(key, values);
        assertThat(result).isEqualTo(delegateResult);
    }

    @Test
    void emptyVersionedValuesReturnsNull() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row5");
        byte[] result = fn.merge(key, List.of());
        assertThat(result).isNull();
    }

    @Test
    void nullVersionedValuesReturnsNull() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row6");
        byte[] result = fn.merge(key, null);
        assertThat(result).isNull();
    }

    @Test
    void latestVersionUsedForExpiryCheck() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row7");
        byte[] expiredValue = tsValue(500L);
        byte[] validValue = tsValue(2000L);

        List<VersionedValue> values =
                List.of(vv(expiredValue, 1L, false), vv(validValue, 2L, false));

        byte[] result = fn.merge(key, values);
        assertThat(result).isEqualTo(validValue);
    }

    @Test
    void latestVersionExpiredReturnsNull() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row8");
        byte[] validValue = tsValue(2000L);
        byte[] expiredValue = tsValue(500L);

        List<VersionedValue> values =
                List.of(vv(validValue, 1L, false), vv(expiredValue, 2L, false));

        byte[] result = fn.merge(key, values);
        assertThat(result).isNull();
    }

    @Test
    void exactThresholdRecordNotFiltered() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row9");
        byte[] exactValue = tsValue(EXPIRY_THRESHOLD_MS);
        List<VersionedValue> values = List.of(vv(exactValue, 1L, false));

        byte[] result = fn.merge(key, values);
        assertThat(result).isEqualTo(exactValue);
    }

    @Test
    void longMinValueTimestampSkipped() {
        TTLFilterMergeFunction fn =
                new TTLFilterMergeFunction(null, EXPIRY_THRESHOLD_MS, extractor);

        byte[] key = key("row10");
        byte[] minValueTs = tsValue(Long.MIN_VALUE);
        List<VersionedValue> values = List.of(vv(minValueTs, 1L, false));

        byte[] result = fn.merge(key, values);
        assertThat(result).isNull();
    }
}
