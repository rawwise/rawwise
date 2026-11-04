package io.github.rawwise.engine.storage;

import java.io.Serializable;
import java.util.Arrays;

public record KeyRange(byte[] startKey, boolean startInclusive, byte[] endKey, boolean endInclusive)
        implements Serializable {

    public static final KeyRange FULL_RANGE = new KeyRange(null, true, null, true);

    public KeyRange {
        startKey = startKey == null ? null : startKey.clone();
        endKey = endKey == null ? null : endKey.clone();
    }

    public boolean isFullRange() {
        return startKey == null && endKey == null && startInclusive && endInclusive;
    }

    public boolean contains(byte[] key) {
        if (key == null) return false;
        boolean startOk;
        if (startKey == null) {
            startOk = true;
        } else {
            startOk = startInclusive ? compare(key, startKey) >= 0 : compare(key, startKey) > 0;
        }
        boolean endOk;
        if (endKey == null) {
            endOk = true;
        } else {
            endOk = endInclusive ? compare(key, endKey) <= 0 : compare(key, endKey) < 0;
        }
        return startOk && endOk;
    }

    public boolean overlaps(KeyRange other) {
        if (other == null) return false;
        if (this.isFullRange() || other.isFullRange()) return true;

        boolean startBeforeOtherEnd;
        if (this.startKey == null || other.endKey == null) {
            startBeforeOtherEnd = true;
        } else {
            int cmp = compare(this.startKey, other.endKey);
            if (cmp < 0) {
                startBeforeOtherEnd = true;
            } else if (cmp == 0) {
                startBeforeOtherEnd = this.startInclusive && other.endInclusive;
            } else {
                startBeforeOtherEnd = false;
            }
        }

        boolean endAfterOtherStart;
        if (this.endKey == null || other.startKey == null) {
            endAfterOtherStart = true;
        } else {
            int cmp = compare(this.endKey, other.startKey);
            if (cmp > 0) {
                endAfterOtherStart = true;
            } else if (cmp == 0) {
                endAfterOtherStart = this.endInclusive && other.startInclusive;
            } else {
                endAfterOtherStart = false;
            }
        }

        return startBeforeOtherEnd && endAfterOtherStart;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyRange that)) return false;
        return startInclusive == that.startInclusive
                && endInclusive == that.endInclusive
                && Arrays.equals(startKey, that.startKey)
                && Arrays.equals(endKey, that.endKey);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(startKey);
        result = 31 * result + Boolean.hashCode(startInclusive);
        result = 31 * result + Arrays.hashCode(endKey);
        result = 31 * result + Boolean.hashCode(endInclusive);
        return result;
    }

    public static int compare(byte[] left, byte[] right) {
        return ByteArrays.compareUnsigned(left, right);
    }
}
