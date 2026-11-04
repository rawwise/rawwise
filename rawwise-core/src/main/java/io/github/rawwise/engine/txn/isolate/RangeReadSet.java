package io.github.rawwise.engine.txn.isolate;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * rangereadset, for SERIALIZABLE isolationlevelunderphantom readdetection.
 *
 * <p>whentransactionexecuterangequerywhen, willqueryrangeandpredicaterecordinto this set. in commit
 * stage ValidateStage, PhantomDetector usethissetcheckwhetherhas
 * othertransactioninrangeinsertnewdata.
 *
 * <p>Thread safety: use CopyOnWriteArrayList ensureconcurrentsecurity.
 * typicalscenarioundereachtransactionsinglethreadaccess(ThreadLocal bound), COW
 * mainneedforsecurityguarantee.
 */
public class RangeReadSet {

    /** maximumrangereadrecordcount, prevent unbounded growth.exceed this valuerecord WARN log. */
    private static final int MAX_RANGE_READS = 10000;

    /** rangereadentry: recordtransactionexecutethroughrangequery. */
    public record RangeReadEntry(
            String tableId,
            byte[] startKey,
            byte[] endKey,
            boolean startInclusive,
            boolean endInclusive,
            String predicateExpression,
            int estimatedRowCount,
            long readAtSnapshotTs) {

        public RangeReadEntry {
            if (tableId == null || tableId.isBlank())
                throw new IllegalArgumentException("tableId must not be blank");
            if (startKey == null) throw new IllegalArgumentException("startKey must not be null");
            if (endKey == null) throw new IllegalArgumentException("endKey must not be null");
        }

        /** checkgiven key whetherinthisrangequerywithin. */
        public boolean contains(byte[] key) {
            if (key == null) return false;

            int cmpStart = Arrays.compareUnsigned(key, startKey);
            if (startInclusive ? cmpStart < 0 : cmpStart <= 0) return false;

            int cmpEnd = Arrays.compareUnsigned(key, endKey);
            return endInclusive ? cmpEnd <= 0 : cmpEnd < 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof RangeReadEntry that)) return false;
            return tableId.equals(that.tableId)
                    && Arrays.equals(startKey, that.startKey)
                    && Arrays.equals(endKey, that.endKey)
                    && startInclusive == that.startInclusive
                    && endInclusive == that.endInclusive;
        }

        @Override
        public int hashCode() {
            int result = tableId.hashCode();
            result = 31 * result + Arrays.hashCode(startKey);
            result = 31 * result + Arrays.hashCode(endKey);
            result = 31 * result + Boolean.hashCode(startInclusive);
            result = 31 * result + Boolean.hashCode(endInclusive);
            return result;
        }
    }

    private final List<RangeReadEntry> entries;

    public RangeReadSet() {
        this.entries = new CopyOnWriteArrayList<>();
    }

    /**
     * recordonetimerange read.
     *
     * @param entry rangereadentry
     */
    public void record(RangeReadEntry entry) {
        if (entries.size() >= MAX_RANGE_READS) {
            // Prevent unbounded growth — log warning and drop oldest entries
            System.getLogger(RangeReadSet.class.getName())
                    .log(
                            java.lang.System.Logger.Level.WARNING,
                            "RangeReadSet exceeded MAX_RANGE_READS={0}, truncating oldest entries",
                            MAX_RANGE_READS);
            // Drop oldest half — CopyOnWriteArrayList.subList() does not support structural
            // modification
            int dropCount = entries.size() / 2;
            for (int i = 0; i < dropCount; i++) {
                entries.remove(0);
            }
        }
        entries.add(entry);
    }

    /**
     * returnallalreadyrecordrangereadentry.
     *
     * @return immutablelist
     */
    public List<RangeReadEntry> entries() {
        return List.copyOf(entries);
    }

    /**
     * determinespecified tableId+key whether in any alreadyrecordrangereadwithin.
     *
     * @param tableId table ID
     * @param key key
     * @return true if key fallinat leastonealreadyrecordrange
     */
    public boolean isInAnyRange(String tableId, byte[] key) {
        for (RangeReadEntry entry : entries) {
            if (entry.tableId().equals(tableId) && entry.contains(key)) {
                return true;
            }
        }
        return false;
    }

    /** returnalreadyrecordrangereadcount. */
    public int size() {
        return entries.size();
    }

    /** clearemptyallrecord. */
    public void clear() {
        entries.clear();
    }

    /** whetheris empty. */
    public boolean isEmpty() {
        return entries.isEmpty();
    }
}
