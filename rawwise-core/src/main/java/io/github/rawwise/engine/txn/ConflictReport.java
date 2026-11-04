package io.github.rawwise.engine.txn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * conflictdetectionreport: recordOCCconflictdetectionresult.
 *
 * <p>conflicttypeexplanation:
 *
 * <ul>
 *   <li>WRITE_WRITE - write-writeconflict: twotransactionwhilewritesameonekey
 *   <li>READ_WRITE - read-writeconflict: thistransactionreadkeybeothertransactionmodified
 *   <li>PHANTOM - phantom readconflict: rangequeryresult setoccurchange
 * </ul>
 *
 * <p>supportmultipleitemconflictentry:
 * whenPARTIAL_COMMITstrategydetectiontomultipleitemconflictkeywhen,
 * allconflictkeyallwillberecordinconflictslistin, to at onceremoveallconflictkey.
 */
public class ConflictReport {

    /** conflicttypeenum */
    public enum ConflictType {
        /** write-writeconflict: twotransactionwritesameonekey */
        WRITE_WRITE,
        /** read-writeconflict: thistransactionreadkeybeothertransactionmodified */
        READ_WRITE,
        /** phantom readconflict: rangequeryresult setchange */
        PHANTOM
    }

    /**
     * conflictentry: recordsingleitemkeyconflictdetails. forPARTIAL_COMMITstrategycollect at once
     * inallconflictkey.
     */
    public static final class ConflictEntry {
        private final ConflictType conflictType;
        private final byte[] conflictKey;
        private final long myVersion;
        private final long conflictingVersion;
        private final long conflictingTxnId;

        public ConflictEntry(
                ConflictType conflictType,
                byte[] conflictKey,
                long myVersion,
                long conflictingVersion,
                long conflictingTxnId) {
            this.conflictType = conflictType;
            this.conflictKey = conflictKey != null ? conflictKey.clone() : null;
            this.myVersion = myVersion;
            this.conflictingVersion = conflictingVersion;
            this.conflictingTxnId = conflictingTxnId;
        }

        public ConflictType conflictType() {
            return conflictType;
        }

        /** returnconflictkeypreventdefenseitycopy */
        public byte[] conflictKey() {
            return conflictKey != null ? conflictKey.clone() : null;
        }

        public long myVersion() {
            return myVersion;
        }

        public long conflictingVersion() {
            return conflictingVersion;
        }

        public long conflictingTxnId() {
            return conflictingTxnId;
        }

        @Override
        public String toString() {
            return "ConflictEntry{type="
                    + conflictType
                    + ", key="
                    + (conflictKey != null ? Arrays.toString(conflictKey) : "null")
                    + ", myVersion="
                    + myVersion
                    + ", conflictingVersion="
                    + conflictingVersion
                    + ", conflictingTxnId="
                    + conflictingTxnId
                    + "}";
        }
    }

    private final boolean hasConflict;
    private final ConflictType conflictType;
    private final byte[] conflictKey;
    private final long myVersion;
    private final long conflictingVersion;
    private final long conflictingTxnId;
    private final String description;

    /** allconflictentrylist(supportmultipleconflictkeyscenario, forPARTIAL_COMMITstrategy) */
    private final List<ConflictEntry> conflicts;

    private ConflictReport(
            boolean hasConflict,
            ConflictType conflictType,
            byte[] conflictKey,
            long myVersion,
            long conflictingVersion,
            long conflictingTxnId,
            String description,
            List<ConflictEntry> conflicts) {
        this.hasConflict = hasConflict;
        this.conflictType = conflictType;
        this.conflictKey = conflictKey != null ? conflictKey.clone() : null;
        this.myVersion = myVersion;
        this.conflictingVersion = conflictingVersion;
        this.conflictingTxnId = conflictingTxnId;
        this.description = description;
        this.conflicts =
                conflicts != null
                        ? Collections.unmodifiableList(new ArrayList<>(conflicts))
                        : Collections.emptyList();
    }

    /** createnoconflictreport. */
    public static ConflictReport noConflict() {
        return new ConflictReport(
                false, null, null, -1, -1, -1, "No conflict detected", Collections.emptyList());
    }

    /**
     * createwrite-writeconflictreport.
     *
     * @param conflictKey conflictkey
     * @param myVersion thistransactionversion number
     * @param conflictingVersion conflicttransactionversion number
     * @param conflictingTxnId conflicttransactionID
     * @return conflictreport
     */
    public static ConflictReport writeWrite(
            byte[] conflictKey, long myVersion, long conflictingVersion, long conflictingTxnId) {
        String desc =
                String.format(
                        "Write-Write conflict on key: myVersion=%d, conflictingVersion=%d, conflictingTxnId=%d",
                        myVersion, conflictingVersion, conflictingTxnId);
        ConflictEntry entry =
                new ConflictEntry(
                        ConflictType.WRITE_WRITE,
                        conflictKey,
                        myVersion,
                        conflictingVersion,
                        conflictingTxnId);
        return new ConflictReport(
                true,
                ConflictType.WRITE_WRITE,
                conflictKey,
                myVersion,
                conflictingVersion,
                conflictingTxnId,
                desc,
                List.of(entry));
    }

    /**
     * createread-writeconflictreport.
     *
     * @param conflictKey conflictkey
     * @param myVersion thistransactionreadwhenversion number
     * @param conflictingVersion conflicttransactionmodifiedafterversion number
     * @param conflictingTxnId conflicttransactionID
     * @return conflictreport
     */
    public static ConflictReport readWrite(
            byte[] conflictKey, long myVersion, long conflictingVersion, long conflictingTxnId) {
        String desc =
                String.format(
                        "Read-Write conflict on key: readAtVersion=%d, nowAtVersion=%d, conflictingTxnId=%d",
                        myVersion, conflictingVersion, conflictingTxnId);
        ConflictEntry entry =
                new ConflictEntry(
                        ConflictType.READ_WRITE,
                        conflictKey,
                        myVersion,
                        conflictingVersion,
                        conflictingTxnId);
        return new ConflictReport(
                true,
                ConflictType.READ_WRITE,
                conflictKey,
                myVersion,
                conflictingVersion,
                conflictingTxnId,
                desc,
                List.of(entry));
    }

    /**
     * createphantom readconflictreport.
     *
     * @param conflictingVersion conflictversion number
     * @param conflictingTxnId conflicttransactionID
     * @param description phantom readdescription
     * @return conflictreport
     */
    public static ConflictReport phantom(
            long conflictingVersion, long conflictingTxnId, String description) {
        ConflictEntry entry =
                new ConflictEntry(
                        ConflictType.PHANTOM, null, -1, conflictingVersion, conflictingTxnId);
        return new ConflictReport(
                true,
                ConflictType.PHANTOM,
                null,
                -1,
                conflictingVersion,
                conflictingTxnId,
                description,
                List.of(entry));
    }

    /**
     * createincludemultipleitemconflictentry report(forPARTIAL_COMMITstrategy,
     * collectallconflictkey).
     *
     * @param conflicts allconflictentrylist
     * @param description conflictdescription
     * @return includemultipleitemconflictreport
     */
    public static ConflictReport multipleConflicts(
            List<ConflictEntry> conflicts, String description) {
        if (conflicts == null || conflicts.isEmpty()) {
            return noConflict();
        }
        // usefirstconflictentryinfofillbackward compatibilityfield
        ConflictEntry first = conflicts.get(0);
        return new ConflictReport(
                true,
                first.conflictType(),
                first.conflictKey(),
                first.myVersion(),
                first.conflictingVersion(),
                first.conflictingTxnId(),
                description,
                conflicts);
    }

    public boolean hasConflict() {
        return hasConflict;
    }

    public ConflictType conflictType() {
        return conflictType;
    }

    /**
     * returnconflictkeypreventdefenseitycopy. note: whenhasmultipleitemconflictkeywhen,
     * thismethodonlyreturnfirstconflictkey. useconflicts()getallconflictkey.
     */
    public byte[] conflictKey() {
        return conflictKey != null ? conflictKey.clone() : null;
    }

    public long myVersion() {
        return myVersion;
    }

    public long conflictingVersion() {
        return conflictingVersion;
    }

    public long conflictingTxnId() {
        return conflictingTxnId;
    }

    public String description() {
        return description;
    }

    /**
     * getallconflictentrylist(immutable). forPARTIAL_COMMITstrategyinat onceremoveallconflictkey.
     *
     * @return conflictentrylist, noconflictwhenreturnemptylist
     */
    public List<ConflictEntry> conflicts() {
        return conflicts;
    }

    @Override
    public String toString() {
        return "ConflictReport{hasConflict="
                + hasConflict
                + ", conflictType="
                + conflictType
                + ", conflictKey="
                + (conflictKey != null ? Arrays.toString(conflictKey) : "null")
                + ", myVersion="
                + myVersion
                + ", conflictingVersion="
                + conflictingVersion
                + ", conflictingTxnId="
                + conflictingTxnId
                + ", conflictCount="
                + conflicts.size()
                + ", description='"
                + description
                + "'"
                + "}";
    }
}
