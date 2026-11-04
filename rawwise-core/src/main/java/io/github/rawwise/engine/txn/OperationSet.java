package io.github.rawwise.engine.txn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * transactionoperationset: mergeoriginalWriteSetandReadSet,
 * unifiedtracktransactionexecuteduringallreadwriteoperation.
 *
 * <h3>design motivation(designdocument3.5node)</h3>
 *
 * <p>originalWriteSetandReadSeteach maintains independentConcurrentHashMap, existbyunderissue:
 *
 * <ul>
 *   <li>WriteSetstoragecompletevaluereplica(byte[]), butvaluealreadyviainWriteBufferstored a copy
 *       in--doublestorage
 *   <li>ReadSetalsostoragecompletevaluereplica, butOCCconflictdetectiononlyrequirescommitTs
 *   <li>sameonekeyreadwriteoperationscattered to twoMapin, querywhenrequireswhilecheck two
 * </ul>
 *
 * <h3>keykeychange</h3>
 *
 * <ol>
 *   <li><b>notstorevalue</b>: valueinWriteBufferin, OperationSetonlystore metainfo
 *   <li><b>mergeReadSet + WriteSet</b>: sameonekeyreadwriteuseREAD_WRITEtableshow
 *   <li><b>addaddreadCommitTs</b>: supportREPEATABLE_READlevelread-writeconflictdetection
 *   <li><b>WriteTypewithinaggregate</b>: WriteType(INSERT/UPDATE/DELETE)fromWriteSetmigrate
 *       toOperationSetinternal
 * </ol>
 *
 * <p>implementationSerializablebysupportSpark Driver->Executorbroadcast transmission.
 * byinByteBuffernotserialization,
 * customwriteObject/readObjectwillkeytransitionasbyte[]enterrowserialization.
 */
public class OperationSet implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    /** operationtypeenum: distinguishpure read、pure write、readwritemerge */
    public enum OpType {
        /** pure readoperation */
        READ,
        /** pure writeoperation */
        WRITE,
        /** sameonekeyhas both reads and writes */
        READ_WRITE
    }

    /**
     * operationrecord: recordsomekeyoperationtype、writesemantics、base versionandreadwhencommitTs
     */
    public enum WriteType {
        INSERT,
        UPDATE,
        DELETE
    }

    public record OpEntry(OpType opType, WriteType writeType, long baseVersion, long readCommitTs)
            implements Serializable {
        @Serial private static final long serialVersionUID = 1L;

        public OpEntry {
            if (opType == null) {
                throw new IllegalArgumentException("OpType must not be null");
            }
            // pure readoperationwhenwriteTyperequiredasnull
            if (opType == OpType.READ && writeType != null) {
                throw new IllegalArgumentException("WriteType must be null for READ operation");
            }
            // writeoperationwhenwriteTyperequirednotasnull
            if ((opType == OpType.WRITE || opType == OpType.READ_WRITE) && writeType == null) {
                throw new IllegalArgumentException(
                        "WriteType must not be null for WRITE/READ_WRITE operation");
            }
        }
    }

    public record RangeEntry(byte[] minKey, byte[] maxKey, long readCommitTs)
            implements Serializable {
        @Serial private static final long serialVersionUID = 1L;

        public RangeEntry {
            minKey = minKey == null ? null : minKey.clone();
            maxKey = maxKey == null ? null : maxKey.clone();
        }
    }

    /**
     * useHashMapstorageoperationrecord.Thread safetybyTxnContextThreadLocalboundensure,
     * OperationSetintransactionlifecyclewithinonlybesinglethreadaccess. markastransient,
     * becauseasByteBuffernotserialization, viacustomwriteObject/readObjecthandle.
     */
    private transient HashMap<ByteBuffer, OpEntry> operations = new HashMap<>();

    private transient List<RangeEntry> rangeReads = new ArrayList<>();

    /**
     * recordonetimereadoperation. ifkeyexistingWRITErecord, upgradeasREAD_WRITE.
     *
     * @param key readkey
     * @param readCommitTs readwhenmomentthekeycommittimestamp
     */
    public void recordRead(byte[] key, long readCommitTs) {
        if (key == null) {
            throw new IllegalArgumentException("Read key must not be null");
        }
        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        operations.compute(
                keyBuffer,
                (k, existing) -> {
                    if (existing == null) {
                        // pure readoperation
                        return new OpEntry(OpType.READ, null, 0, readCommitTs);
                    } else if (existing.opType() == OpType.WRITE) {
                        // existingwriterecord, upgradeasREAD_WRITE
                        return new OpEntry(
                                OpType.READ_WRITE,
                                existing.writeType(),
                                existing.baseVersion(),
                                readCommitTs);
                    } else {
                        // existingREADorREAD_WRITE, updatereadCommitTs(take larger value)
                        long newReadCommitTs = Math.max(existing.readCommitTs(), readCommitTs);
                        if (existing.opType() == OpType.READ) {
                            return new OpEntry(OpType.READ, null, 0, newReadCommitTs);
                        } else {
                            return new OpEntry(
                                    OpType.READ_WRITE,
                                    existing.writeType(),
                                    existing.baseVersion(),
                                    newReadCommitTs);
                        }
                    }
                });
    }

    /**
     * recordonetimewriteoperation. ifkeyexistingREADrecord, upgradeasREAD_WRITE.
     *
     * @param key writekey
     * @param writeType writetype(INSERT/UPDATE/DELETE)
     * @param baseVersion writebased onversion number(forCASdetection)
     */
    public void recordWrite(byte[] key, WriteType writeType, long baseVersion) {
        if (key == null) {
            throw new IllegalArgumentException("Write key must not be null");
        }
        if (writeType == null) {
            throw new IllegalArgumentException("Write type must not be null");
        }
        ByteBuffer keyBuffer = ByteBuffer.wrap(key.clone());
        operations.compute(
                keyBuffer,
                (k, existing) -> {
                    if (existing == null) {
                        // pure writeoperation
                        return new OpEntry(OpType.WRITE, writeType, baseVersion, 0);
                    } else if (existing.opType() == OpType.READ) {
                        // existingreadrecord, upgradeasREAD_WRITE
                        return new OpEntry(
                                OpType.READ_WRITE, writeType, baseVersion, existing.readCommitTs());
                    } else {
                        // existingWRITEorREAD_WRITE, updatewriterelatedinfo
                        if (existing.opType() == OpType.WRITE) {
                            return new OpEntry(OpType.WRITE, writeType, baseVersion, 0);
                        } else {
                            return new OpEntry(
                                    OpType.READ_WRITE,
                                    writeType,
                                    baseVersion,
                                    existing.readCommitTs());
                        }
                    }
                });
    }

    /** checksomekeywhetherinoperationsetin. */
    public boolean containsKey(byte[] key) {
        if (key == null) {
            return false;
        }
        return operations.containsKey(ByteBuffer.wrap(key.clone()));
    }

    /**
     * getoperationsetinallkey. returnSetinByteBufferisoriginalkeywrap, callpartyshould notmodified.
     */
    public Set<ByteBuffer> keys() {
        return Collections.unmodifiableSet(operations.keySet());
    }

    /**
     * getsomekeyoperationrecord.
     *
     * @param key needquerykey
     * @return operationrecord, ifkeynotinoperationsetinreturnnull
     */
    public OpEntry get(byte[] key) {
        if (key == null) {
            return null;
        }
        return operations.get(ByteBuffer.wrap(key.clone()));
    }

    /** operationsetwhetheris empty. */
    public boolean isEmpty() {
        return operations.isEmpty() && rangeReads.isEmpty();
    }

    /** operationsetsize. */
    public int size() {
        return operations.size();
    }

    /**
     * getallwriteoperationentries(forOCCverifyinwrite-writeconflictdetection).
     * includingOpTypeasWRITEandREAD_WRITEentry.
     *
     * @return key -> OpEntryimmutablemapping, onlyincludewriteoperation
     */
    public Map<ByteBuffer, OpEntry> writeEntries() {
        Map<ByteBuffer, OpEntry> result = new HashMap<>();
        for (Map.Entry<ByteBuffer, OpEntry> entry : operations.entrySet()) {
            OpEntry opEntry = entry.getValue();
            if (opEntry.opType() == OpType.WRITE || opEntry.opType() == OpType.READ_WRITE) {
                result.put(ByteBuffer.wrap(entry.getKey().array().clone()), opEntry);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /**
     * getallreadoperationentries(forREPEATABLE_READverifyinread-writeconflictdetection).
     * includingOpTypeasREADandREAD_WRITEentry.
     *
     * @return key -> OpEntryimmutablemapping, onlyincludereadoperation
     */
    public Map<ByteBuffer, OpEntry> readEntries() {
        Map<ByteBuffer, OpEntry> result = new HashMap<>();
        for (Map.Entry<ByteBuffer, OpEntry> entry : operations.entrySet()) {
            OpEntry opEntry = entry.getValue();
            if (opEntry.opType() == OpType.READ || opEntry.opType() == OpType.READ_WRITE) {
                result.put(ByteBuffer.wrap(entry.getKey().array().clone()), opEntry);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    /** getwriteoperationkeyset(forversionupdate). returnSetinbyte[]ispreventdefenseitycopy. */
    public Set<ByteBuffer> writeKeys() {
        Set<ByteBuffer> keySet = new HashSet<>();
        for (Map.Entry<ByteBuffer, OpEntry> entry : operations.entrySet()) {
            OpEntry opEntry = entry.getValue();
            if (opEntry.opType() == OpType.WRITE || opEntry.opType() == OpType.READ_WRITE) {
                keySet.add(ByteBuffer.wrap(entry.getKey().array().clone()));
            }
        }
        return Collections.unmodifiableSet(keySet);
    }

    /**
     * getsomekeybase versionnumber(writeoperation).
     *
     * @param key needquerykey
     * @return base versionnumber, ifkeynotinwriteoperationinreturn-1
     */
    public long getBaseVersion(byte[] key) {
        OpEntry entry = get(key);
        if (entry == null || entry.opType() == OpType.READ) {
            return -1;
        }
        return entry.baseVersion();
    }

    /**
     * getsomekeywritetype.
     *
     * @param key needquerykey
     * @return writetype, ifkeynotinwriteoperationinreturnnull
     */
    public WriteType getWriteType(byte[] key) {
        OpEntry entry = get(key);
        if (entry == null || entry.opType() == OpType.READ) {
            return null;
        }
        return entry.writeType();
    }

    /** checkwriteoperationwhetheris empty. */
    public boolean isWriteEmpty() {
        for (OpEntry entry : operations.values()) {
            if (entry.opType() == OpType.WRITE || entry.opType() == OpType.READ_WRITE) {
                return false;
            }
        }
        return true;
    }

    /** checkreadoperationwhetheris empty. */
    public boolean isReadEmpty() {
        for (OpEntry entry : operations.values()) {
            if (entry.opType() == OpType.READ || entry.opType() == OpType.READ_WRITE) {
                return false;
            }
        }
        return rangeReads.isEmpty();
    }

    public void recordRangeRead(byte[] minKey, byte[] maxKey, long readCommitTs) {
        rangeReads.add(new RangeEntry(minKey, maxKey, readCommitTs));
    }

    public List<RangeEntry> rangeReadEntries() {
        return Collections.unmodifiableList(rangeReads);
    }

    /** clearemptyoperationset. */
    public void clear() {
        operations.clear();
        rangeReads.clear();
    }

    /**
     * customserialization: willByteBuffer keytransitionasbyte[]enterrowserialization.
     * becauseasByteBuffernotserialization, requireswillMap<ByteBuffer,
     * OpEntry>transitionascanserializationformat.
     */
    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeInt(operations.size());
        for (Map.Entry<ByteBuffer, OpEntry> entry : operations.entrySet()) {
            byte[] keyBytes = entry.getKey().array();
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
            OpEntry opEntry = entry.getValue();
            out.writeObject(opEntry.opType());
            out.writeObject(opEntry.writeType());
            out.writeLong(opEntry.baseVersion());
            out.writeLong(opEntry.readCommitTs());
        }
        out.writeInt(rangeReads.size());
        for (RangeEntry re : rangeReads) {
            writeNullableBytes(out, re.minKey());
            writeNullableBytes(out, re.maxKey());
            out.writeLong(re.readCommitTs());
        }
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        operations = new HashMap<>();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            int keyLen = in.readInt();
            byte[] keyBytes = new byte[keyLen];
            in.readFully(keyBytes);
            OpType opType = (OpType) in.readObject();
            WriteType writeType = (WriteType) in.readObject();
            long baseVersion = in.readLong();
            long readCommitTs = in.readLong();
            operations.put(
                    ByteBuffer.wrap(keyBytes),
                    new OpEntry(opType, writeType, baseVersion, readCommitTs));
        }
        int rangeSize = in.readInt();
        rangeReads = new ArrayList<>(rangeSize);
        for (int i = 0; i < rangeSize; i++) {
            byte[] minKey = readNullableBytes(in);
            byte[] maxKey = readNullableBytes(in);
            long readCommitTs = in.readLong();
            rangeReads.add(new RangeEntry(minKey, maxKey, readCommitTs));
        }
    }

    private static void writeNullableBytes(ObjectOutputStream out, byte[] bytes)
            throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private static byte[] readNullableBytes(ObjectInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return bytes;
    }
}
