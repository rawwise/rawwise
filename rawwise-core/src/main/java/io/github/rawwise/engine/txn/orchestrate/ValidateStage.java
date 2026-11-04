package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.engine.txn.IsolationLevel;
import io.github.rawwise.engine.txn.OccConfig;
import io.github.rawwise.engine.txn.ShardVersionTracker;
import io.github.rawwise.engine.txn.TxnContextBridge;
import io.github.rawwise.engine.txn.isolate.PhantomDetector;
import io.github.rawwise.engine.txn.isolate.RangeReadSet;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ValidateStage {

    ValidateStage() {}

    public sealed interface ValidateResult permits ValidateResult.Ok, ValidateResult.Conflict {

        record Ok() implements ValidateResult {}

        record Conflict(String reason, String conflictingTable) implements ValidateResult {}

        static Ok ok() {
            return new Ok();
        }

        static Conflict conflict(String reason, String table) {
            return new Conflict(reason, table);
        }
    }

    /**
     * table-levellockarray, for tracker == null degradepath(no per-key OCC fall back to table-level
     * OCC).
     *
     * <p><b>lockorderconstraint(H-3)</b>: callpartyrequiredensurebyunderlockorder,
     * otherwisehasdeadlockrisk.
     *
     * <ul>
     *   <li>has tracker: commit pathrequiredfirsthold {@code tracker.getCommitLock()} againcall
     *       {@link #execute}.execute internalnotwillagaingetadditionallylock (becauseas tracker !=
     *       null when doOccCheck notdependent tableLocks).
     *   <li>no tracker: execute internalto sortedTableIds orderadd synchronized,
     *       multipletabletransactionby tableId naturallylexicographicaddlock, avoiddeadlock.
     * </ul>
     *
     * <p>specification: prohibitin ValidateStage outsidedirectlyhold tableLocks reference.
     */
    private final ConcurrentHashMap<String, Object> tableLocks = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Long> tableCommitTs = new ConcurrentHashMap<>();

    @Setter private ShardVersionTracker tracker;

    @Setter private OccConfig occConfig = OccConfig.DISABLED;

    @Setter private WAL wal;

    /**
     * Multi-table transaction ShardVersionTracker resolver. injectafter OCC checkby
     * intent.primaryTableId usecorrespondingtable tracker, instead of the global singleton tracker.
     */
    @Setter private java.util.function.Function<String, ShardVersionTracker> tableTrackerResolver;

    public ValidateResult execute(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            RangeReadSet rangeReadSet,
            Map<String, Map<ByteBuffer, Long>> baseVersions) {
        if (writeIntents == null || writeIntents.isEmpty()) {
            log.debug("ValidateStage: txn={} no writes to validate", ctx.txnId());
            return ValidateResult.ok();
        }

        // H-3 fix: tracker != null whencallpartyalreadyhold commitLock, noneedagainadd tableLocks.
        // onlyindegradepath(tracker == null)only thenuse tableLocks stringrowize.
        if (tracker != null) {
            if (occConfig.isPerKey()) {
                return doPerKeyOcc(ctx, writeIntents, baseVersions, rangeReadSet);
            }
            return doOccCheck(ctx, writeIntents, rangeReadSet, baseVersions);
        }

        Set<String> tables = ctx.tableIds();
        List<String> sortedTableIds = new ArrayList<>(tables);
        Collections.sort(sortedTableIds);

        return lockAndValidate(ctx, writeIntents, rangeReadSet, baseVersions, sortedTableIds, 0);
    }

    private ValidateResult lockAndValidate(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            RangeReadSet rangeReadSet,
            Map<String, Map<ByteBuffer, Long>> baseVersions,
            List<String> sortedTableIds,
            int index) {
        if (index >= sortedTableIds.size()) {
            return doOccCheck(ctx, writeIntents, rangeReadSet, baseVersions);
        }

        String tableId = sortedTableIds.get(index);
        Object lock = tableLocks.computeIfAbsent(tableId, k -> new Object());
        synchronized (lock) {
            return lockAndValidate(
                    ctx, writeIntents, rangeReadSet, baseVersions, sortedTableIds, index + 1);
        }
    }

    private ValidateResult doOccCheck(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            RangeReadSet rangeReadSet,
            Map<String, Map<ByteBuffer, Long>> baseVersions) {
        // multipletabletransactionuse tableTrackerResolver bytablequery tracker
        if (tableTrackerResolver != null) {
            for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
                ShardVersionTracker t = tableTrackerResolver.apply(intent.primaryTableId());
                if (t == null) {
                    log.warn(
                            "ValidateStage: txn={} no tracker for table {}, skipping OCC",
                            ctx.txnId(),
                            intent.primaryTableId());
                    continue;
                }
                // FOR UPDATE lock skip OCC
                if (t.holdsExclusiveLock(intent.keyBytes(), ctx.txnId())) continue;
                long currentKeyTs = t.getCommitTs(intent.keyBytes());
                if (intent.baseVersion() > 0 && intent.baseVersion() < currentKeyTs) {
                    String reason =
                            String.format(
                                    "OCC conflict (multi-table): table=%s key baseVersion=%d < currentKeyTs=%d",
                                    intent.primaryTableId(), intent.baseVersion(), currentKeyTs);
                    log.warn("ValidateStage: txn={} {}", ctx.txnId(), reason);
                    return ValidateResult.conflict(reason, intent.primaryTableId());
                }
            }
            // Multi-table read-write check
            if (baseVersions != null && !baseVersions.isEmpty()) {
                return checkMultiTableReadWrite(ctx, writeIntents, baseVersions);
            }
            return ValidateResult.ok();
        }

        if (tracker != null) {
            for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
                // H-8 fix: thistransactionalreadyhold EXCLUSIVE rowlock key skip OCC
                // write-writecheck.
                // rowlockensureholdcannot be by other during periodtransactionmodified, OCC
                // checkinevitablevia, skipitnoharm.
                if (tracker.holdsExclusiveLock(intent.keyBytes(), ctx.txnId())) {
                    log.debug(
                            "ValidateStage: txn={} skipping OCC write-write check (FOR UPDATE held)",
                            ctx.txnId());
                    continue;
                }
                long currentKeyTs = tracker.getCommitTs(intent.keyBytes());
                if (intent.baseVersion() > 0 && intent.baseVersion() < currentKeyTs) {
                    String reason =
                            String.format(
                                    "OCC conflict: key baseVersion=%d < currentKeyCommitTs=%d",
                                    intent.baseVersion(), currentKeyTs);
                    log.warn("ValidateStage: txn={} {}", ctx.txnId(), reason);
                    return ValidateResult.conflict(reason, intent.primaryTableId());
                }
            }
        } else {
            for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
                String tableId = intent.primaryTableId();
                long currentCommitTs = tableCommitTs.getOrDefault(tableId, 0L);
                if (intent.baseVersion() > 0 && intent.baseVersion() < currentCommitTs) {
                    String reason =
                            String.format(
                                    "OCC conflict: table=%s key baseVersion=%d < currentCommitTs=%d",
                                    tableId, intent.baseVersion(), currentCommitTs);
                    log.warn("ValidateStage: txn={} {}", ctx.txnId(), reason);
                    return ValidateResult.conflict(reason, tableId);
                }
            }
        }

        // Read-write conflict detection (REPEATABLE_READ / SERIALIZABLE)
        if (tracker != null
                && (ctx.isolationLevel() == IsolationLevel.REPEATABLE_READ
                        || ctx.isolationLevel() == IsolationLevel.SERIALIZABLE)
                && baseVersions != null
                && !baseVersions.isEmpty()) {
            Set<ByteBuffer> writeKeys = new HashSet<>();
            for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
                writeKeys.add(ByteBuffer.wrap(intent.keyBytes()));
            }
            for (var tableEntry : baseVersions.entrySet()) {
                for (var kv : tableEntry.getValue().entrySet()) {
                    if (writeKeys.contains(kv.getKey())) continue;
                    long readCommitTs = kv.getValue();
                    byte[] keyBytes = kv.getKey().array();
                    long currentTs = tracker.getCommitTs(keyBytes);
                    if (currentTs > readCommitTs) {
                        String reason =
                                String.format(
                                        "Read-write conflict: key readAtTs=%d, currentTs=%d",
                                        readCommitTs, currentTs);
                        log.warn("ValidateStage: txn={} {}", ctx.txnId(), reason);
                        return ValidateResult.conflict(reason, tableEntry.getKey());
                    }
                }
            }
        }

        if (ctx.isolationLevel() == IsolationLevel.SERIALIZABLE
                && rangeReadSet != null
                && !rangeReadSet.isEmpty()) {
            if (tracker != null) {
                PhantomResult result = detectPhantomWithTracker(ctx, writeIntents, rangeReadSet);
                if (result.phantom) {
                    return ValidateResult.conflict(result.reason, ctx.primaryTableId());
                }
            } else {
                PhantomDetector.PhantomResult phantom =
                        detectPhantomLegacy(rangeReadSet, writeIntents);
                if (phantom.phantom()) {
                    log.warn(
                            "ValidateStage: txn={} phantom detected: {}",
                            ctx.txnId(),
                            phantom.reason());
                    return ValidateResult.conflict(phantom.reason(), ctx.primaryTableId());
                }
            }
        }

        log.debug(
                "ValidateStage: txn={} OCC passed, {} intents validated",
                ctx.txnId(),
                writeIntents.size());
        return ValidateResult.ok();
    }

    /** multipletabletransactionread-writeconflictdetection. */
    private ValidateResult checkMultiTableReadWrite(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            Map<String, Map<ByteBuffer, Long>> baseVersions) {
        if (ctx.isolationLevel() != IsolationLevel.REPEATABLE_READ
                && ctx.isolationLevel() != IsolationLevel.SERIALIZABLE) {
            return ValidateResult.ok();
        }
        Set<ByteBuffer> writeKeys = new HashSet<>();
        for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
            writeKeys.add(ByteBuffer.wrap(intent.keyBytes()));
        }
        for (var tableEntry : baseVersions.entrySet()) {
            String tableId = tableEntry.getKey();
            ShardVersionTracker t = tableTrackerResolver.apply(tableId);
            if (t == null) continue;
            for (var kv : tableEntry.getValue().entrySet()) {
                if (writeKeys.contains(kv.getKey())) continue;
                long readCommitTs = kv.getValue();
                byte[] keyBytes = kv.getKey().array();
                long currentTs = t.getCommitTs(keyBytes);
                if (currentTs > readCommitTs) {
                    String reason =
                            String.format(
                                    "Read-write conflict (multi-table): table=%s key readAtTs=%d currentTs=%d",
                                    tableId, readCommitTs, currentTs);
                    log.warn("ValidateStage: txn={} {}", ctx.txnId(), reason);
                    return ValidateResult.conflict(reason, tableId);
                }
            }
        }
        return ValidateResult.ok();
    }

    private PhantomDetector.PhantomResult detectPhantomLegacy(
            RangeReadSet rangeReadSet, List<TxnWALRecord.TxnWriteIntent> writeIntents) {
        List<PhantomDetector.WriteKeyEntry> writtenKeys = new ArrayList<>();
        for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
            writtenKeys.add(
                    new PhantomDetector.WriteKeyEntry(
                            intent.primaryTableId(), intent.keyBytes(),
                            intent.writeType(), intent.baseVersion()));
        }
        return PhantomDetector.detect(rangeReadSet, writtenKeys);
    }

    private PhantomResult detectPhantomWithTracker(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            RangeReadSet rangeReadSet) {
        Set<ByteBuffer> writeKeySet = new HashSet<>();
        for (TxnWALRecord.TxnWriteIntent intent : writeIntents) {
            writeKeySet.add(ByteBuffer.wrap(intent.keyBytes()));
        }

        for (RangeReadSet.RangeReadEntry range : rangeReadSet.entries()) {
            List<byte[]> phantomKeys =
                    tracker.detectPhantomKeys(
                            range.startKey(), range.endKey(), ctx.txnId(), Set.of(), writeKeySet);
            if (!phantomKeys.isEmpty()) {
                return new PhantomResult(
                        true,
                        "Phantom: "
                                + phantomKeys.size()
                                + " keys in range ["
                                + bytesToHex(range.startKey())
                                + ", "
                                + bytesToHex(range.endKey())
                                + "]");
            }
        }
        return PhantomResult.OK;
    }

    private static String bytesToHex(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "[]";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(bytes.length, 8); i++) {
            sb.append(String.format("%02x", bytes[i] & 0xff));
        }
        if (bytes.length > 8) sb.append("...");
        return sb.toString();
    }

    /**
     * Per-key OCC via {@link ShardVersionTracker#validateAndUpdateAtomic}.
     *
     * <p>Bridges state-layer TxnContext to engine-layer TxnContext, then delegates to the tracker's
     * atomic validate-and-update. The caller (TxnOrchestratorImpl) already holds the commitLock;
     * validateAndUpdateAtomic re-acquires it via re-entrant lock semantics and releases one hold,
     * leaving the outer hold intact for PublishStage.
     */
    private ValidateResult doPerKeyOcc(
            TxnContext ctx,
            List<TxnWALRecord.TxnWriteIntent> writeIntents,
            Map<String, Map<ByteBuffer, Long>> baseVersions,
            RangeReadSet rangeReadSet) {
        if (tracker == null) {
            return ValidateResult.ok();
        }

        TxnContext bridged = TxnContextBridge.bridge(ctx, writeIntents, baseVersions, rangeReadSet);

        java.util.concurrent.locks.ReentrantLock commitLock = tracker.getCommitLock();
        ShardVersionTracker.AtomicValidateResult result =
                tracker.validateAndUpdateAtomic(bridged, commitLock, wal);

        if (!result.success()) {
            String reason =
                    String.format(
                            "Per-key OCC conflict: key baseVersion=%d currentVersion=%d",
                            result.expectedVersion(), result.currentVersion());
            log.warn("ValidateStage: txn={} {}", ctx.txnId(), reason);
            return ValidateResult.conflict(reason, ctx.primaryTableId());
        }

        log.debug(
                "ValidateStage: txn={} per-key OCC passed, commitTs={}",
                ctx.txnId(),
                result.commitTs());
        return ValidateResult.ok();
    }

    private record PhantomResult(boolean phantom, String reason) {
        static final PhantomResult OK = new PhantomResult(false, "");
    }

    public void updateCommitTs(String tableId, long commitTs) {
        tableCommitTs.merge(tableId, commitTs, Math::max);
    }

    public long currentCommitTs(String tableId) {
        return tableCommitTs.getOrDefault(tableId, 0L);
    }
}
