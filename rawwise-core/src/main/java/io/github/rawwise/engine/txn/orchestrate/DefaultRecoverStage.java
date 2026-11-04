package io.github.rawwise.engine.txn.orchestrate;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.txn.clock.LogicalClock;
import io.github.rawwise.engine.txn.wal.TxnWALReader;
import io.github.rawwise.engine.txn.wal.TxnWALRecord;
import io.github.rawwise.engine.txn.wal.TxnWALWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32C;
import lombok.extern.slf4j.Slf4j;

/**
 * RecoverStage default implementation.
 *
 * <p>can inject to support fault injection test.
 *
 * <h3>WAL read delegation</h3>
 *
 * <p>this class supports two WAL read modes:
 *
 * <ul>
 *   <li><b>delegation mode</b>(recommended): via {@link TxnWALReader} read WAL record, call {@link
 *       #recover(LogicalClock, LmdbInstance, TxnWALReader)}. this mode sinks WAL scan logic to
 *       TxnWALReader.
 *   <li><b>direct mode</b>(backward compatibility): caller directly passes in {@code
 *       List<WalEntry>}, via {@link #recover(LogicalClock, LmdbInstance, List)}. this mode remains
 *       for compatibility with existing callers.
 * </ul>
 */
@Slf4j
public class DefaultRecoverStage implements RecoverStage {

    private final TxnWALReader walReader;

    /** create DefaultRecoverStage(direct mode, no TxnWALReader delegation). */
    public DefaultRecoverStage() {
        this.walReader = null;
    }

    /**
     * create DefaultRecoverStage(delegation mode, use TxnWALReader to read WAL).
     *
     * @param walWriter WAL writer, forbuild {@link TxnWALReader}
     */
    public DefaultRecoverStage(TxnWALWriter walWriter) {
        this.walReader =
                new io.github.rawwise.engine.txn.wal.ScanBackedTxnWALReader(
                        Objects.requireNonNull(walWriter, "walWriter must not be null"));
    }

    /**
     * create DefaultRecoverStage(delegation mode, use custom TxnWALReader).
     *
     * @param walReader already constructed TxnWALReader
     */
    public DefaultRecoverStage(TxnWALReader walReader) {
        this.walReader = walReader;
    }

    /**
     * delegation mode recovery entry point: use TxnWALReader to read WAL records and execute
     * recovery.
     *
     * <p>this method delegates WAL scan logic to {@link TxnWALReader}, RecoverStage onlyresponsible
     * forrecovery decision.
     *
     * @param clock logical clock
     * @param lmdbInstance LMDB instance
     * @param walReader WAL reader
     * @return recovery report
     */
    public RecoveryReport recover(
            LogicalClock clock, LmdbInstance lmdbInstance, TxnWALReader walReader) {
        Objects.requireNonNull(walReader, "walReader must not be null");

        // Step 1: via TxnWALReader readall WAL record
        List<Object> rawRecords = walReader.readAll();
        log.info("RecoverStage: TxnWALReader scanned {} raw WAL records", rawRecords.size());

        // Step 2: transition to WalEntry list
        List<WalEntry> walEntries = new ArrayList<>(rawRecords.size());
        for (Object record : rawRecords) {
            walEntries.add(TxnWALReader.toWalEntry(record));
        }

        // Step 3: delegate to direct mode recover method to execute recovery decision
        return recover(clock, lmdbInstance, walEntries);
    }

    /**
     * delegation mode recovery entry point(use TxnWALReader injected at construction time).
     *
     * <p>if TxnWALReader not injected at construction time, throw {@link IllegalStateException}.
     *
     * @param clock logical clock
     * @param lmdbInstance LMDB instance
     * @return recovery report
     * @throws IllegalStateException if TxnWALReader not injected
     */
    public RecoveryReport recoverWithReader(LogicalClock clock, LmdbInstance lmdbInstance) {
        if (walReader == null) {
            throw new IllegalStateException(
                    "TxnWALReader not injected; use recover(clock, lmdbInstance, walReader) "
                            + "or construct DefaultRecoverStage with a TxnWALWriter/TxnWALReader");
        }
        return recover(clock, lmdbInstance, walReader);
    }

    @Override
    public RecoveryReport recover(
            LogicalClock clock, LmdbInstance lmdbInstance, List<WalEntry> walEntries) {
        List<RecoveryResult> results = new ArrayList<>();
        int recovered = 0, aborted = 0, skipped = 0;

        // WAL scan logic: group by txnId(this logic can also via TxnWALReader.readByTxnId
        // implementation)
        Map<Long, List<WalEntry>> byTxnId = groupByTxnId(walEntries);

        for (var txnEntries : byTxnId.entrySet()) {
            long txnId = txnEntries.getKey();
            List<WalEntry> entries = txnEntries.getValue();

            RecoveryResult result = recoverSingle(txnId, entries, lmdbInstance, clock);
            results.add(result);

            switch (result.action()) {
                case REDO_COMMITTED -> recovered++;
                case ABORT -> aborted++;
                case SKIP_ALREADY_COMMITTED, SKIP_ALREADY_ABORTED -> skipped++;
                case NEEDS_MANUAL_INTERVENTION ->
                        log.warn(
                                "Transaction {} needs manual intervention: {}",
                                txnId,
                                result.detail());
            }

            checkVectorPreparedEntries(entries);
        }

        log.info(
                "RecoverStage: complete — {} recovered, {} aborted, {} skipped",
                recovered,
                aborted,
                skipped);

        return new RecoveryReport(results, recovered, aborted, skipped);
    }

    /**
     * group WalEntry by txnId.
     *
     * <p>extracted from original inline scan logic, for reuse and testing. this grouping logic with
     * {@link TxnWALReader#readByTxnId(long)} semantically consistent, but for already transitioned
     * WalEntry list.
     *
     * @param walEntries WAL entry list
     * @return txnId → WalEntry listorderedmapping
     */
    private static Map<Long, List<WalEntry>> groupByTxnId(List<WalEntry> walEntries) {
        Map<Long, List<WalEntry>> byTxnId = new LinkedHashMap<>();
        for (WalEntry entry : walEntries) {
            long txnId = extractTxnId(entry);
            byTxnId.computeIfAbsent(txnId, k -> new ArrayList<>()).add(entry);
        }
        return byTxnId;
    }

    private RecoveryResult recoverSingle(
            long txnId, List<WalEntry> entries, LmdbInstance lmdbInstance, LogicalClock clock) {
        boolean hasCommitted = entries.stream().anyMatch(e -> e instanceof WalEntry.Committed);
        if (hasCommitted) {
            clock.advanceToAtLeast(txnId);
            return new RecoveryResult(
                    txnId, RecoveryAction.SKIP_ALREADY_COMMITTED, "WAL TXN_COMMITTED found");
        }

        boolean hasAborting = entries.stream().anyMatch(e -> e instanceof WalEntry.Aborted);
        if (hasAborting) {
            return new RecoveryResult(
                    txnId,
                    RecoveryAction.SKIP_ALREADY_ABORTED,
                    "WAL TXN_ABORT found — staging cleaned");
        }

        WalEntry.PrepareAndDurable pad =
                entries.stream()
                        .filter(e -> e instanceof WalEntry.PrepareAndDurable)
                        .map(e -> (WalEntry.PrepareAndDurable) e)
                        .findFirst()
                        .orElse(null);

        if (pad != null) {
            return recoverWithPrepareAndDurable(txnId, pad, lmdbInstance, clock);
        }

        boolean hasWriteIntent = entries.stream().anyMatch(e -> e instanceof WalEntry.WriteIntent);
        if (hasWriteIntent) {
            return new RecoveryResult(
                    txnId,
                    RecoveryAction.ABORT,
                    "TXN_WRITE_INTENT found without PREPARE_AND_DURABLE or COMMITTED");
        }

        boolean hasBegin = entries.stream().anyMatch(e -> e instanceof WalEntry.Begin);
        if (hasBegin) {
            return new RecoveryResult(
                    txnId, RecoveryAction.ABORT, "Only TXN_BEGIN found — no writes, direct abort");
        }

        return new RecoveryResult(
                txnId,
                RecoveryAction.NEEDS_MANUAL_INTERVENTION,
                "Unknown state — no BEGIN/PREPARE/COMMITTED/ABORT found");
    }

    private RecoveryResult recoverWithPrepareAndDurable(
            long txnId,
            WalEntry.PrepareAndDurable pad,
            LmdbInstance lmdbInstance,
            LogicalClock clock) {

        TxnWALRecord.TxnPrepareAndDurable record = pad.record();

        // Step 1: check whether LMDB data exists
        // CRITICAL-1: use writtenKeys from WAL record to verify actual data
        List<byte[]> writtenKeys = record.writtenKeys();
        boolean dataExists =
                writtenKeys != null && !writtenKeys.isEmpty()
                        ? checkLmdbKeysExist(lmdbInstance, writtenKeys)
                        : checkLmdbDataExists(lmdbInstance, txnId);
        if (!dataExists) {
            return new RecoveryResult(
                    txnId, RecoveryAction.ABORT, "LMDB data missing — staging lost, aborting txn");
        }

        // Step 2: computeactual CRC32 — CRITICAL-1: iterate over actual key+value from LMDB
        byte[] actualCRC32 = computeLmdbCRC32(lmdbInstance, writtenKeys);

        // Step 3: compare actualCRC32 vs sealedDigest
        if (!Arrays.equals(actualCRC32, record.sealedDigest())) {
            log.warn(
                    "RecoverStage: txn={} CRC32 mismatch — actual={} expected={}",
                    txnId,
                    bytesToHex(actualCRC32),
                    bytesToHex(record.sealedDigest()));
            return new RecoveryResult(
                    txnId,
                    RecoveryAction.ABORT,
                    "CRC32 mismatch — partial write detected, aborting");
        }

        // Step 4: CRC32 match → redo COMMITTED
        clock.advanceToAtLeast(txnId);
        log.info("RecoverStage: txn={} sealedDigest verified — redoing COMMITTED", txnId);
        return new RecoveryResult(
                txnId,
                RecoveryAction.REDO_COMMITTED,
                "sealedDigest matched — data integrity verified, redo COMMITTED");
    }

    private boolean checkLmdbDataExists(LmdbInstance lmdbInstance, long txnId) {
        if (lmdbInstance == null) {
            return false;
        }
        byte[] expectedMarkerKey = ("__commit_marker__:" + txnId).getBytes(StandardCharsets.UTF_8);
        byte[] markerValue = lmdbInstance.get(expectedMarkerKey);
        return markerValue != null && markerValue.length > 0 && markerValue[0] == 1;
    }

    private boolean checkLmdbKeysExist(LmdbInstance lmdbInstance, List<byte[]> keys) {
        if (lmdbInstance == null || keys == null || keys.isEmpty()) {
            return false;
        }
        for (byte[] key : keys) {
            byte[] value = lmdbInstance.get(key);
            if (value == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * compute CRC32 of actual data in LMDB.
     *
     * <p>CRITICAL-1 fix: iterate over writtenKeys and CRC key+value from LMDB, matching the same
     * computation as DurableStage.computeSealedDigest. Keys are sorted for deterministic CRC32
     * matching.
     */
    private byte[] computeLmdbCRC32(LmdbInstance lmdbInstance, List<byte[]> writtenKeys) {
        if (lmdbInstance == null) {
            return new byte[8];
        }
        List<byte[]> sortedKeys = new ArrayList<>(writtenKeys != null ? writtenKeys : List.of());
        sortedKeys.sort(Comparator.comparing(ByteBuffer::wrap));
        CRC32C crc = new CRC32C();
        if (writtenKeys != null && !writtenKeys.isEmpty()) {
            for (byte[] key : sortedKeys) {
                byte[] value = lmdbInstance.get(key);
                if (value != null) {
                    crc.update(key);
                    crc.update(value);
                }
            }
        }
        long v = crc.getValue();
        return new byte[] {
            (byte) (v >> 56), (byte) (v >> 48),
            (byte) (v >> 40), (byte) (v >> 32),
            (byte) (v >> 24), (byte) (v >> 16),
            (byte) (v >> 8), (byte) v
        };
    }

    private static long extractTxnId(WalEntry entry) {
        return switch (entry) {
            case WalEntry.Begin b -> b.record().txnId();
            case WalEntry.WriteIntent w -> w.record().txnId();
            case WalEntry.PrepareAndDurable p -> p.record().txnId();
            case WalEntry.Committed c -> c.record().txnId();
            case WalEntry.Aborted a -> a.record().txnId();
            case WalEntry.VectorPrepared vp -> vp.record().txnId();
        };
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private void checkVectorPreparedEntries(List<WalEntry> entries) {
        for (WalEntry entry : entries) {
            if (entry instanceof WalEntry.VectorPrepared vp) {
                TxnWALRecord.TxnVectorPrepared record = vp.record();
                log.info(
                        "RecoverStage: found TXN_VECTOR_PREPARED txnId={} namespace={} entryKey={} dimension={} provider={}",
                        record.txnId(),
                        record.namespace(),
                        record.entryKey(),
                        record.dimension(),
                        record.embeddingProvider());
            }
        }
    }
}
