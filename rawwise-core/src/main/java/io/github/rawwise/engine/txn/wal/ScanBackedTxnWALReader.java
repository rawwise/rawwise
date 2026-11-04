package io.github.rawwise.engine.txn.wal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * WAL-backed transaction WAL readerimplementation.
 *
 * <p>from {@link TxnWALWriter} persistence WAL inreadtransactionrecord.currentlyimplementationvia
 * {@link TxnWALWriter#scan()} getalreadypersistencerecordsnapshot, and provides on it bytransaction
 * ID、commitstateetcdimensionquerycapability.
 *
 * <h3>Thread safety</h3>
 *
 * <p>eachtimereadoperationallwillcall {@link TxnWALWriter#scan()} getlatestsnapshot, {@link
 * TxnWALWriter#scan()} implementationensurereturnimmutablereplica, becausethisthisclassisThread
 * safety. butrecommendinrecoverystagesinglethreaduse, avoidrepeated scan overhead.
 *
 * <h3>useexample</h3>
 *
 * <pre>{@code
 * TxnWALReader reader = new ScanBackedTxnWALReader(walWriter);
 * List<Object> uncommitted = reader.readUncommitted();
 * for (Object record : uncommitted) {
 * RecoverStage.WalEntry entry = TxnWALReader.toWalEntry(record);
 * // ... recoveryhandle
 * }
 * }</pre>
 */
@Slf4j
public class ScanBackedTxnWALReader implements TxnWALReader {

    private final TxnWALWriter walWriter;

    /**
     * create ScanBackedTxnWALReader.
     *
     * @param walWriter WAL writer(for scan readalreadypersistencerecord)
     * @throws NullPointerException if walWriter as null
     */
    public ScanBackedTxnWALReader(TxnWALWriter walWriter) {
        this.walWriter = Objects.requireNonNull(walWriter, "walWriter must not be null");
    }

    @Override
    public List<Object> readAll() {
        List<Object> records = walWriter.scan();
        log.debug("readAll: {} records scanned", records.size());
        return records;
    }

    @Override
    public Optional<Object> readLatest(long txnId) {
        List<Object> records = walWriter.scan();
        Object latest = null;
        for (Object record : records) {
            long recordTxnId = TxnWALReader.extractTxnId(record);
            if (recordTxnId == txnId) {
                latest = record;
            }
        }
        if (latest != null) {
            log.debug(
                    "readLatest: txnId={} found latest record type={}",
                    txnId,
                    latest.getClass().getSimpleName());
        } else {
            log.debug("readLatest: txnId={} no records found", txnId);
        }
        return Optional.ofNullable(latest);
    }

    @Override
    public List<Object> readByTxnId(long txnId) {
        List<Object> records = walWriter.scan();
        List<Object> result = new ArrayList<>();
        for (Object record : records) {
            long recordTxnId = TxnWALReader.extractTxnId(record);
            if (recordTxnId == txnId) {
                result.add(record);
            }
        }
        log.debug("readByTxnId: txnId={} found {} records", txnId, result.size());
        return Collections.unmodifiableList(result);
    }

    @Override
    public List<Object> readUncommitted() {
        List<Object> records = walWriter.scan();

        // Step 1: by txnId group, whilerecordeach txnId whetherhas COMMITTED or ABORT
        Map<Long, List<Object>> byTxnId = new LinkedHashMap<>();
        Set<Long> committedTxnIds = Collections.newSetFromMap(new LinkedHashMap<>());
        Set<Long> abortedTxnIds = Collections.newSetFromMap(new LinkedHashMap<>());

        for (Object record : records) {
            long recordTxnId = TxnWALReader.extractTxnId(record);
            byTxnId.computeIfAbsent(recordTxnId, k -> new ArrayList<>()).add(record);
            if (record instanceof TxnWALRecord.TxnCommitted) {
                committedTxnIds.add(recordTxnId);
            } else if (record instanceof TxnWALRecord.TxnAbort) {
                abortedTxnIds.add(recordTxnId);
            }
        }

        // Step 2: collectallnotcommit(no COMMITTED andno ABORT)transaction WAL record
        List<Object> uncommitted = new ArrayList<>();
        for (Map.Entry<Long, List<Object>> entry : byTxnId.entrySet()) {
            long txnId = entry.getKey();
            if (!committedTxnIds.contains(txnId) && !abortedTxnIds.contains(txnId)) {
                uncommitted.addAll(entry.getValue());
            }
        }

        log.debug(
                "readUncommitted: {} uncommitted txns (out of {} total), {} records",
                byTxnId.size() - committedTxnIds.size() - abortedTxnIds.size(),
                byTxnId.size(),
                uncommitted.size());
        return Collections.unmodifiableList(uncommitted);
    }

    /**
     * readallnotcommittransaction txnId set.
     *
     * <p>convenientmethod, forrecoverystagefastenumrequireshandletransaction.
     *
     * @return notcommittransaction txnId set(by first occurrence order)
     */
    public Set<Long> readUncommittedTxnIds() {
        List<Object> records = walWriter.scan();
        Map<Long, Boolean> txnCommitted = new LinkedHashMap<>();
        for (Object record : records) {
            long recordTxnId = TxnWALReader.extractTxnId(record);
            if (record instanceof TxnWALRecord.TxnCommitted
                    || record instanceof TxnWALRecord.TxnAbort) {
                txnCommitted.put(recordTxnId, true);
            } else {
                txnCommitted.putIfAbsent(recordTxnId, false);
            }
        }
        return txnCommitted.entrySet().stream()
                .filter(e -> !e.getValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
    }
}
