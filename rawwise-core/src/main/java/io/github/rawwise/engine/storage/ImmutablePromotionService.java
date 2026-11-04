package io.github.rawwise.engine.storage;

import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.record.ImmutabilitySource;
import io.github.rawwise.model.record.Record;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImmutablePromotionService {

    private final DynamicMmapStore store;

    private static final int STRIPE_COUNT = 64;
    private final Lock[] keyLocks = new Lock[STRIPE_COUNT];

    public ImmutablePromotionService(DynamicMmapStore store) {
        this.store = store;
        for (int i = 0; i < STRIPE_COUNT; i++) {
            keyLocks[i] = new ReentrantLock();
        }
    }

    private Lock lockFor(byte[] key) {
        return keyLocks[Math.abs(Arrays.hashCode(key) % STRIPE_COUNT)];
    }

    public boolean markImmutable(Record.Key key, long tableId) {
        log.info("markImmutable called for key={}, tableId={}", key, tableId);
        String tableIdentifier = resolveTableIdentifier(tableId);
        if (tableIdentifier == null) {
            log.warn("markImmutable: cannot resolve tableIdentifier for tableId={}", tableId);
            return false;
        }

        byte[] encodedKey = key.encoded();
        Lock lock = lockFor(encodedKey);
        lock.lock();
        try {
            return doMarkImmutable(tableIdentifier, key, encodedKey);
        } finally {
            lock.unlock();
        }
    }

    private boolean doMarkImmutable(String tableIdentifier, Record.Key key, byte[] encodedKey) {
        TableStore ts = store.tableStore(tableIdentifier);
        byte[] rawValue = ts.getRawValue(encodedKey);
        if (rawValue == null) {
            log.debug("markImmutable: record not found for key={}", key);
            return false;
        }

        try {
            if (NullableValueCodec.isTombstone(rawValue)) {
                log.debug("markImmutable: record is tombstone for key={}", key);
                return false;
            }
            byte[] decoded = NullableValueCodec.decode(rawValue);
            if (decoded == null) {
                log.debug("markImmutable: null decoded value for key={}", key);
                return false;
            }
            var recordCodec = ts.getRecordCodec();
            if (!recordCodec.isRecordCodecFormat(decoded)) {
                log.warn("markImmutable: value not in RecordCodec format for key={}", key);
                return false;
            }
            var schemaResolver = ts.getSchemaResolver();
            Record record = recordCodec.decode(decoded, schemaResolver);
            Record.Header header = record.header();

            if (!canPromote(header.immutableSource())) {
                log.debug(
                        "markImmutable: cannot promote source={} for key={}",
                        header.immutableSource(),
                        key);
                return false;
            }

            Record.Header newHeader =
                    new Record.Header(
                            header.key(),
                            header.tableId(),
                            header.schemaVersion(),
                            header.sequenceNumber(),
                            header.commitTimestamp(),
                            header.kind(),
                            header.visible(),
                            true,
                            ImmutabilitySource.PROMOTED);

            return store.updateRecordHeader(tableIdentifier, key, newHeader);
        } catch (Exception e) {
            log.warn("markImmutable: failed for key={}: {}", key, e.getMessage());
            return false;
        }
    }

    public boolean canPromote(ImmutabilitySource current) {
        return current.canPromoteTo(ImmutabilitySource.PROMOTED);
    }

    private String resolveTableIdentifier(long tableId) {
        return store.findTableIdentifierByNumericId(tableId);
    }
}
