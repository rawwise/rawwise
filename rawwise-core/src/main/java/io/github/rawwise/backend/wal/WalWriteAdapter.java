package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.DurabilityLevel;
import io.github.rawwise.engine.storage.DurabilityViolationException;
import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.model.op.KvWriteBuilder;
import io.github.rawwise.model.op.Write;
import io.github.rawwise.model.op.WriteBuilder;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WalWriteAdapter {
    public enum FailurePolicy {
        REJECT,
        DEGRADE;

        public static FailurePolicy fromString(String value) {
            if (value == null) return REJECT;
            return switch (value.toUpperCase()) {
                case "DEGRADE" -> DEGRADE;
                default -> REJECT;
            };
        }
    }

    private final WAL wal;
    private final DurabilityLevel durabilityLevel;
    private final AtomicLong walFailedCount;
    @Getter private final boolean groupCommitEnabled;
    private final FailurePolicy failurePolicy;

    public WalWriteAdapter(WAL wal, DurabilityLevel durabilityLevel, AtomicLong walFailedCount) {
        this(wal, durabilityLevel, walFailedCount, FailurePolicy.REJECT);
    }

    public WalWriteAdapter(
            WAL wal,
            DurabilityLevel durabilityLevel,
            AtomicLong walFailedCount,
            FailurePolicy failurePolicy) {
        this.wal = wal;
        this.durabilityLevel = durabilityLevel;
        this.walFailedCount = walFailedCount;
        this.groupCommitEnabled = wal != null && wal.isGroupCommitEnabled();
        this.failurePolicy = failurePolicy != null ? failurePolicy : FailurePolicy.REJECT;
    }

    public byte[] extractWalData(WriteBuilder builder, String tableIdentifier) {
        if (builder instanceof KvWriteBuilder kvBuilder) {
            byte[] key = kvBuilder.key();
            byte[] value = kvBuilder.value();
            if (key != null) {
                byte[] walValue;
                if (value == null) {
                    walValue = NullableValueCodec.encode(null);
                } else if (kvBuilder.valuePreEncoded()) {
                    walValue = value;
                } else {
                    walValue = NullableValueCodec.encode(value);
                }
                byte[] tableIdBytes =
                        tableIdentifier.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                ByteBuffer data =
                        ByteBuffer.allocate(
                                4 + tableIdBytes.length + 4 + key.length + 4 + walValue.length);
                data.putInt(tableIdBytes.length);
                data.put(tableIdBytes);
                data.putInt(key.length);
                data.put(key);
                data.putInt(walValue.length);
                data.put(walValue);
                return data.array();
            }
            List<Map<String, Object>> dataList = kvBuilder.data();
            if (dataList != null && !dataList.isEmpty()) {
                log.warn(
                        "Batch data list not supported for WAL serialization in table {}, "
                                + "skipping WAL durability for batch writes",
                        tableIdentifier);
                return new byte[0];
            }
        }
        return new byte[0];
    }

    public long calculateWriteBytes(WriteBuilder builder) {
        if (builder instanceof KvWriteBuilder kvBuilder) {
            byte[] key = kvBuilder.key();
            byte[] value = kvBuilder.value();
            if (key != null && value != null) {
                return key.length + value.length;
            }
            List<Map<String, Object>> data = kvBuilder.data();
            if (data != null) {
                return data.size() * 64L;
            }
        }
        return 64L;
    }

    public Write.Result executeWithWalGuard(
            WriteBuilder builder, String tableIdentifier, Supplier<Write.Result> lmdbWrite) {
        if (durabilityLevel.isWalRequired()) {
            if (wal != null && wal.getConsumer() != null && wal.getConsumer().isHdfsUnavailable()) {
                if (failurePolicy == FailurePolicy.REJECT) {
                    throw new DurabilityViolationException(
                            "WAL unavailable (HDFS down), rejecting write per failure-policy=REJECT");
                }
                return lmdbWrite.get();
            }
            byte[] walData = extractWalData(builder, tableIdentifier);
            if (walData.length > 0 && wal != null) {
                return executeWithDisruptorWal(tableIdentifier, walData, lmdbWrite);
            }
        }
        return lmdbWrite.get();
    }

    private Write.Result executeWithDisruptorWal(
            String tableIdentifier, byte[] walData, Supplier<Write.Result> lmdbWrite) {
        try {
            WALRecord walRecord =
                    new DefaultWALRecord(
                            -1, walData, System.nanoTime(), -1L, tableIdentifier, "DATA");
            if (groupCommitEnabled) {
                wal.appendWithGroupCommit(walRecord);
            } else {
                wal.append(walRecord, true);
            }
        } catch (WALWriteException e) {
            log.error("WAL consumer has stopped, write rejected to guarantee data durability", e);
            walFailedCount.incrementAndGet();
            throw new DynamicMmapStore.RecoverableWriteException(
                    "WAL write failed for table "
                            + tableIdentifier
                            + ", rejecting write to prevent data loss");
        } catch (Exception e) {
            log.warn("WAL append failed, rejecting write (data may be lost on crash)", e);
            walFailedCount.incrementAndGet();
            throw new DynamicMmapStore.RecoverableWriteException(
                    "WAL append failed for table " + tableIdentifier + ", rejecting write");
        }
        Write.Result result = lmdbWrite.get();
        // When LMDB write fails, write an ABORT record to WAL to mark the previous DATA record as
        // invalid.
        // The recovery process can skip the corresponding DATA record via the ABORT type, avoiding
        // replay of invalid writes.
        if (!result.success()) {
            try {
                WALRecord abortRecord =
                        new DefaultWALRecord(
                                -1, new byte[0], System.nanoTime(), -1L, tableIdentifier, "ABORT");
                wal.append(abortRecord, false);
                log.warn(
                        "LMDB write failed for table {}, WAL ABORT record written. Result: {}",
                        tableIdentifier,
                        result.message());
            } catch (Exception e) {
                log.warn("Failed to write WAL ABORT record for table {}", tableIdentifier, e);
            }
        }
        return result;
    }
}
