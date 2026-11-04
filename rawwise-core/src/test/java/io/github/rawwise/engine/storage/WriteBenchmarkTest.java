package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Disabled("Benchmark test - run manually only")
class WriteBenchmarkTest {

    private static final int RECORD_COUNT = 100_000;
    private static final int VALUE_SIZE = 100;
    private static final int WARMUP_ROUNDS = 2;
    private static final int MEASURE_ROUNDS = 3;
    private static final long LMDB_MAP_SIZE = 512L * 1024 * 1024;

    private static final Schema KEY_VALUE_SCHEMA =
            new Schema.Parser()
                    .parse(
                            """
            {
              "type": "record",
              "name": "KvRecord",
              "fields": [
                {"name": "key", "type": "bytes"},
                {"name": "value", "type": "bytes"}
              ]
            }
            """);

    @TempDir Path tempDir;

    record KvPair(byte[] key, byte[] value) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof KvPair(byte[] k, byte[] v))) return false;
            return Arrays.equals(key, k) && Arrays.equals(value, v);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(key) + Arrays.hashCode(value);
        }
    }

    private static byte[] longToBytes(long v) {
        return new byte[] {
            (byte) (v >> 56), (byte) (v >> 48), (byte) (v >> 40), (byte) (v >> 32),
            (byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v
        };
    }

    private static long dataBytes(int count, int keySize, int valueSize) {
        return (long) count * (keySize + valueSize);
    }

    private static String formatThroughput(double bytesPerSec) {
        if (bytesPerSec >= 1024 * 1024) {
            return String.format("%.1f MB/s", bytesPerSec / 1024.0 / 1024.0);
        } else if (bytesPerSec >= 1024) {
            return String.format("%.1f KB/s", bytesPerSec / 1024.0);
        } else {
            return String.format("%.1f B/s", bytesPerSec);
        }
    }

    private List<KvPair> generateSequentialData(int count, int valueSize, long seed) {
        Random rng = new Random(seed);
        List<KvPair> data = new ArrayList<>(count);
        for (long i = 0; i < count; i++) {
            byte[] key = longToBytes(i);
            byte[] value = new byte[valueSize];
            rng.nextBytes(value);
            data.add(new KvPair(key, value));
        }
        return data;
    }

    private List<KvPair> generateRandomData(int count, int valueSize, long seed) {
        Random rng = new Random(seed);
        List<Long> keys = new ArrayList<>(count);
        for (long i = 0; i < count; i++) {
            keys.add(i);
        }
        Collections.shuffle(keys, rng);
        List<KvPair> data = new ArrayList<>(count);
        Random valRng = new Random(seed + 1);
        for (long k : keys) {
            byte[] key = longToBytes(k);
            byte[] value = new byte[valueSize];
            valRng.nextBytes(value);
            data.add(new KvPair(key, value));
        }
        return data;
    }

    private void gcPause() {
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
    }

    private LmdbInstance createLmdbInstance(String id) throws IOException {
        return createLmdbInstance(id, LMDB_MAP_SIZE);
    }

    private LmdbInstance createLmdbInstance(String id, long mapSize) throws IOException {
        Path lmdbBase = tempDir.resolve("lmdb-" + id);
        Files.createDirectories(lmdbBase);
        LmdbInstance instance =
                new LmdbInstance(
                        "bench-" + id + "-" + System.currentTimeMillis(), lmdbBase, mapSize);
        String ownerId = "bench-owner-" + id;
        boolean leased = instance.tryAcquireLease(ownerId, 300_000);
        assertThat(leased).isTrue();
        instance.ensureRoutingOwnerLease("bench-exec", 1);
        return instance;
    }

    private void closeLmdb(LmdbInstance inst) {
        if (inst != null) {
            try {
                inst.close();
            } catch (Exception ignored) {
            }
        }
    }

    private List<byte[]> extractKeys(List<KvPair> data) {
        return data.stream().map(KvPair::key).toList();
    }

    private List<byte[]> extractValues(List<KvPair> data) {
        return data.stream().map(KvPair::value).toList();
    }

    @Test
    void benchmarkFinalComparison() throws Exception {
        int keySize = 8;
        long payloadBytes = dataBytes(RECORD_COUNT, keySize, VALUE_SIZE);

        List<KvPair> seqData = generateSequentialData(RECORD_COUNT, VALUE_SIZE, 42);
        List<KvPair> randData = generateRandomData(RECORD_COUNT, VALUE_SIZE, 42);
        List<byte[]> seqKeys = extractKeys(seqData);
        List<byte[]> seqValues = extractValues(seqData);
        List<byte[]> randKeys = extractKeys(randData);
        List<byte[]> randValues = extractValues(randData);

        long lmdbSingleSeqNs = 0, lmdbBatchSeqNs = 0, lmdbAppendSeqNs = 0;
        long lmdbBatchRandNs = 0;
        long skiplistWriteNs = 0, skiplistFlushNs = 0;
        long parquetNs = 0;
        long lmdbBatchSeqRecords = 0, lmdbAppendSeqRecords = 0;

        for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
            gcPause();
            LmdbInstance singleInst = createLmdbInstance("single-seq-" + i);
            try {
                long t0 = System.nanoTime();
                for (KvPair kv : seqData) {
                    assertThat(singleInst.put(kv.key(), kv.value()))
                            .isEqualTo(LmdbInstance.PutStatus.SUCCESS);
                }
                if (i >= WARMUP_ROUNDS) lmdbSingleSeqNs += System.nanoTime() - t0;
            } finally {
                closeLmdb(singleInst);
            }

            gcPause();
            LmdbInstance batchSeqInst = createLmdbInstance("batch-seq-" + i);
            try {
                long t0 = System.nanoTime();
                LmdbInstance.BatchPutResult r = batchSeqInst.batchPut(seqKeys, seqValues);
                if (i >= WARMUP_ROUNDS) {
                    lmdbBatchSeqNs += System.nanoTime() - t0;
                    lmdbBatchSeqRecords = batchSeqInst.getTotalRecords();
                    assertThat(r.isAllSuccess()).isTrue();
                }
            } finally {
                closeLmdb(batchSeqInst);
            }

            gcPause();
            LmdbInstance appendInst = createLmdbInstance("append-seq-" + i);
            try {
                long t0 = System.nanoTime();
                LmdbInstance.BatchPutResult r = appendInst.batchAppendPut(seqKeys, seqValues);
                if (i >= WARMUP_ROUNDS) {
                    lmdbAppendSeqNs += System.nanoTime() - t0;
                    lmdbAppendSeqRecords = appendInst.getTotalRecords();
                    assertThat(r.isAllSuccess()).isTrue();
                }
            } finally {
                closeLmdb(appendInst);
            }

            gcPause();
            LmdbInstance batchRandInst = createLmdbInstance("batch-rand-" + i);
            try {
                long t0 = System.nanoTime();
                LmdbInstance.BatchPutResult r = batchRandInst.batchPut(randKeys, randValues);
                if (i >= WARMUP_ROUNDS) {
                    lmdbBatchRandNs += System.nanoTime() - t0;
                    assertThat(r.isAllSuccess()).isTrue();
                }
            } finally {
                closeLmdb(batchRandInst);
            }

            gcPause();
            {
                ConcurrentSkipListMap<ByteBuffer, byte[]> skiplist = new ConcurrentSkipListMap<>();
                long t0 = System.nanoTime();
                for (KvPair kv : seqData) {
                    skiplist.put(ByteBuffer.wrap(kv.key().clone()), kv.value().clone());
                }
                if (i >= WARMUP_ROUNDS) skiplistWriteNs += System.nanoTime() - t0;

                String p = tempDir.resolve("skiplist-" + i + ".parquet").toString();
                gcPause();
                t0 = System.nanoTime();
                writeSkiplistToParquet(skiplist, p);
                if (i >= WARMUP_ROUNDS) skiplistFlushNs += System.nanoTime() - t0;
            }

            gcPause();
            {
                String p = tempDir.resolve("parquet-" + i + ".parquet").toString();
                long t0 = System.nanoTime();
                writeDirectParquet(seqData, p);
                if (i >= WARMUP_ROUNDS) parquetNs += System.nanoTime() - t0;
            }
        }

        double lmdbSingleSeqBps =
                payloadBytes / (lmdbSingleSeqNs / 1_000_000_000.0 / MEASURE_ROUNDS);
        double lmdbBatchSeqBps = payloadBytes / (lmdbBatchSeqNs / 1_000_000_000.0 / MEASURE_ROUNDS);
        double lmdbAppendSeqBps =
                payloadBytes / (lmdbAppendSeqNs / 1_000_000_000.0 / MEASURE_ROUNDS);
        double lmdbBatchRandBps =
                payloadBytes / (lmdbBatchRandNs / 1_000_000_000.0 / MEASURE_ROUNDS);
        double skiplistWriteBps =
                payloadBytes / (skiplistWriteNs / 1_000_000_000.0 / MEASURE_ROUNDS);
        double skiplistFlushBps =
                payloadBytes / (skiplistFlushNs / 1_000_000_000.0 / MEASURE_ROUNDS);
        double skiplistTotalBps =
                payloadBytes
                        / ((skiplistWriteNs + skiplistFlushNs) / 1_000_000_000.0 / MEASURE_ROUNDS);
        double parquetBps = payloadBytes / (parquetNs / 1_000_000_000.0 / MEASURE_ROUNDS);

        System.out.println("========================================");
        System.out.println("  Final Write Throughput Comparison");
        System.out.println("========================================");
        System.out.printf(
                "  Records: %,d | Key: %dB | Value: %dB | Payload: ~%d MB%n",
                RECORD_COUNT, keySize, VALUE_SIZE, payloadBytes / 1024 / 1024);
        System.out.println();
        System.out.printf("  %-35s %12s%n", "Write Path", "Throughput");
        System.out.println("  " + "-".repeat(49));
        System.out.printf(
                "  %-35s %12s%n", "LMDB single-txn sequential", formatThroughput(lmdbSingleSeqBps));
        System.out.printf(
                "  %-35s %12s%n", "LMDB batch-txn sequential", formatThroughput(lmdbBatchSeqBps));
        System.out.printf(
                "  %-35s %12s%n", "LMDB MDB_APPEND sequential", formatThroughput(lmdbAppendSeqBps));
        System.out.printf(
                "  %-35s %12s%n", "LMDB batch-txn random", formatThroughput(lmdbBatchRandBps));
        System.out.printf(
                "  %-35s %12s%n", "Skiplist write (mem only)", formatThroughput(skiplistWriteBps));
        System.out.printf(
                "  %-35s %12s%n",
                "Skiplist flush (mem->Parquet)", formatThroughput(skiplistFlushBps));
        System.out.printf(
                "  %-35s %12s%n",
                "Skiplist total (write+flush)", formatThroughput(skiplistTotalBps));
        System.out.printf(
                "  %-35s %12s%n", "Parquet direct sequential", formatThroughput(parquetBps));
        System.out.println();
        System.out.println("  [Relative to LMDB batch-txn sequential]");
        System.out.printf("    LMDB single-txn:     %5.2fx%n", lmdbBatchSeqBps / lmdbSingleSeqBps);
        System.out.printf("    LMDB MDB_APPEND:     %5.2fx%n", lmdbAppendSeqBps / lmdbBatchSeqBps);
        System.out.printf("    LMDB batch random:   %5.2fx%n", lmdbBatchRandBps / lmdbBatchSeqBps);
        System.out.printf("    Skiplist total:      %5.2fx%n", skiplistTotalBps / lmdbBatchSeqBps);
        System.out.printf("    Parquet direct:      %5.2fx%n", parquetBps / lmdbBatchSeqBps);
        System.out.println();
        System.out.println("  [Relative to Parquet direct]");
        System.out.printf("    LMDB single-txn:     %5.2fx%n", lmdbSingleSeqBps / parquetBps);
        System.out.printf("    LMDB batch-txn:      %5.2fx%n", lmdbBatchSeqBps / parquetBps);
        System.out.printf("    LMDB MDB_APPEND:     %5.2fx%n", lmdbAppendSeqBps / parquetBps);
        System.out.printf("    LMDB batch random:   %5.2fx%n", lmdbBatchRandBps / parquetBps);
        System.out.printf("    Skiplist total:      %5.2fx%n", skiplistTotalBps / parquetBps);

        assertThat(lmdbBatchSeqRecords).isEqualTo(RECORD_COUNT);
        assertThat(lmdbAppendSeqRecords).isEqualTo(RECORD_COUNT);
    }

    @Test
    void benchmarkBtreePageSplitImpact() throws Exception {
        int keySize = 8;
        int valueSize = 100;
        long mapSize = 2L * 1024 * 1024 * 1024;
        int[] counts = {100_000, 500_000, 1_000_000, 2_000_000};

        System.out.println("========================================");
        System.out.println("  B+tree Page Split Impact at Scale");
        System.out.println("========================================");
        System.out.printf(
                "  Key: %dB | Value: %dB | LMDB map: %d GB%n",
                keySize, valueSize, mapSize / 1024 / 1024 / 1024);
        System.out.println();
        System.out.printf(
                "  %-14s | %12s | %12s | %12s | %8s | %8s | %8s%n",
                "Data Volume", "Batch Seq", "MDB_APPEND", "Batch Rand", "APPEND", "Rand", "Search");
        System.out.printf(
                "  %-14s | %12s | %12s | %12s | %8s | %8s | %8s%n",
                "", "", "", "", "speedup", "penalty", "overhead");
        System.out.println("  " + "-".repeat(95));

        double baselineBatchSeqBps = 0;

        for (int idx = 0; idx < counts.length; idx++) {
            int count = counts[idx];
            long payloadBytes = dataBytes(count, keySize, valueSize);

            List<KvPair> seqData = generateSequentialData(count, valueSize, 42);
            List<KvPair> randData = generateRandomData(count, valueSize, 42);
            List<byte[]> seqKeys = extractKeys(seqData);
            List<byte[]> seqValues = extractValues(seqData);
            List<byte[]> randKeys = extractKeys(randData);
            List<byte[]> randValues = extractValues(randData);

            int warmup = 1;
            int measure = 2;
            long batchSeqNs = 0, appendSeqNs = 0, batchRandNs = 0;

            for (int i = 0; i < warmup + measure; i++) {
                gcPause();
                LmdbInstance batchInst =
                        createLmdbInstance("split-batch-" + count + "-" + i, mapSize);
                try {
                    long t0 = System.nanoTime();
                    batchInst.batchPut(seqKeys, seqValues);
                    if (i >= warmup) batchSeqNs += System.nanoTime() - t0;
                } finally {
                    closeLmdb(batchInst);
                }

                gcPause();
                LmdbInstance appendInst =
                        createLmdbInstance("split-append-" + count + "-" + i, mapSize);
                try {
                    long t0 = System.nanoTime();
                    appendInst.batchAppendPut(seqKeys, seqValues);
                    if (i >= warmup) appendSeqNs += System.nanoTime() - t0;
                } finally {
                    closeLmdb(appendInst);
                }

                gcPause();
                LmdbInstance randInst =
                        createLmdbInstance("split-rand-" + count + "-" + i, mapSize);
                try {
                    long t0 = System.nanoTime();
                    randInst.batchPut(randKeys, randValues);
                    if (i >= warmup) batchRandNs += System.nanoTime() - t0;
                } finally {
                    closeLmdb(randInst);
                }
            }

            double batchSeqBps = payloadBytes / (batchSeqNs / 1_000_000_000.0 / measure);
            double appendSeqBps = payloadBytes / (appendSeqNs / 1_000_000_000.0 / measure);
            double batchRandBps = payloadBytes / (batchRandNs / 1_000_000_000.0 / measure);

            if (idx == 0) baselineBatchSeqBps = batchSeqBps;

            double appendSpeedup = batchSeqBps == 0 ? 0 : appendSeqBps / batchSeqBps;
            double randPenalty = batchRandBps == 0 ? 0 : batchSeqBps / batchRandBps;
            double batchSeqPerNs = (double) batchSeqNs / measure / count;
            double appendSeqPerNs = (double) appendSeqNs / measure / count;
            double searchOverheadNs = batchSeqPerNs - appendSeqPerNs;
            double throughputDrop = (1.0 - batchSeqBps / baselineBatchSeqBps) * 100;

            String dropNote = idx == 0 ? "" : String.format(" (-%.0f%%)", throughputDrop);

            System.out.printf(
                    "  %-14s | %12s | %12s | %12s | %7.1fx | %7.1fx | %6.1f ns%n",
                    formatDataSize(payloadBytes) + dropNote,
                    formatThroughput(batchSeqBps),
                    formatThroughput(appendSeqBps),
                    formatThroughput(batchRandBps),
                    appendSpeedup,
                    randPenalty,
                    searchOverheadNs);
        }
    }

    @Test
    void benchmarkVaryingValueSizes() throws Exception {
        int keySize = 8;
        int count = 100_000;
        int[] valueSizes = {16, 64, 256, 1024};

        System.out.println("========================================");
        System.out.println("  Write Throughput vs Value Size");
        System.out.println("========================================");
        System.out.printf(
                "  Records: %,d | Key: %dB | LMDB map: %d MB%n",
                count, keySize, LMDB_MAP_SIZE / 1024 / 1024);
        System.out.println();
        System.out.printf(
                "  %-10s | %12s | %12s | %12s | %12s | %12s%n",
                "Val Size", "Batch Seq", "MDB_APPEND", "Batch Rand", "Skiplist+Fl", "Parquet Seq");
        System.out.println("  " + "-".repeat(80));

        for (int vSize : valueSizes) {
            long payloadBytes = dataBytes(count, keySize, vSize);
            List<KvPair> seqData = generateSequentialData(count, vSize, 42);
            List<KvPair> randData = generateRandomData(count, vSize, 42);
            List<byte[]> seqKeys = extractKeys(seqData);
            List<byte[]> seqValues = extractValues(seqData);
            List<byte[]> randKeys = extractKeys(randData);
            List<byte[]> randValues = extractValues(randData);

            int warmup = 2;
            int measure = 3;
            long batchSeqNs = 0,
                    appendSeqNs = 0,
                    batchRandNs = 0,
                    skiplistTotalNs = 0,
                    parquetNs = 0;

            for (int i = 0; i < warmup + measure; i++) {
                gcPause();
                LmdbInstance batchInst = createLmdbInstance("vsize-batch-" + vSize + "-" + i);
                try {
                    long t0 = System.nanoTime();
                    batchInst.batchPut(seqKeys, seqValues);
                    if (i >= warmup) batchSeqNs += System.nanoTime() - t0;
                } finally {
                    closeLmdb(batchInst);
                }

                gcPause();
                LmdbInstance appendInst = createLmdbInstance("vsize-append-" + vSize + "-" + i);
                try {
                    long t0 = System.nanoTime();
                    appendInst.batchAppendPut(seqKeys, seqValues);
                    if (i >= warmup) appendSeqNs += System.nanoTime() - t0;
                } finally {
                    closeLmdb(appendInst);
                }

                gcPause();
                LmdbInstance randInst = createLmdbInstance("vsize-rand-" + vSize + "-" + i);
                try {
                    long t0 = System.nanoTime();
                    randInst.batchPut(randKeys, randValues);
                    if (i >= warmup) batchRandNs += System.nanoTime() - t0;
                } finally {
                    closeLmdb(randInst);
                }

                gcPause();
                {
                    ConcurrentSkipListMap<ByteBuffer, byte[]> skiplist =
                            new ConcurrentSkipListMap<>();
                    long t0 = System.nanoTime();
                    for (KvPair kv : seqData) {
                        skiplist.put(ByteBuffer.wrap(kv.key().clone()), kv.value().clone());
                    }
                    String p =
                            tempDir.resolve("vsize-skiplist-" + vSize + "-" + i + ".parquet")
                                    .toString();
                    writeSkiplistToParquet(skiplist, p);
                    if (i >= warmup) skiplistTotalNs += System.nanoTime() - t0;
                }

                gcPause();
                {
                    String p =
                            tempDir.resolve("vsize-parquet-" + vSize + "-" + i + ".parquet")
                                    .toString();
                    long t0 = System.nanoTime();
                    writeDirectParquet(seqData, p);
                    if (i >= warmup) parquetNs += System.nanoTime() - t0;
                }
            }

            double batchSeqBps = payloadBytes / (batchSeqNs / 1_000_000_000.0 / measure);
            double appendSeqBps = payloadBytes / (appendSeqNs / 1_000_000_000.0 / measure);
            double batchRandBps = payloadBytes / (batchRandNs / 1_000_000_000.0 / measure);
            double skiplistBps = payloadBytes / (skiplistTotalNs / 1_000_000_000.0 / measure);
            double parquetBps = payloadBytes / (parquetNs / 1_000_000_000.0 / measure);

            System.out.printf(
                    "  %-10s | %12s | %12s | %12s | %12s | %12s%n",
                    vSize + "B",
                    formatThroughput(batchSeqBps),
                    formatThroughput(appendSeqBps),
                    formatThroughput(batchRandBps),
                    formatThroughput(skiplistBps),
                    formatThroughput(parquetBps));
        }
    }

    private static String formatDataSize(long bytes) {
        if (bytes >= 1024 * 1024) {
            return String.format("%d MB", bytes / 1024 / 1024);
        } else if (bytes >= 1024) {
            return String.format("%d KB", bytes / 1024);
        }
        return bytes + " B";
    }

    private void writeSkiplistToParquet(
            ConcurrentSkipListMap<ByteBuffer, byte[]> skiplist, String path) throws IOException {
        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
                        .withSchema(KEY_VALUE_SCHEMA)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .build()) {
            for (var entry : skiplist.entrySet()) {
                GenericRecord record = new GenericData.Record(KEY_VALUE_SCHEMA);
                byte[] keyBytes = new byte[entry.getKey().remaining()];
                entry.getKey().duplicate().get(keyBytes);
                record.put("key", ByteBuffer.wrap(keyBytes));
                record.put("value", ByteBuffer.wrap(entry.getValue()));
                writer.write(record);
            }
        }
    }

    private void writeDirectParquet(List<KvPair> data, String path) throws IOException {
        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
                        .withSchema(KEY_VALUE_SCHEMA)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .build()) {
            for (KvPair kv : data) {
                GenericRecord record = new GenericData.Record(KEY_VALUE_SCHEMA);
                record.put("key", ByteBuffer.wrap(kv.key()));
                record.put("value", ByteBuffer.wrap(kv.value()));
                writer.write(record);
            }
        }
    }
}
