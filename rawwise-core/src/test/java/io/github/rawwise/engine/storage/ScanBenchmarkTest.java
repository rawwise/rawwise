package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

@Disabled("Benchmark test - run manually only")
class ScanBenchmarkTest {

    private static final int RECORD_COUNT = 100_000;
    private static final int VALUE_SIZE = 100;
    private static final int WARMUP_ROUNDS = 2;
    private static final int MEASURE_ROUNDS = 5;
    private static final int COLD_MEASURE_ROUNDS = 3;
    private static final long LMDB_MAP_SIZE = 512L * 1024 * 1024;

    private static final Schema KV_SCHEMA =
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

    private static final Schema WIDE_SCHEMA =
            new Schema.Parser()
                    .parse(
                            """
            {
              "type": "record",
              "name": "WideRecord",
              "fields": [
                {"name": "key", "type": "bytes"},
                {"name": "f1", "type": "bytes"},
                {"name": "f2", "type": "bytes"},
                {"name": "f3", "type": "bytes"},
                {"name": "f4", "type": "bytes"},
                {"name": "f5", "type": "bytes"},
                {"name": "f6", "type": "bytes"},
                {"name": "f7", "type": "bytes"},
                {"name": "f8", "type": "bytes"},
                {"name": "f9", "type": "bytes"}
              ]
            }
            """);

    private static final Schema WIDE_PROJECTION_SCHEMA =
            new Schema.Parser()
                    .parse(
                            """
            {
              "type": "record",
              "name": "WideRecord",
              "fields": [
                {"name": "key", "type": "bytes"},
                {"name": "f1", "type": "bytes"},
                {"name": "f3", "type": "bytes"},
                {"name": "f7", "type": "bytes"}
              ]
            }
            """);

    @TempDir Path tempDir;

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

    private static String formatDataSize(long bytes) {
        if (bytes >= 1024 * 1024) {
            return String.format("%.1f MB", bytes / 1024.0 / 1024.0);
        } else if (bytes >= 1024) {
            return String.format("%.1f KB", bytes / 1024.0);
        }
        return bytes + " B";
    }

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

    record ByteArrayKey(byte[] data) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ByteArrayKey(byte[] d))) return false;
            return Arrays.equals(data, d);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
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

    private void closeAll(LmdbInstance... instances) {
        for (LmdbInstance inst : instances) {
            closeLmdb(inst);
        }
    }

    @SuppressWarnings("unchecked")
    private LmdbInstance prepareLmdbWithData(String id, List<KvPair> data) throws IOException {
        LmdbInstance instance = createLmdbInstance(id);
        try {
            Field envField = LmdbInstance.class.getDeclaredField("env");
            envField.setAccessible(true);
            Env<ByteBuffer> env = (Env<ByteBuffer>) envField.get(instance);
            Field dbiField = LmdbInstance.class.getDeclaredField("dbi");
            dbiField.setAccessible(true);
            Dbi<ByteBuffer> dbi = (Dbi<ByteBuffer>) dbiField.get(instance);

            int maxKeyLen = 8;
            int maxValLen = 0;
            for (KvPair kv : data) {
                if (kv.value().length > maxValLen) maxValLen = kv.value().length;
            }
            ByteBuffer keyBuf = ByteBuffer.allocateDirect(maxKeyLen);
            ByteBuffer valBuf = ByteBuffer.allocateDirect(maxValLen);

            int batchSize = 5000;
            for (int offset = 0; offset < data.size(); offset += batchSize) {
                int end = Math.min(offset + batchSize, data.size());
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    for (int i = offset; i < end; i++) {
                        keyBuf.clear();
                        keyBuf.put(data.get(i).key()).flip();
                        valBuf.clear();
                        valBuf.put(data.get(i).value()).flip();
                        dbi.put(txn, keyBuf, valBuf);
                    }
                    txn.commit();
                }
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IOException("Failed to access LMDB internals", e);
        }
        return instance;
    }

    private String prepareParquetFile(String id, List<KvPair> data) throws IOException {
        String path = tempDir.resolve("parquet-" + id + ".parquet").toString();
        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
                        .withSchema(KV_SCHEMA)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .build()) {
            for (KvPair kv : data) {
                GenericRecord record = new GenericData.Record(KV_SCHEMA);
                record.put("key", ByteBuffer.wrap(kv.key()));
                record.put("value", ByteBuffer.wrap(kv.value()));
                writer.write(record);
            }
        }
        return path;
    }

    private String prepareWideParquetFile(String id, int count, int fieldSize, long seed)
            throws IOException {
        String path = tempDir.resolve("parquet-wide-" + id + ".parquet").toString();
        Random rng = new Random(seed);
        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
                        .withSchema(WIDE_SCHEMA)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .build()) {
            for (int i = 0; i < count; i++) {
                GenericRecord record = new GenericData.Record(WIDE_SCHEMA);
                record.put("key", ByteBuffer.wrap(longToBytes(i)));
                for (int f = 1; f <= 9; f++) {
                    byte[] fieldData = new byte[fieldSize];
                    rng.nextBytes(fieldData);
                    record.put("f" + f, ByteBuffer.wrap(fieldData));
                }
                writer.write(record);
            }
        }
        return path;
    }

    @SuppressWarnings("unchecked")
    private LmdbInstance prepareWideLmdb(String id, int count, int fieldSize, long seed)
            throws IOException {
        LmdbInstance instance = createLmdbInstance(id, 2L * 1024 * 1024 * 1024);
        Random rng = new Random(seed);
        try {
            Field envField = LmdbInstance.class.getDeclaredField("env");
            envField.setAccessible(true);
            Env<ByteBuffer> env = (Env<ByteBuffer>) envField.get(instance);
            Field dbiField = LmdbInstance.class.getDeclaredField("dbi");
            dbiField.setAccessible(true);
            Dbi<ByteBuffer> dbi = (Dbi<ByteBuffer>) dbiField.get(instance);

            int valLen = fieldSize * 9;
            ByteBuffer keyBuf = ByteBuffer.allocateDirect(8);
            ByteBuffer valBuf = ByteBuffer.allocateDirect(valLen);

            int batchSize = 5000;
            for (int offset = 0; offset < count; offset += batchSize) {
                int end = Math.min(offset + batchSize, count);
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    for (int i = offset; i < end; i++) {
                        keyBuf.clear();
                        keyBuf.put(longToBytes(i)).flip();
                        byte[] value = new byte[valLen];
                        rng.nextBytes(value);
                        valBuf.clear();
                        valBuf.put(value).flip();
                        dbi.put(txn, keyBuf, valBuf);
                    }
                    txn.commit();
                }
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IOException("Failed to access LMDB internals", e);
        }
        return instance;
    }

    private long scanLmdbAll(LmdbInstance inst) throws IOException {
        long rows = 0;
        try (var iterator = inst.closeableIterator()) {
            while (iterator.hasNext()) {
                iterator.next();
                rows++;
            }
        }
        return rows;
    }

    private long scanParquetAll(String path) throws IOException {
        long rows = 0;
        try (var reader =
                AvroParquetReader.<GenericRecord>builder(new org.apache.hadoop.fs.Path(path))
                        .withConf(new Configuration())
                        .build()) {
            while (reader.read() != null) {
                rows++;
            }
        }
        return rows;
    }

    @Test
    void benchmarkFullScanComparison() throws Exception {
        int keySize = 8;
        long payloadBytes = dataBytes(RECORD_COUNT, keySize, VALUE_SIZE);
        List<KvPair> data = generateSequentialData(RECORD_COUNT, VALUE_SIZE, 42);

        LmdbInstance lmdbInst = prepareLmdbWithData("full-scan", data);
        String parquetPath = prepareParquetFile("full-scan", data);
        long parquetFileSize = java.nio.file.Files.size(java.nio.file.Path.of(parquetPath));

        try {
            long lmdbScanNs = 0;
            long lmdbRows = 0;
            for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                gcPause();
                long t0 = System.nanoTime();
                long rows = scanLmdbAll(lmdbInst);
                if (i >= WARMUP_ROUNDS) {
                    lmdbScanNs += System.nanoTime() - t0;
                    lmdbRows = rows;
                }
            }

            long parquetScanNs = 0;
            long parquetRows = 0;
            for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                gcPause();
                long t0 = System.nanoTime();
                long rows = scanParquetAll(parquetPath);
                if (i >= WARMUP_ROUNDS) {
                    parquetScanNs += System.nanoTime() - t0;
                    parquetRows = rows;
                }
            }

            double lmdbBps = payloadBytes / (lmdbScanNs / 1_000_000_000.0 / MEASURE_ROUNDS);
            double parquetBps = payloadBytes / (parquetScanNs / 1_000_000_000.0 / MEASURE_ROUNDS);

            System.out.println();
            System.out.println("========================================");
            System.out.println("  Full Scan: LMDB vs Parquet");
            System.out.println("========================================");
            System.out.printf(
                    "  Records: %,d | Key: %dB | Value: %dB | Payload: %s%n",
                    RECORD_COUNT, keySize, VALUE_SIZE, formatDataSize(payloadBytes));
            System.out.printf(
                    "  Parquet file size: %s (compression ratio: %.2fx)%n",
                    formatDataSize(parquetFileSize), (double) payloadBytes / parquetFileSize);
            System.out.println();
            System.out.printf("  %-30s %12s %12s%n", "Scan Path", "Throughput", "Rows");
            System.out.println("  " + "-".repeat(56));
            System.out.printf(
                    "  %-30s %12s %,12d%n",
                    "LMDB closeableIterator()", formatThroughput(lmdbBps), lmdbRows);
            System.out.printf(
                    "  %-30s %12s %,12d%n",
                    "Parquet AvroParquetReader", formatThroughput(parquetBps), parquetRows);
            System.out.println();
            System.out.printf("  LMDB / Parquet speedup: %.2fx%n", lmdbBps / parquetBps);
            System.out.printf(
                    "  LMDB scan time: %.2f ms%n", lmdbScanNs / 1_000_000.0 / MEASURE_ROUNDS);
            System.out.printf(
                    "  Parquet scan time: %.2f ms%n", parquetScanNs / 1_000_000.0 / MEASURE_ROUNDS);

            assertThat(lmdbRows).isEqualTo(RECORD_COUNT);
            assertThat(parquetRows).isEqualTo(RECORD_COUNT);
        } finally {
            closeLmdb(lmdbInst);
        }
    }

    @Test
    void benchmarkFullScanWithDedup() throws Exception {
        int keySize = 8;
        int uniqueKeyCount = RECORD_COUNT;
        int totalRecordsPerInstance = (int) (RECORD_COUNT * 0.4);
        long payloadBytes = dataBytes(uniqueKeyCount, keySize, VALUE_SIZE);

        int instanceCount = 4;
        LmdbInstance[] instances = new LmdbInstance[instanceCount];
        try {
            Random rng = new Random(99);
            for (int i = 0; i < instanceCount; i++) {
                List<KvPair> overlapping = new ArrayList<>();
                for (int j = 0; j < totalRecordsPerInstance; j++) {
                    int keyIndex;
                    if (j < totalRecordsPerInstance / 2) {
                        keyIndex = rng.nextInt(uniqueKeyCount);
                    } else {
                        keyIndex = j - totalRecordsPerInstance / 2;
                    }
                    byte[] key = longToBytes(keyIndex);
                    byte[] value = new byte[VALUE_SIZE];
                    new Random(42 + i + j).nextBytes(value);
                    overlapping.add(new KvPair(key, value));
                }
                overlapping.sort((a, b) -> Arrays.compareUnsigned(a.key(), b.key()));
                instances[i] = prepareLmdbWithData("dedup-" + i, overlapping);
            }

            long dedupScanNs = 0;
            long naiveScanNs = 0;
            long dedupRows = 0;
            long naiveRows = 0;

            for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                gcPause();
                long t0 = System.nanoTime();
                LinkedHashMap<ByteArrayKey, byte[]> latest = new LinkedHashMap<>();
                for (LmdbInstance inst : instances) {
                    try (var iterator = inst.closeableIterator()) {
                        while (iterator.hasNext()) {
                            var pair = iterator.next();
                            ByteArrayKey key = new ByteArrayKey(pair.getLeft());
                            latest.put(key, pair.getRight());
                        }
                    }
                }
                if (i >= WARMUP_ROUNDS) {
                    dedupScanNs += System.nanoTime() - t0;
                    dedupRows = latest.size();
                }

                gcPause();
                t0 = System.nanoTime();
                long rows = 0;
                for (LmdbInstance inst : instances) {
                    try (var iterator = inst.closeableIterator()) {
                        while (iterator.hasNext()) {
                            iterator.next();
                            rows++;
                        }
                    }
                }
                if (i >= WARMUP_ROUNDS) {
                    naiveScanNs += System.nanoTime() - t0;
                    naiveRows = rows;
                }
            }

            double dedupBps = payloadBytes / (dedupScanNs / 1_000_000_000.0 / MEASURE_ROUNDS);
            double naiveBps = payloadBytes / (naiveScanNs / 1_000_000_000.0 / MEASURE_ROUNDS);

            System.out.println();
            System.out.println("========================================");
            System.out.println("  Multi-Instance Scan: Dedup vs Naive");
            System.out.println("========================================");
            System.out.printf(
                    "  Instances: %d | Unique keys: %,d | Records/instance: %,d | Total records: %,d%n",
                    instanceCount,
                    uniqueKeyCount,
                    totalRecordsPerInstance,
                    totalRecordsPerInstance * instanceCount);
            System.out.printf(
                    "  Key overlap: ~%.0f%% (each instance shares the same key space)%n",
                    (1.0 - 1.0 / instanceCount) * 100);
            System.out.println();
            System.out.printf("  %-35s %12s %12s%n", "Scan Mode", "Throughput", "Rows");
            System.out.println("  " + "-".repeat(61));
            System.out.printf(
                    "  %-35s %12s %,12d%n",
                    "LinkedHashMap dedup (latest wins)", formatThroughput(dedupBps), dedupRows);
            System.out.printf(
                    "  %-35s %12s %,12d%n",
                    "Naive (no dedup, raw iteration)", formatThroughput(naiveBps), naiveRows);
            System.out.println();
            System.out.printf(
                    "  Dedup overhead vs naive: %.2fx slower%n",
                    (double) dedupScanNs / naiveScanNs);
            System.out.printf(
                    "  Dedup scan time: %.2f ms%n", dedupScanNs / 1_000_000.0 / MEASURE_ROUNDS);
            System.out.printf(
                    "  Naive scan time: %.2f ms%n", naiveScanNs / 1_000_000.0 / MEASURE_ROUNDS);
            System.out.printf(
                    "  Dedup reduced %,d -> %,d rows (%.1f%% reduction)%n",
                    naiveRows, dedupRows, (1.0 - (double) dedupRows / naiveRows) * 100);

            assertThat(dedupRows).isGreaterThan(0);
            assertThat(dedupRows).isLessThan(naiveRows);
        } finally {
            closeAll(instances);
        }
    }

    @Test
    void benchmarkProjectionScan() throws Exception {
        int count = 100_000;
        int fieldSize = 50;
        long seed = 42;

        LmdbInstance lmdbInst = prepareWideLmdb("proj", count, fieldSize, seed);
        String parquetPath = prepareWideParquetFile("proj", count, fieldSize, seed);

        try {
            long lmdbFullScanNs = 0;
            for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                gcPause();
                long t0 = System.nanoTime();
                scanLmdbAll(lmdbInst);
                if (i >= WARMUP_ROUNDS) lmdbFullScanNs += System.nanoTime() - t0;
            }

            long parquetFullScanNs = 0;
            for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                gcPause();
                Configuration fullConf = new Configuration();
                long t0 = System.nanoTime();
                try (var reader =
                        AvroParquetReader.<GenericRecord>builder(
                                        new org.apache.hadoop.fs.Path(parquetPath))
                                .withConf(fullConf)
                                .build()) {
                    while (reader.read() != null) {}
                }
                if (i >= WARMUP_ROUNDS) parquetFullScanNs += System.nanoTime() - t0;
            }

            long parquetProjectionScanNs = 0;
            for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                gcPause();
                Configuration projConf = new Configuration();
                AvroReadSupport.setRequestedProjection(projConf, WIDE_PROJECTION_SCHEMA);
                long t0 = System.nanoTime();
                try (var reader =
                        AvroParquetReader.<GenericRecord>builder(
                                        new org.apache.hadoop.fs.Path(parquetPath))
                                .withConf(projConf)
                                .build()) {
                    while (reader.read() != null) {}
                }
                if (i >= WARMUP_ROUNDS) parquetProjectionScanNs += System.nanoTime() - t0;
            }

            long lmdbTotalSize = lmdbInst.getTotalUsageSize();
            long parquetFullSize = java.nio.file.Files.size(java.nio.file.Path.of(parquetPath));

            System.out.println();
            System.out.println("========================================");
            System.out.println("  Projection Scan: LMDB (row) vs Parquet (columnar)");
            System.out.println("========================================");
            System.out.printf(
                    "  Records: %,d | Fields: 9 x %dB | LMDB data: %s | Parquet file: %s%n",
                    count,
                    fieldSize,
                    formatDataSize(lmdbTotalSize),
                    formatDataSize(parquetFullSize));
            System.out.println();
            System.out.printf("  %-35s %12s %10s%n", "Scan Path", "Time (ms)", "Speedup");
            System.out.println("  " + "-".repeat(59));
            double lmdbFullMs = lmdbFullScanNs / 1_000_000.0 / MEASURE_ROUNDS;
            double parquetFullMs = parquetFullScanNs / 1_000_000.0 / MEASURE_ROUNDS;
            double parquetProjMs = parquetProjectionScanNs / 1_000_000.0 / MEASURE_ROUNDS;
            System.out.printf(
                    "  %-35s %12.2f %10s%n", "LMDB full scan (all fields)", lmdbFullMs, "baseline");
            System.out.printf(
                    "  %-35s %12.2f %10s%n",
                    "Parquet full scan (all 10 cols)",
                    parquetFullMs,
                    String.format("%.2fx", lmdbFullMs / parquetFullMs));
            System.out.printf(
                    "  %-35s %12.2f %10s%n",
                    "Parquet projection (4/10 cols)",
                    parquetProjMs,
                    String.format("%.2fx", parquetFullMs / parquetProjMs));
            System.out.println();
            System.out.printf(
                    "  Parquet projection vs LMDB full: %.2fx%n", lmdbFullMs / parquetProjMs);
            System.out.printf("  Note: LMDB is row-store, cannot skip unread fields%n");
        } finally {
            closeLmdb(lmdbInst);
        }
    }

    @Test
    void benchmarkVaryingValueSizes() throws Exception {
        int keySize = 8;
        int count = 100_000;
        int[] valueSizes = {16, 64, 256, 1024};

        System.out.println();
        System.out.println("========================================");
        System.out.println("  Scan Throughput vs Value Size");
        System.out.println("========================================");
        System.out.printf("  Records: %,d | Key: %dB%n", count, keySize);
        System.out.println();
        System.out.printf(
                "  %-10s | %12s | %12s | %12s | %8s%n",
                "Val Size", "LMDB Scan", "Parquet Scan", "Parquet I/O", "LMDB/Par");
        System.out.println("  " + "-".repeat(70));

        for (int vSize : valueSizes) {
            long payloadBytes = dataBytes(count, keySize, vSize);
            List<KvPair> data = generateSequentialData(count, vSize, 42);

            LmdbInstance lmdbInst = prepareLmdbWithData("vsize-" + vSize, data);
            String parquetPath = prepareParquetFile("vsize-" + vSize, data);
            long parquetFileSize = java.nio.file.Files.size(java.nio.file.Path.of(parquetPath));

            try {
                long lmdbNs = 0;
                for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                    gcPause();
                    long t0 = System.nanoTime();
                    scanLmdbAll(lmdbInst);
                    if (i >= WARMUP_ROUNDS) lmdbNs += System.nanoTime() - t0;
                }

                long parquetNs = 0;
                for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                    gcPause();
                    long t0 = System.nanoTime();
                    scanParquetAll(parquetPath);
                    if (i >= WARMUP_ROUNDS) parquetNs += System.nanoTime() - t0;
                }

                double lmdbBps = payloadBytes / (lmdbNs / 1_000_000_000.0 / MEASURE_ROUNDS);
                double parquetBps = payloadBytes / (parquetNs / 1_000_000_000.0 / MEASURE_ROUNDS);
                double ratio = lmdbBps / parquetBps;

                System.out.printf(
                        "  %-10s | %12s | %12s | %12s | %7.2fx%n",
                        vSize + "B",
                        formatThroughput(lmdbBps),
                        formatThroughput(parquetBps),
                        formatDataSize(parquetFileSize),
                        ratio);
            } finally {
                closeLmdb(lmdbInst);
            }
        }
    }

    @Test
    void benchmarkColdVsWarmScan() throws Exception {
        int keySize = 8;
        int count = 100_000;
        int vSize = 256;
        long payloadBytes = dataBytes(count, keySize, vSize);
        List<KvPair> data = generateSequentialData(count, vSize, 42);

        long[] lmdbColdNsArr = new long[COLD_MEASURE_ROUNDS];
        long[] parquetColdNsArr = new long[COLD_MEASURE_ROUNDS];

        for (int round = 0; round < COLD_MEASURE_ROUNDS; round++) {
            LmdbInstance lmdbInst = prepareLmdbWithData("cold-warm-" + round, data);
            String parquetPath = prepareParquetFile("cold-warm-" + round, data);

            try {
                gcPause();
                long t0 = System.nanoTime();
                scanLmdbAll(lmdbInst);
                lmdbColdNsArr[round] = System.nanoTime() - t0;

                gcPause();
                t0 = System.nanoTime();
                scanParquetAll(parquetPath);
                parquetColdNsArr[round] = System.nanoTime() - t0;

                long lmdbWarmNs = 0;
                long parquetWarmNs = 0;
                for (int i = 0; i < MEASURE_ROUNDS; i++) {
                    gcPause();
                    t0 = System.nanoTime();
                    scanLmdbAll(lmdbInst);
                    lmdbWarmNs += System.nanoTime() - t0;

                    gcPause();
                    t0 = System.nanoTime();
                    scanParquetAll(parquetPath);
                    parquetWarmNs += System.nanoTime() - t0;
                }

                if (round == 0) {
                    double lmdbWarmBps =
                            payloadBytes / (lmdbWarmNs / 1_000_000_000.0 / MEASURE_ROUNDS);
                    double parquetWarmBps =
                            payloadBytes / (parquetWarmNs / 1_000_000_000.0 / MEASURE_ROUNDS);
                    System.out.printf(
                            "  [Warm reference] LMDB: %s | Parquet: %s%n",
                            formatThroughput(lmdbWarmBps), formatThroughput(parquetWarmBps));
                }
            } finally {
                closeLmdb(lmdbInst);
            }
        }

        Arrays.sort(lmdbColdNsArr);
        Arrays.sort(parquetColdNsArr);
        long lmdbColdMedian = lmdbColdNsArr[COLD_MEASURE_ROUNDS / 2];
        long parquetColdMedian = parquetColdNsArr[COLD_MEASURE_ROUNDS / 2];

        double lmdbColdBps = payloadBytes / (lmdbColdMedian / 1_000_000_000.0);
        double parquetColdBps = payloadBytes / (parquetColdMedian / 1_000_000_000.0);

        System.out.println();
        System.out.println("========================================");
        System.out.println("  Cold vs Warm Scan (OS Page Cache)");
        System.out.println("========================================");
        System.out.printf(
                "  Records: %,d | Value: %dB | Payload: %s%n",
                count, vSize, formatDataSize(payloadBytes));
        System.out.printf("  Cold: %d independent runs, median reported%n", COLD_MEASURE_ROUNDS);
        System.out.println();
        System.out.printf("  %-20s | %12s | %12s%n", "Scan Path", "Cold (median)", "Cold samples");
        System.out.println("  " + "-".repeat(50));
        StringBuilder lmdbSamples = new StringBuilder();
        for (int i = 0; i < COLD_MEASURE_ROUNDS; i++) {
            if (i > 0) lmdbSamples.append(", ");
            lmdbSamples.append(String.format("%.1f ms", lmdbColdNsArr[i] / 1_000_000.0));
        }
        StringBuilder parquetSamples = new StringBuilder();
        for (int i = 0; i < COLD_MEASURE_ROUNDS; i++) {
            if (i > 0) parquetSamples.append(", ");
            parquetSamples.append(String.format("%.1f ms", parquetColdNsArr[i] / 1_000_000.0));
        }
        System.out.printf(
                "  %-20s | %12s | %s%n", "LMDB", formatThroughput(lmdbColdBps), lmdbSamples);
        System.out.printf(
                "  %-20s | %12s | %s%n",
                "Parquet", formatThroughput(parquetColdBps), parquetSamples);
        System.out.println();
        System.out.printf("  LMDB cold scan (median):   %.2f ms%n", lmdbColdMedian / 1_000_000.0);
        System.out.printf(
                "  Parquet cold scan (median): %.2f ms%n", parquetColdMedian / 1_000_000.0);
        System.out.println();
        System.out.printf("  Cold: LMDB/Parquet = %.2fx%n", lmdbColdBps / parquetColdBps);
    }

    @Test
    void benchmarkScanAtScale() throws Exception {
        int keySize = 8;
        int vSize = 100;
        int[] counts = {50_000, 100_000, 200_000, 500_000};

        System.out.println();
        System.out.println("========================================");
        System.out.println("  Scan Throughput vs Data Volume");
        System.out.println("========================================");
        System.out.printf("  Key: %dB | Value: %dB%n", keySize, vSize);
        System.out.println();
        System.out.printf(
                "  %-12s | %12s | %12s | %8s | %12s | %12s%n",
                "Volume", "LMDB", "Parquet", "Ratio", "LMDB time", "Parquet time");
        System.out.println("  " + "-".repeat(85));

        for (int count : counts) {
            long payloadBytes = dataBytes(count, keySize, vSize);
            List<KvPair> data = generateSequentialData(count, vSize, 42);

            LmdbInstance lmdbInst = prepareLmdbWithData("scale-" + count, data);
            String parquetPath = prepareParquetFile("scale-" + count, data);

            try {
                long lmdbNs = 0;
                for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                    gcPause();
                    long t0 = System.nanoTime();
                    scanLmdbAll(lmdbInst);
                    if (i >= WARMUP_ROUNDS) lmdbNs += System.nanoTime() - t0;
                }

                long parquetNs = 0;
                for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
                    gcPause();
                    long t0 = System.nanoTime();
                    scanParquetAll(parquetPath);
                    if (i >= WARMUP_ROUNDS) parquetNs += System.nanoTime() - t0;
                }

                double lmdbBps = payloadBytes / (lmdbNs / 1_000_000_000.0 / MEASURE_ROUNDS);
                double parquetBps = payloadBytes / (parquetNs / 1_000_000_000.0 / MEASURE_ROUNDS);
                double lmdbMs = lmdbNs / 1_000_000.0 / MEASURE_ROUNDS;
                double parquetMs = parquetNs / 1_000_000.0 / MEASURE_ROUNDS;

                System.out.printf(
                        "  %-12s | %12s | %12s | %7.2fx | %9.2f ms | %9.2f ms%n",
                        formatDataSize(payloadBytes),
                        formatThroughput(lmdbBps),
                        formatThroughput(parquetBps),
                        lmdbBps / parquetBps,
                        lmdbMs,
                        parquetMs);
            } finally {
                closeLmdb(lmdbInst);
            }
        }
    }

    @Test
    void benchmarkParquetDecompressionOverhead() throws Exception {
        int keySize = 8;
        int count = 100_000;
        int vSize = 100;
        long payloadBytes = dataBytes(count, keySize, vSize);
        List<KvPair> data = generateSequentialData(count, vSize, 42);

        String zstdPath = tempDir.resolve("decomp-zstd.parquet").toString();
        String uncompressedPath = tempDir.resolve("decomp-raw.parquet").toString();

        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(zstdPath))
                        .withSchema(KV_SCHEMA)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .build()) {
            for (KvPair kv : data) {
                GenericRecord record = new GenericData.Record(KV_SCHEMA);
                record.put("key", ByteBuffer.wrap(kv.key()));
                record.put("value", ByteBuffer.wrap(kv.value()));
                writer.write(record);
            }
        }

        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(
                                new org.apache.hadoop.fs.Path(uncompressedPath))
                        .withSchema(KV_SCHEMA)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                        .build()) {
            for (KvPair kv : data) {
                GenericRecord record = new GenericData.Record(KV_SCHEMA);
                record.put("key", ByteBuffer.wrap(kv.key()));
                record.put("value", ByteBuffer.wrap(kv.value()));
                writer.write(record);
            }
        }

        long zstdNs = 0;
        for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
            gcPause();
            long t0 = System.nanoTime();
            scanParquetAll(zstdPath);
            if (i >= WARMUP_ROUNDS) zstdNs += System.nanoTime() - t0;
        }

        long uncompressedNs = 0;
        for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
            gcPause();
            long t0 = System.nanoTime();
            scanParquetAll(uncompressedPath);
            if (i >= WARMUP_ROUNDS) uncompressedNs += System.nanoTime() - t0;
        }

        long zstdFileSize = java.nio.file.Files.size(java.nio.file.Path.of(zstdPath));
        long rawFileSize = java.nio.file.Files.size(java.nio.file.Path.of(uncompressedPath));

        double zstdBps = payloadBytes / (zstdNs / 1_000_000_000.0 / MEASURE_ROUNDS);
        double rawBps = payloadBytes / (uncompressedNs / 1_000_000_000.0 / MEASURE_ROUNDS);

        System.out.println();
        System.out.println("========================================");
        System.out.println("  Parquet Decompression Overhead");
        System.out.println("========================================");
        System.out.printf("  Records: %,d | Payload: %s%n", count, formatDataSize(payloadBytes));
        System.out.println();
        System.out.printf(
                "  ZSTD file: %s (%.2fx compression)%n",
                formatDataSize(zstdFileSize), (double) payloadBytes / zstdFileSize);
        System.out.printf("  Uncompressed file: %s%n", formatDataSize(rawFileSize));
        System.out.println();
        System.out.printf("  %-25s %12s %12s%n", "Compression", "Throughput", "Time (ms)");
        System.out.println("  " + "-".repeat(51));
        System.out.printf(
                "  %-25s %12s %9.2f ms%n",
                "ZSTD", formatThroughput(zstdBps), zstdNs / 1_000_000.0 / MEASURE_ROUNDS);
        System.out.printf(
                "  %-25s %12s %9.2f ms%n",
                "UNCOMPRESSED",
                formatThroughput(rawBps),
                uncompressedNs / 1_000_000_000.0 / MEASURE_ROUNDS);
        System.out.println();
        System.out.printf(
                "  Decompression overhead: %.2fx slower (ZSTD vs uncompressed)%n",
                (double) zstdNs / uncompressedNs);
        System.out.printf(
                "  I/O saved by compression: %s (%.1f%% smaller)%n",
                formatDataSize(rawFileSize - zstdFileSize),
                (1.0 - (double) zstdFileSize / rawFileSize) * 100);
    }
}
