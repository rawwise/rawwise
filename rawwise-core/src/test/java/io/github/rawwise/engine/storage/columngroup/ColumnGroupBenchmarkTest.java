package io.github.rawwise.engine.storage.columngroup;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.model.record.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Disabled("Benchmark test - run manually only")
class ColumnGroupBenchmarkTest {

    private static final int ROW_COUNT = 100_000;
    private static final int WARMUP_ROUNDS = 5;
    private static final int MEASURE_ROUNDS = 10;
    private static final int ROW_GROUP_SIZE = 128 * 1024 * 1024;
    private static final int SMALL_ROW_GROUP_SIZE = 1024 * 1024;

    private static final ColumnGroup CG_BASIC =
            new ColumnGroup(
                    "cg_basic",
                    List.of("name", "email"),
                    List.of(PrimitiveType.STRING, PrimitiveType.STRING),
                    ColumnGroup.Encoding.FIXED_OFFSET);

    private static final ColumnGroup CG_STATS =
            new ColumnGroup(
                    "cg_stats",
                    List.of("age", "login_count", "last_login", "score"),
                    List.of(
                            PrimitiveType.INT,
                            PrimitiveType.INT,
                            PrimitiveType.LONG,
                            PrimitiveType.DOUBLE),
                    ColumnGroup.Encoding.FIXED_OFFSET);

    private static final ColumnGroup CG_BASIC_TLV =
            new ColumnGroup(
                    "cg_basic",
                    List.of("name", "email"),
                    List.of(PrimitiveType.STRING, PrimitiveType.STRING),
                    ColumnGroup.Encoding.TLV);

    private static final ColumnGroup CG_STATS_TLV =
            new ColumnGroup(
                    "cg_stats",
                    List.of("age", "login_count", "last_login", "score"),
                    List.of(
                            PrimitiveType.INT,
                            PrimitiveType.INT,
                            PrimitiveType.LONG,
                            PrimitiveType.DOUBLE),
                    ColumnGroup.Encoding.TLV);

    @TempDir java.nio.file.Path tempDir;

    static class RowData {
        final long pk;
        final String name;
        final Integer age;
        final String email;
        final String address;
        final String city;
        final String phone;
        final Integer loginCount;
        final Long lastLogin;
        final Double score;
        final String bio;
        final String company;
        final String department;
        final String title;
        final Integer salary;
        final String education;
        final String university;
        final String graduationYear;
        final String maritalStatus;
        final Integer childrenCount;

        RowData(long pk, Random rng) {
            this.pk = pk;
            this.name = "user_" + pk;
            this.age = rng.nextDouble() < 0.05 ? null : 18 + rng.nextInt(60);
            this.email = "user_" + pk + "@example.com";
            this.address = pk + " Main Street Apt " + rng.nextInt(1000);
            this.city = "City_" + rng.nextInt(100);
            this.phone = "+1-555-" + rng.nextInt(10000);
            this.loginCount = rng.nextDouble() < 0.05 ? null : rng.nextInt(10000);
            this.lastLogin =
                    rng.nextDouble() < 0.05
                            ? null
                            : System.currentTimeMillis() - rng.nextLong(365L * 24 * 3600 * 1000);
            this.score = rng.nextDouble() < 0.05 ? null : rng.nextDouble() * 100;
            this.bio = "Bio text for user " + pk + " with some extra content to make it longer";
            this.company = "Company_" + rng.nextInt(500);
            this.department = "Dept_" + rng.nextInt(20);
            this.title = "Title_" + rng.nextInt(50);
            this.salary = 30000 + rng.nextInt(170000);
            this.education =
                    rng.nextBoolean() ? "Bachelor" : (rng.nextBoolean() ? "Master" : "PhD");
            this.university = "Univ_" + rng.nextInt(200);
            this.graduationYear = String.valueOf(1990 + rng.nextInt(30));
            this.maritalStatus = rng.nextBoolean() ? "Single" : "Married";
            this.childrenCount = rng.nextInt(5);
        }
    }

    private List<RowData> generateData(int count, long seed) {
        Random rng = new Random(seed);
        List<RowData> rows = new ArrayList<>(count);
        for (long i = 0; i < count; i++) {
            rows.add(new RowData(i, rng));
        }
        return rows;
    }

    private Map<String, Object> toBasicValues(RowData row) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("name", row.name);
        values.put("email", row.email);
        return values;
    }

    private Map<String, Object> toStatsValues(RowData row) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("age", row.age);
        values.put("login_count", row.loginCount);
        values.put("last_login", row.lastLogin);
        values.put("score", row.score);
        return values;
    }

    private static final String COLUMNAR_NATIVE_SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                    + "{\"name\":\"pk\",\"type\":\"long\"},"
                    + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null},"
                    + "{\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"address\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"city\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"phone\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"login_count\",\"type\":[\"null\",\"int\"],\"default\":null},"
                    + "{\"name\":\"last_login\",\"type\":[\"null\",\"long\"],\"default\":null},"
                    + "{\"name\":\"score\",\"type\":[\"null\",\"double\"],\"default\":null},"
                    + "{\"name\":\"bio\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"company\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"department\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"title\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"salary\",\"type\":[\"null\",\"int\"],\"default\":null},"
                    + "{\"name\":\"education\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"university\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"graduation_year\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"marital_status\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"children_count\",\"type\":[\"null\",\"int\"],\"default\":null}"
                    + "]}";

    private static final String CG_ONLY_SCHEMA_JSON =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                    + "{\"name\":\"pk\",\"type\":\"long\"},"
                    + "{\"name\":\"cg_basic\",\"type\":\"bytes\"},"
                    + "{\"name\":\"cg_stats\",\"type\":\"bytes\"}"
                    + "]}";

    private static final String COLUMNAR_STATS_PROJECTION_JSON =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                    + "{\"name\":\"pk\",\"type\":\"long\"},"
                    + "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null},"
                    + "{\"name\":\"login_count\",\"type\":[\"null\",\"int\"],\"default\":null},"
                    + "{\"name\":\"last_login\",\"type\":[\"null\",\"long\"],\"default\":null},"
                    + "{\"name\":\"score\",\"type\":[\"null\",\"double\"],\"default\":null}"
                    + "]}";

    private static final String CG_STATS_PROJECTION_JSON =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                    + "{\"name\":\"pk\",\"type\":\"long\"},"
                    + "{\"name\":\"cg_stats\",\"type\":\"bytes\"}"
                    + "]}";

    private static final String COLUMNAR_FILTER_PROJECTION_JSON =
            "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                    + "{\"name\":\"pk\",\"type\":\"long\"},"
                    + "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null},"
                    + "{\"name\":\"score\",\"type\":[\"null\",\"double\"],\"default\":null}"
                    + "]}";

    private void putBytes(GenericRecord record, String field, byte[] data) {
        record.put(field, ByteBuffer.wrap(data));
    }

    private static byte[] getBytesFromRecord(GenericRecord record, String field) {
        ByteBuffer buf = (ByteBuffer) record.get(field);
        if (buf == null) return null;
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
    }

    private void writeColumnarNativeParquet(String path, List<RowData> rows) throws IOException {
        org.apache.avro.Schema schema =
                new org.apache.avro.Schema.Parser().parse(COLUMNAR_NATIVE_SCHEMA_JSON);
        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(new Path(path))
                        .withSchema(schema)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .withRowGroupSize(128 * 1024 * 1024)
                        .build()) {
            for (RowData row : rows) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("pk", row.pk);
                record.put(
                        "name", row.name != null ? new org.apache.avro.util.Utf8(row.name) : null);
                record.put("age", row.age);
                record.put(
                        "email",
                        row.email != null ? new org.apache.avro.util.Utf8(row.email) : null);
                record.put(
                        "address",
                        row.address != null ? new org.apache.avro.util.Utf8(row.address) : null);
                record.put(
                        "city", row.city != null ? new org.apache.avro.util.Utf8(row.city) : null);
                record.put(
                        "phone",
                        row.phone != null ? new org.apache.avro.util.Utf8(row.phone) : null);
                record.put("login_count", row.loginCount);
                record.put("last_login", row.lastLogin);
                record.put("score", row.score);
                record.put("bio", row.bio != null ? new org.apache.avro.util.Utf8(row.bio) : null);
                record.put(
                        "company",
                        row.company != null ? new org.apache.avro.util.Utf8(row.company) : null);
                record.put(
                        "department",
                        row.department != null
                                ? new org.apache.avro.util.Utf8(row.department)
                                : null);
                record.put(
                        "title",
                        row.title != null ? new org.apache.avro.util.Utf8(row.title) : null);
                record.put("salary", row.salary);
                record.put(
                        "education",
                        row.education != null
                                ? new org.apache.avro.util.Utf8(row.education)
                                : null);
                record.put(
                        "university",
                        row.university != null
                                ? new org.apache.avro.util.Utf8(row.university)
                                : null);
                record.put(
                        "graduation_year",
                        row.graduationYear != null
                                ? new org.apache.avro.util.Utf8(row.graduationYear)
                                : null);
                record.put(
                        "marital_status",
                        row.maritalStatus != null
                                ? new org.apache.avro.util.Utf8(row.maritalStatus)
                                : null);
                record.put("children_count", row.childrenCount);
                writer.write(record);
            }
        }
    }

    private void writeCgFixedParquet(String path, List<RowData> rows, int rowGroupSize)
            throws IOException {
        org.apache.avro.Schema schema =
                new org.apache.avro.Schema.Parser().parse(CG_ONLY_SCHEMA_JSON);
        ColumnGroupCodec fixedCodec = ColumnGroupCodecFactory.getCodec(CG_BASIC);
        ColumnGroupCodec statsFixedCodec = ColumnGroupCodecFactory.getCodec(CG_STATS);
        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(new Path(path))
                        .withSchema(schema)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .withRowGroupSize(rowGroupSize)
                        .build()) {
            for (RowData row : rows) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("pk", row.pk);
                putBytes(record, "cg_basic", fixedCodec.encode(CG_BASIC, toBasicValues(row)));
                putBytes(record, "cg_stats", statsFixedCodec.encode(CG_STATS, toStatsValues(row)));
                writer.write(record);
            }
        }
    }

    private void writeCgTlvParquet(String path, List<RowData> rows, int rowGroupSize)
            throws IOException {
        org.apache.avro.Schema schema =
                new org.apache.avro.Schema.Parser().parse(CG_ONLY_SCHEMA_JSON);
        ColumnGroupCodec tlvCodec = ColumnGroupCodecFactory.getCodec(CG_BASIC_TLV);
        ColumnGroupCodec statsTlvCodec = ColumnGroupCodecFactory.getCodec(CG_STATS_TLV);
        try (ParquetWriter<GenericRecord> writer =
                AvroParquetWriter.<GenericRecord>builder(new Path(path))
                        .withSchema(schema)
                        .withConf(new Configuration())
                        .withCompressionCodec(CompressionCodecName.ZSTD)
                        .withRowGroupSize(rowGroupSize)
                        .build()) {
            for (RowData row : rows) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("pk", row.pk);
                putBytes(record, "cg_basic", tlvCodec.encode(CG_BASIC_TLV, toBasicValues(row)));
                putBytes(
                        record, "cg_stats", statsTlvCodec.encode(CG_STATS_TLV, toStatsValues(row)));
                writer.write(record);
            }
        }
    }

    private long getColumnTotalSize(String path, List<String> columnNames) throws IOException {
        Path hadoopPath = new Path(path);
        ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(), hadoopPath);
        long totalSize = 0;
        for (BlockMetaData block : footer.getBlocks()) {
            for (ColumnChunkMetaData col : block.getColumns()) {
                for (String colName : columnNames) {
                    if (col.getPath().toDotString().equals(colName)) {
                        totalSize += col.getTotalSize();
                    }
                }
            }
        }
        return totalSize;
    }

    private long getFileSize(java.nio.file.Path path) {
        try {
            return java.nio.file.Files.size(path);
        } catch (IOException e) {
            return -1;
        }
    }

    private int countRowGroups(String path) throws IOException {
        Path hadoopPath = new Path(path);
        ParquetMetadata footer = ParquetFileReader.readFooter(new Configuration(), hadoopPath);
        return footer.getBlocks().size();
    }

    private long getRowsMatchingAgeGtColumnar(String path, int ageThreshold) throws IOException {
        org.apache.avro.Schema projection =
                new org.apache.avro.Schema.Parser().parse(COLUMNAR_FILTER_PROJECTION_JSON);
        Configuration conf = new Configuration();
        AvroReadSupport.setRequestedProjection(conf, projection);
        long count = 0;
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(new Path(path)).withConf(conf).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                Integer age = (Integer) record.get("age");
                if (age != null && age > ageThreshold) {
                    count++;
                }
            }
        }
        return count;
    }

    private long getRowsMatchingAgeGtCgFixed(String path, int ageThreshold) throws IOException {
        org.apache.avro.Schema projection =
                new org.apache.avro.Schema.Parser().parse(CG_STATS_PROJECTION_JSON);
        Configuration conf = new Configuration();
        AvroReadSupport.setRequestedProjection(conf, projection);
        ColumnGroupCodec codec = ColumnGroupCodecFactory.getCodec(CG_STATS);
        long count = 0;
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(new Path(path)).withConf(conf).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                byte[] cgStats = getBytesFromRecord(record, "cg_stats");
                if (cgStats != null) {
                    Object age = codec.decodeField(CG_STATS, cgStats, "age");
                    if (age instanceof Integer a && a > ageThreshold) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private long getRowsMatchingAgeGtCgTlv(String path, int ageThreshold) throws IOException {
        org.apache.avro.Schema projection =
                new org.apache.avro.Schema.Parser().parse(CG_STATS_PROJECTION_JSON);
        Configuration conf = new Configuration();
        AvroReadSupport.setRequestedProjection(conf, projection);
        ColumnGroupCodec codec = ColumnGroupCodecFactory.getCodec(CG_STATS_TLV);
        long count = 0;
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(new Path(path)).withConf(conf).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                byte[] cgStats = getBytesFromRecord(record, "cg_stats");
                if (cgStats != null) {
                    Object age = codec.decodeField(CG_STATS_TLV, cgStats, "age");
                    if (age instanceof Integer a && a > ageThreshold) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private Map<String, Object> pointQueryDecode(
            String path,
            long targetPk,
            String projectionJson,
            ColumnGroup cgStats,
            ColumnGroup cgBasic,
            boolean useFixed)
            throws IOException {
        org.apache.avro.Schema projection =
                new org.apache.avro.Schema.Parser().parse(projectionJson);
        Configuration conf = new Configuration();
        AvroReadSupport.setRequestedProjection(conf, projection);
        ColumnGroupCodec statsCodec =
                useFixed
                        ? ColumnGroupCodecFactory.getCodec(cgStats)
                        : ColumnGroupCodecFactory.getCodec(cgStats);
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(new Path(path)).withConf(conf).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                if ((Long) record.get("pk") == targetPk) {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("pk", targetPk);
                    byte[] cgStatsBytes = getBytesFromRecord(record, "cg_stats");
                    if (cgStatsBytes != null) {
                        result.putAll(statsCodec.decode(cgStats, cgStatsBytes));
                    }
                    byte[] cgBasicBytes = getBytesFromRecord(record, "cg_basic");
                    if (cgBasicBytes != null && cgBasic != null) {
                        ColumnGroupCodec basicCodec = ColumnGroupCodecFactory.getCodec(cgBasic);
                        result.putAll(basicCodec.decode(cgBasic, cgBasicBytes));
                    }
                    return result;
                }
            }
        }
        return null;
    }

    private Map<String, Object> pointQueryDecodeField(
            String path,
            long targetPk,
            String projectionJson,
            ColumnGroup cgStats,
            boolean useFixed)
            throws IOException {
        org.apache.avro.Schema projection =
                new org.apache.avro.Schema.Parser().parse(projectionJson);
        Configuration conf = new Configuration();
        AvroReadSupport.setRequestedProjection(conf, projection);
        ColumnGroupCodec statsCodec = ColumnGroupCodecFactory.getCodec(cgStats);
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(new Path(path)).withConf(conf).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                if ((Long) record.get("pk") == targetPk) {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("pk", targetPk);
                    byte[] cgStatsBytes = getBytesFromRecord(record, "cg_stats");
                    if (cgStatsBytes != null) {
                        result.put("age", statsCodec.decodeField(cgStats, cgStatsBytes, "age"));
                        result.put(
                                "login_count",
                                statsCodec.decodeField(cgStats, cgStatsBytes, "login_count"));
                        result.put(
                                "last_login",
                                statsCodec.decodeField(cgStats, cgStatsBytes, "last_login"));
                        result.put("score", statsCodec.decodeField(cgStats, cgStatsBytes, "score"));
                    }
                    return result;
                }
            }
        }
        return null;
    }

    private Map<String, Object> columnarPointQuery(
            String path, long targetPk, String projectionJson) throws IOException {
        org.apache.avro.Schema projection =
                new org.apache.avro.Schema.Parser().parse(projectionJson);
        Configuration conf = new Configuration();
        AvroReadSupport.setRequestedProjection(conf, projection);
        try (ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(new Path(path)).withConf(conf).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                if ((Long) record.get("pk") == targetPk) {
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("pk", record.get("pk"));
                    result.put("age", record.get("age"));
                    result.put("login_count", record.get("login_count"));
                    result.put("last_login", record.get("last_login"));
                    result.put("score", record.get("score"));
                    return result;
                }
            }
        }
        return null;
    }

    private void gcPause() {
        System.gc();
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    void benchmarkProjectionScanUntilMatch() throws Exception {
        List<RowData> rows = generateData(ROW_COUNT, 42);
        String columnarPath = tempDir.resolve("columnar_pt.parquet").toString();
        String cgTlvPath = tempDir.resolve("cg_tlv_pt.parquet").toString();
        String cgFixedPath = tempDir.resolve("cg_fixed_pt.parquet").toString();

        writeColumnarNativeParquet(columnarPath, rows);
        writeCgTlvParquet(cgTlvPath, rows, ROW_GROUP_SIZE);
        writeCgFixedParquet(cgFixedPath, rows, ROW_GROUP_SIZE);

        long targetPk = 50_000;
        RowData expected = rows.get((int) targetPk);

        long columnarFileSize = getFileSize(java.nio.file.Path.of(columnarPath));
        long cgTlvFileSize = getFileSize(java.nio.file.Path.of(cgTlvPath));
        long cgFixedFileSize = getFileSize(java.nio.file.Path.of(cgFixedPath));

        long columnarIo =
                getColumnTotalSize(
                        columnarPath, List.of("pk", "age", "login_count", "last_login", "score"));
        long cgTlvIo = getColumnTotalSize(cgTlvPath, List.of("pk", "cg_stats"));
        long cgFixedIo = getColumnTotalSize(cgFixedPath, List.of("pk", "cg_stats"));

        Map<String, Object> colResult =
                columnarPointQuery(columnarPath, targetPk, COLUMNAR_STATS_PROJECTION_JSON);
        Map<String, Object> tlvResult =
                pointQueryDecode(
                        cgTlvPath, targetPk, CG_STATS_PROJECTION_JSON, CG_STATS_TLV, null, false);
        Map<String, Object> fixedResult =
                pointQueryDecode(
                        cgFixedPath, targetPk, CG_STATS_PROJECTION_JSON, CG_STATS, null, true);
        Map<String, Object> fixedFieldResult =
                pointQueryDecodeField(
                        cgFixedPath, targetPk, CG_STATS_PROJECTION_JSON, CG_STATS, true);

        assertThat(colResult).isNotNull();
        assertThat(tlvResult).isNotNull();
        assertThat(fixedResult).isNotNull();
        assertThat(fixedFieldResult).isNotNull();
        assertThat(colResult.get("age")).isEqualTo(expected.age);
        assertThat(fixedResult.get("age")).isEqualTo(expected.age);
        assertThat(fixedFieldResult.get("age")).isEqualTo(expected.age);

        long columnarNanos = 0, tlvNanos = 0, fixedNanos = 0, fixedFieldNanos = 0;

        for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
            gcPause();
            long t0 = System.nanoTime();
            columnarPointQuery(columnarPath, targetPk, COLUMNAR_STATS_PROJECTION_JSON);
            if (i >= WARMUP_ROUNDS) columnarNanos += System.nanoTime() - t0;

            gcPause();
            t0 = System.nanoTime();
            pointQueryDecode(
                    cgTlvPath, targetPk, CG_STATS_PROJECTION_JSON, CG_STATS_TLV, null, false);
            if (i >= WARMUP_ROUNDS) tlvNanos += System.nanoTime() - t0;

            gcPause();
            t0 = System.nanoTime();
            pointQueryDecode(cgFixedPath, targetPk, CG_STATS_PROJECTION_JSON, CG_STATS, null, true);
            if (i >= WARMUP_ROUNDS) fixedNanos += System.nanoTime() - t0;

            gcPause();
            t0 = System.nanoTime();
            pointQueryDecodeField(cgFixedPath, targetPk, CG_STATS_PROJECTION_JSON, CG_STATS, true);
            if (i >= WARMUP_ROUNDS) fixedFieldNanos += System.nanoTime() - t0;
        }

        System.out.println(
                "=== Projection Scan Until Match: 4 stats fields (using production Codec) ===");
        System.out.println();
        System.out.println(
                "  [Column Group Design: no column overlap, whole group add/delete only]");
        System.out.println(
                "    cg_basic: {name, email}       cg_stats: {age, login_count, last_login, score}");
        System.out.println();
        System.out.println("  [Storage comparison]");
        System.out.printf("    Columnar (20 native cols, ZSTD):  %,d bytes%n", columnarFileSize);
        System.out.printf(
                "    CG-TLV   (pk+2 CGs, ZSTD):       %,d bytes (%.1f%% of columnar)%n",
                cgTlvFileSize, (double) cgTlvFileSize / columnarFileSize * 100);
        System.out.printf(
                "    CG-Fixed (pk+2 CGs, ZSTD):       %,d bytes (%.1f%% of columnar)%n",
                cgFixedFileSize, (double) cgFixedFileSize / columnarFileSize * 100);
        System.out.println();
        System.out.println("  [I/O: projected columns only]");
        System.out.printf("    Columnar I/O (pk+4 native cols):  %,d bytes%n", columnarIo);
        System.out.printf("    CG-TLV   I/O (pk+cg_stats):      %,d bytes%n", cgTlvIo);
        System.out.printf("    CG-Fixed I/O (pk+cg_stats):      %,d bytes%n", cgFixedIo);
        System.out.printf(
                "    CG-Fixed I/O change vs Columnar: %+.1f%%%n",
                ((double) cgFixedIo / columnarIo - 1.0) * 100);
        System.out.println();
        System.out.println("  [Decode path]");
        System.out.println(
                "    Columnar:  Parquet reads 5 native column chunks -> direct field access");
        System.out.println(
                "    CG-TLV:   Parquet reads 2 column chunks -> TLV scan O(n) per field");
        System.out.println(
                "    CG-Fixed: Parquet reads 2 column chunks -> fixed offset O(1) per field");
        System.out.println("    CG-Fixed(decodeField): O(1) single-field decode via offset table");
        System.out.println();
        System.out.println("  [Total read+decode latency]");
        System.out.printf(
                "    Columnar avg:           %.2f ms%n",
                columnarNanos / 1_000_000.0 / MEASURE_ROUNDS);
        System.out.printf(
                "    CG-TLV avg:             %.2f ms%n", tlvNanos / 1_000_000.0 / MEASURE_ROUNDS);
        System.out.printf(
                "    CG-Fixed avg (decode):  %.2f ms%n", fixedNanos / 1_000_000.0 / MEASURE_ROUNDS);
        System.out.printf(
                "    CG-Fixed avg (decodeField O(1)): %.2f ms%n",
                fixedFieldNanos / 1_000_000.0 / MEASURE_ROUNDS);

        assertThat(columnarIo).isGreaterThan(0);
    }

    @Test
    void benchmarkPredicatePushdownScan() throws Exception {
        List<RowData> rows = generateData(ROW_COUNT, 42);
        String columnarPath = tempDir.resolve("columnar_filter.parquet").toString();
        String cgTlvPath = tempDir.resolve("cg_tlv_filter.parquet").toString();
        String cgFixedPath = tempDir.resolve("cg_fixed_filter.parquet").toString();

        writeColumnarNativeParquet(columnarPath, rows);
        writeCgTlvParquet(cgTlvPath, rows, SMALL_ROW_GROUP_SIZE);
        writeCgFixedParquet(cgFixedPath, rows, SMALL_ROW_GROUP_SIZE);

        int ageThreshold = 70;
        long expectedCount =
                rows.stream().filter(r -> r.age != null && r.age > ageThreshold).count();

        int colRgCount = countRowGroups(columnarPath);
        int cgRgCount = countRowGroups(cgFixedPath);

        long columnarNanos = 0, tlvNanos = 0, fixedNanos = 0;
        long columnarCount = 0, tlvCount = 0, fixedCount = 0;

        for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
            gcPause();
            long t0 = System.nanoTime();
            long cnt = getRowsMatchingAgeGtColumnar(columnarPath, ageThreshold);
            if (i >= WARMUP_ROUNDS) columnarNanos += System.nanoTime() - t0;
            if (i == WARMUP_ROUNDS) columnarCount = cnt;

            gcPause();
            t0 = System.nanoTime();
            cnt = getRowsMatchingAgeGtCgTlv(cgTlvPath, ageThreshold);
            if (i >= WARMUP_ROUNDS) tlvNanos += System.nanoTime() - t0;
            if (i == WARMUP_ROUNDS) tlvCount = cnt;

            gcPause();
            t0 = System.nanoTime();
            cnt = getRowsMatchingAgeGtCgFixed(cgFixedPath, ageThreshold);
            if (i >= WARMUP_ROUNDS) fixedNanos += System.nanoTime() - t0;
            if (i == WARMUP_ROUNDS) fixedCount = cnt;
        }

        assertThat(columnarCount).isEqualTo(expectedCount);
        assertThat(tlvCount).isEqualTo(expectedCount);
        assertThat(fixedCount).isEqualTo(expectedCount);

        long columnarAgeIo = getColumnTotalSize(columnarPath, List.of("pk", "age", "score"));
        long cgTlvStatsIo = getColumnTotalSize(cgTlvPath, List.of("pk", "cg_stats"));
        long cgFixedStatsIo = getColumnTotalSize(cgFixedPath, List.of("pk", "cg_stats"));

        System.out.println(
                "=== Filter Scan: WHERE age > 70 (1MB RowGroups, " + colRgCount + " RGs) ===");
        System.out.println();
        System.out.println(
                "  [Column Group's critical weakness: cannot use Parquet predicate pushdown for internal fields]");
        System.out.println(
                "    CG-Fixed uses decodeField() for O(1) single-field extraction during filter");
        System.out.printf("  RowGroups: Columnar=%d, CG=%d%n", colRgCount, cgRgCount);
        System.out.println();
        System.out.printf(
                "  Matching rows: %d / %d (%.1f%%)%n",
                expectedCount, ROW_COUNT, (double) expectedCount / ROW_COUNT * 100);
        System.out.println();
        System.out.println("  [I/O comparison]");
        System.out.printf("    Columnar I/O (pk+age+score):       %,d bytes%n", columnarAgeIo);
        System.out.printf("    CG-TLV   I/O (pk+cg_stats):        %,d bytes%n", cgTlvStatsIo);
        System.out.printf("    CG-Fixed I/O (pk+cg_stats):        %,d bytes%n", cgFixedStatsIo);
        System.out.println();
        System.out.println("  [Scan latency (decode + filter)]");
        System.out.printf(
                "    Columnar avg:  %.2f ms%n", columnarNanos / 1_000_000.0 / MEASURE_ROUNDS);
        System.out.printf("    CG-TLV avg:    %.2f ms%n", tlvNanos / 1_000_000.0 / MEASURE_ROUNDS);
        System.out.printf(
                "    CG-Fixed avg:  %.2f ms%n", fixedNanos / 1_000_000.0 / MEASURE_ROUNDS);

        assertThat(columnarAgeIo).isGreaterThan(0);
    }

    @Test
    void benchmarkCpuDecodeOnly() throws Exception {
        List<RowData> rows = generateData(ROW_COUNT, 42);
        int targetRowIndex = 50_000;
        RowData targetRow = rows.get(targetRowIndex);

        ColumnGroupCodec fixedCodec = ColumnGroupCodecFactory.getCodec(CG_STATS);
        ColumnGroupCodec tlvCodec = ColumnGroupCodecFactory.getCodec(CG_STATS_TLV);

        byte[] fixedStatsData = fixedCodec.encode(CG_STATS, toStatsValues(targetRow));
        byte[] tlvStatsData = tlvCodec.encode(CG_STATS_TLV, toStatsValues(targetRow));

        Map<String, Object> fixedCheck = fixedCodec.decode(CG_STATS, fixedStatsData);
        Map<String, Object> tlvCheck = tlvCodec.decode(CG_STATS_TLV, tlvStatsData);
        assertThat(fixedCheck.get("age")).isEqualTo(targetRow.age);
        assertThat(fixedCheck.get("login_count")).isEqualTo(targetRow.loginCount);
        assertThat(tlvCheck.get("age")).isEqualTo(targetRow.age);
        assertThat(tlvCheck.get("login_count")).isEqualTo(targetRow.loginCount);

        int decodeRounds = 10_000;
        long fixedNanos = 0, tlvNanos = 0, fixedFieldNanos = 0;

        for (int i = 0; i < WARMUP_ROUNDS + MEASURE_ROUNDS; i++) {
            gcPause();
            long t0 = System.nanoTime();
            for (int j = 0; j < decodeRounds; j++) {
                fixedCodec.decode(CG_STATS, fixedStatsData);
            }
            if (i >= WARMUP_ROUNDS) fixedNanos += System.nanoTime() - t0;

            gcPause();
            t0 = System.nanoTime();
            for (int j = 0; j < decodeRounds; j++) {
                tlvCodec.decode(CG_STATS_TLV, tlvStatsData);
            }
            if (i >= WARMUP_ROUNDS) tlvNanos += System.nanoTime() - t0;

            gcPause();
            t0 = System.nanoTime();
            for (int j = 0; j < decodeRounds; j++) {
                fixedCodec.decodeField(CG_STATS, fixedStatsData, "score");
            }
            if (i >= WARMUP_ROUNDS) fixedFieldNanos += System.nanoTime() - t0;
        }

        System.out.println(
                "=== CPU Decode Only: Fixed-Offset vs TLV (4 fields, using production Codec) ===");
        System.out.printf("  Decode rounds:                         %,d%n", decodeRounds);
        System.out.printf(
                "  Fixed-offset full decode per row:      %.1f ns%n",
                (double) fixedNanos / MEASURE_ROUNDS / decodeRounds);
        System.out.printf(
                "  TLV full decode per row:               %.1f ns%n",
                (double) tlvNanos / MEASURE_ROUNDS / decodeRounds);
        System.out.printf(
                "  Fixed-offset decodeField (O(1)):       %.1f ns%n",
                (double) fixedFieldNanos / MEASURE_ROUNDS / decodeRounds);
        System.out.printf(
                "  Fixed-offset full decode speedup vs TLV: %.2fx%n",
                (double) tlvNanos / fixedNanos);
        System.out.printf(
                "  Fixed-offset decodeField speedup vs TLV full: %.2fx%n",
                (double) tlvNanos / fixedFieldNanos);

        assertThat(fixedNanos).isGreaterThan(0);
    }

    @Test
    void benchmarkCompressionRatio() throws Exception {
        List<RowData> rows = generateData(ROW_COUNT, 42);
        String columnarPath = tempDir.resolve("columnar_compress.parquet").toString();
        String cgTlvPath = tempDir.resolve("cg_tlv_compress.parquet").toString();
        String cgFixedPath = tempDir.resolve("cg_fixed_compress.parquet").toString();

        writeColumnarNativeParquet(columnarPath, rows);
        writeCgTlvParquet(cgTlvPath, rows, ROW_GROUP_SIZE);
        writeCgFixedParquet(cgFixedPath, rows, ROW_GROUP_SIZE);

        long columnarFileSize = getFileSize(java.nio.file.Path.of(columnarPath));
        long cgTlvFileSize = getFileSize(java.nio.file.Path.of(cgTlvPath));
        long cgFixedFileSize = getFileSize(java.nio.file.Path.of(cgFixedPath));

        long columnarAgeSize = getColumnTotalSize(columnarPath, List.of("age"));
        long columnarLoginCountSize = getColumnTotalSize(columnarPath, List.of("login_count"));
        long columnarLastLoginSize = getColumnTotalSize(columnarPath, List.of("last_login"));
        long columnarScoreSize = getColumnTotalSize(columnarPath, List.of("score"));
        long columnarStatsTotal =
                columnarAgeSize
                        + columnarLoginCountSize
                        + columnarLastLoginSize
                        + columnarScoreSize;
        long cgTlvStatsSize = getColumnTotalSize(cgTlvPath, List.of("cg_stats"));
        long cgFixedStatsSize = getColumnTotalSize(cgFixedPath, List.of("cg_stats"));

        System.out.println(
                "=== Compression Ratio: Native Parquet Types + ZSTD vs ColumnGroup Binary + ZSTD ===");
        System.out.printf("  Columnar file (20 native cols, ZSTD):  %,d bytes%n", columnarFileSize);
        System.out.printf(
                "  CG-TLV   file (pk+2 CGs, ZSTD):       %,d bytes (%.1f%% of columnar)%n",
                cgTlvFileSize, (double) cgTlvFileSize / columnarFileSize * 100);
        System.out.printf(
                "  CG-Fixed file (pk+2 CGs, ZSTD):       %,d bytes (%.1f%% of columnar)%n",
                cgFixedFileSize, (double) cgFixedFileSize / columnarFileSize * 100);
        System.out.println();
        System.out.println("  --- Stats columns: native types vs ColumnGroup binary ---");
        System.out.printf(
                "  Columnar stats total (4 native cols):  %,d bytes%n", columnarStatsTotal);
        System.out.printf("  CG-TLV   cg_stats (binary, ZSTD):     %,d bytes%n", cgTlvStatsSize);
        System.out.printf("  CG-Fixed cg_stats (binary, ZSTD):     %,d bytes%n", cgFixedStatsSize);
        System.out.printf(
                "  CG-TLV   compression penalty:         +%.1f%%%n",
                ((double) cgTlvStatsSize / columnarStatsTotal - 1.0) * 100);
        System.out.printf(
                "  CG-Fixed compression penalty:         +%.1f%%%n",
                ((double) cgFixedStatsSize / columnarStatsTotal - 1.0) * 100);

        assertThat(columnarFileSize).isGreaterThan(0);
    }

    @Test
    void benchmarkNullValueCorrectness() throws Exception {
        List<RowData> rows = generateData(1000, 42);
        ColumnGroupCodec fixedCodec = ColumnGroupCodecFactory.getCodec(CG_STATS);
        ColumnGroupCodec tlvCodec = ColumnGroupCodecFactory.getCodec(CG_STATS_TLV);

        for (RowData row : rows) {
            byte[] fixedStats = fixedCodec.encode(CG_STATS, toStatsValues(row));
            byte[] tlvStats = tlvCodec.encode(CG_STATS_TLV, toStatsValues(row));

            Map<String, Object> fixedResult = fixedCodec.decode(CG_STATS, fixedStats);
            Map<String, Object> tlvResult = tlvCodec.decode(CG_STATS_TLV, tlvStats);

            assertThat(fixedResult.get("age")).as("pk=%d age", row.pk).isEqualTo(row.age);
            assertThat(fixedResult.get("login_count"))
                    .as("pk=%d loginCount", row.pk)
                    .isEqualTo(row.loginCount);
            assertThat(fixedResult.get("last_login"))
                    .as("pk=%d lastLogin", row.pk)
                    .isEqualTo(row.lastLogin);
            if (row.score != null) {
                assertThat((Double) fixedResult.get("score"))
                        .as("pk=%d score", row.pk)
                        .isCloseTo(row.score, org.assertj.core.data.Offset.offset(0.0001));
            } else {
                assertThat(fixedResult.get("score"))
                        .as("pk=%d score should be null", row.pk)
                        .isNull();
            }

            assertThat(tlvResult.get("age")).as("pk=%d age (TLV)", row.pk).isEqualTo(row.age);
            assertThat(tlvResult.get("login_count"))
                    .as("pk=%d loginCount (TLV)", row.pk)
                    .isEqualTo(row.loginCount);
            assertThat(tlvResult.get("last_login"))
                    .as("pk=%d lastLogin (TLV)", row.pk)
                    .isEqualTo(row.lastLogin);
            if (row.score != null) {
                assertThat((Double) tlvResult.get("score"))
                        .as("pk=%d score (TLV)", row.pk)
                        .isCloseTo(row.score, org.assertj.core.data.Offset.offset(0.0001));
            } else {
                assertThat(tlvResult.get("score"))
                        .as("pk=%d score (TLV) should be null", row.pk)
                        .isNull();
            }

            Object fixedAge = fixedCodec.decodeField(CG_STATS, fixedStats, "age");
            assertThat(fixedAge).as("pk=%d decodeField age", row.pk).isEqualTo(row.age);
            Object tlvAge = tlvCodec.decodeField(CG_STATS_TLV, tlvStats, "age");
            assertThat(tlvAge).as("pk=%d TLV decodeField age", row.pk).isEqualTo(row.age);
        }

        long nullCount = rows.stream().filter(r -> r.age == null).count();
        System.out.println("=== Null Value Correctness: 1000 rows with 5%% null probability ===");
        System.out.printf("  Rows with null age:         %d / %d%n", nullCount, rows.size());
        System.out.printf("  All Fixed-Offset round-trip: PASSED%n");
        System.out.printf("  All TLV round-trip:         PASSED%n");
        System.out.printf("  All decodeField O(1):       PASSED%n");

        assertThat(nullCount).isGreaterThan(0);
    }

    @Test
    void benchmarkColumnChunkOverheadAnalysis() throws Exception {
        List<RowData> rows = generateData(ROW_COUNT, 42);
        String columnarPath = tempDir.resolve("columnar_overhead.parquet").toString();
        String cgFixedPath = tempDir.resolve("cg_fixed_overhead.parquet").toString();

        writeColumnarNativeParquet(columnarPath, rows);
        writeCgFixedParquet(cgFixedPath, rows, ROW_GROUP_SIZE);

        Path hadoopColPath = new Path(columnarPath);
        Path hadoopCgPath = new Path(cgFixedPath);
        ParquetMetadata colFooter =
                ParquetFileReader.readFooter(new Configuration(), hadoopColPath);
        ParquetMetadata cgFooter = ParquetFileReader.readFooter(new Configuration(), hadoopCgPath);

        int colChunkCount = 0;
        long colTotalData = 0;
        for (BlockMetaData block : colFooter.getBlocks()) {
            for (ColumnChunkMetaData col : block.getColumns()) {
                colTotalData += col.getTotalSize();
                colChunkCount++;
            }
        }

        int cgChunkCount = 0;
        long cgTotalData = 0;
        for (BlockMetaData block : cgFooter.getBlocks()) {
            for (ColumnChunkMetaData col : block.getColumns()) {
                cgTotalData += col.getTotalSize();
                cgChunkCount++;
            }
        }

        long[] seekCostsMs = {0, 1, 5, 10, 50};

        System.out.println("=== Column Chunk Overhead Analysis ===");
        System.out.printf(
                "  Columnar: %d column chunks, total compressed size: %,d bytes%n",
                colChunkCount, colTotalData);
        System.out.printf(
                "  CG-Fixed: %d column chunks, total compressed size: %,d bytes%n",
                cgChunkCount, cgTotalData);
        System.out.printf(
                "    Columnar has %.1fx more column chunks than CG%n",
                (double) colChunkCount / cgChunkCount);
        System.out.println();

        long colStatsIo =
                getColumnTotalSize(
                        columnarPath, List.of("pk", "age", "login_count", "last_login", "score"));
        long cgStatsIo = getColumnTotalSize(cgFixedPath, List.of("pk", "cg_stats"));
        int colStatsChunks = 5;
        int cgStatsChunks = 2;

        System.out.printf(
                "    %-20s | %-25s | %-25s | %s%n",
                "Seek Cost", "Columnar (5 chunks)", "CG-Fixed (2 chunks)", "CG Speedup");
        System.out.println("    " + "-".repeat(85));

        for (long seekMs : seekCostsMs) {
            double colLatency =
                    (double) colStatsIo / (100 * 1024 * 1024) * 1000 + colStatsChunks * seekMs;
            double cgLatency =
                    (double) cgStatsIo / (100 * 1024 * 1024) * 1000 + cgStatsChunks * seekMs;
            double speedup = colLatency / cgLatency;
            String seekLabel =
                    seekMs == 0
                            ? "0ms (local SSD)"
                            : seekMs == 1
                                    ? "1ms (fast network)"
                                    : seekMs == 5
                                            ? "5ms (typical network)"
                                            : seekMs == 10
                                                    ? "10ms (S3 first-byte)"
                                                    : "50ms (cold S3)";
            System.out.printf(
                    "    %-20s | %12.2f ms         | %12.2f ms         | %.2fx%n",
                    seekLabel, colLatency, cgLatency, speedup);
        }

        assertThat(colTotalData).isGreaterThan(0);
    }
}
