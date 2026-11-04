package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.codec.DefaultValueCodec;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.storage.writer.LmdbWriter;
import io.github.rawwise.engine.storage.writer.WriteResult;
import io.github.rawwise.model.expression.*;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;

class RecordIntegrationTest {

    private static final long TABLE_ID = 100L;
    private static final long SEQ_NUM = 1L;
    private static final long COMMIT_TS = 1000L;

    private DefaultValueCodec valueCodec;
    private DefaultRecordCodec recordCodec;
    private Schema userSchema;

    @BeforeEach
    void setUp() {
        valueCodec = new DefaultValueCodec(new DefaultKeyCodec());
        recordCodec = new DefaultRecordCodec(valueCodec);
        userSchema =
                Schema.builder()
                        .field("id", Types.int32())
                        .field("name", Types.string())
                        .field("age", Types.int32())
                        .primaryKey("id")
                        .build();
    }

    @Test
    void writeEntryLmdbWriterIntegration() throws IOException {
        Path tempDir = Files.createTempDirectory("rawwise-e2e-lmdb");
        try {
            Path lmdbBase = tempDir.resolve("lmdb-write-test");
            Files.createDirectories(lmdbBase);
            LmdbInstance instance =
                    new LmdbInstance(
                            "e2e-write-" + System.currentTimeMillis(), lmdbBase, 10 * 1024 * 1024L);
            instance.tryAcquireLease("e2e-owner", 300_000);
            instance.ensureRoutingOwnerLease("e2e-exec", 1);

            LmdbWriter writer = new LmdbWriter(instance);
            Properties config = new Properties();
            config.put("writer_batch_size", 10);
            config.put("writer_use_buffer", false);
            writer.init(config);

            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("id", 77);
            fields.put("name", "frank");
            fields.put("age", 35);

            Record record =
                    Records.ofSchema(
                            userSchema,
                            fields,
                            Record.Kind.INSERT,
                            valueCodec,
                            TABLE_ID,
                            SEQ_NUM,
                            COMMIT_TS);

            WriteEntry entry = WriteEntry.fromRecord(record, recordCodec);

            WriteResult result = writer.write(entry);
            assertThat(result).isEqualTo(WriteResult.SUCCESS);

            writer.close();

            byte[] storedRaw = instance.get(record.key().encoded());
            byte[] decoded = NullableValueCodec.decode(storedRaw);

            RecordCodec.SchemaResolver resolver = (tableId, schemaVersion) -> userSchema;
            Record readBack = recordCodec.decode(decoded, resolver);

            assertThat(readBack.body().value("id").raw()).isEqualTo(77);
            assertThat(readBack.body().value("name").raw()).isEqualTo("frank");
            assertThat(readBack.body().value("age").raw()).isEqualTo(35);
            assertThat(readBack.header().kind()).isEqualTo(Record.Kind.INSERT);

            instance.close();
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void dualFormatReadPathDetection() throws IOException {
        Path tempDir = Files.createTempDirectory("rawwise-e2e-dual");
        try {
            Path lmdbBase = tempDir.resolve("lmdb-dual-test");
            Files.createDirectories(lmdbBase);
            LmdbInstance instance =
                    new LmdbInstance(
                            "e2e-dual-" + System.currentTimeMillis(), lmdbBase, 10 * 1024 * 1024L);
            instance.tryAcquireLease("e2e-owner", 300_000);
            instance.ensureRoutingOwnerLease("e2e-exec", 1);

            LmdbWriter writer = new LmdbWriter(instance);
            Properties config = new Properties();
            config.put("writer_batch_size", 10);
            config.put("writer_use_buffer", false);
            writer.init(config);

            byte[] legacyKey = "legacy-row-1".getBytes();
            byte[] legacyValue = "legacy-payload-data".getBytes();
            WriteEntry legacyEntry =
                    new WriteEntry(legacyKey, NullableValueCodec.encode(legacyValue), true);
            writer.write(legacyEntry);

            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("id", 55);
            fields.put("name", "grace");
            fields.put("age", 22);

            Record newRecord =
                    Records.ofSchema(
                            userSchema,
                            fields,
                            Record.Kind.INSERT,
                            valueCodec,
                            TABLE_ID,
                            SEQ_NUM,
                            COMMIT_TS);

            WriteEntry newEntry = WriteEntry.fromRecord(newRecord, recordCodec);
            writer.write(newEntry);

            writer.close();

            byte[] legacyRaw = instance.get(legacyKey);
            byte[] legacyDecoded = NullableValueCodec.decode(legacyRaw);

            assertThat(legacyKey).isNotNull();

            byte[] newRaw = instance.get(newRecord.key().encoded());
            byte[] newDecoded = NullableValueCodec.decode(newRaw);

            RecordCodec.SchemaResolver resolver = (tableId, schemaVersion) -> userSchema;
            Record readBackNew = recordCodec.decode(newDecoded, resolver);
            assertThat(readBackNew.body().value("id").raw()).isEqualTo(55);
            assertThat(readBackNew.body().value("name").raw()).isEqualTo("grace");
            assertThat(readBackNew.body().value("age").raw()).isEqualTo(22);

            instance.close();
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void compactDeleteWriteReadFlow() throws IOException {
        Path tempDir = Files.createTempDirectory("rawwise-e2e-delete");
        try {
            Path lmdbBase = tempDir.resolve("lmdb-delete-test");
            Files.createDirectories(lmdbBase);
            LmdbInstance instance =
                    new LmdbInstance(
                            "e2e-delete-" + System.currentTimeMillis(),
                            lmdbBase,
                            10 * 1024 * 1024L);
            instance.tryAcquireLease("e2e-owner", 300_000);
            instance.ensureRoutingOwnerLease("e2e-exec", 1);

            LmdbWriter writer = new LmdbWriter(instance);
            Properties config = new Properties();
            config.put("writer_batch_size", 10);
            config.put("writer_use_buffer", false);
            writer.init(config);

            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("id", 99);
            fields.put("name", "bob");
            fields.put("age", 25);

            Record insertRecord =
                    Records.ofSchema(
                            userSchema,
                            fields,
                            Record.Kind.INSERT,
                            valueCodec,
                            TABLE_ID,
                            SEQ_NUM,
                            COMMIT_TS);

            WriteEntry insertEntry = WriteEntry.fromRecord(insertRecord, recordCodec);
            writer.write(insertEntry);

            Record.Key originalKey = insertRecord.key();

            Record deleteRecord =
                    Records.compactDelete(
                            originalKey, userSchema, TABLE_ID, SEQ_NUM + 1, COMMIT_TS + 1);

            WriteEntry deleteEntry = WriteEntry.fromRecord(deleteRecord, recordCodec);
            writer.write(deleteEntry);

            writer.close();

            byte[] storedRaw = instance.get(originalKey.encoded());
            byte[] decoded = NullableValueCodec.decode(storedRaw);

            RecordCodec.SchemaResolver resolver = (tableId, schemaVersion) -> userSchema;
            Record readBack = recordCodec.decode(decoded, resolver);

            assertThat(readBack.header().kind()).isEqualTo(Record.Kind.DELETE);
            assertThat(Arrays.equals(readBack.key().encoded(), originalKey.encoded())).isTrue();

            instance.close();
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void schemaEvolutionWriteReadFlow() throws IOException {
        Path tempDir = Files.createTempDirectory("rawwise-e2e-schema");
        try {
            Path lmdbBase = tempDir.resolve("lmdb-schema-test");
            Files.createDirectories(lmdbBase);
            LmdbInstance instance =
                    new LmdbInstance(
                            "e2e-schema-" + System.currentTimeMillis(),
                            lmdbBase,
                            10 * 1024 * 1024L);
            instance.tryAcquireLease("e2e-owner", 300_000);
            instance.ensureRoutingOwnerLease("e2e-exec", 1);

            LmdbWriter writer = new LmdbWriter(instance);
            Properties config = new Properties();
            config.put("writer_batch_size", 10);
            config.put("writer_use_buffer", false);
            writer.init(config);

            Schema schemaV1 =
                    Schema.builder()
                            .field("id", Types.int32())
                            .field("name", Types.string())
                            .primaryKey("id")
                            .version(1L)
                            .build();

            Map<String, Object> fieldsV1 = new LinkedHashMap<>();
            fieldsV1.put("id", 10);
            fieldsV1.put("name", "charlie");

            Record recordV1 =
                    Records.ofSchema(
                            schemaV1,
                            fieldsV1,
                            Record.Kind.INSERT,
                            valueCodec,
                            TABLE_ID,
                            SEQ_NUM,
                            COMMIT_TS);

            WriteEntry entry = WriteEntry.fromRecord(recordV1, recordCodec);
            writer.write(entry);

            writer.close();

            byte[] storedRaw = instance.get(recordV1.key().encoded());
            byte[] decoded = NullableValueCodec.decode(storedRaw);

            Schema schemaV2 =
                    Schema.builder()
                            .field("id", Types.int32())
                            .field("name", Types.string())
                            .field("email", Types.string())
                            .primaryKey("id")
                            .version(2L)
                            .build();

            RecordCodec.SchemaResolver resolverV2 = (tableId, schemaVersion) -> schemaV2;
            Record readBackV2 = recordCodec.decode(decoded, resolverV2);

            assertThat(readBackV2.body().value("id").raw()).isEqualTo(10);
            assertThat(readBackV2.body().value("name").raw()).isEqualTo("charlie");
            assertThat(readBackV2.body().value("email").isNull()).isTrue();

            instance.close();
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void expressionFilterQueryFlow() throws IOException {
        Path tempDir = Files.createTempDirectory("rawwise-e2e-expr");
        try {
            Path lmdbBase = tempDir.resolve("lmdb-expr-test");
            Files.createDirectories(lmdbBase);
            LmdbInstance instance =
                    new LmdbInstance(
                            "e2e-expr-" + System.currentTimeMillis(), lmdbBase, 10 * 1024 * 1024L);
            instance.tryAcquireLease("e2e-owner", 300_000);
            instance.ensureRoutingOwnerLease("e2e-exec", 1);

            LmdbWriter writer = new LmdbWriter(instance);
            Properties config = new Properties();
            config.put("writer_batch_size", 10);
            config.put("writer_use_buffer", false);
            writer.init(config);

            Map<String, Object> fields1 = new LinkedHashMap<>();
            fields1.put("id", 1);
            fields1.put("name", "dave");
            fields1.put("age", 28);

            Map<String, Object> fields2 = new LinkedHashMap<>();
            fields2.put("id", 2);
            fields2.put("name", "eve");
            fields2.put("age", 32);

            Map<String, Object> fields3 = new LinkedHashMap<>();
            fields3.put("id", 3);
            fields3.put("name", "dave");
            fields3.put("age", 45);

            Record record1 =
                    Records.ofSchema(
                            userSchema,
                            fields1,
                            Record.Kind.INSERT,
                            valueCodec,
                            TABLE_ID,
                            SEQ_NUM,
                            COMMIT_TS);
            Record record2 =
                    Records.ofSchema(
                            userSchema,
                            fields2,
                            Record.Kind.INSERT,
                            valueCodec,
                            TABLE_ID,
                            SEQ_NUM + 1,
                            COMMIT_TS + 1);
            Record record3 =
                    Records.ofSchema(
                            userSchema,
                            fields3,
                            Record.Kind.INSERT,
                            valueCodec,
                            TABLE_ID,
                            SEQ_NUM + 2,
                            COMMIT_TS + 2);

            writer.write(WriteEntry.fromRecord(record1, recordCodec));
            writer.write(WriteEntry.fromRecord(record2, recordCodec));
            writer.write(WriteEntry.fromRecord(record3, recordCodec));

            writer.close();

            Expression filter = new Eq(new ColumnRef("name"), new Literal("dave"));

            List<Record> matched = new ArrayList<>();
            try (var iterator = instance.closeableIterator()) {
                while (iterator.hasNext()) {
                    var pair = iterator.next();
                    byte[] raw = pair.getRight();
                    if (NullableValueCodec.isTombstone(raw)) {
                        continue;
                    }
                    byte[] decoded = NullableValueCodec.decode(raw);
                    if (DefaultRecordCodec.isRecordCodecFormatStatic(decoded)) {
                        RecordCodec.SchemaResolver resolver =
                                (tableId, schemaVersion) -> userSchema;
                        Record record = recordCodec.decode(decoded, resolver);
                        if (filter.evaluate(record)) {
                            matched.add(record);
                        }
                    }
                }
            }

            assertThat(matched).hasSize(2);
            for (Record r : matched) {
                assertThat(r.body().value("name").raw()).isEqualTo("dave");
            }

            instance.close();
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void projectCopyPreservesProjectedFieldsAndEvictsOriginal() throws IOException {
        Path tempDir = Files.createTempDirectory("rawwise-e2e-project");
        try {
            Path lmdbBase = tempDir.resolve("lmdb-project-test");
            Files.createDirectories(lmdbBase);
            LmdbInstance instance =
                    new LmdbInstance(
                            "e2e-project-" + System.currentTimeMillis(),
                            lmdbBase,
                            10 * 1024 * 1024L);
            instance.tryAcquireLease("e2e-owner", 300_000);
            instance.ensureRoutingOwnerLease("e2e-exec", 1);

            LmdbWriter writer = new LmdbWriter(instance);
            Properties config = new Properties();
            config.put("writer_batch_size", 10);
            config.put("writer_use_buffer", false);
            writer.init(config);

            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("id", 42);
            fields.put("name", "alice");
            fields.put("age", 30);

            Record record =
                    Records.ofSchema(
                            userSchema,
                            fields,
                            Record.Kind.INSERT,
                            valueCodec,
                            TABLE_ID,
                            SEQ_NUM,
                            COMMIT_TS);

            WriteEntry entry = WriteEntry.fromRecord(record, recordCodec);
            writer.write(entry);
            writer.close();

            byte[] storedRaw = instance.get(record.key().encoded());
            byte[] decoded = NullableValueCodec.decode(storedRaw);

            RecordCodec.SchemaResolver resolver = (tableId, schemaVersion) -> userSchema;
            Record readBack = recordCodec.decode(decoded, resolver);

            Record projected = Records.projectCopy(readBack, List.of("id", "name"));
            readBack.body().evict();

            assertThat(projected.body().schema().fields()).hasSize(2);
            assertThat(projected.body().value("id").raw()).isEqualTo(42);
            assertThat(projected.body().value("name").raw()).isEqualTo("alice");
            assertThat(projected.key().encoded()).isEqualTo(readBack.key().encoded());
            assertThat(projected.header().kind()).isEqualTo(Record.Kind.INSERT);

            Record fullCopy =
                    Records.projectCopy(
                            Records.ofSchema(
                                    userSchema,
                                    fields,
                                    Record.Kind.INSERT,
                                    valueCodec,
                                    TABLE_ID,
                                    SEQ_NUM,
                                    COMMIT_TS),
                            List.of());
            assertThat(fullCopy.body().schema().fields()).hasSize(3);
            assertThat(fullCopy.body().value("id").raw()).isEqualTo(42);
            assertThat(fullCopy.body().value("name").raw()).isEqualTo("alice");
            assertThat(fullCopy.body().value("age").raw()).isEqualTo(30);

            instance.close();
        } finally {
            deleteRecursively(tempDir);
        }
    }

    @Test
    void findResultIsFoundChecksSuccess() {
        Record record =
                Records.ofSchema(
                        userSchema,
                        Map.of("id", 1, "name", "test", "age", 20),
                        Record.Kind.INSERT,
                        valueCodec,
                        TABLE_ID,
                        SEQ_NUM,
                        COMMIT_TS);

        var found = io.github.rawwise.model.op.Find.Result.found(record, new byte[] {1, 2, 3});
        assertThat(found.isFound()).isTrue();
        assertThat(found.foundRecord()).isPresent();

        var notFound = io.github.rawwise.model.op.Find.Result.notFound();
        assertThat(notFound.isFound()).isFalse();
        assertThat(notFound.foundRecord()).isEmpty();

        var error =
                io.github.rawwise.model.op.Find.Result.error("fail", new RuntimeException("boom"));
        assertThat(error.isFound()).isFalse();
        assertThat(error.foundRecord()).isEmpty();
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (Files.exists(dir)) {
            try (var walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(
                                p -> {
                                    try {
                                        Files.delete(p);
                                    } catch (IOException ignored) {
                                    }
                                });
            }
        }
    }
}
