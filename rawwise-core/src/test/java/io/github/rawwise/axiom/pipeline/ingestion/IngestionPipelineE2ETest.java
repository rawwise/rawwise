package io.github.rawwise.axiom.pipeline.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.codec.DefaultValueCodec;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.storage.writer.LmdbWriter;
import io.github.rawwise.model.record.DefaultBody;
import io.github.rawwise.model.record.ImmutabilitySource;
import io.github.rawwise.model.record.Record;
import io.github.rawwise.model.record.RecordCodec;
import io.github.rawwise.model.record.Records;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.Types;
import io.github.rawwise.model.record.WriteEntry;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("IngestionPipeline E2E - immutable lifecycletest")
class IngestionPipelineE2ETest {

    private static final long TABLE_ID = 42L;
    private static final long SCHEMA_VERSION = 1L;

    private Schema testSchema;
    private DefaultValueCodec valueCodec;
    private DefaultRecordCodec recordCodec;
    private RecordCodec.SchemaResolver schemaResolver;

    @TempDir Path tempDir;

    @BeforeEach
    void setUp() {
        testSchema =
                Schema.builder()
                        .field("id", Types.int32(false))
                        .field("name", Types.string())
                        .primaryKey("id")
                        .build();
        valueCodec = new DefaultValueCodec(new DefaultKeyCodec());
        recordCodec = new DefaultRecordCodec(valueCodec);
        schemaResolver = (tableId, schemaVersion) -> testSchema;
    }

    private LmdbInstance createLmdbInstance(String nameSuffix) throws IOException {
        Path lmdbBase = tempDir.resolve("lmdb-" + nameSuffix + "-" + System.currentTimeMillis());
        Files.createDirectories(lmdbBase);
        LmdbInstance instance = new LmdbInstance(nameSuffix, lmdbBase, 10 * 1024 * 1024L);
        instance.tryAcquireLease("e2e-owner", 300_000);
        instance.ensureRoutingOwnerLease("e2e-exec", 1);
        return instance;
    }

    private void writeRecord(LmdbInstance instance, Record record) throws IOException {
        LmdbWriter writer = new LmdbWriter(instance);
        Properties config = new Properties();
        config.put("writer_batch_size", 10);
        config.put("writer_use_buffer", false);
        writer.init(config);

        byte[] encoded = recordCodec.encode(record);
        byte[] wrapped = NullableValueCodec.encode(encoded);
        writer.write(new WriteEntry(record.key().encoded(), wrapped, true));
        writer.close();
    }

    private Record readRecord(LmdbInstance instance, Record.Key key) throws IOException {
        byte[] storedRaw = instance.get(key.encoded());
        if (storedRaw == null) return null;
        if (NullableValueCodec.isTombstone(storedRaw)) return null;
        byte[] decoded = NullableValueCodec.decode(storedRaw);
        if (decoded == null) return null;
        return recordCodec.decode(decoded, schemaResolver);
    }

    @Nested
    @DisplayName("S1-MutableWrite - can recordwrite and read back")
    class MutableWrite {

        @Test
        @DisplayName("writecan record -> read-back verification immutability=false, source=DEFAULT")
        void mutableRecordWriteThenRead() throws Exception {
            LmdbInstance instance = createLmdbInstance("mutable-write");
            try {
                Record.Key key = Records.keyOf("mutable-key".getBytes());
                Record.Body body = new DefaultBody(testSchema, new Object[] {1, "alice"});
                Record record =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                SCHEMA_VERSION,
                                1000L,
                                Record.Kind.INSERT,
                                true,
                                false,
                                ImmutabilitySource.DEFAULT);

                writeRecord(instance, record);

                Record readBack = readRecord(instance, key);
                assertThat(readBack).isNotNull();
                assertThat(readBack.header().immutable()).isFalse();
                assertThat(readBack.header().immutableSource())
                        .isEqualTo(ImmutabilitySource.DEFAULT);
                assertThat(readBack.body().value("id").raw()).isEqualTo(1);
                assertThat(readBack.body().value("name").raw()).isEqualTo("alice");
            } finally {
                instance.close();
            }
        }
    }

    @Nested
    @DisplayName("S2-ImmutableWrite - immutable record write and read back")
    class ImmutableWrite {

        @Test
        @DisplayName("write CREATED immutablerecord -> read-back verification immutability=true")
        void immutableCreatedWriteThenRead() throws Exception {
            LmdbInstance instance = createLmdbInstance("immutable-created");
            try {
                Record.Key key = Records.keyOf("imm-key".getBytes());
                Record.Body body = new DefaultBody(testSchema, new Object[] {2, "bob"});
                Record record =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                SCHEMA_VERSION,
                                2000L,
                                Record.Kind.INSERT,
                                true,
                                true,
                                ImmutabilitySource.CREATED);

                writeRecord(instance, record);

                Record readBack = readRecord(instance, key);
                assertThat(readBack).isNotNull();
                assertThat(readBack.header().immutable()).isTrue();
                assertThat(readBack.header().immutableSource())
                        .isEqualTo(ImmutabilitySource.CREATED);
            } finally {
                instance.close();
            }
        }

        @Test
        @DisplayName("write FEEDBACK immutablerecord -> verify source=FEEDBACK")
        void immutableFeedbackWriteThenRead() throws Exception {
            LmdbInstance instance = createLmdbInstance("immutable-feedback");
            try {
                Record.Key key = Records.keyOf("fb-key".getBytes());
                Record.Body body = new DefaultBody(testSchema, new Object[] {3, "charlie"});
                Record record =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                SCHEMA_VERSION,
                                3000L,
                                Record.Kind.INSERT,
                                true,
                                true,
                                ImmutabilitySource.FEEDBACK);

                writeRecord(instance, record);

                Record readBack = readRecord(instance, key);
                assertThat(readBack).isNotNull();
                assertThat(readBack.header().immutable()).isTrue();
                assertThat(readBack.header().immutableSource())
                        .isEqualTo(ImmutabilitySource.FEEDBACK);
            } finally {
                instance.close();
            }
        }
    }

    @Nested
    @DisplayName("S3-ImmutabilityPromotion - immutability improvement")
    class ImmutabilityPromotion {

        @Test
        @DisplayName(
                "DEFAULT -> PROMOTED improvement: writecan record -> writeis PROMOTED -> read-back verification")
        void defaultToPromoted() throws Exception {
            LmdbInstance instance = createLmdbInstance("promote-default");
            try {
                Record.Key key = Records.keyOf("promote-key".getBytes());
                Record.Body body = new DefaultBody(testSchema, new Object[] {4, "dave"});
                Record mutable =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                SCHEMA_VERSION,
                                1000L,
                                Record.Kind.INSERT,
                                true,
                                false,
                                ImmutabilitySource.DEFAULT);

                writeRecord(instance, mutable);

                Record promoted =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                SCHEMA_VERSION,
                                2000L,
                                Record.Kind.INSERT,
                                true,
                                true,
                                ImmutabilitySource.PROMOTED);

                writeRecord(instance, promoted);

                Record readBack = readRecord(instance, key);
                assertThat(readBack).isNotNull();
                assertThat(readBack.header().immutable()).isTrue();
                assertThat(readBack.header().immutableSource())
                        .isEqualTo(ImmutabilitySource.PROMOTED);
            } finally {
                instance.close();
            }
        }

        @Test
        @DisplayName("DEFAULT -> FEEDBACK improvement: can record F_self improvementisimmutable")
        void defaultToFeedback() throws Exception {
            LmdbInstance instance = createLmdbInstance("promote-feedback");
            try {
                Record.Key key = Records.keyOf("feedback-key".getBytes());
                Record.Body body = new DefaultBody(testSchema, new Object[] {5, "eve"});
                Record mutable =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                SCHEMA_VERSION,
                                1000L,
                                Record.Kind.INSERT,
                                true,
                                false,
                                ImmutabilitySource.DEFAULT);

                writeRecord(instance, mutable);

                Record feedbackPromoted =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                SCHEMA_VERSION,
                                3000L,
                                Record.Kind.INSERT,
                                true,
                                true,
                                ImmutabilitySource.FEEDBACK);

                writeRecord(instance, feedbackPromoted);

                Record readBack = readRecord(instance, key);
                assertThat(readBack).isNotNull();
                assertThat(readBack.header().immutable()).isTrue();
                assertThat(readBack.header().immutableSource())
                        .isEqualTo(ImmutabilitySource.FEEDBACK);
            } finally {
                instance.close();
            }
        }
    }

    @Nested
    @DisplayName("S4-ImplicitViewIntegration - implicit view")
    class ImplicitViewIntegration {

        @Test
        @DisplayName("writerecordaftervia LMDB read -> verifydata")
        void writeThenReadDataIntegrity() throws Exception {
            LmdbInstance instance = createLmdbInstance("view-after-write");
            try {
                Record.Key key = Records.keyOf("view-key".getBytes());
                Record.Body body = new DefaultBody(testSchema, new Object[] {7, "grace"});
                Record record =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                SCHEMA_VERSION,
                                1000L,
                                Record.Kind.INSERT,
                                true,
                                false,
                                ImmutabilitySource.DEFAULT);

                writeRecord(instance, record);

                Record readBack = readRecord(instance, key);
                assertThat(readBack).isNotNull();
                assertThat(readBack.key().encoded()).isEqualTo(key.encoded());
                assertThat(readBack.body().value("id").raw()).isEqualTo(7);
                assertThat(readBack.body().value("name").raw()).isEqualTo("grace");
            } finally {
                instance.close();
            }
        }

        @Test
        @DisplayName("batch writemultiitemrecord -> cancorrectread")
        void batchWriteMultipleRecords() throws Exception {
            LmdbInstance instance = createLmdbInstance("batch-write");
            try {
                for (int i = 0; i < 10; i++) {
                    Record.Key key = Records.keyOf(("batch-" + i).getBytes());
                    Record.Body body =
                            new DefaultBody(testSchema, new Object[] {i + 100, "user-" + i});
                    Record record =
                            Records.create(
                                    key,
                                    body,
                                    TABLE_ID,
                                    SCHEMA_VERSION,
                                    1000L + i,
                                    Record.Kind.INSERT,
                                    true,
                                    false,
                                    ImmutabilitySource.DEFAULT);
                    writeRecord(instance, record);
                }

                for (int i = 0; i < 10; i++) {
                    Record.Key key = Records.keyOf(("batch-" + i).getBytes());
                    Record readBack = readRecord(instance, key);
                    assertThat(readBack).isNotNull();
                    assertThat(readBack.body().value("id").raw()).isEqualTo(i + 100);
                }
            } finally {
                instance.close();
            }
        }
    }
}
