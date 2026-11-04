package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.compaction.merger.ImmutableAwareMergeFunction;
import io.github.rawwise.engine.compaction.merger.VersionedValue;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.codec.DefaultValueCodec;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.table.key.DefaultKeyCodec;
import io.github.rawwise.engine.storage.writer.LmdbWriter;
import io.github.rawwise.model.record.impl.DefaultRecordCodec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;

class AiNativeImmutableE2ETest {

    private static final long TABLE_ID = 200L;

    private DefaultValueCodec valueCodec;
    private DefaultRecordCodec recordCodec;
    private Schema testSchema;

    @BeforeEach
    void setUp() {
        valueCodec = new DefaultValueCodec(new DefaultKeyCodec());
        recordCodec = new DefaultRecordCodec(valueCodec);
        testSchema =
                Schema.builder()
                        .field("id", Types.int32(false))
                        .field("name", Types.string())
                        .primaryKey("id")
                        .build();
    }

    @Nested
    @DisplayName("ImmutabilitySource correct")
    class ImmutabilitySourcePartialOrder {

        @Test
        @DisplayName("MUTABLE can improvementto DEFAULT, PROMOTED and FEEDBACK")
        void mutableCanPromoteToDefaultPromotedAndFeedback() {
            assertThat(ImmutabilitySource.MUTABLE.canPromoteTo(ImmutabilitySource.DEFAULT))
                    .isTrue();
            assertThat(ImmutabilitySource.MUTABLE.canPromoteTo(ImmutabilitySource.PROMOTED))
                    .isTrue();
            assertThat(ImmutabilitySource.MUTABLE.canPromoteTo(ImmutabilitySource.FEEDBACK))
                    .isTrue();
        }

        @Test
        @DisplayName("MUTABLE cannot promote to CREATED")
        void mutableCannotPromoteToCreated() {
            assertThat(ImmutabilitySource.MUTABLE.canPromoteTo(ImmutabilitySource.CREATED))
                    .isFalse();
        }

        @Test
        @DisplayName("PROMOTED can improvementto FEEDBACK cannotto CREATED")
        void promotedCanPromoteToFeedbackNotCreated() {
            assertThat(ImmutabilitySource.PROMOTED.canPromoteTo(ImmutabilitySource.FEEDBACK))
                    .isTrue();
            assertThat(ImmutabilitySource.PROMOTED.canPromoteTo(ImmutabilitySource.CREATED))
                    .isFalse();
        }

        @Test
        @DisplayName("FEEDBACK cannot degrade to PROMOTED DEFAULT or MUTABLE")
        void feedbackCannotDowngrade() {
            assertThat(ImmutabilitySource.FEEDBACK.canPromoteTo(ImmutabilitySource.PROMOTED))
                    .isFalse();
            assertThat(ImmutabilitySource.FEEDBACK.canPromoteTo(ImmutabilitySource.DEFAULT))
                    .isFalse();
            assertThat(ImmutabilitySource.FEEDBACK.canPromoteTo(ImmutabilitySource.MUTABLE))
                    .isFalse();
        }

        @Test
        @DisplayName("CREATED canetc CREATED")
        void createdOnlyEqualsCreated() {
            assertThat(ImmutabilitySource.CREATED.canPromoteTo(ImmutabilitySource.CREATED))
                    .isTrue();
            assertThat(ImmutabilitySource.CREATED.canPromoteTo(ImmutabilitySource.PROMOTED))
                    .isFalse();
            assertThat(ImmutabilitySource.CREATED.canPromoteTo(ImmutabilitySource.FEEDBACK))
                    .isFalse();
            assertThat(ImmutabilitySource.CREATED.canPromoteTo(ImmutabilitySource.DEFAULT))
                    .isFalse();
            assertThat(ImmutabilitySource.CREATED.canPromoteTo(ImmutabilitySource.MUTABLE))
                    .isFalse();
        }

        @Test
        @DisplayName("isImmutable: has MUTABLE is not immutable")
        void isImmutableOnlyMutableIsNot() {
            assertThat(ImmutabilitySource.DEFAULT.isImmutable()).isTrue();
            assertThat(ImmutabilitySource.PROMOTED.isImmutable()).isTrue();
            assertThat(ImmutabilitySource.FEEDBACK.isImmutable()).isTrue();
            assertThat(ImmutabilitySource.CREATED.isImmutable()).isTrue();
            assertThat(ImmutabilitySource.MUTABLE.isImmutable()).isFalse();
        }
    }

    @Nested
    @DisplayName("ImmutableAwareMergeFunction Compaction merge")
    class ImmutableAwareMerge {

        private final ImmutableAwareMergeFunction fn = new ImmutableAwareMergeFunction();

        @Test
        @DisplayName("immutable CREATED datain Compaction in , updateof mutable version")
        void immutableCreatedSurvivesCompaction() {
            VersionedValue immutable =
                    new VersionedValue(
                            "immutable-val".getBytes(),
                            1L,
                            100L,
                            false,
                            true,
                            ImmutabilitySource.CREATED);
            VersionedValue mutable =
                    new VersionedValue(
                            "mutable-val".getBytes(),
                            2L,
                            200L,
                            false,
                            false,
                            ImmutabilitySource.MUTABLE);

            byte[] result = fn.merge("key".getBytes(), List.of(immutable, mutable));

            assertThat(result).isEqualTo("immutable-val".getBytes());
        }

        @Test
        @DisplayName("immutable tombstone delete")
        void immutableTombstoneCausesPermanentDelete() {
            VersionedValue immutable =
                    new VersionedValue(
                            "immutable-val".getBytes(),
                            1L,
                            100L,
                            false,
                            true,
                            ImmutabilitySource.CREATED);
            VersionedValue immutableTombstone =
                    new VersionedValue(
                            new byte[0], 2L, 200L, true, true, ImmutabilitySource.PROMOTED);

            byte[] result = fn.merge("key".getBytes(), List.of(immutable, immutableTombstone));

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("no immutable , tombstone befilter, ofnon- tombstone version")
        void mutableTombstoneFiltersOutButOldVersionRemains() {
            VersionedValue mutable =
                    new VersionedValue(
                            "mutable-val".getBytes(),
                            1L,
                            100L,
                            false,
                            false,
                            ImmutabilitySource.MUTABLE);
            VersionedValue tombstone =
                    new VersionedValue(
                            new byte[0], 2L, 200L, true, false, ImmutabilitySource.MUTABLE);

            byte[] result = fn.merge("key".getBytes(), List.of(mutable, tombstone));

            assertThat(result).isEqualTo("mutable-val".getBytes());
        }

        @Test
        @DisplayName("no immutable , has tombstone null(recordbedelete)")
        void onlyTombstoneReturnsNull() {
            VersionedValue tombstone =
                    new VersionedValue(
                            new byte[0], 2L, 200L, true, false, ImmutabilitySource.MUTABLE);

            byte[] result = fn.merge("key".getBytes(), List.of(tombstone));

            assertThat(result).isNull();
        }

        @Test
        @DisplayName("no immutable , seqNum ofnon- tombstone")
        void mutableLatestWinsWithoutImmutable() {
            VersionedValue v1 =
                    new VersionedValue(
                            "val-1".getBytes(), 1L, 100L, false, false, ImmutabilitySource.MUTABLE);
            VersionedValue v2 =
                    new VersionedValue(
                            "val-2".getBytes(), 2L, 200L, false, false, ImmutabilitySource.MUTABLE);

            byte[] result = fn.merge("key".getBytes(), List.of(v1, v2));

            assertThat(result).isEqualTo("val-2".getBytes());
        }

        @Test
        @DisplayName("multiitem immutable version seqNum ofnon- tombstone")
        void multipleImmutableTakesLatestNonTombstone() {
            VersionedValue v1 =
                    new VersionedValue(
                            "immutable-1".getBytes(),
                            1L,
                            100L,
                            false,
                            true,
                            ImmutabilitySource.PROMOTED);
            VersionedValue v2 =
                    new VersionedValue(
                            "immutable-2".getBytes(),
                            2L,
                            200L,
                            false,
                            true,
                            ImmutabilitySource.FEEDBACK);

            byte[] result = fn.merge("key".getBytes(), List.of(v1, v2));

            assertThat(result).isEqualTo("immutable-2".getBytes());
        }
    }

    @Nested
    @DisplayName("DefaultRecordCodec FLAG_HAS_IMMUTABLE decode")
    class CodecImmutableFlag {

        @Test
        @DisplayName("immutable=true, source=CREATED decode")
        void immutableCreatedRoundTrip() {
            Record.Key key = Records.keyOf("test-key".getBytes());
            Record.Body body = new DefaultBody(testSchema, new Object[] {1, "alice"});
            Record record =
                    Records.create(
                            key,
                            body,
                            TABLE_ID,
                            1L,
                            1000L,
                            Record.Kind.INSERT,
                            true,
                            true,
                            ImmutabilitySource.CREATED);

            byte[] encoded = recordCodec.encode(record);
            RecordCodec.SchemaResolver resolver = (tableId, schemaVersion) -> testSchema;
            Record decoded = recordCodec.decode(encoded, resolver);

            assertThat(decoded.header().immutable()).isTrue();
            assertThat(decoded.header().immutableSource()).isEqualTo(ImmutabilitySource.CREATED);
            assertThat(decoded.body().value("id").raw()).isEqualTo(1);
            assertThat(decoded.body().value("name").raw()).isEqualTo("alice");
        }

        @Test
        @DisplayName("immutable=false, source=MUTABLE decode")
        void mutableDefaultRoundTrip() {
            Record.Key key = Records.keyOf("test-key".getBytes());
            Record.Body body = new DefaultBody(testSchema, new Object[] {2, "bob"});
            Record record =
                    Records.create(
                            key,
                            body,
                            TABLE_ID,
                            1L,
                            1000L,
                            Record.Kind.INSERT,
                            true,
                            false,
                            ImmutabilitySource.MUTABLE);

            byte[] encoded = recordCodec.encode(record);
            RecordCodec.SchemaResolver resolver = (tableId, schemaVersion) -> testSchema;
            Record decoded = recordCodec.decode(encoded, resolver);

            assertThat(decoded.header().immutable()).isFalse();
            assertThat(decoded.header().immutableSource()).isEqualTo(ImmutabilitySource.MUTABLE);
        }

        @Test
        @DisplayName("NullableValueCodec package after extractImmutableStatic correctly extracts")
        void extractImmutableAfterNullableValueCodecWrap() {
            Record.Key key = Records.keyOf("wrapped-key".getBytes());
            Record.Body body = new DefaultBody(testSchema, new Object[] {3, "charlie"});
            Record record =
                    Records.create(
                            key,
                            body,
                            TABLE_ID,
                            1L,
                            1000L,
                            Record.Kind.INSERT,
                            true,
                            true,
                            ImmutabilitySource.FEEDBACK);

            byte[] encoded = recordCodec.encode(record);
            byte[] wrapped = NullableValueCodec.encode(encoded);

            assertThat(NullableValueCodec.isTombstone(wrapped)).isFalse();
            byte[] decoded = NullableValueCodec.decode(wrapped);
            assertThat(DefaultRecordCodec.extractImmutableStatic(decoded)).isTrue();
            assertThat(DefaultRecordCodec.extractImmutabilitySourceStatic(decoded))
                    .isEqualTo(ImmutabilitySource.FEEDBACK);
        }

        @Test
        @DisplayName("LMDB write+read immutable recordfull round-trip")
        void lmdbWriteReadImmutableRoundTrip() throws IOException {
            Path tempDir = Files.createTempDirectory("rawwise-ai-native-lmdb");
            try {
                Path lmdbBase = tempDir.resolve("lmdb-immutable-test");
                Files.createDirectories(lmdbBase);
                LmdbInstance instance =
                        new LmdbInstance(
                                "e2e-immutable-" + System.currentTimeMillis(),
                                lmdbBase,
                                10 * 1024 * 1024L);
                instance.tryAcquireLease("e2e-owner", 300_000);
                instance.ensureRoutingOwnerLease("e2e-exec", 1);

                LmdbWriter writer = new LmdbWriter(instance);
                Properties config = new Properties();
                config.put("writer_batch_size", 10);
                config.put("writer_use_buffer", false);
                writer.init(config);

                Record.Key key = Records.keyOf("lmdb-imm-key".getBytes());
                Record.Body body = new DefaultBody(testSchema, new Object[] {42, "dave"});
                Record record =
                        Records.create(
                                key,
                                body,
                                TABLE_ID,
                                1L,
                                1000L,
                                Record.Kind.INSERT,
                                true,
                                true,
                                ImmutabilitySource.CREATED);

                byte[] encoded = recordCodec.encode(record);
                byte[] wrapped = NullableValueCodec.encode(encoded);
                writer.write(new WriteEntry(key.encoded(), wrapped, true));
                writer.close();

                byte[] storedRaw = instance.get(key.encoded());
                assertThat(NullableValueCodec.isTombstone(storedRaw)).isFalse();
                byte[] decoded = NullableValueCodec.decode(storedRaw);

                RecordCodec.SchemaResolver resolver = (tableId, schemaVersion) -> testSchema;
                Record readBack = recordCodec.decode(decoded, resolver);

                assertThat(readBack.header().immutable()).isTrue();
                assertThat(readBack.header().immutableSource())
                        .isEqualTo(ImmutabilitySource.CREATED);
                assertThat(readBack.body().value("id").raw()).isEqualTo(42);
                assertThat(readBack.body().value("name").raw()).isEqualTo("dave");

                instance.close();
            } finally {
                deleteRecursively(tempDir);
            }
        }
    }

    @Nested
    @DisplayName("VersionedValue")
    class VersionedValueFactory {

        @Test
        @DisplayName("mutable() create MUTABLE immutable")
        void mutableFactoryCreatesMutable() {
            VersionedValue vv = VersionedValue.mutable("val".getBytes(), 1L, 100L, false);

            assertThat(vv.immutable()).isFalse();
            assertThat(vv.immutableSource()).isEqualTo(ImmutabilitySource.MUTABLE);
            assertThat(vv.sequenceNumber()).isEqualTo(1L);
            assertThat(vv.isTombstone()).isFalse();
        }

        @Test
        @DisplayName("mutable() create tombstone")
        void mutableFactoryCreatesTombstone() {
            VersionedValue vv = VersionedValue.mutable("val".getBytes(), 2L, 200L, true);

            assertThat(vv.isTombstone()).isTrue();
            assertThat(vv.immutable()).isFalse();
        }
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
