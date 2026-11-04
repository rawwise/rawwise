package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.rawwise.common.catalog.SchemaValidator;
import io.github.rawwise.common.catalog.SchemaValidator.Compatibility;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class SchemaEvolutionPBTTest {

    @Nested
    @DisplayName("PBT-1: Type compatibility reflexivity")
    class PBT1 {

        static Stream<RawWiseType> types() {
            return Stream.of(
                    Types.bool(),
                    Types.int8(),
                    Types.int32(),
                    Types.int64(),
                    Types.float32(),
                    Types.float64(),
                    Types.string(),
                    Types.binary(),
                    Types.date(),
                    Types.timestamp(),
                    Types.decimal(10, 2),
                    new ArrayType(Types.int32(), 0, true),
                    new ArrayType(Types.string(), 3, true),
                    new MapType(Types.string(), Types.int64(), true, null),
                    new StructType(
                            List.of(
                                    new StructField(1, "a", Types.int32(), "", Map.of()),
                                    new StructField(2, "b", Types.string(), "", Map.of())),
                            true,
                            null));
        }

        @ParameterizedTest(name = "isTypeCompatible({0}, {0}) == true")
        @MethodSource("types")
        @DisplayName("isTypeCompatible is reflexive")
        void reflexivity(RawWiseType type) {
            assertThat(SchemaCompatibility.isTypeCompatible(type, type)).isTrue();
        }
    }

    @Nested
    @DisplayName("PBT-2: BACKWARD + FORWARD = FULL")
    class PBT2 {

        static Stream<Schema> schemas() {
            return Stream.of(
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build(),
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "email", Types.string())
                            .build(),
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string(false))
                            .build());
        }

        @ParameterizedTest(name = "BACKWARD+FORWARD=FULL for schema version {0}")
        @MethodSource("schemas")
        @DisplayName("BACKWARD + FORWARD = FULL")
        void backwardPlusForwardEqualsFull(Schema schema) {
            Schema base =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();

            boolean backward = isCompatible(base, schema, Compatibility.BACKWARD);
            boolean forward = isCompatible(schema, base, Compatibility.FORWARD);
            boolean full = isCompatible(base, schema, Compatibility.FULL);

            if (backward && forward) {
                assertThat(full).isTrue();
            }
            if (!backward || !forward) {
                assertThat(full).isFalse();
            }
        }

        private boolean isCompatible(Schema current, Schema proposed, Compatibility mode) {
            try {
                SchemaValidator.validate(current, proposed, mode);
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }

    @Nested
    @DisplayName("PBT-3: Schema evolution chain preserves fieldId")
    class PBT3 {

        @Test
        @DisplayName("fieldId preserved across add column + rename")
        void fieldIdPreservedAcrossEvolution() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();

            Schema v2 = v1.withAddedField("email", Types.string());
            assertThat(v2.field("id").get().fieldId()).isEqualTo(1);
            assertThat(v2.field("name").get().fieldId()).isEqualTo(2);

            Schema v3 = v2.withRenamedField("name", "full_name");
            assertThat(v3.field("id").get().fieldId()).isEqualTo(1);
            assertThat(v3.field("full_name").get().fieldId()).isEqualTo(2);
        }

        @Test
        @DisplayName("fieldId preserved across type widening")
        void fieldIdPreservedAcrossTypeWidening() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int32())
                            .field(2, "score", Types.float32())
                            .build();

            Schema v2 = v1.withUpdatedFieldType("id", Types.int64());
            assertThat(v2.field("id").get().fieldId()).isEqualTo(1);
            assertThat(v2.field("score").get().fieldId()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("PBT-4: fieldId uniqueness")
    class PBT4 {

        @Test
        @DisplayName("All fieldIds in a schema are unique")
        void fieldIdsAreUnique() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "email", Types.string())
                            .field(4, "score", Types.float64())
                            .build();

            long distinctCount =
                    schema.structType().fields().stream()
                            .filter(StructField::hasAssignedFieldId)
                            .map(StructField::fieldId)
                            .distinct()
                            .count();
            long totalCount =
                    schema.structType().fields().stream()
                            .filter(StructField::hasAssignedFieldId)
                            .count();

            assertThat(distinctCount).isEqualTo(totalCount);
        }

        @Test
        @DisplayName("fieldIds remain unique after rename")
        void fieldIdsRemainUniqueAfterRename() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();
            Schema v2 = v1.withRenamedField("name", "full_name");

            long distinctCount =
                    v2.structType().fields().stream()
                            .filter(StructField::hasAssignedFieldId)
                            .map(StructField::fieldId)
                            .distinct()
                            .count();
            long totalCount =
                    v2.structType().fields().stream()
                            .filter(StructField::hasAssignedFieldId)
                            .count();

            assertThat(distinctCount).isEqualTo(totalCount);
        }
    }

    @Nested
    @DisplayName("PBT-5: coerceDefault roundtrip")
    class PBT5 {

        static Stream<Object[]> defaults() {
            return Stream.of(
                    new Object[] {"true", Types.bool(), true},
                    new Object[] {"42", Types.int32(), 42},
                    new Object[] {"100", Types.int64(), 100L},
                    new Object[] {"3.14", Types.float64(), 3.14},
                    new Object[] {"hello", Types.string(), "hello"},
                    new Object[] {"10.50", Types.decimal(10, 2), new BigDecimal("10.50")});
        }

        @ParameterizedTest(name = "coerceDefault({0}, {1}) == {2}")
        @MethodSource("defaults")
        @DisplayName("coerceDefault produces correct value")
        void coerceDefaultRoundtrip(String defaultStr, RawWiseType type, Object expected) {
            Object result = SchemaCompatibility.coerceDefault(defaultStr, type);
            assertThat(result).isEqualTo(expected);
        }
    }

    @Nested
    @DisplayName("PBT-6: Schema History chain integrity")
    class PBT6 {

        @Test
        @DisplayName("Schema version is monotonically increasing")
        void versionMonotonicallyIncreasing() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();
            assertThat(v1.version()).isEqualTo(1L);

            Schema v2 = v1.withAddedField("email", Types.string());
            assertThat(v2.version()).isGreaterThan(v1.version());

            Schema v3 = v2.withRenamedField("name", "full_name");
            assertThat(v3.version()).isGreaterThan(v2.version());
        }

        @Test
        @DisplayName("Schema evolution never reduces field count for BACKWARD compatible changes")
        void backwardCompatibleNeverReducesFieldCount() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();

            Schema v2 = v1.withAddedField("email", Types.string());
            assertThatCode(() -> SchemaValidator.validate(v1, v2, Compatibility.BACKWARD))
                    .doesNotThrowAnyException();
            assertThat(v2.structType().fields().size())
                    .isGreaterThanOrEqualTo(v1.structType().fields().size());
        }
    }
}
