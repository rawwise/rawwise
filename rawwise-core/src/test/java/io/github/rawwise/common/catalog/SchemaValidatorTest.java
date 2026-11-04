package io.github.rawwise.common.catalog;

import static io.github.rawwise.common.catalog.SchemaValidator.Compatibility;
import static io.github.rawwise.common.catalog.SchemaValidator.validate;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.common.exception.SchemaIncompatibleException;
import io.github.rawwise.model.record.PrimitiveType;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.StructType;
import io.github.rawwise.model.record.Types;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SchemaValidatorTest {

    // -- 1. Identical schemas should always pass --

    @Test
    void shouldAllowIdenticalSchemas() {
        Schema s1 =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .field("score", PrimitiveType.INT)
                        .build();

        Schema s2 =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .field("score", PrimitiveType.INT)
                        .build();

        assertThatCode(() -> validate(s1, s2, Compatibility.BACKWARD)).doesNotThrowAnyException();
        assertThatCode(() -> validate(s1, s2, Compatibility.FULL)).doesNotThrowAnyException();
    }

    // -- 2. Adding a field at the end is allowed under BACKWARD --

    @Test
    void shouldAllowAddingFieldAtEnd() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .field("age", PrimitiveType.INT)
                        .build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 3. Removing a field is rejected under BACKWARD --

    @Test
    void shouldRejectRemovingField() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .field("age", PrimitiveType.INT)
                        .build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("remove");
    }

    // -- 4. Field name change (renaming) is rejected --

    @Test
    void shouldRejectFieldNameChange() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("username", PrimitiveType.STRING)
                        .build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("user_name", PrimitiveType.STRING)
                        .build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("Field name mismatch");
    }

    // -- 5. Incompatible type change (e.g. String -> Int) is rejected --

    @Test
    void shouldRejectIncompatibleTypeChange() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("value", PrimitiveType.STRING)
                        .build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("value", PrimitiveType.INT)
                        .build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 6. Widening type (Int -> Long) is allowed under BACKWARD --

    @Test
    void shouldAllowWideningType() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("count", PrimitiveType.INT)
                        .build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("count", PrimitiveType.LONG)
                        .build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 7. NONE compatibility strategy always passes --

    @Test
    void shouldAllowNoneCompatibilityStrategy() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .build();

        // Proposed schema is completely different, but NONE should ignore everything.
        Schema proposed = Schema.builder().field("x", PrimitiveType.BINARY).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.NONE))
                .doesNotThrowAnyException();
    }

    // -- 8. Nullable contraction (nullable -> non-nullable) is rejected --

    @Test
    @DisplayName("Nullable -> non-nullable contraction should be rejected")
    void shouldRejectNullableContraction() {
        Schema current =
                Schema.builder()
                        .field("id", Types.int64(false))
                        .field("name", Types.string(true))
                        .build();

        Schema proposed =
                Schema.builder()
                        .field("id", Types.int64(false))
                        .field("name", Types.string(false))
                        .build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("nullable changed incompatibly");
    }

    // -- 9. New non-nullable field rejected under BACKWARD --

    @Test
    @DisplayName("New non-nullable field rejected under BACKWARD")
    void shouldRejectNewNonNullableFieldUnderBackward() {
        Schema current = Schema.builder().field("id", PrimitiveType.LONG).build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("extra", Types.int64(false))
                        .build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("must be nullable");
    }

    // -- 10. New nullable field allowed under BACKWARD --

    @Test
    @DisplayName("New nullable field allowed under BACKWARD")
    void shouldAllowNewNullableFieldUnderBackward() {
        Schema current = Schema.builder().field("id", PrimitiveType.LONG).build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("extra", PrimitiveType.INT)
                        .build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 11. Decimal precision/scale widening is allowed --

    @Test
    @DisplayName("Decimal(10,2) -> Decimal(18,4) allowed")
    void shouldAllowDecimalWidening() {
        Schema current = Schema.builder().field("price", Types.decimal(10, 2)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(18, 4)).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 12. Decimal precision narrowing is rejected --

    @Test
    @DisplayName("Decimal(18,4) -> Decimal(10,2) rejected")
    void shouldRejectDecimalNarrowing() {
        Schema current = Schema.builder().field("price", Types.decimal(18, 4)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(10, 2)).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 13. FieldId-based matching: reordering is allowed --

    @Test
    @DisplayName("FieldId-based matching allows reordering")
    void shouldAllowReorderWithFieldIds() {
        Schema current =
                Schema.builder()
                        .field(1, "id", PrimitiveType.LONG)
                        .field(2, "name", PrimitiveType.STRING)
                        .field(3, "score", PrimitiveType.INT)
                        .build();

        Schema proposed =
                Schema.builder()
                        .field(3, "score", PrimitiveType.INT)
                        .field(1, "id", PrimitiveType.LONG)
                        .field(2, "name", PrimitiveType.STRING)
                        .build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 14. FieldId-based matching: wrong name with same fieldId is rejected --

    @Test
    @DisplayName("FieldId matched but name changed — rejected")
    void shouldRejectNameChangeWithFieldId() {
        Schema current = Schema.builder().field(1, "user_id", PrimitiveType.LONG).build();

        Schema proposed = Schema.builder().field(1, "user_identifier", PrimitiveType.LONG).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("Field name mismatch");
    }

    // -- 15. DateTime not-null contraction is rejected --

    @Test
    @DisplayName("Nullable -> non-nullable contraction rejected for timestamp")
    void shouldRejectDateTimeNullabilityContraction() {
        Schema current = Schema.builder().field("ts", Types.timestamp(true)).build();

        Schema proposed = Schema.builder().field("ts", Types.timestamp(false)).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("nullable changed incompatibly");
    }

    // -- 16. Nullable relaxation (non-null -> nullable) is allowed --

    @Test
    @DisplayName("Non-null -> nullable relaxation should be allowed")
    void shouldAllowNullableRelaxation() {
        Schema current = Schema.builder().field("id", Types.int64(false)).build();

        Schema proposed = Schema.builder().field("id", Types.int64(true)).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 17. Decimal precision^ scalev is rejected --

    @Test
    @DisplayName("Decimal(10,2) -> Decimal(18,1) rejected (scale narrowed)")
    void shouldRejectDecimalScaleNarrowing() {
        Schema current = Schema.builder().field("price", Types.decimal(10, 2)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(18, 1)).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 18. Decimal scale^ precision unchanged is allowed --

    @Test
    @DisplayName(
            "Decimal(10,2) -> Decimal(12,4) allowed (precision^, scale^, integer part preserved)")
    void shouldAllowDecimalScaleWidening() {
        Schema current = Schema.builder().field("price", Types.decimal(10, 2)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(12, 4)).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 19. FORWARD: new non-nullable field is allowed (old reader ignores unknown fields) --

    @Test
    @DisplayName("FORWARD allows new non-nullable field")
    void shouldAllowNewNonNullableUnderForward() {
        Schema current = Schema.builder().field("id", PrimitiveType.LONG).build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("extra", Types.int64(false))
                        .build();

        assertThatCode(() -> validate(current, proposed, Compatibility.FORWARD))
                .doesNotThrowAnyException();
    }

    // -- 20. FORWARD: new nullable field is allowed --

    @Test
    @DisplayName("FORWARD allows new nullable field")
    void shouldAllowNewNullableUnderForward() {
        Schema current = Schema.builder().field("id", PrimitiveType.LONG).build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("extra", PrimitiveType.INT)
                        .build();

        assertThatCode(() -> validate(current, proposed, Compatibility.FORWARD))
                .doesNotThrowAnyException();
    }

    // -- 21. FieldId in proposed but not in current -> treated as new --

    @Test
    @DisplayName("FieldId only in proposed treated as new field")
    void shouldTreatUnknownFieldIdAsNewField() {
        Schema current = Schema.builder().field(1, "id", PrimitiveType.LONG).build();

        Schema proposed =
                Schema.builder()
                        .field(1, "id", PrimitiveType.LONG)
                        .field(99, "extra", PrimitiveType.INT)
                        .build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 22. Type widening + nullable contraction is rejected --

    @Test
    @DisplayName("int32(nullable) -> int64(non-nullable) rejected")
    void shouldRejectWideningWithNullableContraction() {
        Schema current = Schema.builder().field("count", PrimitiveType.INT).build();

        Schema proposed = Schema.builder().field("count", Types.int64(false)).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("nullable changed incompatibly");
    }

    // -- 23. float32 -> float64 widening is allowed --

    @Test
    @DisplayName("float32 -> float64 widening allowed")
    void shouldAllowFloat32ToFloat64Widening() {
        Schema current = Schema.builder().field("val", PrimitiveType.FLOAT).build();

        Schema proposed = Schema.builder().field("val", PrimitiveType.DOUBLE).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 24. int64 -> int32 narrowing is rejected --

    @Test
    @DisplayName("int64 -> int32 narrowing rejected")
    void shouldRejectInt64ToInt32Narrowing() {
        Schema current = Schema.builder().field("count", PrimitiveType.LONG).build();

        Schema proposed = Schema.builder().field("count", PrimitiveType.INT).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 25. float64 -> float32 narrowing is rejected --

    @Test
    @DisplayName("float64 -> float32 narrowing rejected")
    void shouldRejectFloat64ToFloat32Narrowing() {
        Schema current = Schema.builder().field("val", PrimitiveType.DOUBLE).build();

        Schema proposed = Schema.builder().field("val", PrimitiveType.FLOAT).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 26. FULL compatibility rejects field removal --

    @Test
    @DisplayName("FULL rejects field removal")
    void shouldRejectFieldRemovalUnderFull() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .build();

        Schema proposed = Schema.builder().field("id", PrimitiveType.LONG).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.FULL))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("remove");
    }

    // -- 27. FULL allows nullable field addition --

    @Test
    @DisplayName("FULL allows nullable field addition")
    void shouldAllowNullableFieldAdditionUnderFull() {
        Schema current = Schema.builder().field("id", PrimitiveType.LONG).build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("extra", PrimitiveType.INT)
                        .build();

        assertThatCode(() -> validate(current, proposed, Compatibility.FULL))
                .doesNotThrowAnyException();
    }

    // -- 28. FORWARD allows removing nullable field --

    @Test
    @DisplayName("FORWARD allows removing nullable field")
    void shouldAllowRemovingNullableFieldUnderForward() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .build();

        Schema proposed = Schema.builder().field("id", PrimitiveType.LONG).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.FORWARD))
                .doesNotThrowAnyException();
    }

    // -- 29. FORWARD rejects removing non-nullable field without default --

    @Test
    @DisplayName("FORWARD rejects removing non-nullable field without default")
    void shouldRejectRemovingNonNullableFieldUnderForward() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", Types.string(false))
                        .build();

        Schema proposed = Schema.builder().field("id", PrimitiveType.LONG).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.FORWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("must be nullable or have a default value");
    }

    // -- 30. FULL rejects field removal even for nullable fields --

    @Test
    @DisplayName("FULL rejects field removal even for nullable fields")
    void shouldRejectNullableFieldRemovalUnderFull() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .build();

        Schema proposed = Schema.builder().field("id", PrimitiveType.LONG).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.FULL))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("was removed");
    }

    // -- 31. BACKWARD rejects field removal --

    @Test
    @DisplayName("BACKWARD rejects field removal unconditionally")
    void shouldRejectFieldRemovalUnderBackward() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("name", PrimitiveType.STRING)
                        .build();

        Schema proposed = Schema.builder().field("id", PrimitiveType.LONG).build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("was removed");
    }

    // -- 32. Null current schema passes validation --

    @Test
    @DisplayName("Null current schema passes validation")
    void shouldAllowNullCurrentSchema() {
        Schema proposed = Schema.builder().field("id", PrimitiveType.LONG).build();

        assertThatCode(() -> validate(null, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 33. validateLegacy: type incompatibility rejected (no fieldId) --

    @Test
    @DisplayName("Legacy schema (no fieldId) with incompatible types rejected")
    void shouldRejectLegacyTypeIncompatibility() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("value", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("value", PrimitiveType.INT))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 34. Decimal integer part narrowing rejected --

    @Test
    @DisplayName("Decimal(10,2) -> Decimal(10,4) allowed (scale expansion)")
    void shouldAllowDecimalScaleExpansion() {
        Schema current = Schema.builder().field("price", Types.decimal(10, 2)).build();

        Schema proposed = Schema.builder().field("price", Types.decimal(10, 4)).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 35. validateLegacy: name mismatch rejected (no fieldId) --

    @Test
    @DisplayName("Legacy schema (no fieldId) name mismatch rejected")
    void shouldRejectLegacyNameMismatch() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("name", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("username", PrimitiveType.STRING))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("Field name mismatch at position");
    }

    // -- 36. validateLegacy: field addition rejected under BACKWARD (no fieldId) --

    @Test
    @DisplayName("Legacy schema (no fieldId) non-nullable field addition rejected under BACKWARD")
    void shouldRejectLegacyNonNullableAdditionUnderBackward() {
        Schema current =
                new Schema(new StructType(List.of(new StructField("id", PrimitiveType.LONG))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("extra", Types.int64(false)))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("must be nullable");
    }

    // -- 37. validateLegacy: field removal rejected under BACKWARD (no fieldId) --

    @Test
    @DisplayName("Legacy schema (no fieldId) field removal rejected under BACKWARD")
    void shouldRejectLegacyFieldRemovalUnderBackward() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("name", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(new StructType(List.of(new StructField("id", PrimitiveType.LONG))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("was removed");
    }

    // -- 38. validateLegacy: identical schemas pass (no fieldId) --

    @Test
    @DisplayName("Legacy schema (no fieldId) identical schemas pass")
    void shouldAllowLegacyIdenticalSchemas() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("name", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("name", PrimitiveType.STRING))));

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 39. validatePrimaryKeyEvolution: removing PK rejected --

    @Test
    @DisplayName("Removing primary key constraint rejected")
    void shouldRejectPrimaryKeyRemoval() {
        Schema current =
                Schema.builder()
                        .field("id", Types.int64(false))
                        .field("name", PrimitiveType.STRING)
                        .primaryKey("id")
                        .build();

        Schema proposed =
                Schema.builder()
                        .field("id", Types.int64(false))
                        .field("name", PrimitiveType.STRING)
                        .build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("Removing primary key constraint");
    }

    // -- 40. validatePrimaryKeyEvolution: changing PK fields rejected --

    @Test
    @DisplayName("Changing primary key fields rejected")
    void shouldRejectPrimaryKeyFieldChange() {
        Schema current =
                Schema.builder()
                        .field("id", Types.int64(false))
                        .field("name", Types.string(false))
                        .primaryKey("id")
                        .build();

        Schema proposed =
                Schema.builder()
                        .field("id", Types.int64(false))
                        .field("name", Types.string(false))
                        .primaryKey("name")
                        .build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("Primary key field set change");
    }

    // -- 41. FULL rejects non-nullable field addition --

    @Test
    @DisplayName("FULL rejects non-nullable field addition")
    void shouldRejectNonNullableFieldAdditionUnderFull() {
        Schema current = Schema.builder().field("id", PrimitiveType.LONG).build();

        Schema proposed =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field("extra", Types.int64(false))
                        .build();

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.FULL))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("must be nullable");
    }

    // -- 42. FORWARD allows removing non-nullable field with default value --

    @Test
    @DisplayName("FORWARD allows removing non-nullable field with default value")
    void shouldAllowRemovingNonNullableFieldWithDefaultUnderForward() {
        Schema current =
                Schema.builder()
                        .field("id", PrimitiveType.LONG)
                        .field(
                                new StructField(
                                        2,
                                        "status",
                                        Types.int64(false),
                                        "",
                                        Map.of("default", "0")))
                        .build();

        Schema proposed = Schema.builder().field("id", PrimitiveType.LONG).build();

        assertThatCode(() -> validate(current, proposed, Compatibility.FORWARD))
                .doesNotThrowAnyException();
    }

    // -- 43. Proposed null schema throws exception --

    @Test
    @DisplayName("Proposed null schema throws exception")
    void shouldRejectNullProposedSchema() {
        Schema current = Schema.builder().field("id", PrimitiveType.LONG).build();

        assertThatThrownBy(() -> validate(current, null, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("Proposed schema cannot be null");
    }

    // -- 44. Mixed fieldId schema: some fields with fieldId, some without --

    @Test
    @DisplayName("Mixed fieldId schema: unassigned fields validated by name matching")
    void shouldHandleMixedFieldIdSchema() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING),
                                        new StructField("email", PrimitiveType.STRING))));

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 45. Mixed fieldId schema: unassigned field type change rejected --

    @Test
    @DisplayName("Mixed fieldId schema: unassigned field type change rejected")
    void shouldRejectMixedFieldIdTypeChange() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("value", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("value", PrimitiveType.INT))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 46. validateLegacy: nullable contraction rejected (no fieldId) --

    @Test
    @DisplayName("Legacy schema (no fieldId) nullable contraction rejected")
    void shouldRejectLegacyNullableContraction() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("name", Types.string(true)))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("name", Types.string(false)))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("nullable changed incompatibly");
    }

    // -- 47. Mixed fieldId: unassigned field removal detected --

    @Test
    @DisplayName("Mixed fieldId schema: unassigned field removal rejected under BACKWARD")
    void shouldRejectMixedFieldIdFieldRemovalUnderBackward() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(
                                                1, "id", PrimitiveType.LONG, "", Map.of()))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("was removed");
    }

    // -- 48. Mixed fieldId: unassigned non-nullable field addition rejected under BACKWARD --

    @Test
    @DisplayName(
            "Mixed fieldId schema: unassigned non-nullable field addition rejected under BACKWARD")
    void shouldRejectMixedFieldIdNonNullableAdditionUnderBackward() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING),
                                        new StructField("email", Types.string(false)))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("must be nullable");
    }

    // -- 49. Unassigned->assigned fieldId migration: existing field not treated as new --

    @Test
    @DisplayName("Field migrating from unassigned to assigned fieldId treated as existing, not new")
    void shouldAllowFieldIdMigrationFromUnassignedToAssigned() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", Types.string(false)))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField(
                                                2, "name", Types.string(false), "", Map.of()))));

        assertThatCode(() -> validate(current, proposed, Compatibility.BACKWARD))
                .doesNotThrowAnyException();
    }

    // -- 50. Unassigned->assigned fieldId migration with type change rejected --

    @Test
    @DisplayName("Field migrating from unassigned to assigned with type change rejected")
    void shouldRejectFieldIdMigrationWithTypeChange() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField(
                                                2, "name", PrimitiveType.INT, "", Map.of()))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 51. Assigned->unassigned regression with type change rejected under FORWARD --

    @Test
    @DisplayName("Assigned->unassigned regression with type change rejected under FORWARD")
    void shouldRejectAssignedToUnassignedWithTypeChangeUnderForward() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField(
                                                2, "name", PrimitiveType.STRING, "", Map.of()))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.INT))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.FORWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("type changed incompatibly");
    }

    // -- 52. Assigned->unassigned regression with same type allowed under FORWARD --

    @Test
    @DisplayName("Assigned->unassigned regression with same type allowed under FORWARD")
    void shouldAllowAssignedToUnassignedWithSameTypeUnderForward() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField(
                                                2, "name", PrimitiveType.STRING, "", Map.of()))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING))));

        assertThatCode(() -> validate(current, proposed, Compatibility.FORWARD))
                .doesNotThrowAnyException();
    }

    // -- 53. Assigned->unassigned regression rejected under BACKWARD --

    @Test
    @DisplayName("Assigned->unassigned regression rejected under BACKWARD (fieldId removal)")
    void shouldRejectAssignedToUnassignedUnderBackward() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField(
                                                2, "name", PrimitiveType.STRING, "", Map.of()))));

        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("fieldId was removed");
    }

    // -- 54. Assigned->unassigned regression rejected under FULL --

    @Test
    @DisplayName("Assigned->unassigned regression rejected under FULL (fieldId removal)")
    void shouldRejectAssignedToUnassignedUnderFull() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField(
                                                2, "name", PrimitiveType.STRING, "", Map.of()))));
        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.STRING))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.FULL))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("fieldId was removed");
    }

    // -- 55. NONE allows assigned->unassigned fieldId regression --

    @Test
    @DisplayName("NONE allows assigned->unassigned fieldId regression")
    void shouldAllowAssignedToUnassignedUnderNone() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField(
                                                2, "name", PrimitiveType.STRING, "", Map.of()))));
        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", PrimitiveType.INT))));

        assertThatCode(() -> validate(current, proposed, Compatibility.NONE))
                .doesNotThrowAnyException();
    }

    // -- 56. Legacy schema nullable field removal allowed under FORWARD --

    @Test
    @DisplayName("Legacy schema (no fieldId) nullable field removal allowed under FORWARD")
    void shouldAllowLegacyNullableFieldRemovalUnderForward() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("name", PrimitiveType.STRING))));
        Schema proposed =
                new Schema(new StructType(List.of(new StructField("id", PrimitiveType.LONG))));

        assertThatCode(() -> validate(current, proposed, Compatibility.FORWARD))
                .doesNotThrowAnyException();
    }

    // -- 57. Legacy schema field removal rejected under FULL --

    @Test
    @DisplayName("Legacy schema (no fieldId) field removal rejected under FULL")
    void shouldRejectLegacyFieldRemovalUnderFull() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField("id", PrimitiveType.LONG),
                                        new StructField("name", PrimitiveType.STRING))));
        Schema proposed =
                new Schema(new StructType(List.of(new StructField("id", PrimitiveType.LONG))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.FULL))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("was removed");
    }

    // -- 58. Field migrating from unassigned to assigned with nullable contraction rejected --

    @Test
    @DisplayName("Field migrating from unassigned to assigned with nullable contraction rejected")
    void shouldRejectFieldIdMigrationWithNullableContraction() {
        Schema current =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField("name", Types.string(true)))));
        Schema proposed =
                new Schema(
                        new StructType(
                                List.of(
                                        new StructField(1, "id", PrimitiveType.LONG, "", Map.of()),
                                        new StructField(
                                                2, "name", Types.string(false), "", Map.of()))));

        assertThatThrownBy(() -> validate(current, proposed, Compatibility.BACKWARD))
                .isInstanceOf(SchemaIncompatibleException.class)
                .hasMessageContaining("nullable changed incompatibly");
    }
}
