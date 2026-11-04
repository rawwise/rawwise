package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("Schema API")
class SchemaEvolutionTest {

    private Schema createTestSchema() {
        return Schema.builder()
                .field(1, "id", Types.int64())
                .field(2, "name", Types.string())
                .field(3, "age", Types.int32())
                .primaryKey("id")
                .build();
    }

    private Schema createSchemaWithColumnGroup() {
        return Schema.builder()
                .field(1, "id", Types.int64())
                .field(2, "first_name", Types.string())
                .field(3, "last_name", Types.string())
                .field(4, "age", Types.int32())
                .primaryKey("id")
                .build()
                .withColumnGroups(
                        List.of(
                                new ColumnGroup(
                                        "name_cg",
                                        List.of("first_name", "last_name"),
                                        List.of(Types.string(), Types.string()),
                                        ColumnGroup.Encoding.FIXED_OFFSET,
                                        1)));
    }

    @Nested
    @DisplayName("withAddedField")
    class WithAddedField {

        @Test
        @DisplayName("field fieldId")
        void autoAssignFieldId() {
            Schema original = createTestSchema();
            Schema evolved = original.withAddedField("email", Types.string());

            assertThat(evolved.fields()).hasSize(4);
            assertThat(evolved.field("email")).isPresent();
            assertThat(evolved.field("email").get().fieldId()).isEqualTo(4);
            assertThat(evolved.version()).isEqualTo(original.version() + 1);
        }

        @Test
        @DisplayName("explicit fieldId offield")
        void explicitFieldId() {
            Schema original = createTestSchema();
            StructField newField = new StructField(10, "email", Types.string(), "", Map.of());
            Schema evolved = original.withAddedField(newField);

            assertThat(evolved.field("email").get().fieldId()).isEqualTo(10);
        }

        @Test
        @DisplayName("of fieldId andexplicit fieldId conflict")
        void autoAssignSkipsExplicit() {
            Schema original = createTestSchema();
            Schema step1 =
                    original.withAddedField(
                            new StructField(10, "email", Types.string(), "", Map.of()));
            Schema step2 = step1.withAddedField("phone", Types.string());

            assertThat(step2.field("phone").get().fieldId()).isEqualTo(11);
        }

        @Test
        @DisplayName("null fieldthrows exception")
        void nullField() {
            assertThatThrownBy(() -> createTestSchema().withAddedField(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("name throws exception")
        void addDuplicateName() {
            assertThatThrownBy(() -> createTestSchema().withAddedField("id", Types.int64()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Field already exists: id");
        }

        @Test
        @DisplayName("duplicate fieldId offieldthrows exception")
        void addDuplicateFieldId() {
            assertThatThrownBy(
                            () ->
                                    createTestSchema()
                                            .withAddedField(
                                                    new StructField(
                                                            1,
                                                            "duplicate_id",
                                                            Types.int64(),
                                                            "",
                                                            Map.of())))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Field id already assigned: 1");
        }

        @Test
        @DisplayName("original Schema unchanged")
        void originalUnchanged() {
            Schema original = createTestSchema();
            original.withAddedField("email", Types.string());
            assertThat(original.fields()).hasSize(3);
        }
    }

    @Nested
    @DisplayName("withDroppedField")
    class WithDroppedField {

        @Test
        @DisplayName("deletenon-constraintfieldsuccess")
        void dropNonConstraintField() {
            Schema original = createTestSchema();
            Schema evolved = original.withDroppedField("age");

            assertThat(evolved.fields()).hasSize(2);
            assertThat(evolved.field("age")).isEmpty();
            assertThat(evolved.version()).isEqualTo(original.version() + 1);
        }

        @Test
        @DisplayName("deleteprimary keyfieldthrows exception")
        void dropPrimaryKeyField() {
            assertThatThrownBy(() -> createTestSchema().withDroppedField("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Cannot drop primary key field");
        }

        @Test
        @DisplayName("delete ColumnGroup inoffieldthrows exception")
        void dropColumnGroupField() {
            Schema schema = createSchemaWithColumnGroup();
            assertThatThrownBy(() -> schema.withDroppedField("first_name"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ColumnGroup");
        }

        @Test
        @DisplayName("deletenon-existentfieldthrows exception")
        void dropNonExistingField() {
            assertThatThrownBy(() -> createTestSchema().withDroppedField("nonexistent"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Field not found: nonexistent");
        }

        @Test
        @DisplayName("deleteafter removeprimary keyconstraint success")
        void dropAfterRemovingPrimaryKey() {
            Schema original = createTestSchema();
            Schema noPK = original.withoutPrimaryKey();
            Schema evolved = noPK.withDroppedField("id");

            assertThat(evolved.field("id")).isEmpty();
            assertThat(evolved.hasPrimaryKey()).isFalse();
        }

        @Test
        @DisplayName("remove ColumnGroup afterdeletefieldsuccess")
        void dropAfterRemovingColumnGroup() {
            Schema schema = createSchemaWithColumnGroup();
            Schema noCG = schema.withDroppedColumnGroup("name_cg");
            Schema evolved = noCG.withDroppedField("first_name");

            assertThat(evolved.field("first_name")).isEmpty();
            assertThat(evolved.columnGroups()).isEmpty();
        }

        @Test
        @DisplayName("null parameter throws exception")
        void nullName() {
            assertThatThrownBy(() -> createTestSchema().withDroppedField(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("withRenamedField")
    class WithRenamedField {

        @Test
        @DisplayName("name success, fieldId unchanged")
        void renamePreservesFieldId() {
            Schema original = createTestSchema();
            Schema evolved = original.withRenamedField("name", "full_name");

            assertThat(evolved.field("name")).isEmpty();
            assertThat(evolved.field("full_name")).isPresent();
            assertThat(evolved.field("full_name").get().fieldId()).isEqualTo(2);
            assertThat(evolved.version()).isEqualTo(original.version() + 1);
        }

        @Test
        @DisplayName("same , version unchanged")
        void renameToSameNameReturnsSame() {
            Schema original = createTestSchema();
            Schema result = original.withRenamedField("name", "name");
            assertThat(result).isSameAs(original);
            assertThat(result.version()).isEqualTo(original.version());
        }

        @Test
        @DisplayName("primary keyfield update PK constraint")
        void renameCascadesToPrimaryKey() {
            Schema original = createTestSchema();
            Schema evolved = original.withRenamedField("id", "user_id");

            assertThat(evolved.primaryKeys()).containsExactly("user_id");
            assertThat(evolved.isPrimaryKeyField("user_id")).isTrue();
            assertThat(evolved.isPrimaryKeyField("id")).isFalse();
        }

        @Test
        @DisplayName("ColumnGroup inoffield update CG")
        void renameCascadesToColumnGroup() {
            Schema schema = createSchemaWithColumnGroup();
            Schema evolved = schema.withRenamedField("first_name", "given_name");

            assertThat(evolved.columnGroupForColumn("given_name")).isPresent();
            assertThat(evolved.columnGroupForColumn("first_name")).isEmpty();
            ColumnGroup cg = evolved.columnGroup("name_cg").orElseThrow();
            assertThat(cg.columnNames()).containsExactly("given_name", "last_name");
        }

        @Test
        @DisplayName("non-existentfieldthrows exception")
        void renameNonExisting() {
            assertThatThrownBy(() -> createTestSchema().withRenamedField("nonexistent", "new_name"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("isalready inoffield throws exception")
        void renameToExisting() {
            assertThatThrownBy(() -> createTestSchema().withRenamedField("name", "id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Field already exists: id");
        }

        @Test
        @DisplayName("null parameter throws exception")
        void nullArgs() {
            assertThatThrownBy(() -> createTestSchema().withRenamedField(null, "x"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> createTestSchema().withRenamedField("name", null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("withUpdatedFieldType")
    class WithUpdatedFieldType {

        @Test
        @DisplayName("updatefieldtypesuccess, fieldId unchanged")
        void updateTypePreservesFieldId() {
            Schema original = createTestSchema();
            Schema evolved = original.withUpdatedFieldType("age", Types.int64());

            assertThat(evolved.fieldType("age")).isEqualTo(Types.int64());
            assertThat(evolved.field("age").get().fieldId()).isEqualTo(3);
            assertThat(evolved.version()).isEqualTo(original.version() + 1);
        }

        @Test
        @DisplayName("update ColumnGroup infieldoftype update CG columnTypes and schemaVersion")
        void updateTypeCascadesToColumnGroup() {
            Schema schema = createSchemaWithColumnGroup();
            int oldCgVersion = schema.columnGroup("name_cg").get().schemaVersion();
            Schema evolved = schema.withUpdatedFieldType("first_name", Types.int64());

            ColumnGroup cg = evolved.columnGroup("name_cg").orElseThrow();
            int idx = cg.columnNames().indexOf("first_name");
            assertThat(cg.columnTypes().get(idx)).isEqualTo(Types.int64());
            assertThat(cg.schemaVersion()).isEqualTo(oldCgVersion + 1);
        }

        @Test
        @DisplayName("updateprimary keyfieldoftypesuccess")
        void updatePrimaryKeyFieldType() {
            Schema original = createTestSchema();
            Schema evolved = original.withUpdatedFieldType("id", Types.int32());

            assertThat(evolved.fieldType("id").isNullable()).isFalse();
            assertThat(evolved.primaryKeys()).containsExactly("id");
        }

        @Test
        @DisplayName("updatedoes not existfieldoftypethrows exception")
        void updateNonExisting() {
            assertThatThrownBy(
                            () ->
                                    createTestSchema()
                                            .withUpdatedFieldType("nonexistent", Types.string()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("updateafterits fieldunchanged")
        void otherFieldsUnchanged() {
            Schema evolved = createTestSchema().withUpdatedFieldType("age", Types.int64());
            assertThat(evolved.field("id").get().fieldId()).isEqualTo(1);
            assertThat(evolved.fieldType("id").isNullable()).isFalse();
            assertThat(evolved.field("name").get().fieldId()).isEqualTo(2);
            assertThat(evolved.fieldType("name")).isEqualTo(Types.string());
        }

        @Test
        @DisplayName("null parameter throws exception")
        void nullArgs() {
            assertThatThrownBy(() -> createTestSchema().withUpdatedFieldType(null, Types.string()))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> createTestSchema().withUpdatedFieldType("age", null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("withDroppedColumnGroup")
    class WithDroppedColumnGroup {

        @Test
        @DisplayName("remove ColumnGroup success")
        void dropColumnGroup() {
            Schema schema = createSchemaWithColumnGroup();
            Schema evolved = schema.withDroppedColumnGroup("name_cg");

            assertThat(evolved.columnGroups()).isEmpty();
            assertThat(evolved.version()).isEqualTo(schema.version() + 1);
        }

        @Test
        @DisplayName("remove ColumnGroup aftercandeleteitsfield")
        void dropFieldAfterRemovingCG() {
            Schema schema = createSchemaWithColumnGroup();
            Schema noCG = schema.withDroppedColumnGroup("name_cg");
            Schema evolved = noCG.withDroppedField("first_name");

            assertThat(evolved.field("first_name")).isEmpty();
        }

        @Test
        @DisplayName("removenon-existent ColumnGroup throws exception")
        void dropNonExistingColumnGroup() {
            assertThatThrownBy(() -> createTestSchema().withDroppedColumnGroup("nonexistent"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ColumnGroup not found: nonexistent");
        }

        @Test
        @DisplayName("null parameter throws exception")
        void nullName() {
            assertThatThrownBy(() -> createTestSchema().withDroppedColumnGroup(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("SchemaBuilder fieldId")
    class SchemaBuilderFieldIdAssignment {

        @Test
        @DisplayName("explicit fieldId and , skipexplicit ID")
        void mixedExplicitAndAuto() {
            Schema schema =
                    Schema.builder()
                            .field(10, "id", Types.int64())
                            .field("name", Types.string())
                            .field(30, "age", Types.int32())
                            .field("email", Types.string())
                            .build();

            assertThat(schema.field("id").get().fieldId()).isEqualTo(10);
            assertThat(schema.field("name").get().fieldId()).isEqualTo(1);
            assertThat(schema.field("age").get().fieldId()).isEqualTo(30);
            assertThat(schema.field("email").get().fieldId()).isEqualTo(2);
        }

        @Test
        @DisplayName("explicit fieldId and conflict -- skipalready of ID")
        void autoAssignDoesNotCollideWithExplicit() {
            Schema schema =
                    Schema.builder()
                            .field(2, "id", Types.int64())
                            .field("name", Types.string())
                            .field("age", Types.int32())
                            .build();

            assertThat(schema.field("id").get().fieldId()).isEqualTo(2);
            assertThat(schema.field("name").get().fieldId()).isEqualTo(1);
            assertThat(schema.field("age").get().fieldId()).isEqualTo(3);
        }
    }
}
