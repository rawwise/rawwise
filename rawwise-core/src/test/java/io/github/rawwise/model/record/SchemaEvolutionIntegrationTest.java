package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.engine.storage.DynamicMmapStore;
import io.github.rawwise.engine.storage.StorageTestHelper;
import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Schema integration test")
class SchemaEvolutionIntegrationTest {

    @BeforeAll
    static void registerStorageProvider() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
            // Already registered
        }
    }

    @TempDir Path tempDir;

    private DynamicMmapStore store;
    private String tableIdentifier;

    @BeforeEach
    void setUp() throws Exception {
        tableIdentifier = "catalog.db.schema_test_" + UUID.randomUUID().toString().substring(0, 8);
        store =
                DynamicMmapStore.getInstance(
                        tempDir.toString(), "schema-store-" + UUID.randomUUID());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    private String readRawValue(Map<String, Comparable> pk) throws Exception {
        TableStore ts = store.tableStore(tableIdentifier);
        KeyCodec keyCodec = ts.getKeyCodec();
        byte[] encodedKey = keyCodec.encodePrimaryKey(pk);
        byte[] rawValue = ts.getRawValue(encodedKey);
        if (rawValue == null) return null;
        return new String(NullableValueCodec.decode(rawValue), StandardCharsets.UTF_8);
    }

    private Schema schemaWithPk(
            StructType structType,
            long version,
            List<String> pkFields,
            List<ColumnGroup> columnGroups) {
        List<Constraint> constraints =
                pkFields.isEmpty() ? List.of() : List.of(new PrimaryKeyConstraint(pkFields));
        return new Schema(structType, Map.of(), version, constraints, columnGroups);
    }

    @Nested
    @DisplayName("Schema andstoragewriteread")
    class SchemaEvolutionWithStorageTests {

        @Test
        @DisplayName("fieldafterversion , data readable")
        void testAddFieldOldDataStillReadable() throws Exception {
            Schema v1 =
                    new Schema(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("value", Types.string()))),
                            1L);

            Map<String, Comparable> pk = Map.of("id", "add-field-key");
            StorageTestHelper.writeKv(store, tableIdentifier, pk, "original-value");

            assertThat(readRawValue(pk)).isEqualTo("original-value");

            Schema v2 = v1.withAddedField("description", Types.string());
            assertThat(v2.version()).isEqualTo(2L);
            assertThat(v2.fields()).hasSize(3);
            assertThat(v2.field("description")).isPresent();

            assertThat(readRawValue(pk)).isEqualTo("original-value");
        }

        @Test
        @DisplayName("field fieldId, afterfieldId")
        void testAddedFieldAutoFieldId() {
            Schema v1 =
                    new Schema(
                            new StructType(
                                    List.of(
                                            new StructField(1, "id", Types.int64(), "", null),
                                            new StructField(2, "name", Types.string(), "", null))),
                            1L);

            Schema v2 = v1.withAddedField("email", Types.string());

            StructField emailField = v2.field("email").orElseThrow();
            assertThat(emailField.hasAssignedFieldId()).isTrue();
            assertThat(emailField.fieldId()).isEqualTo(3);
        }

        @Test
        @DisplayName("deletefieldafterversion , cannotdeleteprimary keyfield")
        void testDropFieldVersionIncrementAndPrimaryKeyProtection() throws Exception {
            Schema v1 =
                    schemaWithPk(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("name", Types.string()),
                                            new StructField("age", Types.int32()))),
                            1L,
                            List.of("id"),
                            List.of());

            Map<String, Comparable> pk = Map.of("id", "drop-field-key");
            StorageTestHelper.writeKv(store, tableIdentifier, pk, "before-drop");

            Schema v2 = v1.withDroppedField("age");
            assertThat(v2.version()).isEqualTo(2L);
            assertThat(v2.field("age")).isEmpty();

            assertThat(readRawValue(pk)).isEqualTo("before-drop");

            assertThatThrownBy(() -> v1.withDroppedField("id"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("primary key");
        }

        @Test
        @DisplayName("deleteColumnGroupinoffieldbedenied")
        void testCannotDropColumnGroupField() {
            Schema v1 =
                    schemaWithPk(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("a", Types.int32()),
                                            new StructField("b", Types.int32()))),
                            1L,
                            List.of(),
                            List.of(
                                    new ColumnGroup(
                                            "cg1",
                                            List.of("a", "b"),
                                            List.of(Types.int32(), Types.int32()),
                                            ColumnGroup.Encoding.FIXED_OFFSET)));

            assertThatThrownBy(() -> v1.withDroppedField("a"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ColumnGroup");
        }
    }

    @Nested
    @DisplayName("name andfieldId")
    class RenameFieldIdTrackingTests {

        @Test
        @DisplayName("name maintainfieldIdunchanged, data readable")
        void testRenamePreservesFieldIdDataStillReadable() throws Exception {
            Schema v1 =
                    new Schema(
                            new StructType(
                                    List.of(
                                            new StructField(
                                                    5, "oldName", Types.int64(), "", null))),
                            1L);

            Map<String, Comparable> pk = Map.of("id", "rename-key");
            StorageTestHelper.writeKv(store, tableIdentifier, pk, "rename-value");

            Schema v2 = v1.withRenamedField("oldName", "newName");

            StructField renamed = v2.field("newName").orElseThrow();
            assertThat(renamed.fieldId()).isEqualTo(5);

            assertThat(readRawValue(pk)).isEqualTo("rename-value");
        }

        @Test
        @DisplayName("multiple times afterfieldIdunchanged")
        void testMultipleRenamesPreserveFieldId() {
            Schema v1 =
                    new Schema(
                            new StructType(
                                    List.of(new StructField(10, "a", Types.int64(), "", null))),
                            1L);

            Schema v2 = v1.withRenamedField("a", "b");
            Schema v3 = v2.withRenamedField("b", "c");

            assertThat(v3.version()).isEqualTo(3L);
            StructField finalField = v3.field("c").orElseThrow();
            assertThat(finalField.fieldId()).isEqualTo(10);
        }

        @Test
        @DisplayName("primary keyfield updatePrimaryKeyConstraint")
        void testRenamePrimaryKeyFieldCascadesToConstraint() {
            Schema v1 =
                    schemaWithPk(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("name", Types.string()))),
                            1L,
                            List.of("id"),
                            List.of());

            Schema v2 = v1.withRenamedField("id", "userId");

            assertThat(v2.primaryKeys()).containsExactly("userId");
        }

        @Test
        @DisplayName("name updateColumnGroup")
        void testRenameFieldCascadesToColumnGroup() {
            Schema v1 =
                    schemaWithPk(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("oldCol", Types.int32()),
                                            new StructField("other", Types.string()))),
                            1L,
                            List.of(),
                            List.of(
                                    new ColumnGroup(
                                            "cg1",
                                            List.of("oldCol", "other"),
                                            List.of(Types.int32(), Types.string()),
                                            ColumnGroup.Encoding.FIXED_OFFSET)));

            Schema v2 = v1.withRenamedField("oldCol", "newCol");

            ColumnGroup cg = v2.columnGroup("cg1").orElseThrow();
            assertThat(cg.columnNames()).containsExactly("newCol", "other");
        }
    }

    @Nested
    @DisplayName("type andcoerce")
    class TypeCompatibilityIntegrationTests {

        @Test
        @DisplayName("int32->int64type , coercecorrect")
        void testInt32ToInt64WideningWithCoerce() {
            assertThat(SchemaCompatibility.isTypeCompatible(Types.int32(), Types.int64())).isTrue();

            Object result = SchemaCompatibility.coerce(42, Types.int32(), Types.int64());
            assertThat(result).isInstanceOf(Long.class);
            assertThat(result).isEqualTo(42L);
        }

        @Test
        @DisplayName("int64->float32incompatible (rejected due to precision loss)")
        void testInt64ToFloat32Incompatible() {
            assertThat(SchemaCompatibility.isTypeCompatible(Types.int64(), Types.float32()))
                    .isFalse();

            assertThatThrownBy(
                            () -> SchemaCompatibility.coerce(42L, Types.int64(), Types.float32()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("incompatible");
        }

        @Test
        @DisplayName("int32->string , coerce")
        void testInt32ToStringCoerce() {
            assertThat(SchemaCompatibility.isTypeCompatible(Types.int32(), Types.string()))
                    .isTrue();

            Object result = SchemaCompatibility.coerce(42, Types.int32(), Types.string());
            assertThat(result).isEqualTo("42");
        }

        @Test
        @DisplayName("DECIMAL , denied")
        void testDecimalPrecisionWideningAndNarrowing() {
            assertThat(
                            SchemaCompatibility.isTypeCompatible(
                                    Types.decimal(10, 2), Types.decimal(12, 2)))
                    .isTrue();
            assertThat(
                            SchemaCompatibility.isTypeCompatible(
                                    Types.decimal(12, 2), Types.decimal(10, 2)))
                    .isFalse();

            Object result = SchemaCompatibility.coerce(100L, Types.int64(), Types.decimal(10, 2));
            assertThat(result).isInstanceOf(BigDecimal.class);
            assertThat((BigDecimal) result).isEqualByComparingTo(BigDecimal.valueOf(100L));
        }

        @Test
        @DisplayName("Schema checkreturns empty list, incompatible Incompatibility")
        void testSchemaCompatibilityCheck() {
            Schema fileSchema =
                    new Schema(
                            new StructType(
                                    List.of(
                                            new StructField(1, "id", Types.int32(), "", null),
                                            new StructField(2, "name", Types.string(), "", null))),
                            1L);

            Schema compatibleReadSchema =
                    new Schema(
                            new StructType(
                                    List.of(
                                            new StructField(1, "id", Types.int64(), "", null),
                                            new StructField(2, "name", Types.string(), "", null))),
                            2L);

            List<SchemaCompatibility.Incompatibility> noIssues =
                    SchemaCompatibility.check(fileSchema, compatibleReadSchema);
            assertThat(noIssues).isEmpty();

            Schema incompatibleReadSchema =
                    new Schema(
                            new StructType(
                                    List.of(new StructField(1, "id", Types.float32(), "", null))),
                            2L);

            List<SchemaCompatibility.Incompatibility> issues =
                    SchemaCompatibility.check(fileSchema, incompatibleReadSchema);
            assertThat(issues).hasSize(1);
            assertThat(issues.getFirst().fieldName()).isEqualTo("id");
        }
    }

    @Nested
    @DisplayName("multiversion")
    class MultiVersionEvolutionTests {

        @Test
        @DisplayName("V1->V2(addField)->V3(renameField) , data readable")
        void testMultiStepEvolutionChainDataReadable() throws Exception {
            Schema v1 =
                    new Schema(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("value", Types.string()))),
                            1L);

            Map<String, Comparable> pk = Map.of("id", "evolution-key");
            StorageTestHelper.writeKv(store, tableIdentifier, pk, "evolution-value");

            Schema v2 = v1.withAddedField("description", Types.string());
            assertThat(v2.version()).isEqualTo(2L);
            assertThat(v2.fields()).hasSize(3);

            assertThat(readRawValue(pk)).isEqualTo("evolution-value");

            Schema v3 = v2.withRenamedField("value", "content");
            assertThat(v3.version()).isEqualTo(3L);
            assertThat(v3.field("value")).isEmpty();
            assertThat(v3.field("content")).isPresent();

            assertThat(readRawValue(pk)).isEqualTo("evolution-value");
        }

        @Test
        @DisplayName("V1->V2(addField)->V3(dropField)")
        void testAddThenDropEvolution() {
            Schema v1 =
                    new Schema(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("name", Types.string()))),
                            1L);

            Schema v2 = v1.withAddedField("temp", Types.string());
            assertThat(v2.fields()).hasSize(3);

            Schema v3 = v2.withDroppedField("temp");
            assertThat(v3.fields()).hasSize(2);
            assertThat(v3.field("temp")).isEmpty();
            assertThat(v3.field("id")).isPresent();
            assertThat(v3.field("name")).isPresent();
        }
    }

    @Nested
    @DisplayName("ColumnGroup")
    class ColumnGroupEvolutionTests {

        @Test
        @DisplayName("dropColumnGroupafterfield can , can delete")
        void testDropColumnGroupFieldsStillAccessibleThenDeletable() {
            Schema v1 =
                    schemaWithPk(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("a", Types.int32()),
                                            new StructField("b", Types.int32()))),
                            1L,
                            List.of(),
                            List.of(
                                    new ColumnGroup(
                                            "cg1",
                                            List.of("a", "b"),
                                            List.of(Types.int32(), Types.int32()),
                                            ColumnGroup.Encoding.FIXED_OFFSET)));

            Schema v2 = v1.withDroppedColumnGroup("cg1");
            assertThat(v2.columnGroups()).isEmpty();
            assertThat(v2.field("a")).isPresent();
            assertThat(v2.field("b")).isPresent();

            Schema v3 = v2.withDroppedField("a");
            assertThat(v3.field("a")).isEmpty();
            assertThat(v3.field("b")).isPresent();
        }

        @Test
        @DisplayName("dropnon-existent ColumnGroup throws exception")
        void testDropNonExistentColumnGroupThrows() {
            Schema v1 =
                    new Schema(new StructType(List.of(new StructField("id", Types.int64()))), 1L);

            assertThatThrownBy(() -> v1.withDroppedColumnGroup("nonexistent"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("not found");
        }
    }

    @Nested
    @DisplayName("Schema projectproject")
    class SchemaProjectTests {

        @Test
        @DisplayName("project fieldandprimary keyconstraint")
        void testProjectRetainsSpecifiedFieldsAndPrimaryKey() {
            Schema full =
                    schemaWithPk(
                            new StructType(
                                    List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("name", Types.string()),
                                            new StructField("age", Types.int32()),
                                            new StructField("email", Types.string()))),
                            1L,
                            List.of("id"),
                            List.of());

            Schema projected = full.project(List.of("id", "name"));

            assertThat(projected.fields()).hasSize(2);
            assertThat(projected.field("id")).isPresent();
            assertThat(projected.field("name")).isPresent();
            assertThat(projected.field("age")).isEmpty();
            assertThat(projected.field("email")).isEmpty();
            assertThat(projected.primaryKeys()).containsExactly("id");
        }
    }
}
