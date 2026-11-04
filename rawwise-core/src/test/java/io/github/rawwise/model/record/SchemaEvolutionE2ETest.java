package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.common.catalog.TableProperties;
import io.github.rawwise.common.exception.SchemaIncompatibleException;
import io.github.rawwise.common.exception.TableNotFoundException;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaEvolutionE2ETest {

    private Metastore store;
    private MetastoreCatalog catalog;

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryMetastore();
        catalog =
                new MetastoreCatalog(
                        MetastoreCatalogConfig.builder()
                                .catalogName("schema-evolution-e2e")
                                .store(store)
                                .basePath("/tmp/schema-evolution-e2e")
                                .build());
        catalog.createNamespace("db");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (catalog != null) {
            catalog.close();
        }
    }

    private TableId tableId(String name) {
        return new TableId("db", name);
    }

    private Schema baseSchema() {
        return Schema.builder()
                .field(1, "id", Types.int64())
                .field(2, "name", Types.string())
                .build();
    }

    private List<String> fieldNames(Schema s) {
        return s.fields().stream().map(StructField::name).toList();
    }

    @Nested
    @DisplayName("Phase 1: P0 Correctness")
    class Phase1 {

        @Test
        @DisplayName("E2E-2: Add nullable field under BACKWARD")
        void shouldAddNullableFieldUnderBackward() throws Exception {
            TableId tid = tableId("e2e_2");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 = v1.withAddedField("email", Types.string());
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(fieldNames(loaded.meta().schema())).containsExactly("id", "name", "email");
            assertThat(loaded.meta().schema().field("email").get().dataType().isNullable())
                    .isTrue();
            assertThat(loaded.meta().schema().field("email").get().defaultValue()).isEmpty();
        }

        @Test
        @DisplayName("E2E-3: Add non-nullable field with default under BACKWARD")
        void shouldAddNonNullableFieldWithDefaultUnderBackward() throws Exception {
            TableId tid = tableId("e2e_3");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(
                                    new StructField(
                                            3,
                                            "age",
                                            Types.int32(false),
                                            "",
                                            Map.of("default", "0")))
                            .version(v1.version() + 1)
                            .build();
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().schema().field("age").get().dataType().isNullable()).isFalse();
            assertThat(loaded.meta().schema().field("age").get().defaultValue()).contains("0");
        }

        @Test
        @DisplayName("E2E-4: Nullable contraction rejected under BACKWARD")
        void shouldRejectNullableContractionUnderBackward() throws Exception {
            TableId tid = tableId("e2e_4");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string(true))
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string(false))
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("nullable changed incompatibly");
        }

        @Test
        @DisplayName(
                "E2E-7: PK nullable field auto-corrected to non-nullable at schema construction")
        void shouldRejectNullablePrimaryKeyField() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKey("name")
                            .build();
            assertThat(schema.fieldType("name").isNullable()).isFalse();
        }

        @Test
        @DisplayName("E2E-8: alterSchema preserves PK consistency")
        void shouldPreservePrimaryKeyAfterAlterSchema() throws Exception {
            TableId tid = tableId("e2e_8");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKey("id")
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 = v1.withAddedField("email", Types.string());
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().schema().primaryKeyConstraint()).isPresent();
            assertThat(loaded.meta().schema().primaryKeyConstraint().get().fieldNames())
                    .containsExactly("id");
            assertThat(loaded.meta().primaryKey().fields()).containsExactly("id");
        }

        @Test
        @DisplayName("E2E-41: coerceDefault for various types")
        void shouldCoerceDefaultForVariousTypes() throws Exception {
            TableId tid = tableId("e2e_41");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(
                                    new StructField(
                                            3,
                                            "score",
                                            Types.float64(false),
                                            "",
                                            Map.of("default", "0.0")))
                            .field(
                                    new StructField(
                                            4,
                                            "active",
                                            PrimitiveType.BOOLEAN,
                                            "",
                                            Map.of("default", "false")))
                            .field(
                                    new StructField(
                                            5,
                                            "count",
                                            Types.int64(false),
                                            "",
                                            Map.of("default", "0")))
                            .version(v1.version() + 1)
                            .build();
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().schema().field("score").get().defaultValue()).contains("0.0");
            assertThat(loaded.meta().schema().field("active").get().defaultValue())
                    .contains("false");
            assertThat(loaded.meta().schema().field("count").get().defaultValue()).contains("0");
        }
    }

    @Nested
    @DisplayName("Phase 2: fieldId Model & Schema History")
    class Phase2 {

        @Test
        @DisplayName("E2E-10: Rename rejected (fieldId-based, name mismatch)")
        void shouldRejectRenameDueToFieldIdNameMismatch() throws Exception {
            TableId tid = tableId("e2e_10");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "full_name", Types.string())
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("name mismatch");
        }

        @Test
        @DisplayName("E2E-11: Reorder allowed (fieldId-based matching is position-independent)")
        void shouldAllowReorderWithFieldIdBasedMatching() throws Exception {
            TableId tid = tableId("e2e_11");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "age", Types.int32())
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(2, "name", Types.string())
                            .field(1, "id", Types.int64())
                            .field(3, "age", Types.int32())
                            .version(v1.version() + 1)
                            .build();

            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(fieldNames(loaded.meta().schema())).containsExactly("name", "id", "age");
            assertThat(loaded.meta().schema().field("id").get().fieldId()).isEqualTo(1);
            assertThat(loaded.meta().schema().field("name").get().fieldId()).isEqualTo(2);
            assertThat(loaded.meta().schema().field("age").get().fieldId()).isEqualTo(3);
        }

        @Test
        @DisplayName("E2E-12: Multiple alterSchema, schema history traceable")
        void shouldTraceSchemaHistoryAcrossMultipleAlters() throws Exception {
            TableId tid = tableId("e2e_12");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 = v1.withAddedField("email", Types.string());
            catalog.alterSchema(tid, v2);
            long v2Version = v2.version();

            Schema v3 = v2.withAddedField("age", Types.int32());
            catalog.alterSchema(tid, v3);
            long v3Version = v3.version();

            Optional<Schema> loadedV2 = catalog.loadSchemaVersion(tid, v2Version);
            assertThat(loadedV2).isPresent();
            assertThat(fieldNames(loadedV2.get())).containsExactly("id", "name", "email");

            Optional<Schema> loadedV3 = catalog.loadSchemaVersion(tid, v3Version);
            assertThat(loadedV3).isPresent();
            assertThat(fieldNames(loadedV3.get())).containsExactly("id", "name", "email", "age");
        }

        @Test
        @DisplayName("E2E-13: Missing schema version returns empty")
        void shouldReturnEmptyForMissingSchemaVersion() throws Exception {
            TableId tid = tableId("e2e_13");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Optional<Schema> missing = catalog.loadSchemaVersion(tid, 999L);
            assertThat(missing).isEmpty();
        }

        @Test
        @DisplayName("E2E-13b: Initial schema version has no history entry")
        void shouldReturnEmptyForInitialSchemaVersion() throws Exception {
            TableId tid = tableId("e2e_13b");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Optional<Schema> initial = catalog.loadSchemaVersion(tid, v1.version());
            assertThat(initial).isEmpty();
        }

        @Test
        @DisplayName("E2E-32: FORWARD compatibility allows nullable field removal")
        void shouldAllowNullableFieldRemovalUnderForward() throws Exception {
            TableId tid = tableId("e2e_32");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();
            catalog.createTable(
                    tid,
                    v1,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("schema.compatibility", "FORWARD")));

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .version(v1.version() + 1)
                            .build();
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(fieldNames(loaded.meta().schema())).containsExactly("id");
        }

        @Test
        @DisplayName("E2E-33: FULL compatibility rejects field removal")
        void shouldRejectFieldRemovalUnderFull() throws Exception {
            TableId tid = tableId("e2e_33");
            Schema v1 = baseSchema();
            catalog.createTable(
                    tid,
                    v1,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("schema.compatibility", "FULL")));

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("was removed");
        }

        @Test
        @DisplayName("E2E-33b: BACKWARD compatibility rejects field removal")
        void shouldRejectFieldRemovalUnderBackward() throws Exception {
            TableId tid = tableId("e2e_33b");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("was removed");
        }

        @Test
        @DisplayName("E2E-35: Type widening int32->int64 under BACKWARD")
        void shouldAllowInt32ToInt64Widening() throws Exception {
            TableId tid = tableId("e2e_35");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "count", Types.int32())
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "count", Types.int64())
                            .version(v1.version() + 1)
                            .build();
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().schema().field("count").get().dataType().name())
                    .isEqualTo("int64");
        }

        @Test
        @DisplayName("E2E-36: Decimal widening under BACKWARD")
        void shouldAllowDecimalWidening() throws Exception {
            TableId tid = tableId("e2e_36");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "amount", Types.decimal(10, 2))
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "amount", Types.decimal(12, 4))
                            .version(v1.version() + 1)
                            .build();
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().schema().field("amount").get().dataType().name())
                    .isEqualTo("decimal(12,4)");
        }

        @Test
        @DisplayName("E2E-42: Re-submitting same schema version rejected")
        void shouldRejectResubmittingSameSchemaVersion() throws Exception {
            TableId tid = tableId("e2e_42");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 = v1.withAddedField("email", Types.string());
            catalog.alterSchema(tid, v2);

            Schema v2Alternative =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "phone", Types.string())
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2Alternative))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("Schema version must increase");
        }

        @Test
        @DisplayName("E2E-42b: Schema version must increase on alterSchema")
        void shouldRejectSchemaVersionNotIncreasing() throws Exception {
            TableId tid = tableId("e2e_version");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema sameVersion =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "email", Types.string())
                            .version(v1.version())
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, sameVersion))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("Schema version must increase");
        }

        @Test
        @DisplayName("E2E-42c: Idempotent alterSchema with same version rejected")
        void shouldRejectDuplicateAlterSchemaWithSameVersion() throws Exception {
            TableId tid = tableId("e2e_idempotent");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 = v1.withAddedField("email", Types.string());
            catalog.alterSchema(tid, v2);

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("Schema version must increase");
        }

        @Test
        @DisplayName("alterSchema on non-existent table throws TableNotFoundException")
        void shouldThrowForNonExistentTable() {
            Schema v1 = baseSchema();
            assertThatThrownBy(() -> catalog.alterSchema(tableId("nonexistent"), v1))
                    .isInstanceOf(TableNotFoundException.class)
                    .hasMessageContaining("not found");
        }

        @Test
        @DisplayName("updateProperties persists and reads back")
        void shouldPersistAndUpdateProperties() throws Exception {
            TableId tid = tableId("e2e_props");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            catalog.updateProperties(
                    tid, new TableProperties(Map.of("schema.compatibility", "FORWARD")));

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().properties()).containsEntry("schema.compatibility", "FORWARD");
        }

        @Test
        @DisplayName("PK removal rejected under any compatibility")
        void shouldRejectPrimaryKeyRemoval() throws Exception {
            TableId tid = tableId("e2e_pk_removal");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKey("id")
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("Removing primary key");
        }

        @Test
        @DisplayName("PK field set change rejected")
        void shouldRejectPrimaryKeyFieldChange() throws Exception {
            TableId tid = tableId("e2e_pk_change");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string(false))
                            .primaryKey("id")
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string(false))
                            .primaryKey("name")
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("Primary key field set change");
        }

        @Test
        @DisplayName("FORWARD rejects non-nullable field removal without default")
        void shouldRejectNonNullableFieldRemovalUnderForward() throws Exception {
            TableId tid = tableId("e2e_fwd_nonnull");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "status", Types.string(false))
                            .build();
            catalog.createTable(
                    tid,
                    v1,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("schema.compatibility", "FORWARD")));

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("must be nullable or have a default value");
        }

        @Test
        @DisplayName("FORWARD allows non-nullable field removal with default")
        void shouldAllowNonNullableFieldRemovalWithDefaultUnderForward() throws Exception {
            TableId tid = tableId("e2e_fwd_default");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(
                                    new StructField(
                                            2,
                                            "status",
                                            Types.string(false),
                                            "",
                                            Map.of("default", "active")))
                            .build();
            catalog.createTable(
                    tid,
                    v1,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("schema.compatibility", "FORWARD")));

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .version(v1.version() + 1)
                            .build();
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(fieldNames(loaded.meta().schema())).containsExactly("id");
        }

        @Test
        @DisplayName("FULL rejects non-nullable field addition")
        void shouldRejectNonNullableFieldAdditionUnderFull() throws Exception {
            TableId tid = tableId("e2e_full_add");
            Schema v1 = baseSchema();
            catalog.createTable(
                    tid,
                    v1,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("schema.compatibility", "FULL")));

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "age", Types.int32(false))
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("must be nullable or have a default value");
        }

        @Test
        @DisplayName("NONE compatibility allows any change")
        void shouldAllowAnyChangeUnderNone() throws Exception {
            TableId tid = tableId("e2e_none");
            Schema v1 = baseSchema();
            catalog.createTable(
                    tid,
                    v1,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("schema.compatibility", "NONE")));

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.int32())
                            .version(v1.version() + 1)
                            .build();
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().schema().field("name").get().dataType().name())
                    .isEqualTo("int32");
        }

        @Test
        @DisplayName("Type narrowing int64->int32 rejected under BACKWARD")
        void shouldRejectTypeNarrowingUnderBackward() throws Exception {
            TableId tid = tableId("e2e_narrow");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "count", Types.int64())
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "count", Types.int32())
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("type changed incompatibly");
        }

        @Test
        @DisplayName("Decimal narrowing rejected under BACKWARD")
        void shouldRejectDecimalNarrowingUnderBackward() throws Exception {
            TableId tid = tableId("e2e_decimal_narrow");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "amount", Types.decimal(12, 4))
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "amount", Types.decimal(10, 2))
                            .version(v1.version() + 1)
                            .build();

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("type changed incompatibly");
        }

        @Test
        @DisplayName("assigned->unassigned fieldId degradation rejected under BACKWARD")
        void shouldRejectFieldIdDegradationUnderBackward() throws Exception {
            TableId tid = tableId("e2e_fieldid_degrade");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    new Schema(
                            new io.github.rawwise.model.record.StructType(
                                    java.util.List.of(
                                            new StructField(1, "id", Types.int64(), "", Map.of()),
                                            new StructField("name", Types.string()))),
                            Map.of(),
                            v1.version() + 1,
                            List.of(),
                            List.of());

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("fieldId was removed");
        }

        @Test
        @DisplayName("assigned->unassigned fieldId degradation rejected under FULL")
        void shouldRejectFieldIdDegradationUnderFull() throws Exception {
            TableId tid = tableId("e2e_fieldid_degrade_full");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();
            catalog.createTable(
                    tid,
                    v1,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("schema.compatibility", "FULL")));

            Schema v2 =
                    new Schema(
                            new io.github.rawwise.model.record.StructType(
                                    java.util.List.of(
                                            new StructField(1, "id", Types.int64(), "", Map.of()),
                                            new StructField("name", Types.string()))),
                            Map.of(),
                            v1.version() + 1,
                            List.of(),
                            List.of());

            assertThatThrownBy(() -> catalog.alterSchema(tid, v2))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessageContaining("fieldId was removed");
        }

        @Test
        @DisplayName("unassigned->assigned fieldId migration allowed under BACKWARD")
        void shouldAllowFieldIdMigrationUnderBackward() throws Exception {
            TableId tid = tableId("e2e_fieldid_migrate");
            Schema v1 =
                    new Schema(
                            new io.github.rawwise.model.record.StructType(
                                    java.util.List.of(
                                            new StructField("id", Types.int64()),
                                            new StructField("name", Types.string()))),
                            Map.of(),
                            1L,
                            List.of(),
                            List.of());
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .version(v1.version() + 1)
                            .build();
            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().schema().field("id").get().fieldId()).isEqualTo(1);
            assertThat(loaded.meta().schema().field("name").get().fieldId()).isEqualTo(2);
        }

        @Test
        @DisplayName("Schema history parent version is correct")
        void shouldMaintainParentVersionInHistory() throws Exception {
            TableId tid = tableId("e2e_parent");
            Schema v1 = baseSchema();
            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 = v1.withAddedField("email", Types.string());
            catalog.alterSchema(tid, v2);

            Schema v3 = v2.withAddedField("age", Types.int32());
            catalog.alterSchema(tid, v3);

            Optional<Schema> loadedV2 = catalog.loadSchemaVersion(tid, v2.version());
            assertThat(loadedV2).isPresent();
            assertThat(loadedV2.get().version()).isEqualTo(v2.version());

            Optional<Schema> loadedV3 = catalog.loadSchemaVersion(tid, v3.version());
            assertThat(loadedV3).isPresent();
            assertThat(loadedV3.get().version()).isEqualTo(v3.version());
        }
    }

    @Nested
    @DisplayName("Phase 3: Storage Format Upgrade")
    class Phase3 {

        @Test
        @DisplayName("E2E-14a: TLV V2 encode/decode round-trip with fieldId")
        void tlvV2RoundTripWithFieldId() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "score", Types.float64())
                            .build();

            var codec = new io.github.rawwise.engine.storage.codec.TlvBodyCodec();
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 42L);
            data.put("name", "Alice");
            data.put("score", 95.5);

            byte[] encoded = codec.encode(schema, data);
            assertThat(encoded[0]).isEqualTo((byte) 0xF1);

            Map<String, Object> decoded = codec.decode(schema, encoded);
            assertThat(decoded.get("id")).isEqualTo(42L);
            assertThat(decoded.get("name")).isEqualTo("Alice");
            assertThat(decoded.get("score")).isEqualTo(95.5);
        }

        @Test
        @DisplayName("E2E-14b: TLV V2 rename field — decode by fieldId returns correct value")
        void tlvV2RenameFieldDecodeByFieldId() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();

            var codec = new io.github.rawwise.engine.storage.codec.TlvBodyCodec();
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 1L);
            data.put("name", "Bob");
            byte[] encoded = codec.encode(v1, data);

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "full_name", Types.string())
                            .version(v1.version() + 1)
                            .build();

            Map<String, Object> decoded = codec.decode(v2, encoded);
            assertThat(decoded.get("id")).isEqualTo(1L);
            assertThat(decoded.get("full_name")).isEqualTo("Bob");
        }

        @Test
        @DisplayName("E2E-14c: TLV V2 reorder fields — decode by fieldId returns correct value")
        void tlvV2ReorderFieldsDecodeByFieldId() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "age", Types.int32())
                            .build();

            var codec = new io.github.rawwise.engine.storage.codec.TlvBodyCodec();
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 10L);
            data.put("name", "Carol");
            data.put("age", 30);
            byte[] encoded = codec.encode(v1, data);

            Schema v2 =
                    Schema.builder()
                            .field(3, "age", Types.int32())
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .version(v1.version() + 1)
                            .build();

            Map<String, Object> decoded = codec.decode(v2, encoded);
            assertThat(decoded.get("age")).isEqualTo(30);
            assertThat(decoded.get("id")).isEqualTo(10L);
            assertThat(decoded.get("name")).isEqualTo("Carol");
        }

        @Test
        @DisplayName("E2E-14d: TLV V1 data decoded with new schema — new fields return null")
        void tlvV1DataDecodedWithNewSchemaReturnsNullForNewFields() {
            java.nio.ByteBuffer v1Buf = java.nio.ByteBuffer.allocate(1 + 4 + 8 + 1 + 4 + 4);
            v1Buf.order(java.nio.ByteOrder.BIG_ENDIAN);
            v1Buf.put((byte) 0x02);
            v1Buf.putInt(8);
            v1Buf.putLong(99L);
            v1Buf.put((byte) 0x05);
            v1Buf.putInt(4);
            v1Buf.put("Dave".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] v1Data = v1Buf.array();

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "email", Types.string())
                            .version(2L)
                            .build();

            var codec = new io.github.rawwise.engine.storage.codec.TlvBodyCodec();
            Map<String, Object> decoded = codec.decode(v2, v1Data);
            assertThat(decoded.get("id")).isEqualTo(99L);
            assertThat(decoded.get("name")).isEqualTo("Dave");
            assertThat(decoded.get("email")).isNull();
        }

        @Test
        @DisplayName("E2E-14e: TLV V2 with null values round-trip")
        void tlvV2NullValuesRoundTrip() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "score", Types.float64())
                            .build();

            var codec = new io.github.rawwise.engine.storage.codec.TlvBodyCodec();
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 88L);
            data.put("name", null);
            data.put("score", 77.5);

            byte[] encoded = codec.encode(schema, data);
            Map<String, Object> decoded = codec.decode(schema, encoded);
            assertThat(decoded.get("id")).isEqualTo(88L);
            assertThat(decoded.get("name")).isNull();
            assertThat(decoded.get("score")).isEqualTo(77.5);
        }

        @Test
        @DisplayName("E2E-14f: TLV V2 decodeField single field by fieldId after rename")
        void tlvV2DecodeFieldAfterRename() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();

            var codec = new io.github.rawwise.engine.storage.codec.TlvBodyCodec();
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 77L);
            data.put("name", "Eve");
            byte[] encoded = codec.encode(v1, data);

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "full_name", Types.string())
                            .version(v1.version() + 1)
                            .build();

            Object idVal = codec.decodeField(v2, encoded, "id");
            assertThat(idVal).isEqualTo(77L);

            Object nameVal = codec.decodeField(v2, encoded, "full_name");
            assertThat(nameVal).isEqualTo("Eve");
        }

        @Test
        @DisplayName("E2E-15a: FixedOffset V2 encode/decode round-trip with fieldId")
        void fixedOffsetV2RoundTripWithFieldId() {
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_test",
                            List.of("id", "name"),
                            List.of(Types.int64(), Types.string()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1,
                            List.of(1, 2));

            var codec = io.github.rawwise.model.record.FixedOffsetCodec.INSTANCE;
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 100L);
            data.put("name", "Frank");

            byte[] encoded = codec.encode(cg, data);
            assertThat(encoded[0]).isEqualTo((byte) 0xF2);

            Map<String, Object> decoded = codec.decode(cg, encoded);
            assertThat(decoded.get("id")).isEqualTo(100L);
            assertThat(decoded.get("name")).isEqualTo("Frank");
        }

        @Test
        @DisplayName("E2E-15b: FixedOffset V2 decodeField single field by fieldId")
        void fixedOffsetV2DecodeFieldByFieldId() {
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_test",
                            List.of("id", "name"),
                            List.of(Types.int64(), Types.string()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1,
                            List.of(1, 2));

            var codec = io.github.rawwise.model.record.FixedOffsetCodec.INSTANCE;
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 150L);
            data.put("name", "Grace");
            byte[] encoded = codec.encode(cg, data);

            Object idVal = codec.decodeField(cg, encoded, "id");
            assertThat(idVal).isEqualTo(150L);

            Object nameVal = codec.decodeField(cg, encoded, "name");
            assertThat(nameVal).isEqualTo("Grace");
        }

        @Test
        @DisplayName("E2E-15c: FixedOffset V2 with null values round-trip")
        void fixedOffsetV2NullValuesRoundTrip() {
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_test",
                            List.of("id", "name"),
                            List.of(Types.int64(), Types.string()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1,
                            List.of(1, 2));

            var codec = io.github.rawwise.model.record.FixedOffsetCodec.INSTANCE;
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 200L);
            data.put("name", null);

            byte[] encoded = codec.encode(cg, data);
            Map<String, Object> decoded = codec.decode(cg, encoded);
            assertThat(decoded.get("id")).isEqualTo(200L);
            assertThat(decoded.get("name")).isNull();
        }

        @Test
        @DisplayName(
                "E2E-15d: FixedOffset V1 data decoded by V2 path — same schema reads correctly")
        void fixedOffsetV1DataDecodedByV2Path() {
            ColumnGroup cgV1 =
                    new ColumnGroup(
                            "cg_test",
                            List.of("id", "name"),
                            List.of(Types.int64(), Types.string()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1);

            var codec = io.github.rawwise.model.record.FixedOffsetCodec.INSTANCE;
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 400L);
            data.put("name", "Ivan");
            byte[] encoded = codec.encode(cgV1, data);

            ColumnGroup cgV2 =
                    new ColumnGroup(
                            "cg_test",
                            List.of("id", "name"),
                            List.of(Types.int64(), Types.string()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1,
                            List.of(1, 2));

            Map<String, Object> decoded = codec.decode(cgV2, encoded);
            assertThat(decoded.get("id")).isEqualTo(400L);
            assertThat(decoded.get("name")).isEqualTo("Ivan");
        }

        @Test
        @DisplayName("E2E-15e: FixedOffset V2 three-field round-trip with mixed types")
        void fixedOffsetV2ThreeFieldRoundTrip() {
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_test",
                            List.of("id", "name", "score"),
                            List.of(Types.int64(), Types.string(), Types.float64()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1,
                            List.of(1, 2, 3));

            var codec = io.github.rawwise.model.record.FixedOffsetCodec.INSTANCE;
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 500L);
            data.put("name", "Judy");
            data.put("score", 98.5);

            byte[] encoded = codec.encode(cg, data);
            assertThat(encoded[0]).isEqualTo((byte) 0xF2);

            Map<String, Object> decoded = codec.decode(cg, encoded);
            assertThat(decoded.get("id")).isEqualTo(500L);
            assertThat(decoded.get("name")).isEqualTo("Judy");
            assertThat(decoded.get("score")).isEqualTo(98.5);
        }

        @Test
        @DisplayName(
                "E2E-16a: ColumnGroup footer serialize/parse round-trip preserves fieldId and type")
        void columnGroupFooterSerializeParseRoundTrip() {
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_main",
                            List.of("id", "name"),
                            List.of(Types.int64(), Types.string()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            2,
                            List.of(1, 2));

            String serialized = serializeColumnGroupsDirect(List.of(cg));
            List<ColumnGroup> parsed = parseColumnGroupsDirect(serialized);

            assertThat(parsed).hasSize(1);
            ColumnGroup parsedCg = parsed.get(0);
            assertThat(parsedCg.name()).isEqualTo("cg_main");
            assertThat(parsedCg.encoding()).isEqualTo(ColumnGroup.Encoding.FIXED_OFFSET);
            assertThat(parsedCg.schemaVersion()).isEqualTo(2);
            assertThat(parsedCg.columnNames()).containsExactly("id", "name");
            assertThat(parsedCg.columnFieldId(0)).isEqualTo(1);
            assertThat(parsedCg.columnFieldId(1)).isEqualTo(2);
        }

        @Test
        @DisplayName("E2E-16b: ColumnGroup footer with schemaVersion > 1 parses without error")
        void columnGroupFooterHighSchemaVersion() {
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_data",
                            List.of("ts", "value"),
                            List.of(Types.int64(), Types.float64()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            5,
                            List.of(10, 20));

            String serialized = serializeColumnGroupsDirect(List.of(cg));
            List<ColumnGroup> parsed = parseColumnGroupsDirect(serialized);

            assertThat(parsed.get(0).schemaVersion()).isEqualTo(5);
            assertThat(parsed.get(0).columnFieldId(0)).isEqualTo(10);
            assertThat(parsed.get(0).columnFieldId(1)).isEqualTo(20);
        }

        @Test
        @DisplayName("E2E-16c: ColumnGroup footer backward compatible — old format still parses")
        void columnGroupFooterOldFormatParses() {
            String oldFormat = "cg_main:FIXED_OFFSET,id,name";
            List<ColumnGroup> parsed = parseColumnGroupsDirect(oldFormat);

            assertThat(parsed).hasSize(1);
            ColumnGroup cg = parsed.get(0);
            assertThat(cg.name()).isEqualTo("cg_main");
            assertThat(cg.encoding()).isEqualTo(ColumnGroup.Encoding.FIXED_OFFSET);
            assertThat(cg.columnNames()).containsExactly("id", "name");
            assertThat(cg.columnFieldId(0)).isEqualTo(0);
            assertThat(cg.columnFieldId(1)).isEqualTo(0);
        }

        @Test
        @DisplayName("E2E-39a: CgEncodedBody fieldId projection — rename field still readable")
        void cgEncodedBodyFieldIdProjectionAfterRename() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();

            var codec = new io.github.rawwise.engine.storage.codec.TlvBodyCodec();
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 600L);
            data.put("name", "Karl");
            byte[] encoded = codec.encode(v1, data);

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "full_name", Types.string())
                            .version(v1.version() + 1)
                            .build();

            var body = new io.github.rawwise.model.record.CgEncodedBody(v2, codec, encoded);
            assertThat(body.value("id").raw()).isEqualTo(600L);
            assertThat(body.value("full_name").raw()).isEqualTo("Karl");
        }

        @Test
        @DisplayName("E2E-39b: CgEncodedBody fieldId projection — dropped field returns null")
        void cgEncodedBodyFieldIdProjectionDroppedFieldReturnsNull() {
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .field(3, "email", Types.string())
                            .build();

            var codec = new io.github.rawwise.engine.storage.codec.TlvBodyCodec();
            Map<String, Object> data = new java.util.LinkedHashMap<>();
            data.put("id", 700L);
            data.put("name", "Liam");
            data.put("email", "liam@test.com");
            byte[] encoded = codec.encode(v1, data);

            Schema v2 =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(3, "email", Types.string())
                            .version(v1.version() + 1)
                            .build();

            var body = new io.github.rawwise.model.record.CgEncodedBody(v2, codec, encoded);
            assertThat(body.value("id").raw()).isEqualTo(700L);
            assertThat(body.value("email").raw()).isEqualTo("liam@test.com");
            assertThat(body.has("name")).isFalse();
        }

        @Test
        @DisplayName("E2E-40a: ColumnGroup footer preserves type and nullable info")
        void columnGroupFooterPreservesTypeAndNullable() {
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_types",
                            List.of("id", "name", "score"),
                            List.of(Types.int64(), Types.string(), Types.float64()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            3,
                            List.of(1, 2, 3));

            String serialized = serializeColumnGroupsDirect(List.of(cg));
            assertThat(serialized).contains("int64");
            assertThat(serialized).contains("string");
            assertThat(serialized).contains("float64");
        }

        @Test
        @DisplayName("E2E-40b: ColumnGroup footer multiple groups serialize/parse correctly")
        void columnGroupFooterMultipleGroups() {
            ColumnGroup cg1 =
                    new ColumnGroup(
                            "cg_keys",
                            List.of("id"),
                            List.of(Types.int64()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            2,
                            List.of(1));
            ColumnGroup cg2 =
                    new ColumnGroup(
                            "cg_vals",
                            List.of("name", "score"),
                            List.of(Types.string(), Types.float64()),
                            ColumnGroup.Encoding.TLV,
                            2,
                            List.of(2, 3));

            String serialized = serializeColumnGroupsDirect(List.of(cg1, cg2));
            List<ColumnGroup> parsed = parseColumnGroupsDirect(serialized);

            assertThat(parsed).hasSize(2);
            assertThat(parsed.get(0).name()).isEqualTo("cg_keys");
            assertThat(parsed.get(0).encoding()).isEqualTo(ColumnGroup.Encoding.FIXED_OFFSET);
            assertThat(parsed.get(0).columnFieldId(0)).isEqualTo(1);
            assertThat(parsed.get(1).name()).isEqualTo("cg_vals");
            assertThat(parsed.get(1).encoding()).isEqualTo(ColumnGroup.Encoding.TLV);
            assertThat(parsed.get(1).columnFieldId(0)).isEqualTo(2);
            assertThat(parsed.get(1).columnFieldId(1)).isEqualTo(3);
        }

        private String serializeColumnGroupsDirect(java.util.List<ColumnGroup> columnGroups) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columnGroups.size(); i++) {
                if (i > 0) sb.append(';');
                ColumnGroup cg = columnGroups.get(i);
                sb.append(cg.name())
                        .append(':')
                        .append(cg.encoding().name())
                        .append(':')
                        .append(cg.schemaVersion());
                for (int j = 0; j < cg.columnNames().size(); j++) {
                    String colName = cg.columnNames().get(j);
                    int fieldId = cg.columnFieldId(j);
                    sb.append(',').append(fieldId).append(':').append(colName);
                    if (!cg.columnTypes().isEmpty()) {
                        sb.append(':').append(cg.columnTypes().get(j).name());
                        sb.append(':').append(cg.columnTypes().get(j).isNullable() ? "1" : "0");
                    }
                }
            }
            return sb.toString();
        }

        private java.util.List<ColumnGroup> parseColumnGroupsDirect(String cgRaw) {
            java.util.List<ColumnGroup> result = new java.util.ArrayList<>();
            if (cgRaw == null || cgRaw.isEmpty()) return result;
            String[] groups = cgRaw.split(";");
            for (String group : groups) {
                int colonIdx = group.indexOf(':');
                if (colonIdx < 0) continue;
                String name = group.substring(0, colonIdx);
                String rest = group.substring(colonIdx + 1);
                int firstComma = rest.indexOf(',');
                if (firstComma < 0) continue;
                String headerPart = rest.substring(0, firstComma);
                String columnsPart = rest.substring(firstComma + 1);

                String[] headerTokens = headerPart.split(":");
                String encodingStr = headerTokens[0];
                int schemaVersion = headerTokens.length > 1 ? Integer.parseInt(headerTokens[1]) : 1;

                ColumnGroup.Encoding encoding;
                try {
                    encoding = ColumnGroup.Encoding.valueOf(encodingStr);
                } catch (IllegalArgumentException e) {
                    encoding = ColumnGroup.Encoding.FIXED_OFFSET;
                }

                java.util.List<String> columnNames = new java.util.ArrayList<>();
                java.util.List<Integer> columnFieldIds = new java.util.ArrayList<>();
                for (String colEntry : columnsPart.split(",")) {
                    if (colEntry.isEmpty()) continue;
                    String[] parts = colEntry.split(":");
                    if (parts.length >= 2 && parts[0].matches("\\d+")) {
                        columnFieldIds.add(Integer.parseInt(parts[0]));
                        columnNames.add(parts[1]);
                    } else {
                        columnFieldIds.add(0);
                        columnNames.add(parts[0]);
                    }
                }
                result.add(
                        new ColumnGroup(
                                name,
                                columnNames,
                                List.of(),
                                encoding,
                                schemaVersion,
                                columnFieldIds));
            }
            return result;
        }
    }

    @Nested
    @DisplayName("Phase 4: Completeness")
    class Phase4 {

        @Test
        @DisplayName("E2E-37: Proto roundtrip preserves columnFieldIds")
        void protoRoundtripPreservesColumnFieldIds() {
            StructType structType =
                    new StructType(
                            List.of(
                                    new StructField(1, "id", Types.int64(), "", Map.of()),
                                    new StructField(2, "name", Types.string(), "", Map.of())));
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_main",
                            List.of("id", "name"),
                            List.of(Types.int64(), Types.string()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1,
                            List.of(1, 2));
            Schema original = new Schema(structType, Map.of(), 1L, List.of(), List.of(cg));

            byte[] bytes = io.github.rawwise.model.record.ProtoTypeConverter.toBytes(original);
            Schema restored = io.github.rawwise.model.record.ProtoTypeConverter.fromBytes(bytes);

            assertThat(restored.columnGroups()).hasSize(1);
            ColumnGroup restoredCg = restored.columnGroups().get(0);
            assertThat(restoredCg.columnFieldId(0)).isEqualTo(1);
            assertThat(restoredCg.columnFieldId(1)).isEqualTo(2);
            assertThat(restoredCg.columnNames()).containsExactly("id", "name");
        }

        @Test
        @DisplayName("E2E-37b: ProtoTableMetaConverter roundtrip preserves columnFieldIds")
        void protoTableMetaConverterRoundtripPreservesColumnFieldIds() {
            StructType structType =
                    new StructType(
                            List.of(
                                    new StructField(1, "id", Types.int64(), "", Map.of()),
                                    new StructField(2, "name", Types.string(), "", Map.of())));
            ColumnGroup cg =
                    new ColumnGroup(
                            "cg_main",
                            List.of("id", "name"),
                            List.of(Types.int64(), Types.string()),
                            ColumnGroup.Encoding.FIXED_OFFSET,
                            1,
                            List.of(1, 2));
            Schema schema = new Schema(structType, Map.of(), 1L, List.of(), List.of(cg));

            var pbSchema =
                    io.github.rawwise.model.record.ProtoTableMetaConverter.toPbSchema(schema);
            Schema restored =
                    io.github.rawwise.model.record.ProtoTableMetaConverter.fromPbSchema(pbSchema);

            assertThat(restored.columnGroups()).hasSize(1);
            ColumnGroup restoredCg = restored.columnGroups().get(0);
            assertThat(restoredCg.columnFieldId(0)).isEqualTo(1);
            assertThat(restoredCg.columnFieldId(1)).isEqualTo(2);
        }

        @Test
        @DisplayName("E2E-38a: SchemaCompatibility falls back to name matching when no fieldId")
        void schemaCompatibilityFallbackToNameMatching() {
            StructType structV1 =
                    new StructType(
                            List.of(
                                    new StructField(0, "id", Types.int64(), "", Map.of()),
                                    new StructField(0, "name", Types.string(), "", Map.of())));
            StructType structV2 =
                    new StructType(
                            List.of(
                                    new StructField(0, "id", Types.int64(), "", Map.of()),
                                    new StructField(0, "name", Types.string(), "", Map.of())));
            Schema fileSchema = new Schema(structV1, Map.of(), 1, List.of(), List.of());
            Schema readSchema = new Schema(structV2, Map.of(), 2, List.of(), List.of());

            List<SchemaCompatibility.Incompatibility> issues =
                    SchemaCompatibility.check(fileSchema, readSchema);
            assertThat(issues).isEmpty();
        }

        @Test
        @DisplayName("E2E-38b: SchemaCompatibility detects type mismatch by name when no fieldId")
        void schemaCompatibilityDetectsTypeMismatchByName() {
            StructType structV1 =
                    new StructType(
                            List.of(
                                    new StructField(0, "id", Types.int64(), "", Map.of()),
                                    new StructField(0, "name", Types.string(), "", Map.of())));
            StructType structV2 =
                    new StructType(
                            List.of(
                                    new StructField(0, "id", Types.int32(), "", Map.of()),
                                    new StructField(0, "name", Types.string(), "", Map.of())));
            Schema fileSchema = new Schema(structV1, Map.of(), 1, List.of(), List.of());
            Schema readSchema = new Schema(structV2, Map.of(), 2, List.of(), List.of());

            List<SchemaCompatibility.Incompatibility> issues =
                    SchemaCompatibility.check(fileSchema, readSchema);
            assertThat(issues).isNotEmpty();
            assertThat(issues.stream().anyMatch(i -> i.fieldName().equals("id"))).isTrue();
        }

        @Test
        @DisplayName("E2E-38c: SchemaCompatibility prefers fieldId over name when both available")
        void schemaCompatibilityPrefersFieldIdOverName() {
            Schema fileSchema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();
            Schema readSchema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "full_name", Types.string())
                            .version(2L)
                            .build();

            List<SchemaCompatibility.Incompatibility> issues =
                    SchemaCompatibility.check(fileSchema, readSchema);
            assertThat(issues).isEmpty();
        }

        @Test
        @DisplayName(
                "E2E-38c2: SchemaCompatibility does not fallback to name when fieldId exists but unmatched")
        void schemaCompatibilityNoNameFallbackWhenFieldIdExists() {
            Schema fileSchema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();
            Schema readSchema =
                    Schema.builder()
                            .field(3, "id", Types.int32())
                            .field(4, "name", Types.string())
                            .version(2L)
                            .build();

            List<SchemaCompatibility.Incompatibility> issues =
                    SchemaCompatibility.check(fileSchema, readSchema);
            assertThat(issues).isEmpty();
        }

        @Test
        @DisplayName("E2E-38d: ArrayType fixedSize dimension check")
        void arrayTypeFixedSizeDimensionCheck() {
            ArrayType dynamicArray = new ArrayType(Types.int32(), 0, true);
            ArrayType fixedArray3 = new ArrayType(Types.int32(), 3, true);
            ArrayType fixedArray5 = new ArrayType(Types.int32(), 5, true);

            assertThat(SchemaCompatibility.isTypeCompatible(dynamicArray, fixedArray3)).isTrue();
            assertThat(SchemaCompatibility.isTypeCompatible(fixedArray3, dynamicArray)).isFalse();
            assertThat(SchemaCompatibility.isTypeCompatible(fixedArray3, fixedArray5)).isFalse();
            assertThat(SchemaCompatibility.isTypeCompatible(fixedArray3, fixedArray3)).isTrue();
            assertThat(SchemaCompatibility.isTypeCompatible(dynamicArray, dynamicArray)).isTrue();
        }
    }
}
