package io.github.rawwise.model.record;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.common.catalog.TableProperties;
import io.github.rawwise.metastore.InMemoryMetastore;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PrimaryKeyConsistencyE2ETest {

    private InMemoryMetastore store;
    private MetastoreCatalog catalog;

    @BeforeEach
    void setUp() throws IOException {
        store = new InMemoryMetastore();
        catalog =
                new MetastoreCatalog(
                        MetastoreCatalogConfig.builder()
                                .catalogName("pk-consistency-e2e")
                                .store(store)
                                .basePath("/tmp/pk-consistency-e2e")
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

    @Nested
    @DisplayName("T6: PrimaryKey.fromSchema()")
    class FromSchemaTests {

        @Test
        @DisplayName("E2E-6: SchemahasPKconstraint fromSchema correspondingPrimaryKey")
        void shouldCreateFromSchemaWithPkConstraint() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKeys("id")
                            .build();

            PrimaryKey pk = PrimaryKey.fromSchema(schema);
            assertThat(pk.fields()).containsExactly("id");
            assertThat(pk.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("SchemanoPKconstraint fromSchema none")
        void shouldReturnNoneWhenNoPkConstraint() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();

            PrimaryKey pk = PrimaryKey.fromSchema(schema);
            assertThat(pk.isEmpty()).isTrue();
        }

        @Test
        @DisplayName("composite primary keyfromSchemacorrectly extractsallfield")
        void shouldHandleCompositePrimaryKey() {
            Schema schema =
                    Schema.builder()
                            .field(1, "pk1", Types.int64(false))
                            .field(2, "pk2", Types.string(false))
                            .field(3, "value", Types.string())
                            .primaryKeys("pk1", "pk2")
                            .build();

            PrimaryKey pk = PrimaryKey.fromSchema(schema);
            assertThat(pk.fields()).containsExactly("pk1", "pk2");
        }
    }

    @Nested
    @DisplayName("T7: MetastoreCatalog alterSchema PKsync")
    class MetastoreCatalogSyncTests {

        @Test
        @DisplayName("E2E-7: noPKtablealterSchema PKafterprimaryKeysync")
        void shouldSyncPrimaryKeyWhenAddedViaAlterSchema() throws Exception {
            TableId tid = tableId("sync_add_pk");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .build();

            catalog.createTable(tid, v1, PartitionSpec.unpartitioned(), TableProperties.empty());

            Schema v2 =
                    Schema.builder()
                            .version(2)
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKeys("id")
                            .build();

            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().primaryKey().fields()).containsExactly("id");
            assertThat(loaded.meta().schema().primaryKeys()).containsExactly("id");
            assertThat(loaded.meta().primaryKey().fields())
                    .isEqualTo(loaded.meta().schema().primaryKeys());
        }

        @Test
        @DisplayName("E2E-8: alterSchemawhen PK unchanged primaryKey stays consistent")
        void shouldKeepPrimaryKeyWhenAlterSchemaKeepsPk() throws Exception {
            TableId tid = tableId("sync_keep_pk");
            Schema v1 =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKeys("id")
                            .build();

            catalog.createTable(
                    tid,
                    v1,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("table.type", "primary_key")));

            Schema v2 =
                    Schema.builder()
                            .version(2)
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .field(3, "email", Types.string())
                            .primaryKeys("id")
                            .build();

            catalog.alterSchema(tid, v2);

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().primaryKey().fields()).containsExactly("id");
            assertThat(loaded.meta().primaryKey().fields())
                    .isEqualTo(loaded.meta().schema().primaryKeys());
        }

        @Test
        @DisplayName("E2E-11: createTableafterprimaryKeyandschemaconsistent")
        void shouldBeConsistentOnCreateTable() throws Exception {
            TableId tid = tableId("sync_create");
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKeys("id")
                            .build();

            catalog.createTable(
                    tid,
                    schema,
                    PartitionSpec.unpartitioned(),
                    new TableProperties(Map.of("table.type", "primary_key")));

            var loaded = catalog.loadTable(tid);
            assertThat(loaded.meta().primaryKey().fields())
                    .isEqualTo(loaded.meta().schema().primaryKeys());
        }
    }

    @Nested
    @DisplayName("T9: TableMeta.Builder verify")
    class BuilderDefensiveCheckTests {

        @Test
        @DisplayName("E2E-9: Protohistory consistentdatabuild repeat")
        void shouldAutoCorrectInconsistentProtoData() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKeys("id")
                            .build();

            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(1)
                            .withDatabaseName("db")
                            .withTableName("test")
                            .withTableType(TableMeta.TableType.PRIMARY_KEY)
                            .withSchema(schema)
                            .withPrimaryKey(PrimaryKey.none())
                            .build();

            assertThat(meta.primaryKey().fields()).containsExactly("id");
            assertThat(meta.primaryKey().fields()).isEqualTo(schema.primaryKeys());
        }

        @Test
        @DisplayName("TableMeta.primaryKeyandSchema consistent fromSchemacover")
        void shouldAutoCorrectWhenPrimaryKeyDiffersFromSchema() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKeys("id")
                            .build();

            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(1)
                            .withDatabaseName("db")
                            .withTableName("test")
                            .withTableType(TableMeta.TableType.PRIMARY_KEY)
                            .withSchema(schema)
                            .withPrimaryKey(PrimaryKey.of("name"))
                            .build();

            assertThat(meta.primaryKey().fields()).containsExactly("id");
        }

        @Test
        @DisplayName(
                "Schema has no PK constraint but TableMeta has explicit PK - PK should be retained")
        void shouldClearPrimaryKeyWhenSchemaHasNoPk() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64())
                            .field(2, "name", Types.string())
                            .build();

            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(1)
                            .withDatabaseName("db")
                            .withTableName("test")
                            .withTableType(TableMeta.TableType.APPEND_ONLY)
                            .withSchema(schema)
                            .withPrimaryKey(PrimaryKey.of("id"))
                            .build();

            assertThat(meta.primaryKey().isEmpty()).isFalse();
            assertThat(meta.primaryKey().fields()).containsExactly("id");
        }

        @Test
        @DisplayName("TableMeta.primaryKeyandSchemaconsistent")
        void shouldNotModifyWhenConsistent() {
            Schema schema =
                    Schema.builder()
                            .field(1, "id", Types.int64(false))
                            .field(2, "name", Types.string())
                            .primaryKeys("id")
                            .build();

            TableMeta meta =
                    TableMeta.builder()
                            .withTableId(1)
                            .withDatabaseName("db")
                            .withTableName("test")
                            .withTableType(TableMeta.TableType.PRIMARY_KEY)
                            .withSchema(schema)
                            .withPrimaryKey(PrimaryKey.fromSchema(schema))
                            .build();

            assertThat(meta.primaryKey().fields()).containsExactly("id");
        }
    }
}
