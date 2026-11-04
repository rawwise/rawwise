package io.github.rawwise.frontend.ir.parser;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.common.catalog.TableProperties;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.frontend.ir.DDLIR;
import io.github.rawwise.frontend.ir.DDLType;
import io.github.rawwise.frontend.ir.IRType;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.record.PartitionSpec;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.Types;
import java.io.IOException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Spec 04 E2E: secondary index DDL + UNIQUE constraintparser verify.
 *
 * <p>coverage scenario:
 *
 * <ul>
 *   <li>S1 CREATE INDEX -> DDLIR(CREATE_INDEX)
 *   <li>S2 DROP INDEX -> DDLIR(DROP_INDEX) / fallback
 *   <li>S3 ALTER TABLE DROP UNIQUE -> DDLIR(ALTER_TABLE)
 *   <li>S4 UNIQUE constraint check via UniqueConstraintChecker
 *   <li>S5 does not affect existing SELECT/INSERT/UPDATE paths
 * </ul>
 */
@DisplayName("Index & Constraint DDL E2E — Spec 04")
class IndexConstraintE2ETest {

    private InMemoryMetastore metastore;
    private MetastoreCatalog catalog;
    private SnapshotManager snapshotManager;
    private SqlQueryParser parser;

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        catalog =
                new MetastoreCatalog(
                        MetastoreCatalogConfig.builder()
                                .catalogName("idx-constraint-e2e")
                                .store(metastore)
                                .basePath("/tmp/idx-constraint-e2e")
                                .build());
        catalog.createNamespace("db");
        snapshotManager = new SnapshotManager(catalog, metastore);
        parser = new SqlQueryParser(catalog, snapshotManager);

        catalog.createTable(
                new TableId("db", "orders"),
                Schema.builder()
                        .field(1, "o_id", Types.int32(false))
                        .field(2, "o_w_id", Types.int32())
                        .field(3, "o_d_id", Types.int32())
                        .field(4, "o_amount", Types.int64())
                        .primaryKeys("o_id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (catalog != null) catalog.close();
    }

    @Nested
    @DisplayName("S1: CREATE INDEX")
    class CreateIndexTests {

        @Test
        @DisplayName("S1.1: CREATE INDEX idx1 ON tbl (col1, col2) -> DDLIR(CREATE_INDEX)")
        void createIndexProducesDDLIR() {
            ParseResult result =
                    parser.parse("CREATE INDEX idx_orders_w_d ON db.orders (o_w_id, o_d_id)");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.CREATE_INDEX);
            assertThat(ir).isInstanceOf(DDLIR.class);

            DDLIR ddl = (DDLIR) ir;
            assertThat(ddl.ddlType()).isEqualTo(DDLType.CREATE_INDEX);
            assertThat(ddl.options()).containsEntry("indexName", "idx_orders_w_d");
            assertThat(ddl.options().get("columns")).asList().containsExactly("o_w_id", "o_d_id");
        }

        @Test
        @DisplayName("S1.2: non-existent table fallback")
        void unknownTableFallback() {
            ParseResult result = parser.parse("CREATE INDEX idx ON db.nonexistent (col)");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }
    }

    @Nested
    @DisplayName("S2: DROP INDEX")
    class DropIndexTests {

        @Test
        @DisplayName("S2.1: DROP INDEX -> fallback raw SQL(Phase 1 DROP by Backend handle)")
        void dropIndexFallback() {
            // JSqlParser 4.9 parses DROP INDEX as net.sf.jsqlparser.statement.drop.Drop
            // with type="INDEX". Our parser doesn't have a dedicated handler yet,
            // so it falls back to raw SQL.
            ParseResult result = parser.parse("DROP INDEX idx_orders_w_d ON db.orders");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }
    }

    @Nested
    @DisplayName("S3: ALTER TABLE constraints")
    class AlterTableConstraintTests {

        @Test
        @DisplayName(
                "S3.1: ALTER TABLE DROP UNIQUE -> fallback (JSqlParser 4.9 does not natively parse constraint identifiers)")
        void dropUniqueFallback() {
            ParseResult result = parser.parse("ALTER TABLE db.orders DROP UNIQUE idx_unique_email");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }

        @Test
        @DisplayName("S3.2: ALTER TABLE DROP FOREIGN KEY -> fallback (same)")
        void dropForeignKeyFallback() {
            ParseResult result = parser.parse("ALTER TABLE db.orders DROP FOREIGN KEY fk_customer");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }

        @Test
        @DisplayName(
                "S3.3: ALTER TABLE ADD UNIQUE -> fallback (JSqlParser 4.9 does not natively parse)")
        void addUniqueFallback() {
            ParseResult result = parser.parse("ALTER TABLE db.orders ADD UNIQUE (o_d_id)");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }
    }

    @Nested
    @DisplayName("S4: UNIQUE constraint checker (OCC-based)")
    class UniqueConstraintCheckerTests {

        @Test
        @DisplayName("S4.1: key commitTs=0 -> isDuplicate=false")
        void uniqueKeyNotExists() {
            io.github.rawwise.engine.txn.ShardVersionTracker tracker =
                    new io.github.rawwise.engine.txn.ShardVersionTracker(0);
            byte[] uniqueKey = "unique-1@db.orders".getBytes();
            assertThat(
                            io.github.rawwise.engine.txn.UniqueConstraintChecker.isDuplicate(
                                    tracker, uniqueKey))
                    .isFalse();
        }

        @Test
        @DisplayName("S4.2: alreadycommit key commitTs>0 -> isDuplicate=true")
        void uniqueKeyAlreadyExists() {
            io.github.rawwise.engine.txn.ShardVersionTracker tracker =
                    new io.github.rawwise.engine.txn.ShardVersionTracker(0);
            byte[] uniqueKey = "unique-2@db.orders".getBytes();
            tracker.setCommitTs(uniqueKey, 100L, 42L);
            assertThat(
                            io.github.rawwise.engine.txn.UniqueConstraintChecker.isDuplicate(
                                    tracker, uniqueKey))
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("S5: does not affect existing paths")
    class BackCompatTests {

        @Test
        @DisplayName("S5.1: SELECT still returns ScanQueryIR")
        void selectStillScan() {
            ParseResult result = parser.parse("SELECT * FROM db.orders WHERE o_id = 1");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            assertThat(((ParseResult.IRResult) result).ir().type()).isEqualTo(IRType.SCAN);
        }

        @Test
        @DisplayName("S5.2: INSERT still returns WriteQueryIR")
        void insertStillWrite() {
            ParseResult result =
                    parser.parse(
                            "INSERT INTO db.orders (o_id, o_w_id, o_d_id, o_amount) "
                                    + "VALUES (100, 1, 7, 500)");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            assertThat(((ParseResult.IRResult) result).ir().type()).isEqualTo(IRType.BATCH_INSERT);
        }
    }
}
