package io.github.rawwise.frontend.ir.parser;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.common.catalog.TableProperties;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.frontend.ir.IRType;
import io.github.rawwise.frontend.ir.JoinQueryIR;
import io.github.rawwise.frontend.ir.JoinType;
import io.github.rawwise.frontend.ir.QueryIR;
import io.github.rawwise.frontend.ir.ScanQueryIR;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.expression.And;
import io.github.rawwise.model.expression.Eq;
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
 * Spec 02 E2E: JOIN IR-path parser end-to-endverify.
 *
 * <p>coverage scenario:
 *
 * <ul>
 *   <li>S1 Happy Path: 2 table INNER JOIN ON column=column go through JoinQueryIR, go through
 *       fallback
 *   <li>S2 H-3 repeat: snapshotId = min(latestSnapshotId(t1), latestSnapshotId(t2)), sort
 *   <li>S3 project/filter: SELECT column, WHERE correctly translates
 *   <li>S4 boundary: CROSS/OUTER/USING/3 table JOIN/missing ON -> fallback
 *   <li>S5 ON expression: repeat AND conditioncorrectly merges
 * </ul>
 */
@DisplayName("JOIN IR-path E2E — Spec 02")
class JoinQueryIRParserE2ETest {

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
                                .catalogName("join-ir-e2e")
                                .store(metastore)
                                .basePath("/tmp/join-ir-e2e")
                                .build());
        catalog.createNamespace("db");
        snapshotManager = new SnapshotManager(catalog, metastore);
        parser = new SqlQueryParser(catalog, snapshotManager);

        // table a: (id INT, name STRING), PK=id
        catalog.createTable(
                new TableId("db", "a"),
                Schema.builder()
                        .field(1, "id", Types.int32(false))
                        .field(2, "name", Types.string())
                        .primaryKeys("id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());

        // table b: (aid INT, score INT), PK=aid
        catalog.createTable(
                new TableId("db", "b"),
                Schema.builder()
                        .field(1, "aid", Types.int32(false))
                        .field(2, "score", Types.int32())
                        .primaryKeys("aid")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());

        // table c: No. table multi-table JOIN fallback test
        catalog.createTable(
                new TableId("db", "c"),
                Schema.builder()
                        .field(1, "id", Types.int32(false))
                        .field(2, "tag", Types.string())
                        .primaryKeys("id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (catalog != null) catalog.close();
    }

    @Nested
    @DisplayName("S1: Happy Path - 2 table INNER JOIN")
    class HappyPathTests {

        @Test
        @DisplayName("S1.1: INNER JOIN ON eq produce JoinQueryIR")
        void simpleInnerJoinProducesJoinIR() {
            ParseResult result = parser.parse("SELECT * FROM db.a JOIN db.b ON a.id = b.aid");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.JOIN);
            assertThat(ir).isInstanceOf(JoinQueryIR.class);

            JoinQueryIR jq = (JoinQueryIR) ir;
            assertThat(jq.tableMetas()).hasSize(2);
            assertThat(jq.joinType()).isEqualTo(JoinType.INNER);
            assertThat(jq.joinCondition()).isInstanceOf(Eq.class);
            assertThat(jq.left()).isInstanceOf(ScanQueryIR.class);
            assertThat(jq.right()).isInstanceOf(ScanQueryIR.class);
        }

        @Test
        @DisplayName("S1.2: explicit INNER JOIN key can")
        void explicitInnerJoinKeyword() {
            ParseResult result = parser.parse("SELECT * FROM db.a INNER JOIN db.b ON a.id = b.aid");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            assertThat(ir.type()).isEqualTo(IRType.JOIN);
        }
    }

    @Nested
    @DisplayName("S2: H-3 — snapshotId consistency")
    class SnapshotConsistencyTests {

        @Test
        @DisplayName("S2.1: no snapshot snapshotId=0(latest LMDB)")
        void noSnapshotFallsBackToZero() {
            ParseResult result = parser.parse("SELECT * FROM db.a JOIN db.b ON a.id = b.aid");
            JoinQueryIR jq = (JoinQueryIR) ((ParseResult.IRResult) result).ir();

            // tableall has snapshot, resolveJoinedSnapshotId fallbackto 0
            assertThat(jq.snapshotId()).isEqualTo(0L);
            // ScanQueryIR sync sameof snapshotId
            ScanQueryIR left = (ScanQueryIR) jq.left();
            ScanQueryIR right = (ScanQueryIR) jq.right();
            assertThat(left.snapshotId()).isEqualTo(0L);
            assertThat(right.snapshotId()).isEqualTo(0L);
            assertThat(left.snapshotId()).isEqualTo(right.snapshotId());
        }

        @Test
        @DisplayName("S2.2: tableMetas tableId sort(H-3 ordering)")
        void tableMetasAreSortedByTableId() {
            // SQL in write b write a, produceof IR should : a inbefore
            ParseResult result = parser.parse("SELECT * FROM db.b JOIN db.a ON b.aid = a.id");
            JoinQueryIR jq = (JoinQueryIR) ((ParseResult.IRResult) result).ir();

            assertThat(jq.tableMetas()).hasSize(2);
            assertThat(jq.tableMetas().get(0).tableIdentifier())
                    .isLessThanOrEqualTo(jq.tableMetas().get(1).tableIdentifier());
        }

        @Test
        @DisplayName("S2.3: explicit requestedSnapshotId coverdefaultparse")
        void requestedSnapshotIdOverrides() {
            ParseResult result = parser.parse("SELECT * FROM db.a JOIN db.b ON a.id = b.aid", 42L);
            JoinQueryIR jq = (JoinQueryIR) ((ParseResult.IRResult) result).ir();

            assertThat(jq.snapshotId()).isEqualTo(42L);
            assertThat(((ScanQueryIR) jq.left()).snapshotId()).isEqualTo(42L);
            assertThat(((ScanQueryIR) jq.right()).snapshotId()).isEqualTo(42L);
        }
    }

    @Nested
    @DisplayName("S3: project / WHERE / LIMIT")
    class ProjectionFilterTests {

        @Test
        @DisplayName("S3.1: SELECT column projection non-")
        void specificColumnsCapturedAsProjection() {
            ParseResult result =
                    parser.parse("SELECT a.name, b.score FROM db.a JOIN db.b ON a.id = b.aid");
            JoinQueryIR jq = (JoinQueryIR) ((ParseResult.IRResult) result).ir();

            // capture 2 columnproject
            assertThat(jq.projection()).hasSize(2);
        }

        @Test
        @DisplayName("S3.2: WHERE is postFilter")
        void whereBecomesPostFilter() {
            ParseResult result =
                    parser.parse("SELECT * FROM db.a JOIN db.b ON a.id = b.aid WHERE b.score > 10");
            JoinQueryIR jq = (JoinQueryIR) ((ParseResult.IRResult) result).ir();

            assertThat(jq.postFilter()).isNotNull();
        }

        @Test
        @DisplayName("S3.3: LIMIT becapture")
        void limitCaptured() {
            ParseResult result =
                    parser.parse("SELECT * FROM db.a JOIN db.b ON a.id = b.aid LIMIT 100");
            JoinQueryIR jq = (JoinQueryIR) ((ParseResult.IRResult) result).ir();

            assertThat(jq.limit()).isEqualTo(100);
        }
    }

    @Nested
    @DisplayName("S4: Boundary — unsupported form fallback")
    class FallbackTests {

        @Test
        @DisplayName("S4.1: 3 table JOIN fallback to raw SQL")
        void threeTableJoinFallsBack() {
            ParseResult result =
                    parser.parse(
                            "SELECT * FROM db.a JOIN db.b ON a.id = b.aid "
                                    + "JOIN db.c ON a.id = c.id");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }

        @Test
        @DisplayName("S4.2: LEFT JOIN fallback(Phase 1 INNER)")
        void leftJoinFallsBack() {
            ParseResult result = parser.parse("SELECT * FROM db.a LEFT JOIN db.b ON a.id = b.aid");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }

        @Test
        @DisplayName("S4.3: CROSS JOIN fallback")
        void crossJoinFallsBack() {
            ParseResult result = parser.parse("SELECT * FROM db.a CROSS JOIN db.b");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }

        @Test
        @DisplayName("S4.4: missing ON of INNER JOIN fallback")
        void missingOnFallsBack() {
            // SQL requirement INNER JOIN has ON, no ON parsedirectlyfail -> fallback
            ParseResult result = parser.parse("SELECT * FROM db.a JOIN db.b");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }

        @Test
        @DisplayName("S4.5: non-existent table fallback")
        void unknownTableFallsBack() {
            ParseResult result =
                    parser.parse("SELECT * FROM db.nonexistent JOIN db.b ON a.id = b.aid");
            assertThat(result).isInstanceOf(ParseResult.FallbackResult.class);
        }
    }

    @Nested
    @DisplayName("S5: ON repeat condition")
    class OnConditionTests {

        @Test
        @DisplayName("S5.1: ON a.id=b.aid AND a.name=b.tag mergeis And expression")
        void compositeOnConditionCombinedToAnd() {
            // a.id and b.aid etcvalue + a.name='x'(is comparison, verify AND merge)
            ParseResult result =
                    parser.parse("SELECT * FROM db.a JOIN db.b ON a.id = b.aid AND a.name = 'foo'");

            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            JoinQueryIR jq = (JoinQueryIR) ((ParseResult.IRResult) result).ir();
            assertThat(jq.joinCondition()).isInstanceOf(And.class);
        }
    }

    @Nested
    @DisplayName("S6: single-tablequery go through JoinQueryIR")
    class SingleTableTests {

        @Test
        @DisplayName("S6.1: no JOIN of SELECT go through ScanQueryIR")
        void singleTableSelectStillScan() {
            ParseResult result = parser.parse("SELECT * FROM db.a WHERE id = 1");
            assertThat(result).isInstanceOf(ParseResult.IRResult.class);
            QueryIR ir = ((ParseResult.IRResult) result).ir();
            // is not JOIN
            assertThat(ir.type()).isNotEqualTo(IRType.JOIN);
        }
    }
}
