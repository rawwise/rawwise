package io.github.rawwise.frontend.ir.routing;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.common.catalog.TableProperties;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.frontend.ir.ScanQueryIR;
import io.github.rawwise.frontend.ir.TableMetaIR;
import io.github.rawwise.frontend.ir.TableType;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.expression.And;
import io.github.rawwise.model.expression.ColumnRef;
import io.github.rawwise.model.expression.Eq;
import io.github.rawwise.model.record.IndexMeta;
import io.github.rawwise.model.record.PartitionSpec;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.Types;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Spec 04 IndexSelector E2E: score + ScanQueryIR write.
 *
 * <p>coverage scenario:
 *
 * <ul>
 *   <li>S1 noindex -> tryRewrite empty
 *   <li>S2 index prefix coverage -> in + score >= 100
 *   <li>S3 multiindex -> , same tie-break
 *   <li>S4 cover (STORING match)-> +50
 *   <li>S5 extractEqualityColumns extractetcvaluecolumn
 *   <li>S6 Catalog.indexesOf persistence round-trip
 * </ul>
 */
@DisplayName("IndexSelector E2E — Spec 04")
class IndexSelectorE2ETest {

    private InMemoryMetastore metastore;
    private MetastoreCatalog catalog;
    private SnapshotManager snapshotManager;
    private IndexSelector selector;

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        catalog =
                new MetastoreCatalog(
                        MetastoreCatalogConfig.builder()
                                .catalogName("index-selector-e2e")
                                .store(metastore)
                                .basePath("/tmp/index-selector-e2e")
                                .build());
        catalog.createNamespace("db");
        snapshotManager = new SnapshotManager(catalog, metastore);
        selector = new IndexSelector(catalog);

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

        // createindex table(and consistency: table is RawWise table, PK = index column)
        catalog.createTable(
                new TableId("db", "__idx_orders_w_d"),
                Schema.builder()
                        .field(1, "o_w_id", Types.int32(false))
                        .field(2, "o_d_id", Types.int32(false))
                        .field(3, "o_id", Types.int32())
                        .field(4, "o_amount", Types.int64())
                        .primaryKeys("o_w_id", "o_d_id")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());

        catalog.createTable(
                new TableId("db", "__idx_orders_amount"),
                Schema.builder()
                        .field(1, "o_amount", Types.int64(false))
                        .field(2, "o_id", Types.int32())
                        .primaryKeys("o_amount")
                        .build(),
                PartitionSpec.unpartitioned(),
                TableProperties.empty());
    }

    @AfterEach
    void tearDown() throws IOException {
        if (catalog != null) catalog.close();
    }

    private void registerIndexes(IndexMeta... indexes) {
        try {
            Map<String, String> props = new HashMap<>();
            MetastoreCatalog.storeIndexes(props, List.of(indexes));
            catalog.updateProperties(new TableId("db", "orders"), new TableProperties(props));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ScanQueryIR sq(
            String table,
            List<String> projectionNames,
            io.github.rawwise.model.expression.Expression filter) {
        var tableMeta = catalog.loadTable(TableId.parseLenient("db." + table));
        var meta = tableMeta.meta();
        var projections =
                projectionNames == null
                        ? List.<io.github.rawwise.frontend.ir.ProjectionColumn>of()
                        : projectionNames.stream()
                                .map(
                                        n ->
                                                new io.github.rawwise.frontend.ir.ProjectionColumn(
                                                        n, -1, null))
                                .toList();
        return new ScanQueryIR(
                new TableMetaIR(
                        tableMeta.getTableIdentifier(),
                        meta.tableId(),
                        meta.tableType()
                                        == io.github.rawwise.model.record.TableMeta.TableType
                                                .APPEND_ONLY
                                ? TableType.APPEND_ONLY
                                : TableType.PRIMARY_KEY,
                        meta.schema().structType(),
                        meta.primaryKey().fields(),
                        meta.properties(),
                        1),
                0L,
                projections,
                filter,
                List.of(),
                0,
                0,
                null,
                null);
    }

    @Nested
    @DisplayName("S1: noindex")
    class NoIndexTests {

        @Test
        @DisplayName("S1.1: noindex tryRewrite empty")
        void noIndexReturnsEmpty() {
            ScanQueryIR query =
                    sq(
                            "orders",
                            List.of(),
                            new Eq(
                                    new ColumnRef("o_w_id"),
                                    new io.github.rawwise.model.expression.Literal(1L)));
            assertThat(selector.tryRewrite(query)).isEmpty();
        }
    }

    @Nested
    @DisplayName("S2: index prefix coverage")
    class IndexCoverageTests {

        @Test
        @DisplayName("S2.1: index cover before -> in write")
        void indexCoversPredicatePrefix() {
            registerIndexes(
                    new IndexMeta(
                            "idx_orders_w_d",
                            new TableId("db", "orders"),
                            List.of("o_w_id", "o_d_id"),
                            List.of("o_id", "o_amount")));

            ScanQueryIR query =
                    sq(
                            "orders",
                            List.of(),
                            new And(
                                    List.of(
                                            new Eq(
                                                    new ColumnRef("o_w_id"),
                                                    new io.github.rawwise.model.expression.Literal(
                                                            1L)),
                                            new Eq(
                                                    new ColumnRef("o_d_id"),
                                                    new io.github.rawwise.model.expression.Literal(
                                                            7L)))));

            var rewritten = selector.tryRewrite(query);
            assertThat(rewritten).isPresent();
            assertThat(rewritten.get().tableMeta().tableIdentifier()).contains("__idx_orders_w_d");
        }

        @Test
        @DisplayName("S2.2: index coverpartial -> score >= 100 in")
        void partialCoverageStillSelected() {
            registerIndexes(
                    new IndexMeta(
                            "idx_orders_w_d",
                            new TableId("db", "orders"),
                            List.of("o_w_id", "o_d_id"),
                            List.of("o_id")));

            ScanQueryIR query =
                    sq(
                            "orders",
                            List.of(),
                            new Eq(
                                    new ColumnRef("o_w_id"),
                                    new io.github.rawwise.model.expression.Literal(1L)));

            var rewritten = selector.tryRewrite(query);
            assertThat(rewritten).isPresent();
        }
    }

    @Nested
    @DisplayName("S3: multiindex")
    class MultiIndexTests {

        @Test
        @DisplayName("S3.1: index -> (scoreverify)")
        void higherScoreWins() {
            var idxA =
                    new IndexMeta(
                            "idx_a",
                            new TableId("db", "orders"),
                            List.of("o_w_id"),
                            List.of("o_id"));
            var idxB =
                    new IndexMeta(
                            "idx_b",
                            new TableId("db", "orders"),
                            List.of("o_w_id", "o_d_id"),
                            List.of("o_id", "o_amount"));

            Set<String> eqCols = Set.of("o_w_id", "o_d_id");
            Set<String> projCols = Set.of("o_id", "o_amount");

            int scoreA = selector.score(idxA, eqCols, projCols);
            int scoreB = selector.score(idxB, eqCols, projCols);
            assertThat(scoreB).isGreaterThan(scoreA);
        }

        @Test
        @DisplayName("S3.2: same tie-break")
        void sameScoreDictOrder() {
            var idxZ =
                    new IndexMeta(
                            "idx_z", new TableId("db", "orders"), List.of("o_w_id"), List.of());
            var idxA =
                    new IndexMeta(
                            "idx_a", new TableId("db", "orders"), List.of("o_w_id"), List.of());

            Set<String> eqCols = Set.of("o_w_id");
            Set<String> projCols = Set.of();

            int scoreZ = selector.score(idxZ, eqCols, projCols);
            int scoreA = selector.score(idxA, eqCols, projCols);
            assertThat(scoreA).isEqualTo(scoreZ);

            // tie-break: idx_a < idx_z
            var cA = new IndexSelector.Candidate(idxA, scoreA);
            var cZ = new IndexSelector.Candidate(idxZ, scoreZ);
            assertThat(cA.compareTo(cZ)).isNegative();
        }
    }

    @Nested
    @DisplayName("S4: cover (+50)")
    class CoveringOutputTests {

        @Test
        @DisplayName("S4.1: STORING all SELECT column -> score +50")
        void coveringIndexHigherScore() {
            var noCover =
                    new IndexMeta(
                            "nocover",
                            new TableId("db", "orders"),
                            List.of("o_w_id", "o_d_id"),
                            List.of("o_id"));
            var cover =
                    new IndexMeta(
                            "cover",
                            new TableId("db", "orders"),
                            List.of("o_w_id", "o_d_id"),
                            List.of("o_id", "o_amount"));

            Set<String> eqCols = Set.of("o_w_id", "o_d_id");
            Set<String> projCols = Set.of("o_id", "o_amount");

            int scoreNoCover = selector.score(noCover, eqCols, projCols);
            int scoreCover = selector.score(cover, eqCols, projCols);
            assertThat(scoreCover).isGreaterThan(scoreNoCover);
        }
    }

    @Nested
    @DisplayName("S5: extractEqualityColumns")
    class EqualityExtractionTests {

        @Test
        @DisplayName("S5.1: AND inextractetcvaluecolumn")
        void extractFromAnd() {
            Set<String> cols =
                    IndexSelector.extractEqualityColumns(
                            new And(
                                    List.of(
                                            new Eq(
                                                    new ColumnRef("a"),
                                                    new io.github.rawwise.model.expression.Literal(
                                                            1L)),
                                            new Eq(
                                                    new ColumnRef("b"),
                                                    new io.github.rawwise.model.expression.Literal(
                                                            2L)))));
            assertThat(cols).containsExactlyInAnyOrder("a", "b");
        }

        @Test
        @DisplayName("S5.2: single Eq directlyextract")
        void extractSingleEq() {
            Set<String> cols =
                    IndexSelector.extractEqualityColumns(
                            new Eq(
                                    new ColumnRef("x"),
                                    new io.github.rawwise.model.expression.Literal(42L)));
            assertThat(cols).containsExactly("x");
        }

        @Test
        @DisplayName("S5.3: null filter -> empty")
        void nullFilter() {
            assertThat(IndexSelector.extractEqualityColumns(null)).isEmpty();
        }
    }

    @Nested
    @DisplayName("S6: Catalog.indexesOf persistence round-trip")
    class CatalogIndexPersistenceTests {

        @Test
        @DisplayName("S6.1: storeIndexes + indexesOf consistent")
        void roundtripStoreAndLoad() throws Exception {
            IndexMeta idx =
                    new IndexMeta(
                            "idx_test",
                            new TableId("db", "orders"),
                            List.of("col_a", "col_b"),
                            List.of("col_c"));

            Map<String, String> props = new HashMap<>();
            MetastoreCatalog.storeIndexes(props, List.of(idx));
            catalog.updateProperties(new TableId("db", "orders"), new TableProperties(props));

            List<IndexMeta> loaded = catalog.indexesOf(new TableId("db", "orders"));
            assertThat(loaded).hasSize(1);
            assertThat(loaded.get(0).indexName()).isEqualTo("idx_test");
            assertThat(loaded.get(0).columns()).containsExactly("col_a", "col_b");
            assertThat(loaded.get(0).storing()).containsExactly("col_c");
        }

        @Test
        @DisplayName("S6.2: noindexreturns empty list")
        void noIndexesReturnsEmpty() {
            List<IndexMeta> loaded = catalog.indexesOf(new TableId("db", "orders"));
            assertThat(loaded).isEmpty();
        }
    }
}
