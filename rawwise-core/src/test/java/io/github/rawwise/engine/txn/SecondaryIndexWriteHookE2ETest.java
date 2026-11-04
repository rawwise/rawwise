package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.common.catalog.MetastoreCatalog;
import io.github.rawwise.common.catalog.MetastoreCatalogConfig;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.model.record.IndexMeta;
import io.github.rawwise.model.record.TableId;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * SecondaryIndexWriteHook E2E: index key/value decode + tablematch.
 *
 * <p>coverage scenario:
 *
 * <ul>
 *   <li>S1 single-column index key encode
 *   <li>S2 multi-column composite index key encode(U+001F)
 *   <li>S3 STORING value encode
 *   <li>S4 missingindex column -> key=null(skipindexwrite)
 *   <li>S5 matchesTable table filter
 *   <li>S6 SecondaryIndexManager hook register/query/remove
 * </ul>
 */
@DisplayName("SecondaryIndexWriteHook E2E — Spec 04")
class SecondaryIndexWriteHookE2ETest {

    @Nested
    @DisplayName("S1-S3: Key & Value encode")
    class EncodingTests {

        @Test
        @DisplayName("S1.1: single-column index key encode = columnvalue")
        void singleColumnKeyEncoding() {
            var hook =
                    new SecondaryIndexWriteHook(
                            new IndexMeta(
                                    "idx", new TableId("db", "t"), List.of("status"), List.of()),
                            "db.t");

            byte[] key = hook.encodeIndexKey(Map.of("status", "ACTIVE", "name", "foo"));
            assertThat(key).isNotEmpty();
            assertThat(new String(key, StandardCharsets.UTF_8)).isEqualTo("ACTIVE");
        }

        @Test
        @DisplayName("S2.1: composite index key = col1 \\x1F col2")
        void compositeKeyEncoding() {
            var hook =
                    new SecondaryIndexWriteHook(
                            new IndexMeta(
                                    "idx2",
                                    new TableId("db", "t"),
                                    List.of("w_id", "d_id"),
                                    List.of()),
                            "db.t");

            byte[] key = hook.encodeIndexKey(Map.of("w_id", 1, "d_id", 7, "name", "x"));
            assertThat(key).isNotEmpty();
            // U+001F separator
            assertThat(new String(key, StandardCharsets.UTF_8)).contains("\u001F");
            // value
            assertThat(new String(key, StandardCharsets.UTF_8)).startsWith("1\u001F7");
        }

        @Test
        @DisplayName("S3.1: STORING value encode")
        void storingValueEncoding() {
            var hook =
                    new SecondaryIndexWriteHook(
                            new IndexMeta(
                                    "idx3",
                                    new TableId("db", "t"),
                                    List.of("pk"),
                                    List.of("name", "amount")),
                            "db.t");

            byte[] value =
                    hook.encodeIndexValue(Map.of("pk", 100, "name", "Alice", "amount", 250L));
            assertThat(value).isNotEmpty();
            assertThat(new String(value, StandardCharsets.UTF_8)).isEqualTo("Alice\u001F250");
        }

        @Test
        @DisplayName("S3.2: STORING is -> value")
        void emptyStoringProducesEmptyValue() {
            var hook =
                    new SecondaryIndexWriteHook(
                            new IndexMeta("idx", new TableId("db", "t"), List.of("col"), List.of()),
                            "db.t");

            byte[] value = hook.encodeIndexValue(Map.of("col", 1));
            assertThat(value).isEmpty();
        }
    }

    @Nested
    @DisplayName("S4: boundary conditions")
    class BoundaryTests {

        @Test
        @DisplayName("S4.1: missingindex column -> key=null")
        void missingIndexColumnReturnsNull() {
            var hook =
                    new SecondaryIndexWriteHook(
                            new IndexMeta(
                                    "idx", new TableId("db", "t"), List.of("a", "b"), List.of()),
                            "db.t");

            // missing b column
            byte[] key = hook.encodeIndexKey(Map.of("a", 1, "c", 3));
            assertThat(key).isNull();
        }

        @Test
        @DisplayName("S4.2: STORING columnpartialmissing ->")
        void partialStoringColumns() {
            var hook =
                    new SecondaryIndexWriteHook(
                            new IndexMeta(
                                    "idx",
                                    new TableId("db", "t"),
                                    List.of("pk"),
                                    List.of("val1", "val2")),
                            "db.t");

            // val2 missing
            byte[] value = hook.encodeIndexValue(Map.of("pk", 1, "val1", "hello"));
            assertThat(new String(value, StandardCharsets.UTF_8)).isEqualTo("hello\u001F");
        }
    }

    @Nested
    @DisplayName("S5: table match")
    class TableMatchingTests {

        @Test
        @DisplayName("S5.1: matchof table -> true")
        void matchesCorrectTable() {
            var hook =
                    new SecondaryIndexWriteHook(
                            new IndexMeta("idx", new TableId("db", "t"), List.of("col"), List.of()),
                            "db.t");
            assertThat(hook.matchesTable("db.t")).isTrue();
        }

        @Test
        @DisplayName("S5.2: mismatched table -> false")
        void mismatchesOtherTable() {
            var hook =
                    new SecondaryIndexWriteHook(
                            new IndexMeta("idx", new TableId("db", "t"), List.of("col"), List.of()),
                            "db.t");
            assertThat(hook.matchesTable("db.other")).isFalse();
        }
    }

    @Nested
    @DisplayName("S6: SecondaryIndexManager lifecycle")
    class ManagerTests {

        private MetastoreCatalog catalog;

        @BeforeEach
        void setUp() throws IOException {
            catalog =
                    new MetastoreCatalog(
                            MetastoreCatalogConfig.builder()
                                    .catalogName("idx-manager")
                                    .store(new InMemoryMetastore())
                                    .basePath("/tmp/idx-manager")
                                    .build());
        }

        @Test
        @DisplayName("S6.1: registerHook -> getHook")
        void registerAndRetrieve() {
            var mgr = new SecondaryIndexManager(catalog);
            var idx = new IndexMeta("idx1", new TableId("db", "t"), List.of("col"), List.of());

            mgr.registerHook(idx, null, "db.t");
            SecondaryIndexWriteHook h = mgr.getHook("idx1");
            assertThat(h).isNotNull();
            assertThat(h.indexMeta().indexName()).isEqualTo("idx1");
            assertThat(h.mainTableIdentifier()).isEqualTo("db.t");
        }

        @Test
        @DisplayName("S6.2: unregister -> getHook null")
        void unregisterRemovesHook() {
            var mgr = new SecondaryIndexManager(catalog);
            mgr.registerHook(
                    new IndexMeta("tmp", new TableId("db", "t"), List.of("c"), List.of()),
                    null,
                    "db.t");
            mgr.unregisterHook("tmp");
            assertThat(mgr.getHook("tmp")).isNull();
        }

        @Test
        @DisplayName("S6.3: allHooks allalreadyregister Hook")
        void allHooksReturnsAll() {
            var mgr = new SecondaryIndexManager(catalog);
            mgr.registerHook(
                    new IndexMeta("a", new TableId("db", "t"), List.of("c1"), List.of()),
                    null,
                    "db.t");
            mgr.registerHook(
                    new IndexMeta("b", new TableId("db", "t"), List.of("c2"), List.of()),
                    null,
                    "db.t");

            assertThat(mgr.allHooks()).hasSize(2);
        }
    }
}
