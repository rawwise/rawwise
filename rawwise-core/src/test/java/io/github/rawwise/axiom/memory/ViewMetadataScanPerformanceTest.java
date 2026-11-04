package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("ViewMetadataScanPerformance - scanByTable performance baselinetest")
class ViewMetadataScanPerformanceTest {

    private static final int ENTRY_COUNT = 1000;
    private static final long SCAN_BASELINE_MS = 50;

    private Metastore metastore;
    private ViewMetadataStore store;

    @BeforeEach
    void setUp() throws IOException {
        metastore = new InMemoryMetastore();
        store = new ViewMetadataStore(metastore);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            ViewMetadata md =
                    new ViewMetadata("perf-entry-" + i, "default.dev_dialogs", 1.0f, 0, i % 10);
            store.put(md);
        }
    }

    @Test
    @DisplayName("scanByTable 1000 entries ViewMetadata takeswhen < 50ms")
    void scanByTablePerformanceUnder50ms() throws IOException {
        long start = System.nanoTime();
        List<ViewMetadata> results = store.scanByTable("default.dev_dialogs", 10_000);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(results).hasSize(ENTRY_COUNT);
        assertThat(elapsedMs)
                .as(
                        "scanByTable for %d entries should complete within %d ms, took %d ms",
                        ENTRY_COUNT, SCAN_BASELINE_MS, elapsedMs)
                .isLessThan(SCAN_BASELINE_MS);
    }

    @Test
    @DisplayName("scanByTable limit 100 whenreturns 100 entriesandtakeswhen < 50ms")
    void scanByTableWithLimitPerformance() throws IOException {
        long start = System.nanoTime();
        List<ViewMetadata> results = store.scanByTable("default.dev_dialogs", 100);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(results).hasSize(100);
        assertThat(elapsedMs)
                .as(
                        "scanByTable limit=100 should complete within %d ms, took %d ms",
                        SCAN_BASELINE_MS, elapsedMs)
                .isLessThan(SCAN_BASELINE_MS);
    }

    @Test
    @DisplayName("scanByTable cross-table isolation:onlyreturnsspecifiedtable'sdata")
    void scanByTableIsolatesTables() throws IOException {
        for (int i = 0; i < 100; i++) {
            store.put(new ViewMetadata("docs-entry-" + i, "default.project_docs"));
        }

        long start = System.nanoTime();
        List<ViewMetadata> dialogs = store.scanByTable("default.dev_dialogs", 10_000);
        List<ViewMetadata> docs = store.scanByTable("default.project_docs", 10_000);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(dialogs).hasSize(ENTRY_COUNT);
        assertThat(docs).hasSize(100);
        assertThat(elapsedMs)
                .as(
                        "two scanByTable calls should complete within %d ms, took %d ms",
                        SCAN_BASELINE_MS, elapsedMs)
                .isLessThan(SCAN_BASELINE_MS);
    }

    @Test
    @DisplayName("scanByTable resultcancorrectdeserialization")
    void scanByTableResultsDeserializable() throws IOException {
        List<ViewMetadata> results = store.scanByTable("default.dev_dialogs", 10_000);

        assertThat(results).hasSize(ENTRY_COUNT);
        ViewMetadata first = results.get(0);
        assertThat(first.entryKey()).startsWith("perf-entry-");
        assertThat(first.tableId()).isEqualTo("default.dev_dialogs");
        assertThat(first.decayFactor()).isEqualTo(1.0f);
    }
}
