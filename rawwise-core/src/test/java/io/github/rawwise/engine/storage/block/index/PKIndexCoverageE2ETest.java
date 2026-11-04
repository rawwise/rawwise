package io.github.rawwise.engine.storage.block.index;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class PKIndexCoverageE2ETest {

    private PrimaryKeyIndex pkIndex;

    @BeforeEach
    void setUp() {
        pkIndex = new PrimaryKeyIndex();
    }

    private static byte[] k(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Nested
    @DisplayName("T11: PrimaryKeyIndexcover")
    class CoverageTests {

        @Test
        @DisplayName("E2E-12: indexcoverage EMPTY")
        void shouldReturnEmptyWhenIndexIsEmpty() {
            assertThat(pkIndex.coverage()).isEqualTo(PrimaryKeyIndex.IndexCoverage.EMPTY);
        }

        @Test
        @DisplayName("E2E-13: hasdataindexcoverage UNKNOWN")
        void shouldReturnUnknownWhenIndexHasData() {
            pkIndex.put(k("key1"), "block-1", 0, 1L);
            assertThat(pkIndex.coverage()).isEqualTo(PrimaryKeyIndex.IndexCoverage.UNKNOWN);
        }

        @Test
        @DisplayName("clearaftercoverage EMPTY")
        void shouldReturnEmptyAfterClear() {
            pkIndex.put(k("key1"), "block-1", 0, 1L);
            pkIndex.clear();
            assertThat(pkIndex.coverage()).isEqualTo(PrimaryKeyIndex.IndexCoverage.EMPTY);
        }
    }

    @Nested
    @DisplayName("T10: PKIndexcan filterlogic")
    class VisibilityFilterTests {

        @Test
        @DisplayName("E2E-14: PKIndexpoint queryreturns correctly")
        void shouldLookupCorrectly() {
            pkIndex.put(k("user1"), "block-A", 0, 1L);
            pkIndex.put(k("user2"), "block-B", 0, 1L);

            List<PrimaryKeyIndex.PKLocation> locs = pkIndex.lookup(k("user1"));
            assertThat(locs).hasSize(1);
            assertThat(locs.get(0).blockId()).isEqualTo("block-A");
        }

        @Test
        @DisplayName("E2E-15: lookupnon-existent keyreturns empty list")
        void shouldReturnEmptyForMissingKey() {
            pkIndex.put(k("user1"), "block-A", 0, 1L);

            List<PrimaryKeyIndex.PKLocation> locs = pkIndex.lookup(k("nonexistent"));
            assertThat(locs).isEmpty();
        }

        @Test
        @DisplayName("E2E-16: removeBlockafterlookup alreadydeleteblockof")
        void shouldNotReturnRemovedBlockLocations() {
            pkIndex.put(k("user1"), "block-A", 0, 1L);
            pkIndex.put(k("user1"), "block-B", 0, 2L);

            pkIndex.removeBlock("block-A");

            List<PrimaryKeyIndex.PKLocation> locs = pkIndex.lookup(k("user1"));
            assertThat(locs).hasSize(1);
            assertThat(locs.get(0).blockId()).isEqualTo("block-B");
        }

        @Test
        @DisplayName("E2E-17: samekeymultipleblock")
        void shouldReturnMultipleLocationsForSameKey() {
            pkIndex.put(k("user1"), "block-A", 0, 1L);
            pkIndex.put(k("user1"), "block-B", 2, 2L);

            List<PrimaryKeyIndex.PKLocation> locs = pkIndex.lookup(k("user1"));
            assertThat(locs).hasSize(2);
        }

        @Test
        @DisplayName("E2E-18: indexlookupreturns empty list")
        void shouldReturnEmptyForEmptyIndex() {
            List<PrimaryKeyIndex.PKLocation> locs = pkIndex.lookup(k("any"));
            assertThat(locs).isEmpty();
        }

        @Test
        @DisplayName("mightContaincorrect keyis in")
        void shouldCheckKeyExistence() {
            pkIndex.put(k("user1"), "block-A", 0, 1L);
            assertThat(pkIndex.mightContain(k("user1"))).isTrue();
            assertThat(pkIndex.mightContain(k("nonexistent"))).isFalse();
        }
    }

    @Nested
    @DisplayName("T11: column / column maintaincover")
    class SerializationTests {

        @Test
        @DisplayName("column column afterdata")
        void shouldPreserveDataAfterSerialization() throws IOException {
            pkIndex.put(k("key1"), "block-A", 0, 1L);
            pkIndex.put(k("key2"), "block-B", 1, 2L);

            byte[] data = pkIndex.serialize();
            PrimaryKeyIndex restored = new PrimaryKeyIndex();
            restored.deserializeAndMerge(data);

            assertThat(restored.lookup(k("key1"))).hasSize(1);
            assertThat(restored.lookup(k("key2"))).hasSize(1);
            assertThat(restored.coverage()).isEqualTo(PrimaryKeyIndex.IndexCoverage.UNKNOWN);
        }
    }
}
