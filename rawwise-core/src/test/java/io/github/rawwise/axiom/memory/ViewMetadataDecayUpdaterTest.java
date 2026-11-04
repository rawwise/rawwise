package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName(
        "ViewMetadataDecayUpdater - batch decay ViewMetadata.decayFactor(does not modify View/Raw this)")
class ViewMetadataDecayUpdaterTest {

    private static final String TABLE_ID = "default.dev_dialogs";
    private static final long HALF_LIFE_MS = (long) DecayPolicy.DEFAULT.halfLifeMs();

    private Metastore metastore;
    private ViewMetadataStore store;
    private ViewMetadataDecayUpdater updater;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        store = new ViewMetadataStore(metastore);
        updater = new ViewMetadataDecayUpdater(store, DecayPolicy.DEFAULT);
    }

    private ViewMetadata entryAtTime(String entryKey, long lastAccessedAt, float decayFactor) {
        return new ViewMetadata(entryKey, TABLE_ID, decayFactor, lastAccessedAt, 0);
    }

    private ViewMetadata fullEntryAtTime(String entryKey, long lastAccessedAt, float decayFactor) {
        return new ViewMetadata(entryKey, TABLE_ID, decayFactor, lastAccessedAt, 3);
    }

    @Nested
    @DisplayName("EmptyTable - empty tabledecay")
    class EmptyTable {

        @Test
        @DisplayName("decayTable forempty tablereturns 0")
        void decayTableEmptyReturnsZero() throws IOException {
            int decayed = updater.decayTable(TABLE_ID);

            assertThat(decayed).isZero();
        }
    }

    @Nested
    @DisplayName("FreshData - newdatanotdecay")
    class FreshData {

        @Test
        @DisplayName("decayTable fornewdata(age < halfLife)notdecay")
        void decayTableFreshDataNotDecayed() throws IOException {
            long now = System.currentTimeMillis();
            store.put(entryAtTime("fresh-1", now, 1.0f));

            int decayed = updater.decayTable(TABLE_ID);

            assertThat(decayed).isZero();
            Optional<ViewMetadata> retrieved = store.get(TABLE_ID, "fresh-1");
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor()).isEqualTo(1.0f);
        }
    }

    @Nested
    @DisplayName("StaleData - olddatadecay")
    class StaleData {

        @Test
        @DisplayName("decayTable forolddata(age > halfLife)decay decayFactor")
        void decayTableStaleDataDecayed() throws IOException {
            long now = System.currentTimeMillis();
            long oldTime = now - 2 * HALF_LIFE_MS;
            store.put(entryAtTime("stale-1", oldTime, 1.0f));

            int decayed = updater.decayTable(TABLE_ID);

            assertThat(decayed).isEqualTo(1);
            Optional<ViewMetadata> retrieved = store.get(TABLE_ID, "stale-1");
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor()).isNotEqualTo(1.0f);
        }

        @Test
        @DisplayName("decayTable decayafter decayFactor small")
        void decayTableDecayFactorDecreases() throws IOException {
            long now = System.currentTimeMillis();
            long oldTime = now - 2 * HALF_LIFE_MS;
            store.put(entryAtTime("stale-2", oldTime, 1.0f));

            updater.decayTable(TABLE_ID);

            Optional<ViewMetadata> retrieved = store.get(TABLE_ID, "stale-2");
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor()).isLessThan(1.0f);
            assertThat(retrieved.get().decayFactor()).isGreaterThan(0.0f);
        }

        @Test
        @DisplayName("decayTable decayafter ViewMetadata otherfieldnot(only decayFactor)")
        void decayTablePreservesOtherFields() throws IOException {
            long now = System.currentTimeMillis();
            long oldTime = now - 2 * HALF_LIFE_MS;
            ViewMetadata original = fullEntryAtTime("stale-full", oldTime, 1.0f);
            store.put(original);

            updater.decayTable(TABLE_ID);

            Optional<ViewMetadata> retrieved = store.get(TABLE_ID, "stale-full");
            assertThat(retrieved).isPresent();
            ViewMetadata got = retrieved.get();
            assertThat(got.entryKey()).isEqualTo("stale-full");
            assertThat(got.tableId()).isEqualTo(TABLE_ID);
            assertThat(got.lastAccessedAt()).isEqualTo(oldTime);
            assertThat(got.accessCount()).isEqualTo(3);
            assertThat(got.decayFactor()).isLessThan(1.0f);
        }
    }

    @Nested
    @DisplayName("MixedData - newolddata")
    class MixedData {

        @Test
        @DisplayName("decayTable multipledatapartialdecay(newolddata)")
        void decayTableMixedDataPartialDecay() throws IOException {
            long now = System.currentTimeMillis();
            long oldTime = now - 2 * HALF_LIFE_MS;
            store.put(entryAtTime("fresh-a", now, 1.0f));
            store.put(entryAtTime("fresh-b", now, 1.0f));
            store.put(entryAtTime("stale-a", oldTime, 1.0f));
            store.put(entryAtTime("stale-b", oldTime, 1.0f));

            int decayed = updater.decayTable(TABLE_ID);

            assertThat(decayed).isEqualTo(2);
            assertThat(store.get(TABLE_ID, "fresh-a").get().decayFactor()).isEqualTo(1.0f);
            assertThat(store.get(TABLE_ID, "fresh-b").get().decayFactor()).isEqualTo(1.0f);
            assertThat(store.get(TABLE_ID, "stale-a").get().decayFactor()).isLessThan(1.0f);
            assertThat(store.get(TABLE_ID, "stale-b").get().decayFactor()).isLessThan(1.0f);
        }
    }

    @Nested
    @DisplayName("SkipNoOp - skipmeaningless write")
    class SkipNoOp {

        @Test
        @DisplayName("decayFactor < 0.01 whenskipupdate(meaningless write)")
        void decayTableSkipWhenChangeBelowThreshold() throws IOException {
            long now = System.currentTimeMillis();
            long veryOldTime = now - 100 * HALF_LIFE_MS;
            store.put(
                    entryAtTime(
                            "noop-1", veryOldTime, (float) DecayPolicy.DEFAULT.minDecayFactor()));

            int decayed = updater.decayTable(TABLE_ID);

            assertThat(decayed).isZero();
            Optional<ViewMetadata> retrieved = store.get(TABLE_ID, "noop-1");
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor())
                    .isEqualTo((float) DecayPolicy.DEFAULT.minDecayFactor());
        }
    }

    @Nested
    @DisplayName("DefaultPolicy - default strategyoverload")
    class DefaultPolicy {

        @Test
        @DisplayName(
                "decayTable(tableId) usedefault policy andexplicit DEFAULT policy rowisconsistent")
        void decayTableDefaultPolicyMatchesExplicit() throws IOException {
            long now = System.currentTimeMillis();
            long oldTime = now - 2 * HALF_LIFE_MS;
            store.put(entryAtTime("default-1", oldTime, 1.0f));

            int decayedDefault = updater.decayTable(TABLE_ID);
            int decayedExplicit = updater.decayTable(TABLE_ID, DecayPolicy.DEFAULT);

            assertThat(decayedDefault).isEqualTo(1);
            assertThat(decayedExplicit).isZero();
        }
    }
}
