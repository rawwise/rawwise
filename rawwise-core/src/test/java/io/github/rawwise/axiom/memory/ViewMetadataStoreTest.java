package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ViewMetadataStore - ViewMetadata side table Metastore access test")
class ViewMetadataStoreTest {

    private Metastore metastore;
    private ViewMetadataStore store;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        store = new ViewMetadataStore(metastore);
    }

    @Nested
    @DisplayName("PutGet - basicaccess")
    class PutGet {

        @Test
        @DisplayName("put + get round-trip:stored ViewMetadata,read outafterfieldconsistent")
        void putGetRoundTrip() throws IOException {
            ViewMetadata original =
                    new ViewMetadata("entry-1", "default.dev_dialogs").withDecayFactor(0.75f);

            store.put(original);

            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", "entry-1");

            assertThat(retrieved).isPresent();
            ViewMetadata md = retrieved.get();
            assertThat(md.entryKey()).isEqualTo("entry-1");
            assertThat(md.tableId()).isEqualTo("default.dev_dialogs");
            assertThat(md.decayFactor()).isEqualTo(0.75f);
        }

        @Test
        @DisplayName("get non-existent key returns Optional.empty()")
        void getNonExistentReturnsEmpty() throws IOException {
            Optional<ViewMetadata> result = store.get("default.dev_dialogs", "nonexistent");

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("default constructed's ViewMetadata fielddefault valuecorrect")
        void defaultConstructorDefaults() throws IOException {
            ViewMetadata md = new ViewMetadata("key-1", "default.project_docs");

            store.put(md);
            Optional<ViewMetadata> retrieved = store.get("default.project_docs", "key-1");

            assertThat(retrieved).isPresent();
            ViewMetadata got = retrieved.get();
            assertThat(got.decayFactor()).isEqualTo(1.0f);
            assertThat(got.accessCount()).isZero();
        }
    }

    @Nested
    @DisplayName("ScanByTable - scan by table")
    class ScanByTable {

        @Test
        @DisplayName("scanByTable returns table underall ViewMetadata")
        void scanByTableReturnsAllEntries() throws IOException {
            store.put(new ViewMetadata("k1", "default.dev_dialogs"));
            store.put(new ViewMetadata("k2", "default.dev_dialogs"));
            store.put(new ViewMetadata("k3", "default.dev_dialogs"));

            List<ViewMetadata> results = store.scanByTable("default.dev_dialogs", 100);

            assertThat(results).hasSize(3);
            assertThat(results)
                    .extracting(ViewMetadata::entryKey)
                    .containsExactlyInAnyOrder("k1", "k2", "k3");
        }

        @Test
        @DisplayName("scanByTable limit take effect")
        void scanByTableLimitEnforced() throws IOException {
            for (int i = 0; i < 10; i++) {
                store.put(new ViewMetadata("k" + i, "default.dev_dialogs"));
            }

            List<ViewMetadata> results = store.scanByTable("default.dev_dialogs", 3);

            assertThat(results).hasSize(3);
        }

        @Test
        @DisplayName("scanByTable onlyreturnsspecified table 'sdata(different table isolation)")
        void scanByTableIsolatesTables() throws IOException {
            store.put(new ViewMetadata("a1", "default.dev_dialogs"));
            store.put(new ViewMetadata("a2", "default.dev_dialogs"));
            store.put(new ViewMetadata("b1", "default.project_docs"));
            store.put(new ViewMetadata("b2", "default.project_docs"));
            store.put(new ViewMetadata("b3", "default.project_docs"));

            List<ViewMetadata> dialogsResults = store.scanByTable("default.dev_dialogs", 100);
            List<ViewMetadata> docsResults = store.scanByTable("default.project_docs", 100);

            assertThat(dialogsResults).hasSize(2);
            assertThat(dialogsResults)
                    .extracting(ViewMetadata::entryKey)
                    .containsExactlyInAnyOrder("a1", "a2");
            assertThat(docsResults).hasSize(3);
            assertThat(docsResults)
                    .extracting(ViewMetadata::entryKey)
                    .containsExactlyInAnyOrder("b1", "b2", "b3");
        }

        @Test
        @DisplayName("scanByTable empty table returns empty list")
        void scanByTableEmptyReturnsEmptyList() throws IOException {
            List<ViewMetadata> results = store.scanByTable("nonexistent.table", 100);

            assertThat(results).isEmpty();
        }
    }

    @Nested
    @DisplayName("CAS - Compare And Swap")
    class CompareAndSwap {

        @Test
        @DisplayName("CAS NotExists:firstwritesuccess")
        void casNotExistsFirstWriteSucceeds() throws IOException {
            ViewMetadata md = new ViewMetadata("cas-1", "default.dev_dialogs");

            boolean success = store.compareAndSet("default.dev_dialogs", "cas-1", null, md);

            assertThat(success).isTrue();
            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", "cas-1");
            assertThat(retrieved).isPresent();
        }

        @Test
        @DisplayName("CAS NotExists:repeated writefailure(key alreadyexists)")
        void casNotExistsDuplicateFails() throws IOException {
            ViewMetadata md = new ViewMetadata("cas-2", "default.dev_dialogs");
            store.put(md);

            ViewMetadata another =
                    new ViewMetadata("cas-2", "default.dev_dialogs").withDecayFactor(0.5f);
            boolean success = store.compareAndSet("default.dev_dialogs", "cas-2", null, another);

            assertThat(success).isFalse();
            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", "cas-2");
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor()).isEqualTo(1.0f);
        }

        @Test
        @DisplayName("CAS ValueEquals:value matcheswhenupdatesuccess")
        void casValueEqualsMatchSucceeds() throws IOException {
            ViewMetadata original = new ViewMetadata("cas-3", "default.dev_dialogs");
            store.put(original);

            ViewMetadata updated = original.withDecayFactor(0.5f);
            boolean success =
                    store.compareAndSet("default.dev_dialogs", "cas-3", original, updated);

            assertThat(success).isTrue();
            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", "cas-3");
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor()).isEqualTo(0.5f);
        }

        @Test
        @DisplayName("CAS ValueEquals:value does not matchwhenupdatefailure")
        void casValueEqualsMismatchFails() throws IOException {
            ViewMetadata original = new ViewMetadata("cas-4", "default.dev_dialogs");
            store.put(original);

            ViewMetadata staleExpected = original.withDecayFactor(0.5f);
            ViewMetadata newValue = original.withDecayFactor(0.3f);
            boolean success =
                    store.compareAndSet("default.dev_dialogs", "cas-4", staleExpected, newValue);

            assertThat(success).isFalse();
            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", "cas-4");
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor()).isEqualTo(1.0f);
        }

        @Test
        @DisplayName("CAS chainstyleupdate:read-modify-write loop")
        void casChainedUpdates() throws IOException {
            ViewMetadata md = new ViewMetadata("cas-chain", "default.dev_dialogs");
            store.compareAndSet("default.dev_dialogs", "cas-chain", null, md);

            Optional<ViewMetadata> current = store.get("default.dev_dialogs", "cas-chain");
            assertThat(current).isPresent();
            ViewMetadata withDecay = current.get().withDecayFactor(0.5f);
            assertThat(
                            store.compareAndSet(
                                    "default.dev_dialogs", "cas-chain", current.get(), withDecay))
                    .isTrue();

            current = store.get("default.dev_dialogs", "cas-chain");
            assertThat(current).isPresent();
            ViewMetadata withAccess = current.get().withAccessUpdate(1_234_567_890L);
            assertThat(
                            store.compareAndSet(
                                    "default.dev_dialogs", "cas-chain", current.get(), withAccess))
                    .isTrue();

            current = store.get("default.dev_dialogs", "cas-chain");
            assertThat(current).isPresent();
            assertThat(current.get().decayFactor()).isEqualTo(0.5f);
            assertThat(current.get().accessCount()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("Delete - delete")
    class Delete {

        @Test
        @DisplayName("delete after get returns empty")
        void deleteThenGetReturnsEmpty() throws IOException {
            ViewMetadata md = new ViewMetadata("del-1", "default.dev_dialogs");
            store.put(md);
            assertThat(store.get("default.dev_dialogs", "del-1")).isPresent();

            store.delete("default.dev_dialogs", "del-1");

            assertThat(store.get("default.dev_dialogs", "del-1")).isEmpty();
        }

        @Test
        @DisplayName("delete non-existent key does not throw")
        void deleteNonExistentNoThrow() {
            assertThatCode(() -> store.delete("default.dev_dialogs", "nonexistent"))
                    .doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("Update - updateoperation")
    class Update {

        @Test
        @DisplayName("update overridealreadyhasvalue")
        void updateOverwritesExisting() throws IOException {
            ViewMetadata original = new ViewMetadata("upd-1", "default.dev_dialogs");
            store.put(original);

            ViewMetadata updated =
                    new ViewMetadata("upd-1", "default.dev_dialogs").withDecayFactor(0.2f);
            boolean success = store.update("default.dev_dialogs", "upd-1", updated);

            assertThat(success).isTrue();
            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", "upd-1");
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor()).isEqualTo(0.2f);
        }
    }

    @Nested
    @DisplayName("Validation - ViewMetadata boundary validation")
    class Validation {

        @Test
        @DisplayName("decayFactor 0.0 legal")
        void decayFactorZeroIsValid() {
            ViewMetadata md = new ViewMetadata("v-1", "default.dev_dialogs").withDecayFactor(0.0f);

            assertThat(md.decayFactor()).isEqualTo(0.0f);
        }

        @Test
        @DisplayName("decayFactor 1.0 legal")
        void decayFactorOneIsValid() {
            ViewMetadata md = new ViewMetadata("v-2", "default.dev_dialogs").withDecayFactor(1.0f);

            assertThat(md.decayFactor()).isEqualTo(1.0f);
        }

        @Test
        @DisplayName("decayFactor -0.1 exception")
        void decayFactorNegativeThrows() {
            assertThatThrownBy(
                            () ->
                                    new ViewMetadata("v-3", "default.dev_dialogs")
                                            .withDecayFactor(-0.1f))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("decayFactor");
        }

        @Test
        @DisplayName("decayFactor 1.1 exception")
        void decayFactorAboveOneThrows() {
            assertThatThrownBy(
                            () ->
                                    new ViewMetadata("v-4", "default.dev_dialogs")
                                            .withDecayFactor(1.1f))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("decayFactor");
        }

        @Test
        @DisplayName("entryKey is null exception")
        void entryKeyNullThrows() {
            assertThatThrownBy(() -> new ViewMetadata(null, "default.dev_dialogs"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("entryKey");
        }

        @Test
        @DisplayName("entryKey isempty stringexception")
        void entryKeyEmptyThrows() {
            assertThatThrownBy(() -> new ViewMetadata("", "default.dev_dialogs"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("entryKey");
        }

        @Test
        @DisplayName("tableId is null exception")
        void tableIdNullThrows() {
            assertThatThrownBy(() -> new ViewMetadata("key-1", null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("tableId");
        }

        @Test
        @DisplayName("tableId isempty stringexception")
        void tableIdEmptyThrows() {
            assertThatThrownBy(() -> new ViewMetadata("key-1", ""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("tableId");
        }
    }

    @Nested
    @DisplayName("KeySeparator - entryKey contains colon when <NUL> min symbol verify")
    class KeySeparator {

        @Test
        @DisplayName("entryKey contains colonwhen put + get correct")
        void entryKeyWithColonsPutGet() throws IOException {
            String entryKey = "rec:turn:2026-07-21:Q";
            ViewMetadata md =
                    new ViewMetadata(entryKey, "default.dev_dialogs").withDecayFactor(0.9f);

            store.put(md);

            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", entryKey);

            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().entryKey()).isEqualTo(entryKey);
            assertThat(retrieved.get().decayFactor()).isEqualTo(0.9f);
        }

        @Test
        @DisplayName("entryKey contains colonwhen scanByTable correctly returns")
        void entryKeyWithColonsScan() throws IOException {
            store.put(new ViewMetadata("rec:turn:2026-07-21:Q", "default.dev_dialogs"));
            store.put(new ViewMetadata("rec:turn:2026-07-21:A", "default.dev_dialogs"));
            store.put(new ViewMetadata("doc:design:memory", "default.dev_dialogs"));

            List<ViewMetadata> results = store.scanByTable("default.dev_dialogs", 100);

            assertThat(results).hasSize(3);
            assertThat(results)
                    .extracting(ViewMetadata::entryKey)
                    .containsExactlyInAnyOrder(
                            "rec:turn:2026-07-21:Q", "rec:turn:2026-07-21:A", "doc:design:memory");
        }

        @Test
        @DisplayName("entryKey contains colonwhendoes notother table")
        void entryKeyWithColonsTableIsolation() throws IOException {
            String sameKey = "rec:turn:2026-07-21:Q";
            store.put(new ViewMetadata(sameKey, "default.dev_dialogs").withDecayFactor(0.4f));
            store.put(new ViewMetadata(sameKey, "default.project_docs").withDecayFactor(0.6f));

            Optional<ViewMetadata> dialogMd = store.get("default.dev_dialogs", sameKey);
            Optional<ViewMetadata> docMd = store.get("default.project_docs", sameKey);

            assertThat(dialogMd).isPresent();
            assertThat(dialogMd.get().decayFactor()).isEqualTo(0.4f);
            assertThat(docMd).isPresent();
            assertThat(docMd.get().decayFactor()).isEqualTo(0.6f);
        }

        @Test
        @DisplayName("entryKey contains colonwhen CAS correct")
        void entryKeyWithColonsCas() throws IOException {
            String entryKey = "rec:turn:2026-07-21:Q";
            ViewMetadata original = new ViewMetadata(entryKey, "default.dev_dialogs");
            store.compareAndSet("default.dev_dialogs", entryKey, null, original);

            ViewMetadata updated = original.withDecayFactor(0.4f);
            boolean success =
                    store.compareAndSet("default.dev_dialogs", entryKey, original, updated);

            assertThat(success).isTrue();
            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", entryKey);
            assertThat(retrieved).isPresent();
            assertThat(retrieved.get().decayFactor()).isEqualTo(0.4f);
        }
    }

    @Nested
    @DisplayName("Immutability - record immutability")
    class Immutability {

        @Test
        @DisplayName("withDecayFactor returnsnewinstance,originalinstancenot")
        void withDecayFactorReturnsNewInstance() {
            ViewMetadata original = new ViewMetadata("imm-3", "default.dev_dialogs");

            ViewMetadata updated = original.withDecayFactor(0.5f);

            assertThat(original.decayFactor()).isEqualTo(1.0f);
            assertThat(updated.decayFactor()).isEqualTo(0.5f);
            assertThat(updated).isNotSameAs(original);
        }

        @Test
        @DisplayName(
                "withAccessUpdate returnsnewinstance,originalinstancenot,accessCount increment")
        void withAccessUpdateReturnsNewInstance() {
            ViewMetadata original = new ViewMetadata("imm-4", "default.dev_dialogs");
            long accessTime = 9999999999999L;

            ViewMetadata updated = original.withAccessUpdate(accessTime);

            assertThat(original.accessCount()).isZero();
            assertThat(original.lastAccessedAt()).isNotEqualTo(accessTime);
            assertThat(updated.accessCount()).isEqualTo(1);
            assertThat(updated.lastAccessedAt()).isEqualTo(accessTime);
            assertThat(updated).isNotSameAs(original);
        }

        @Test
        @DisplayName("chainstyle with operationmutuallydoes not affect")
        void chainedWithOperationsIndependent() {
            ViewMetadata base = new ViewMetadata("imm-5", "default.dev_dialogs");

            ViewMetadata withDecay = base.withDecayFactor(0.3f);
            ViewMetadata withAccess = base.withAccessUpdate(1_234_567_890L);

            assertThat(withDecay.decayFactor()).isEqualTo(0.3f);
            assertThat(withDecay.accessCount()).isZero();
            assertThat(withAccess.accessCount()).isEqualTo(1);
            assertThat(withAccess.decayFactor()).isEqualTo(1.0f);
            assertThat(base.decayFactor()).isEqualTo(1.0f);
            assertThat(base.accessCount()).isZero();
        }
    }

    @Nested
    @DisplayName("JsonSerialization - JSON serializationround-trip")
    class JsonSerialization {

        @Test
        @DisplayName("complete ViewMetadata JSON serializationround-trip")
        void fullRoundTripSerialization() throws IOException {
            ViewMetadata md =
                    new ViewMetadata("json-1", "default.dev_dialogs")
                            .withDecayFactor(0.6f)
                            .withAccessUpdate(1234567890L);

            store.put(md);
            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", "json-1");

            assertThat(retrieved).isPresent();
            ViewMetadata got = retrieved.get();
            assertThat(got.entryKey()).isEqualTo("json-1");
            assertThat(got.tableId()).isEqualTo("default.dev_dialogs");
            assertThat(got.decayFactor()).isEqualTo(0.6f);
            assertThat(got.accessCount()).isEqualTo(1);
            assertThat(got.lastAccessedAt()).isEqualTo(1234567890L);
        }

        @Test
        @DisplayName("JSON deserialization null field(fromJson default valuefill)")
        void jsonDeserializationToleratesNulls() throws IOException {
            ViewMetadata md = new ViewMetadata("json-2", "default.dev_dialogs");
            store.put(md);
            Optional<ViewMetadata> retrieved = store.get("default.dev_dialogs", "json-2");

            assertThat(retrieved).isPresent();
            ViewMetadata got = retrieved.get();
            assertThat(got.decayFactor()).isEqualTo(1.0f);
            assertThat(got.accessCount()).isZero();
        }
    }
}
