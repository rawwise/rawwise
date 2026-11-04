package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.axiom.memory.exception.MemoryEntryNotFoundException;
import io.github.rawwise.axiom.memory.exception.MemoryShareException;
import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("AxiomMemoryFacade - Axiom API adapter layer MemoryFacade indeedpresent")
class AxiomMemoryFacadeTest {

    private InMemoryAxiomClient axiomClient;
    private Metastore metastore;
    private ViewMetadataStore viewMetadataStore;
    private TestSummarizationService summarizationService;
    private AxiomMemoryFacade facade;

    @BeforeEach
    void setUp() {
        axiomClient = new InMemoryAxiomClient();
        metastore = new InMemoryMetastore();
        viewMetadataStore = new ViewMetadataStore(metastore);
        summarizationService = new TestSummarizationService();
        facade = new AxiomMemoryFacade(axiomClient, viewMetadataStore, DecayPolicy.DEFAULT);
    }

    @Nested
    @DisplayName("Store - store memory")
    class Store {

        @Test
        @DisplayName("store after writereturns MemoryEntry,AxiomClient.ingest becall")
        void storeReturnsMemoryEntryAndCallsIngest() {
            MemoryEntry entry =
                    facade.store("agent-1", Map.of("topic", "test"), MemoryTier.EPISODIC, 5.0);

            assertThat(entry).isNotNull();
            assertThat(entry.entryId()).isNotEmpty();
            assertThat(entry.agentId()).isEqualTo("agent-1");
            assertThat(entry.memoryTier()).isEqualTo(MemoryTier.EPISODIC);
            assertThat(entry.importance()).isEqualTo(5.0);
            assertThat(axiomClient.ingestCount).isEqualTo(1);
        }

        @Test
        @DisplayName("store after write ViewMetadataStore hasrecord")
        void storeCreatesViewMetadata() throws Exception {
            MemoryEntry entry =
                    facade.store("agent-1", Map.of("topic", "test"), MemoryTier.EPISODIC, 5.0);

            Optional<ViewMetadata> vm =
                    viewMetadataStore.get("default.agent_memory:agent-1", entry.entryId());
            assertThat(vm).isPresent();
            assertThat(vm.get().entryKey()).isEqualTo(entry.entryId());
            assertThat(vm.get().tableId()).isEqualTo("default.agent_memory:agent-1");
            assertThat(vm.get().decayFactor()).isEqualTo(1.0f);
        }

        @Test
        @DisplayName("store not ViewMetadataStore whenstill works normally")
        void storeWorksWithoutViewMetadataStore() {
            AxiomMemoryFacade bareFacade = new AxiomMemoryFacade(axiomClient);
            MemoryEntry entry =
                    bareFacade.store("agent-1", Map.of("data", "test"), MemoryTier.EPISODIC, 5.0);

            assertThat(entry.entryId()).isNotEmpty();
            assertThat(axiomClient.ingestCount).isEqualTo(1);
        }

        @Test
        @DisplayName("store multiplememorygeneratedifferent entryId")
        void storeMultipleGeneratesDifferentEntryIds() {
            MemoryEntry e1 = facade.store("agent-1", Map.of("n", 1), MemoryTier.EPISODIC, 5.0);
            MemoryEntry e2 = facade.store("agent-1", Map.of("n", 2), MemoryTier.EPISODIC, 5.0);

            assertThat(e1.entryId()).isNotEqualTo(e2.entryId());
            assertThat(axiomClient.ingestCount).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Recall - recall memory")
    class Recall {

        @Test
        @DisplayName("recall call AxiomClient.search,namespace=agentId")
        void recallCallsSearchWithNamespace() {
            facade.store("agent-1", Map.of("topic", "earnings report"), MemoryTier.EPISODIC, 5.0);

            facade.recall("agent-1", "earnings", 5, MemoryTier.EPISODIC, Depth.DETAIL);

            assertThat(axiomClient.searchCount).isEqualTo(1);
            assertThat(axiomClient.lastSearchNamespace).isEqualTo("agent-1");
            assertThat(axiomClient.lastSearchTableId).isEqualTo("default.agent_memory");
            assertThat(axiomClient.lastSearchTopK).isEqualTo(5);
        }

        @Test
        @DisplayName("recall returns's MemoryEntry listfrom search resulttransition")
        void recallReturnsConvertedMemoryEntries() {
            facade.store(
                    "agent-1",
                    Map.of("topic", "quarterly earnings report"),
                    MemoryTier.EPISODIC,
                    5.0);

            List<MemoryEntry> results =
                    facade.recall("agent-1", "earnings", 5, MemoryTier.EPISODIC, Depth.DETAIL);

            assertThat(results).isNotEmpty();
            MemoryEntry entry = results.get(0);
            assertThat(entry.agentId()).isEqualTo("agent-1");
            assertThat(entry.memoryTier()).isEqualTo(MemoryTier.EPISODIC);
            assertThat(entry.depth()).isEqualTo(Depth.DETAIL);
            assertThat(entry.entryId()).isNotEmpty();
            assertThat(entry.content()).containsKey("summary");
            assertThat(entry.relevanceScore()).isGreaterThanOrEqualTo(0.0);
        }

        @Test
        @DisplayName("recall cross-agent isolation:agent-2 cannot searchto agent-1 'sdata")
        void recallIsolatesByAgent() {
            facade.store("agent-1", Map.of("topic", "secret earnings"), MemoryTier.EPISODIC, 5.0);

            List<MemoryEntry> results =
                    facade.recall("agent-2", "earnings", 5, MemoryTier.EPISODIC, Depth.DETAIL);

            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("recall noresultwhenreturns empty list")
        void recallEmptyReturnsEmptyList() {
            List<MemoryEntry> results =
                    facade.recall("agent-1", "nonexistent", 5, MemoryTier.EPISODIC, Depth.DETAIL);

            assertThat(results).isEmpty();
        }
    }

    @Nested
    @DisplayName("Consolidate - no-op (derivation handled by AutoDerivationHook)")
    class Consolidate {

        @Test
        @DisplayName("consolidate is a no-op: does not fill summaries into ViewMetadata")
        void consolidateIsNoOp() throws Exception {
            MemoryEntry entry =
                    facade.store(
                            "agent-1",
                            Map.of("topic", "quarterly earnings report with detailed numbers"),
                            MemoryTier.EPISODIC,
                            5.0);

            facade.consolidate("agent-1", MemoryTier.EPISODIC, MemoryTier.SEMANTIC);

            Optional<ViewMetadata> vm =
                    viewMetadataStore.get("default.agent_memory:agent-1", entry.entryId());
            assertThat(vm).isPresent();
            assertThat(vm.get().decayFactor()).isEqualTo(1.0f);
            assertThat(vm.get().accessCount()).isZero();
            assertThat(vm.get().toString()).doesNotContain("l0Abstract");
            assertThat(vm.get().toString()).doesNotContain("l1Overview");
        }

        @Test
        @DisplayName("consolidate never invokes SummarizationService")
        void consolidateNeverInvokesSummarizationService() {
            facade.store("agent-1", Map.of("topic", "test"), MemoryTier.EPISODIC, 5.0);

            facade.consolidate("agent-1", MemoryTier.EPISODIC, MemoryTier.SEMANTIC);
            facade.consolidate("agent-1", MemoryTier.EPISODIC, MemoryTier.SEMANTIC);

            assertThat(summarizationService.callCount).isZero();
        }

        @Test
        @DisplayName("consolidate no ViewMetadataStore when no-op")
        void consolidateWithoutViewMetadataStoreIsNoOp() {
            AxiomMemoryFacade bare = new AxiomMemoryFacade(axiomClient);

            bare.consolidate("agent-1", MemoryTier.EPISODIC, MemoryTier.SEMANTIC);

            assertThat(summarizationService.callCount).isZero();
        }
    }

    @Nested
    @DisplayName("Decay - decay")
    class Decay {

        @Test
        @DisplayName("decay call ViewMetadataDecayUpdater update ViewMetadata.decayFactor")
        void decayUpdatesViewMetadataDecayFactor() throws Exception {
            MemoryEntry entry =
                    facade.store("agent-1", Map.of("topic", "test"), MemoryTier.EPISODIC, 5.0);

            ViewMetadata original =
                    new ViewMetadata(
                            entry.entryId(),
                            "default.agent_memory:agent-1",
                            1.0f,
                            System.currentTimeMillis() - 200_000L,
                            0);
            viewMetadataStore.update("default.agent_memory:agent-1", entry.entryId(), original);

            facade.decay("agent-1", Duration.ofMillis(100_000));

            Optional<ViewMetadata> after =
                    viewMetadataStore.get("default.agent_memory:agent-1", entry.entryId());
            assertThat(after).isPresent();
            assertThat(after.get().decayFactor()).isLessThan(1.0f);
        }

        @Test
        @DisplayName("decay newdatanotdecay(age < halfLife)")
        void decayFreshDataNotDecayed() {
            facade.store("agent-1", Map.of("topic", "test"), MemoryTier.EPISODIC, 5.0);

            facade.decay("agent-1", Duration.ofDays(1));

            assertThat(axiomClient.scanCount).isZero();
        }

        @Test
        @DisplayName("decay no ViewMetadataStore when no-op")
        void decayWithoutViewMetadataStoreIsNoOp() {
            AxiomMemoryFacade bare = new AxiomMemoryFacade(axiomClient);

            bare.decay("agent-1", Duration.ofHours(1));
        }
    }

    @Nested
    @DisplayName("Reflect - reverse")
    class Reflect {

        @Test
        @DisplayName("reflect uses search top result and returns ABSTRACT summary")
        void reflectScansL0AndReturnsTopMatch() throws Exception {
            MemoryEntry e1 =
                    facade.store(
                            "agent-1",
                            Map.of("topic", "quarterly earnings"),
                            MemoryTier.EPISODIC,
                            5.0);
            MemoryEntry e2 =
                    facade.store(
                            "agent-1", Map.of("topic", "annual budget"), MemoryTier.EPISODIC, 5.0);

            Optional<MemoryEntry> result = facade.reflect("agent-1", "earnings");

            assertThat(result).isPresent();
            assertThat(result.get().entryId()).isEqualTo(e1.entryId());
            assertThat(result.get().content())
                    .containsEntry("summary", "{topic=quarterly earnings}");
            assertThat(result.get().depth()).isEqualTo(Depth.ABSTRACT);
        }

        @Test
        @DisplayName("reflect nodatawhenreturns empty")
        void reflectEmptyReturnsEmpty() {
            Optional<MemoryEntry> result = facade.reflect("agent-1", "anything");

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("reflect no ViewMetadataStore whenreturns empty")
        void reflectWithoutViewMetadataStoreReturnsEmpty() {
            AxiomMemoryFacade bare = new AxiomMemoryFacade(axiomClient);

            Optional<MemoryEntry> result = bare.reflect("agent-1", "anything");

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("reflect cross-agent isolation")
        void reflectIsolatesByAgent() throws Exception {
            MemoryEntry e1 =
                    facade.store("agent-1", Map.of("topic", "earnings"), MemoryTier.EPISODIC, 5.0);
            ViewMetadata vm1 =
                    new ViewMetadata(
                            e1.entryId(),
                            "default.agent_memory:agent-1",
                            1.0f,
                            System.currentTimeMillis(),
                            0);
            viewMetadataStore.update("default.agent_memory:agent-1", e1.entryId(), vm1);

            Optional<MemoryEntry> result = facade.reflect("agent-2", "earnings");

            assertThat(result).isEmpty();
        }
    }

    @Nested
    @DisplayName("Share - share memory")
    class Share {

        @Test
        @DisplayName("share permission check:fromAgent nothas entryId when MemoryShareException")
        void shareThrowsWhenAgentDoesNotOwnEntry() {
            facade.store("agent-1", Map.of("topic", "secret"), MemoryTier.EPISODIC, 5.0);
            String entryId = axiomClient.entries.keySet().iterator().next();

            assertThatThrownBy(
                            () ->
                                    facade.share(
                                            "agent-2",
                                            "agent-3",
                                            entryId,
                                            new MemorySharePolicy(
                                                    Duration.ofHours(1), true, "read")))
                    .isInstanceOf(MemoryShareException.class)
                    .hasMessageContaining("does not own");
        }

        @Test
        @DisplayName("share non-existent entryId when MemoryEntryNotFoundException")
        void shareThrowsWhenEntryNotFound() {
            assertThatThrownBy(
                            () ->
                                    facade.share(
                                            "agent-1",
                                            "agent-2",
                                            "nonexistent-id",
                                            new MemorySharePolicy(
                                                    Duration.ofHours(1), true, "read")))
                    .isInstanceOf(MemoryEntryNotFoundException.class)
                    .hasMessageContaining("not found");
        }

        @Test
        @DisplayName("share on successcall AxiomClient.ingest to targetAgent namespace")
        void shareSuccessIngestsToTargetAgent() {
            MemoryEntry entry =
                    facade.store(
                            "agent-1",
                            Map.of("topic", "shared knowledge"),
                            MemoryTier.SEMANTIC,
                            7.0);
            int ingestCountBefore = axiomClient.ingestCount;

            facade.share(
                    "agent-1",
                    "agent-2",
                    entry.entryId(),
                    new MemorySharePolicy(Duration.ofHours(1), true, "read"));

            assertThat(axiomClient.ingestCount).isEqualTo(ingestCountBefore + 1);
            InMemoryAxiomClient.StoredEntry sharedEntry =
                    axiomClient.entries.values().stream()
                            .filter(e -> "agent-2".equals(e.properties.get("agent_id")))
                            .findFirst()
                            .orElseThrow();
            assertThat(sharedEntry.properties).containsEntry("shared_from", "agent-1");
            assertThat(sharedEntry.properties).containsEntry("memory_tier", "SEMANTIC");
            assertThat(sharedEntry.body).isNotEmpty();
        }

        @Test
        @DisplayName("share after success targetAgent canwith recall to'smemory")
        void shareAllowsTargetToRecall() {
            MemoryEntry entry =
                    facade.store(
                            "agent-1",
                            Map.of("topic", "shared knowledge base"),
                            MemoryTier.SEMANTIC,
                            7.0);

            facade.share(
                    "agent-1",
                    "agent-2",
                    entry.entryId(),
                    new MemorySharePolicy(Duration.ofHours(1), true, "read"));

            List<MemoryEntry> recalled =
                    facade.recall("agent-2", "shared", 5, MemoryTier.SEMANTIC, Depth.DETAIL);
            assertThat(recalled).isNotEmpty();
            assertThat(recalled.get(0).agentId()).isEqualTo("agent-2");
        }

        @Test
        @DisplayName("share after success ViewMetadata alsoto targetAgent")
        void shareCopiesViewMetadata() throws Exception {
            MemoryEntry entry =
                    facade.store("agent-1", Map.of("topic", "shared"), MemoryTier.SEMANTIC, 7.0);
            ViewMetadata vm =
                    new ViewMetadata(
                            entry.entryId(),
                            "default.agent_memory:agent-1",
                            0.8f,
                            System.currentTimeMillis(),
                            1);
            viewMetadataStore.update("default.agent_memory:agent-1", entry.entryId(), vm);

            facade.share(
                    "agent-1",
                    "agent-2",
                    entry.entryId(),
                    new MemorySharePolicy(Duration.ofHours(1), true, "read"));

            List<ViewMetadata> agent2Entries =
                    viewMetadataStore.scanByTable("default.agent_memory:agent-2", 100);
            assertThat(agent2Entries).hasSize(1);
            ViewMetadata copied = agent2Entries.get(0);
            assertThat(copied.decayFactor()).isEqualTo(1.0f);
            assertThat(copied.entryKey()).isNotEqualTo(entry.entryId());
        }
    }

    @Nested
    @DisplayName("Status - status statistics")
    class Status {

        @Test
        @DisplayName("status returns correctly'sstatisticsinfo")
        void statusReturnsCorrectCounts() {
            facade.store("agent-1", Map.of("d", 1), MemoryTier.WORKING, 5.0);
            facade.store("agent-1", Map.of("d", 2), MemoryTier.WORKING, 5.0);
            facade.store("agent-1", Map.of("d", 3), MemoryTier.EPISODIC, 5.0);
            facade.store("agent-1", Map.of("d", 4), MemoryTier.SEMANTIC, 5.0);
            facade.store("agent-1", Map.of("d", 5), MemoryTier.PROCEDURAL, 5.0);

            MemoryStatus status = facade.status("agent-1");

            assertThat(status.agentId()).isEqualTo("agent-1");
            assertThat(status.workingCount()).isEqualTo(2);
            assertThat(status.episodicCount()).isEqualTo(1);
            assertThat(status.semanticCount()).isEqualTo(1);
            assertThat(status.proceduralCount()).isEqualTo(1);
            assertThat(status.totalCount()).isEqualTo(5);
        }

        @Test
        @DisplayName("status nodatawhenreturnsallzero")
        void statusEmptyReturnsZeros() {
            MemoryStatus status = facade.status("agent-1");

            assertThat(status.agentId()).isEqualTo("agent-1");
            assertThat(status.totalCount()).isZero();
            assertThat(status.workingCount()).isZero();
        }

        @Test
        @DisplayName("status cross-agent isolation")
        void statusIsolatesByAgent() {
            facade.store("agent-1", Map.of("d", 1), MemoryTier.EPISODIC, 5.0);
            facade.store("agent-2", Map.of("d", 2), MemoryTier.WORKING, 5.0);
            facade.store("agent-2", Map.of("d", 3), MemoryTier.WORKING, 5.0);

            MemoryStatus s1 = facade.status("agent-1");
            MemoryStatus s2 = facade.status("agent-2");

            assertThat(s1.totalCount()).isEqualTo(1);
            assertThat(s1.episodicCount()).isEqualTo(1);
            assertThat(s2.totalCount()).isEqualTo(2);
            assertThat(s2.workingCount()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("ListEntries - paginated list")
    class ListEntries {

        @Test
        @DisplayName("listEntries paginationcorrect")
        void listEntriesPaginationCorrect() {
            for (int i = 0; i < 10; i++) {
                facade.store("agent-1", Map.of("index", i), MemoryTier.EPISODIC, 5.0);
            }

            List<MemoryEntry> page1 = facade.listEntries("agent-1", MemoryTier.EPISODIC, 0, 3);
            List<MemoryEntry> page2 = facade.listEntries("agent-1", MemoryTier.EPISODIC, 3, 3);
            List<MemoryEntry> page3 = facade.listEntries("agent-1", MemoryTier.EPISODIC, 6, 3);
            List<MemoryEntry> page4 = facade.listEntries("agent-1", MemoryTier.EPISODIC, 9, 3);

            assertThat(page1).hasSize(3);
            assertThat(page2).hasSize(3);
            assertThat(page3).hasSize(3);
            assertThat(page4).hasSize(1);
        }

        @Test
        @DisplayName("listEntries offset exceedoutrangereturns empty list")
        void listEntriesOffsetBeyondRangeReturnsEmpty() {
            facade.store("agent-1", Map.of("d", 1), MemoryTier.EPISODIC, 5.0);

            List<MemoryEntry> result = facade.listEntries("agent-1", MemoryTier.EPISODIC, 100, 10);

            assertThat(result).isEmpty();
        }

        @Test
        @DisplayName("listEntries by memoryTier filter")
        void listEntriesFiltersByMemoryTier() {
            facade.store("agent-1", Map.of("d", 1), MemoryTier.WORKING, 5.0);
            facade.store("agent-1", Map.of("d", 2), MemoryTier.EPISODIC, 5.0);
            facade.store("agent-1", Map.of("d", 3), MemoryTier.SEMANTIC, 5.0);

            List<MemoryEntry> episodic = facade.listEntries("agent-1", MemoryTier.EPISODIC, 0, 100);
            List<MemoryEntry> semantic = facade.listEntries("agent-1", MemoryTier.SEMANTIC, 0, 100);

            assertThat(episodic).hasSize(1);
            assertThat(episodic.get(0).memoryTier()).isEqualTo(MemoryTier.EPISODIC);
            assertThat(semantic).hasSize(1);
            assertThat(semantic.get(0).memoryTier()).isEqualTo(MemoryTier.SEMANTIC);
        }

        @Test
        @DisplayName("listEntries returns's MemoryEntry contains content body")
        void listEntriesReturnsContentBody() {
            facade.store("agent-1", Map.of("topic", "test data"), MemoryTier.EPISODIC, 5.0);

            List<MemoryEntry> entries = facade.listEntries("agent-1", MemoryTier.EPISODIC, 0, 100);

            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).content()).containsKey("body");
            assertThat(entries.get(0).importance()).isEqualTo(5.0);
        }

        @Test
        @DisplayName("listEntries cross-agent isolation")
        void listEntriesIsolatesByAgent() {
            facade.store("agent-1", Map.of("d", 1), MemoryTier.EPISODIC, 5.0);
            facade.store("agent-2", Map.of("d", 2), MemoryTier.EPISODIC, 5.0);
            facade.store("agent-2", Map.of("d", 3), MemoryTier.EPISODIC, 5.0);

            List<MemoryEntry> a1 = facade.listEntries("agent-1", MemoryTier.EPISODIC, 0, 100);
            List<MemoryEntry> a2 = facade.listEntries("agent-2", MemoryTier.EPISODIC, 0, 100);

            assertThat(a1).hasSize(1);
            assertThat(a2).hasSize(2);
        }
    }

    @Nested
    @DisplayName("Close - resource close")
    class Close {

        @Test
        @DisplayName("close does not throw")
        void closeDoesNotThrow() {
            facade.store("agent-1", Map.of("d", 1), MemoryTier.EPISODIC, 5.0);

            facade.close();
        }
    }

    @Nested
    @DisplayName("AgentIndexIntegration - AgentIndexStore succeed")
    class AgentIndexIntegration {

        private AgentIndexStore agentIndexStore;
        private AxiomMemoryFacade facadeWithIndex;

        @BeforeEach
        void setUpIndex() {
            agentIndexStore = new AgentIndexStore(metastore);
            facadeWithIndex =
                    new AxiomMemoryFacade(axiomClient, viewMetadataStore, DecayPolicy.DEFAULT);
            facadeWithIndex.setAgentIndexStore(agentIndexStore);
        }

        @Test
        @DisplayName("store after agentIndexStore hasindex")
        void storeCreatesAgentIndex() throws Exception {
            MemoryEntry entry =
                    facadeWithIndex.store(
                            "agent-1", Map.of("topic", "indexed memory"), MemoryTier.EPISODIC, 5.0);

            assertThat(agentIndexStore.exists("agent-1", entry.entryId())).isTrue();
            List<String> entryKeys = agentIndexStore.scanAgent("agent-1", 100);
            assertThat(entryKeys).contains(entry.entryId());
        }

        @Test
        @DisplayName("store multipleafter agentIndexStore indexallrecord")
        void storeMultipleCreatesAllIndexes() throws Exception {
            MemoryEntry e1 =
                    facadeWithIndex.store("agent-1", Map.of("n", 1), MemoryTier.EPISODIC, 5.0);
            MemoryEntry e2 =
                    facadeWithIndex.store("agent-1", Map.of("n", 2), MemoryTier.EPISODIC, 5.0);
            MemoryEntry e3 =
                    facadeWithIndex.store("agent-1", Map.of("n", 3), MemoryTier.SEMANTIC, 5.0);

            List<String> entryKeys = agentIndexStore.scanAgent("agent-1", 100);
            assertThat(entryKeys)
                    .containsExactlyInAnyOrder(e1.entryId(), e2.entryId(), e3.entryId());
        }

        @Test
        @DisplayName("listEntries use agentIndexStore scan(scanCount not)")
        void listEntriesUsesAgentIndexStore() {
            facadeWithIndex.store("agent-1", Map.of("d", 1), MemoryTier.EPISODIC, 5.0);
            facadeWithIndex.store("agent-1", Map.of("d", 2), MemoryTier.EPISODIC, 5.0);
            int scanCountBefore = axiomClient.scanCount;

            List<MemoryEntry> entries =
                    facadeWithIndex.listEntries("agent-1", MemoryTier.EPISODIC, 0, 100);

            assertThat(entries).hasSize(2);
            assertThat(axiomClient.scanCount).isEqualTo(scanCountBefore);
        }

        @Test
        @DisplayName("listEntries use agentIndexStore whenby memoryTier filtercorrect")
        void listEntriesWithAgentIndexFiltersByTier() {
            facadeWithIndex.store("agent-1", Map.of("d", 1), MemoryTier.WORKING, 5.0);
            facadeWithIndex.store("agent-1", Map.of("d", 2), MemoryTier.EPISODIC, 5.0);
            facadeWithIndex.store("agent-1", Map.of("d", 3), MemoryTier.SEMANTIC, 5.0);

            List<MemoryEntry> episodic =
                    facadeWithIndex.listEntries("agent-1", MemoryTier.EPISODIC, 0, 100);
            List<MemoryEntry> semantic =
                    facadeWithIndex.listEntries("agent-1", MemoryTier.SEMANTIC, 0, 100);

            assertThat(episodic).hasSize(1);
            assertThat(semantic).hasSize(1);
            assertThat(episodic.get(0).memoryTier()).isEqualTo(MemoryTier.EPISODIC);
        }

        @Test
        @DisplayName("listEntries use agentIndexStore whenpaginationcorrect")
        void listEntriesWithAgentIndexPaginatesCorrectly() {
            for (int i = 0; i < 10; i++) {
                facadeWithIndex.store("agent-1", Map.of("index", i), MemoryTier.EPISODIC, 5.0);
            }

            List<MemoryEntry> page1 =
                    facadeWithIndex.listEntries("agent-1", MemoryTier.EPISODIC, 0, 3);
            List<MemoryEntry> page2 =
                    facadeWithIndex.listEntries("agent-1", MemoryTier.EPISODIC, 3, 3);
            List<MemoryEntry> page3 =
                    facadeWithIndex.listEntries("agent-1", MemoryTier.EPISODIC, 6, 3);
            List<MemoryEntry> page4 =
                    facadeWithIndex.listEntries("agent-1", MemoryTier.EPISODIC, 9, 3);

            assertThat(page1).hasSize(3);
            assertThat(page2).hasSize(3);
            assertThat(page3).hasSize(3);
            assertThat(page4).hasSize(1);
        }

        @Test
        @DisplayName("listEntries use agentIndexStore whencross-agent isolation")
        void listEntriesWithAgentIndexIsolatesByAgent() {
            facadeWithIndex.store("agent-1", Map.of("d", 1), MemoryTier.EPISODIC, 5.0);
            facadeWithIndex.store("agent-2", Map.of("d", 2), MemoryTier.EPISODIC, 5.0);
            facadeWithIndex.store("agent-2", Map.of("d", 3), MemoryTier.EPISODIC, 5.0);

            List<MemoryEntry> a1 =
                    facadeWithIndex.listEntries("agent-1", MemoryTier.EPISODIC, 0, 100);
            List<MemoryEntry> a2 =
                    facadeWithIndex.listEntries("agent-2", MemoryTier.EPISODIC, 0, 100);

            assertThat(a1).hasSize(1);
            assertThat(a2).hasSize(2);
        }

        @Test
        @DisplayName("share after targetAgent 's agentIndexStore hasindex")
        void shareCreatesTargetAgentIndex() throws Exception {
            MemoryEntry entry =
                    facadeWithIndex.store(
                            "agent-1",
                            Map.of("topic", "shared knowledge"),
                            MemoryTier.SEMANTIC,
                            7.0);

            facadeWithIndex.share(
                    "agent-1",
                    "agent-2",
                    entry.entryId(),
                    new MemorySharePolicy(Duration.ofHours(1), true, "read"));

            List<String> agent1Keys = agentIndexStore.scanAgent("agent-1", 100);
            List<String> agent2Keys = agentIndexStore.scanAgent("agent-2", 100);
            assertThat(agent1Keys).contains(entry.entryId());
            assertThat(agent2Keys).hasSize(1);
            assertThat(agent2Keys.get(0)).isNotEqualTo(entry.entryId());
            assertThat(agentIndexStore.exists("agent-2", agent2Keys.get(0))).isTrue();
        }

        @Test
        @DisplayName("reflect uses search result with agent index present")
        void reflectUsesAgentIndexStore() throws Exception {
            MemoryEntry e1 =
                    facadeWithIndex.store(
                            "agent-1",
                            Map.of("topic", "quarterly earnings"),
                            MemoryTier.EPISODIC,
                            5.0);

            Optional<MemoryEntry> result = facadeWithIndex.reflect("agent-1", "earnings");

            assertThat(result).isPresent();
            assertThat(result.get().entryId()).isEqualTo(e1.entryId());
            assertThat(result.get().content()).containsKey("summary");
        }

        @Test
        @DisplayName("reflect use agentIndexStore whencross-agent isolation")
        void reflectWithAgentIndexIsolatesByAgent() throws Exception {
            MemoryEntry e1 =
                    facadeWithIndex.store(
                            "agent-1", Map.of("topic", "earnings"), MemoryTier.EPISODIC, 5.0);
            ViewMetadata vm1 =
                    new ViewMetadata(
                            e1.entryId(),
                            "default.agent_memory:agent-1",
                            1.0f,
                            System.currentTimeMillis(),
                            0);
            viewMetadataStore.update("default.agent_memory:agent-1", e1.entryId(), vm1);

            Optional<MemoryEntry> result = facadeWithIndex.reflect("agent-2", "earnings");

            assertThat(result).isEmpty();
        }
    }

    @Nested
    @DisplayName("AgentIndexBackwardCompat - agentIndexStore not injectedwhenbackward compatible")
    class AgentIndexBackwardCompat {

        @Test
        @DisplayName("agentIndexStore not injectedwhen store still works normally")
        void storeWorksWithoutAgentIndexStore() {
            MemoryEntry entry =
                    facade.store("agent-1", Map.of("topic", "no index"), MemoryTier.EPISODIC, 5.0);

            assertThat(entry.entryId()).isNotEmpty();
            assertThat(axiomClient.ingestCount).isEqualTo(1);
        }

        @Test
        @DisplayName("agentIndexStore not injectedwhen listEntries stilluse axiomClient.scan")
        void listEntriesWithoutAgentIndexUsesScan() {
            facade.store("agent-1", Map.of("d", 1), MemoryTier.EPISODIC, 5.0);
            facade.store("agent-1", Map.of("d", 2), MemoryTier.EPISODIC, 5.0);
            int scanCountBefore = axiomClient.scanCount;

            List<MemoryEntry> entries = facade.listEntries("agent-1", MemoryTier.EPISODIC, 0, 100);

            assertThat(entries).hasSize(2);
            assertThat(axiomClient.scanCount).isGreaterThan(scanCountBefore);
        }

        @Test
        @DisplayName("agentIndexStore not injectedwhen share still works normally")
        void shareWithoutAgentIndexStoreWorks() {
            MemoryEntry entry =
                    facade.store("agent-1", Map.of("topic", "shared"), MemoryTier.SEMANTIC, 7.0);

            facade.share(
                    "agent-1",
                    "agent-2",
                    entry.entryId(),
                    new MemorySharePolicy(Duration.ofHours(1), true, "read"));

            List<MemoryEntry> recalled =
                    facade.recall("agent-2", "shared", 5, MemoryTier.SEMANTIC, Depth.DETAIL);
            assertThat(recalled).isNotEmpty();
        }

        @Test
        @DisplayName(
                "agentIndexStore not injectedwhen reflect stilluse viewMetadataStore.scanByTable")
        void reflectWithoutAgentIndexUsesScanByTable() throws Exception {
            MemoryEntry e1 =
                    facade.store("agent-1", Map.of("topic", "earnings"), MemoryTier.EPISODIC, 5.0);
            ViewMetadata vm1 =
                    new ViewMetadata(
                            e1.entryId(),
                            "default.agent_memory:agent-1",
                            1.0f,
                            System.currentTimeMillis(),
                            0);
            viewMetadataStore.update("default.agent_memory:agent-1", e1.entryId(), vm1);

            Optional<MemoryEntry> result = facade.reflect("agent-1", "earnings");

            assertThat(result).isPresent();
            assertThat(result.get().entryId()).isEqualTo(e1.entryId());
        }
    }

    static class InMemoryAxiomClient implements AxiomClient {

        final Map<String, StoredEntry> entries = new ConcurrentHashMap<>();
        int ingestCount = 0;
        int searchCount = 0;
        int scanCount = 0;
        String lastSearchTableId;
        String lastSearchNamespace;
        int lastSearchTopK;

        @Override
        public String ingest(
                String tableId, String key, String body, Map<String, String> properties) {
            ingestCount++;
            entries.put(key, new StoredEntry(tableId, key, body, new LinkedHashMap<>(properties)));
            return key;
        }

        @Override
        public List<Map<String, Object>> search(
                String tableId, String query, int topK, String namespace) {
            searchCount++;
            lastSearchTableId = tableId;
            lastSearchNamespace = namespace;
            lastSearchTopK = topK;

            String lowerQuery = query != null ? query.toLowerCase() : "";
            List<StoredEntry> matched =
                    entries.values().stream()
                            .filter(e -> e.tableId.equals(tableId))
                            .filter(
                                    e ->
                                            namespace == null
                                                    || namespace.equals(
                                                            e.properties.get("agent_id")))
                            .filter(
                                    e -> {
                                        if (lowerQuery.isEmpty()) return true;
                                        return e.body.toLowerCase().contains(lowerQuery);
                                    })
                            .sorted(
                                    (a, b) -> {
                                        if (lowerQuery.isEmpty()) return 0;
                                        return Integer.compare(
                                                countOccurrences(b.body.toLowerCase(), lowerQuery),
                                                countOccurrences(a.body.toLowerCase(), lowerQuery));
                                    })
                            .limit(topK)
                            .collect(Collectors.toList());

            List<Map<String, Object>> results = new ArrayList<>();
            for (StoredEntry e : matched) {
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("entryKey", e.key);
                result.put("viewId", e.key);
                result.put("score", 1.0);
                result.put("summary", e.body);
                results.add(result);
            }
            return results;
        }

        @Override
        public Optional<Map<String, Object>> getEntry(String tableId, String key) {
            StoredEntry entry = entries.get(key);
            if (entry == null || !entry.tableId.equals(tableId)) {
                return Optional.empty();
            }
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("tableId", entry.tableId);
            result.put("key", entry.key);
            result.put("body", entry.body);
            result.put("properties", new LinkedHashMap<>(entry.properties));
            return Optional.of(result);
        }

        @Override
        public List<Map<String, Object>> scan(String tableId, String namespace) {
            scanCount++;
            List<Map<String, Object>> results = new ArrayList<>();
            for (StoredEntry e : entries.values()) {
                if (!e.tableId.equals(tableId)) continue;
                if (namespace != null && !namespace.equals(e.properties.get("agent_id"))) continue;
                Map<String, Object> result = new LinkedHashMap<>();
                result.put("tableId", e.tableId);
                result.put("key", e.key);
                result.put("body", e.body);
                result.put("properties", new LinkedHashMap<>(e.properties));
                results.add(result);
            }
            return results;
        }

        private int countOccurrences(String text, String substring) {
            int count = 0;
            int idx = 0;
            while ((idx = text.indexOf(substring, idx)) != -1) {
                count++;
                idx += substring.length();
            }
            return count;
        }

        record StoredEntry(
                String tableId, String key, String body, Map<String, String> properties) {}
    }

    static class TestSummarizationService implements SummarizationService {

        int callCount = 0;

        @Override
        public SummarizationResult summarize(String content, SummarizationRequest request) {
            callCount++;
            return new SummarizationResult(
                    "L0_SUMMARY:test summary",
                    "L1_OVERVIEW:test overview",
                    50,
                    200,
                    "test-model",
                    System.currentTimeMillis());
        }

        @Override
        public boolean isAvailable() {
            return true;
        }
    }
}
