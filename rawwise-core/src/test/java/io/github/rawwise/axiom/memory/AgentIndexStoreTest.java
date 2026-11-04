package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.rawwise.metastore.InMemoryMetastore;
import io.github.rawwise.metastore.Metastore;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("AgentIndexStore - Metastore agent indextest")
class AgentIndexStoreTest {

    private Metastore metastore;
    private AgentIndexStore store;

    @BeforeEach
    void setUp() {
        metastore = new InMemoryMetastore();
        store = new AgentIndexStore(metastore);
    }

    @Nested
    @DisplayName("IndexScan - write and scan")
    class IndexScan {

        @Test
        @DisplayName("index + scanAgent round-trip:after writecanscanout entryKey")
        void indexScanRoundTrip() throws IOException {
            store.index("agent-1", "entry-a");

            List<String> results = store.scanAgent("agent-1", 100);

            assertThat(results).containsExactly("entry-a");
        }

        @Test
        @DisplayName("index many entryKey after scanAgent returnsall")
        void indexMultipleThenScan() throws IOException {
            store.index("agent-1", "entry-1");
            store.index("agent-1", "entry-2");
            store.index("agent-1", "entry-3");

            List<String> results = store.scanAgent("agent-1", 100);

            assertThat(results).containsExactlyInAnyOrder("entry-1", "entry-2", "entry-3");
        }
    }

    @Nested
    @DisplayName("AgentIsolation - cross-agent isolation")
    class AgentIsolation {

        @Test
        @DisplayName("scanAgent onlyreturnsspecified agent 's entryKeys(cross-agent isolation)")
        void scanAgentIsolatesByAgent() throws IOException {
            store.index("agent-1", "a1-entry-1");
            store.index("agent-1", "a1-entry-2");
            store.index("agent-2", "a2-entry-1");
            store.index("agent-2", "a2-entry-2");
            store.index("agent-2", "a2-entry-3");

            List<String> a1Results = store.scanAgent("agent-1", 100);
            List<String> a2Results = store.scanAgent("agent-2", 100);

            assertThat(a1Results).containsExactlyInAnyOrder("a1-entry-1", "a1-entry-2");
            assertThat(a2Results)
                    .containsExactlyInAnyOrder("a2-entry-1", "a2-entry-2", "a2-entry-3");
        }

        @Test
        @DisplayName("scanAgent same entryKey indifferent agent undermutuallyno cross-interference")
        void scanAgentSameEntryKeyDifferentAgents() throws IOException {
            store.index("agent-1", "shared-key");
            store.index("agent-2", "shared-key");

            List<String> a1 = store.scanAgent("agent-1", 100);
            List<String> a2 = store.scanAgent("agent-2", 100);

            assertThat(a1).containsExactly("shared-key");
            assertThat(a2).containsExactly("shared-key");
        }
    }

    @Nested
    @DisplayName("Limit - limit return count")
    class Limit {

        @Test
        @DisplayName("scanAgent limit take effect")
        void scanAgentLimitEnforced() throws IOException {
            for (int i = 0; i < 10; i++) {
                store.index("agent-1", "entry-" + i);
            }

            List<String> results = store.scanAgent("agent-1", 3);

            assertThat(results).hasSize(3);
        }

        @Test
        @DisplayName("scanAgent limit=0 returns empty list")
        void scanAgentLimitZeroReturnsEmpty() throws IOException {
            store.index("agent-1", "entry-1");

            List<String> results = store.scanAgent("agent-1", 0);

            assertThat(results).isEmpty();
        }

        @Test
        @DisplayName("scanAgent nodatareturns empty list")
        void scanAgentEmptyReturnsEmptyList() throws IOException {
            List<String> results = store.scanAgent("nonexistent-agent", 100);

            assertThat(results).isEmpty();
        }
    }

    @Nested
    @DisplayName("Exists - existence check")
    class Exists {

        @Test
        @DisplayName("exists returns true whenindexalreadyexists")
        void existsReturnsTrueWhenIndexed() throws IOException {
            store.index("agent-1", "entry-1");

            assertThat(store.exists("agent-1", "entry-1")).isTrue();
        }

        @Test
        @DisplayName("exists returns false whenindexnot found")
        void existsReturnsFalseWhenNotIndexed() throws IOException {
            assertThat(store.exists("agent-1", "nonexistent")).isFalse();
        }

        @Test
        @DisplayName("exists cross-agent isolation:agent-2 's entry does not affect agent-1")
        void existsIsolatesByAgent() throws IOException {
            store.index("agent-2", "entry-1");

            assertThat(store.exists("agent-1", "entry-1")).isFalse();
            assertThat(store.exists("agent-2", "entry-1")).isTrue();
        }
    }

    @Nested
    @DisplayName("Remove - delete")
    class Remove {

        @Test
        @DisplayName("remove after exists returns false")
        void removeThenExistsReturnsFalse() throws IOException {
            store.index("agent-1", "entry-1");
            assertThat(store.exists("agent-1", "entry-1")).isTrue();

            store.remove("agent-1", "entry-1");

            assertThat(store.exists("agent-1", "entry-1")).isFalse();
        }

        @Test
        @DisplayName("remove non-existent key does not throw")
        void removeNonExistentNoThrow() {
            assertThatCode(() -> store.remove("agent-1", "nonexistent")).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("remove onlydeletespecified agent 'sindex,does not affectother agent")
        void removeOnlyAffectsSpecifiedAgent() throws IOException {
            store.index("agent-1", "shared-key");
            store.index("agent-2", "shared-key");

            store.remove("agent-1", "shared-key");

            assertThat(store.exists("agent-1", "shared-key")).isFalse();
            assertThat(store.exists("agent-2", "shared-key")).isTrue();
        }
    }

    @Nested
    @DisplayName("Get - read entryKey")
    class Get {

        @Test
        @DisplayName("get returns entryKey whenindexalreadyexists")
        void getReturnsEntryKeyWhenIndexed() throws IOException {
            store.index("agent-1", "entry-1");

            assertThat(store.get("agent-1", "entry-1")).contains("entry-1");
        }

        @Test
        @DisplayName("get returns empty whenindexnot found")
        void getReturnsEmptyWhenNotIndexed() throws IOException {
            assertThat(store.get("agent-1", "nonexistent")).isEmpty();
        }
    }

    @Nested
    @DisplayName("KeySeparator - entryKey contains colon when <NUL> min symbol verify")
    class KeySeparator {

        @Test
        @DisplayName("entryKey contains colonwhen index + scanAgent correct")
        void entryKeyWithColonsIndexScan() throws IOException {
            String entryKey = "rec:turn:2026-07-21:Q";
            store.index("agent-1", entryKey);

            List<String> results = store.scanAgent("agent-1", 100);

            assertThat(results).containsExactly(entryKey);
        }

        @Test
        @DisplayName("entryKey contains colonwhen exists correct")
        void entryKeyWithColonsExists() throws IOException {
            String entryKey = "rec:turn:2026-07-21:Q";
            store.index("agent-1", entryKey);

            assertThat(store.exists("agent-1", entryKey)).isTrue();
        }

        @Test
        @DisplayName("entryKey contains colonwhen remove after exists returns false")
        void entryKeyWithColonsRemove() throws IOException {
            String entryKey = "rec:turn:2026-07-21:Q";
            store.index("agent-1", entryKey);

            store.remove("agent-1", entryKey);

            assertThat(store.exists("agent-1", entryKey)).isFalse();
        }

        @Test
        @DisplayName("agentId contains colonwhen index + scanAgent correct")
        void agentIdWithColonsIndexScan() throws IOException {
            String agentId = "tenant:org:agent-1";
            store.index(agentId, "entry-1");

            List<String> results = store.scanAgent(agentId, 100);

            assertThat(results).containsExactly("entry-1");
            assertThat(store.exists(agentId, "entry-1")).isTrue();
        }

        @Test
        @DisplayName("agentId and entryKey allcontains colonwhenno cross-interference")
        void bothWithColonsNoCrossTalk() throws IOException {
            store.index("tenant:a:agent-1", "rec:turn:1:Q");
            store.index("tenant:b:agent-1", "rec:turn:1:Q");

            List<String> a = store.scanAgent("tenant:a:agent-1", 100);
            List<String> b = store.scanAgent("tenant:b:agent-1", 100);

            assertThat(a).containsExactly("rec:turn:1:Q");
            assertThat(b).containsExactly("rec:turn:1:Q");
        }
    }
}
