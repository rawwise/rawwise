package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AxiomMemoryFacadeRecallTest {

    @Test
    void recallPassesFilterAndRespectsDepth() {
        AxiomClient client = mock(AxiomClient.class);
        when(client.search(eq("default.agent_memory"), eq("hello"), anyInt(), eq("alice"), any()))
                .thenReturn(
                        List.of(
                                Map.of(
                                        "entryKey", "k1",
                                        "score", 0.9,
                                        "summary", "sum",
                                        "body", "full body",
                                        "properties",
                                                Map.of(
                                                        "agent_id", "alice",
                                                        "memory_tier", "SEMANTIC"))));
        AxiomMemoryFacade facade = new AxiomMemoryFacade(client);

        var detail = facade.recall("alice", "hello", 5, MemoryTier.SEMANTIC, Depth.DETAIL);
        assertThat(detail).hasSize(1);
        assertThat(detail.get(0).content()).containsEntry("body", "full body");
        assertThat(detail.get(0).content()).containsEntry("summary", "sum");
        assertThat(detail.get(0).depth()).isSameAs(Depth.DETAIL);
        assertThat(detail.get(0).content()).doesNotContainKey("l0Abstract");
        assertThat(detail.get(0).content()).doesNotContainKey("l1Overview");

        var abstract_ = facade.recall("alice", "hello", 5, MemoryTier.SEMANTIC, Depth.ABSTRACT);
        assertThat(abstract_.get(0).content()).containsEntry("summary", "sum");
        assertThat(abstract_.get(0).content()).doesNotContainKey("body");
        assertThat(abstract_.get(0).depth()).isSameAs(Depth.ABSTRACT);
    }
}
