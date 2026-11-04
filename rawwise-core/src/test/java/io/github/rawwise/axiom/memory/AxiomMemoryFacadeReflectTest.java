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

class AxiomMemoryFacadeReflectTest {

    @Test
    void reflectUsesSearchInsteadOfL0TextSimilarity() {
        AxiomClient client = mock(AxiomClient.class);
        when(client.search(
                        eq("default.agent_memory"),
                        eq("what happened"),
                        anyInt(),
                        eq("alice"),
                        any()))
                .thenReturn(
                        List.of(
                                Map.of(
                                        "entryKey",
                                        "k9",
                                        "score",
                                        0.85,
                                        "summary",
                                        "reflected",
                                        "properties",
                                        Map.of("agent_id", "alice"))));
        AxiomMemoryFacade facade = new AxiomMemoryFacade(client);
        var entry = facade.reflect("alice", "what happened");
        assertThat(entry).isPresent();
        assertThat(entry.get().entryId()).isEqualTo("k9");
        assertThat(entry.get().content()).containsEntry("summary", "reflected");
        assertThat(entry.get().depth()).isSameAs(Depth.ABSTRACT);
    }

    @Test
    void reflectEmptySearchReturnsEmpty() {
        AxiomClient client = mock(AxiomClient.class);
        when(client.search(eq("default.agent_memory"), any(), anyInt(), eq("alice"), any()))
                .thenReturn(List.of());
        AxiomMemoryFacade facade = new AxiomMemoryFacade(client);
        assertThat(facade.reflect("alice", "nothing")).isEmpty();
    }
}
