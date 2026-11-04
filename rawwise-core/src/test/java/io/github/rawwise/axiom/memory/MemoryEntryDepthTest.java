package io.github.rawwise.axiom.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Task 1.6 - MemoryEntry carries Depth instead of MemoryLoadTier")
class MemoryEntryDepthTest {

    @Test
    @DisplayName("entry carries depth instead of loadTier (10-arg + 9-arg constructors)")
    void entryCarriesDepthInsteadOfLoadTier() {
        MemoryEntry e =
                new MemoryEntry(
                        "k", "a", MemoryTier.SEMANTIC, Depth.DETAIL, Map.of(), 5.0, 1.0, 0L, 0L);
        assertThat(e.depth()).isSameAs(Depth.DETAIL);
        assertThat(e.entryId()).isEqualTo("k");
        assertThat(e.agentId()).isEqualTo("a");
        assertThat(e.memoryTier()).isSameAs(MemoryTier.SEMANTIC);
    }

    @Test
    @DisplayName("recall entry carries depth + score via 10-arg constructor")
    void recallEntryCarriesDepthAndScore() {
        MemoryEntry e =
                new MemoryEntry(
                        "k",
                        "a",
                        MemoryTier.EPISODIC,
                        Depth.ABSTRACT,
                        Map.of("summary", "s"),
                        5.0,
                        1.0,
                        0L,
                        0L,
                        0.9);
        assertThat(e.depth()).isSameAs(Depth.ABSTRACT);
        assertThat(e.relevanceScore()).isEqualTo(0.9);
        assertThat(e.content()).containsEntry("summary", "s");
    }

    @Test
    @DisplayName("null content is normalized to empty map")
    void nullContentNormalized() {
        MemoryEntry e =
                new MemoryEntry(
                        "k", "a", MemoryTier.SEMANTIC, Depth.DETAIL, null, 5.0, 1.0, 0L, 0L);
        assertThat(e.content()).isEmpty();
    }

    @Test
    @DisplayName("no loadTier accessor remains")
    void noLoadTierAccessor() {
        assertThatThrownBy(
                        () ->
                                MemoryEntry.class
                                        .getMethod("loadTier")
                                        .invoke(
                                                new MemoryEntry(
                                                        "k",
                                                        "a",
                                                        MemoryTier.SEMANTIC,
                                                        Depth.DETAIL,
                                                        Map.of(),
                                                        5.0,
                                                        1.0,
                                                        0L,
                                                        0L)))
                .isInstanceOf(NoSuchMethodException.class);
    }
}
