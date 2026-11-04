package io.github.rawwise.axiom.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

import io.github.rawwise.axiom.memory.Depth;
import io.github.rawwise.axiom.memory.MemoryEntry;
import io.github.rawwise.axiom.memory.MemoryFacade;
import io.github.rawwise.axiom.memory.MemorySharePolicy;
import io.github.rawwise.axiom.memory.MemoryStatus;
import io.github.rawwise.axiom.memory.MemoryTier;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("AxiomScheduler E2E - Decay/Consolidation periodic scheduling")
class AxiomSchedulerE2ETest {

    @Test
    @DisplayName("S1 enabled+warmup: scheduleAtFixedRate trigger decay")
    void enabledWithWarmupTriggersDecay() throws Exception {
        RecordingMemoryFacade facade = new RecordingMemoryFacade();
        AxiomSchedulerConfig config =
                new AxiomSchedulerConfig(
                        true,
                        true,
                        2,
                        5L,
                        List.of("agent-A", "agent-B"),
                        true,
                        200L,
                        false,
                        60_000L);
        try (AxiomScheduler scheduler = new AxiomScheduler(facade, config)) {
            scheduler.start();

            await().atMost(Duration.ofMillis(2_000)).until(() -> facade.decayCount.get() >= 2 * 2);

            int decayCalls = facade.decayCount.get();
            int consolCalls = facade.consolidateCount.get();
            assertThat(decayCalls)
                    .as("warmup once + at least one fixed-rate tick across 2 agents")
                    .isGreaterThanOrEqualTo(4);
            assertThat(consolCalls).as("consolidate disabled: must remain zero").isZero();
        }

        assertThatCode(
                        () -> {
                            try (AxiomScheduler s =
                                    new AxiomScheduler(
                                            facade,
                                            new AxiomSchedulerConfig(
                                                    true,
                                                    false,
                                                    1,
                                                    1L,
                                                    List.of("agent-A"),
                                                    true,
                                                    100L,
                                                    true,
                                                    100L))) {
                                s.start();
                            }
                        })
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("S2 disabled: start is no-op, close does not throw")
    void disabledIsNoOp() {
        RecordingMemoryFacade facade = new RecordingMemoryFacade();
        AxiomSchedulerConfig config =
                new AxiomSchedulerConfig(
                        false, true, 4, 5L, List.of("agent-A"), true, 100L, true, 100L);

        AxiomScheduler scheduler = new AxiomScheduler(facade, config);
        scheduler.start();

        try {
            Thread.sleep(300L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertThat(facade.decayCount.get()).isZero();
        assertThat(facade.consolidateCount.get()).isZero();

        assertThatCode(scheduler::close).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("S3 facade without scheduled lifecycle: start skips scheduling (no-op guard)")
    void facadeWithoutScheduledLifecycleSkips() throws Exception {
        NoScheduledLifecycleFacade facade = new NoScheduledLifecycleFacade();
        AxiomSchedulerConfig config =
                new AxiomSchedulerConfig(
                        true, true, 2, 5L, List.of("agent-A"), true, 50L, true, 50L);

        try (AxiomScheduler scheduler = new AxiomScheduler(facade, config)) {
            scheduler.start();
            Thread.sleep(300L);
            assertThat(facade.decayCount.get())
                    .as("decay must not run when facade has no scheduled lifecycle")
                    .isZero();
            assertThat(facade.consolidateCount.get())
                    .as("consolidate must not run when facade has no scheduled lifecycle")
                    .isZero();
        }
    }

    private static final class NoScheduledLifecycleFacade implements MemoryFacade {
        final AtomicInteger decayCount = new AtomicInteger();
        final AtomicInteger consolidateCount = new AtomicInteger();

        @Override
        public boolean supportsScheduledLifecycle() {
            return false;
        }

        @Override
        public MemoryEntry store(
                String agentId,
                Map<String, Object> content,
                MemoryTier memoryTier,
                double importance) {
            throw new UnsupportedOperationException("not used in scheduler test");
        }

        @Override
        public List<MemoryEntry> recall(
                String agentId, String query, int topK, MemoryTier memoryTier, Depth depth) {
            return List.of();
        }

        @Override
        public void consolidate(String agentId, MemoryTier fromTier, MemoryTier toTier) {
            consolidateCount.incrementAndGet();
        }

        @Override
        public void decay(String agentId, Duration age) {
            decayCount.incrementAndGet();
        }

        @Override
        public Optional<MemoryEntry> reflect(String agentId, String query) {
            return Optional.empty();
        }

        @Override
        public void share(
                String fromAgentId, String toAgentId, String entryId, MemorySharePolicy policy) {}

        @Override
        public MemoryStatus status(String agentId) {
            return new MemoryStatus(agentId, 0, 0, 0, 0, 0);
        }

        @Override
        public List<MemoryEntry> listEntries(
                String agentId, MemoryTier memoryTier, int offset, int limit) {
            return List.of();
        }
    }

    private static final class RecordingMemoryFacade implements MemoryFacade {
        final AtomicInteger decayCount = new AtomicInteger();
        final AtomicInteger consolidateCount = new AtomicInteger();

        @Override
        public MemoryEntry store(
                String agentId,
                Map<String, Object> content,
                MemoryTier memoryTier,
                double importance) {
            throw new UnsupportedOperationException("not used in scheduler test");
        }

        @Override
        public List<MemoryEntry> recall(
                String agentId, String query, int topK, MemoryTier memoryTier, Depth depth) {
            return List.of();
        }

        @Override
        public void consolidate(String agentId, MemoryTier fromTier, MemoryTier toTier) {
            consolidateCount.incrementAndGet();
        }

        @Override
        public void decay(String agentId, Duration age) {
            decayCount.incrementAndGet();
        }

        @Override
        public Optional<MemoryEntry> reflect(String agentId, String query) {
            return Optional.empty();
        }

        @Override
        public void share(
                String fromAgentId, String toAgentId, String entryId, MemorySharePolicy policy) {}

        @Override
        public MemoryStatus status(String agentId) {
            return new MemoryStatus(agentId, 0, 0, 0, 0, 0);
        }

        @Override
        public List<MemoryEntry> listEntries(
                String agentId, MemoryTier memoryTier, int offset, int limit) {
            return List.of();
        }
    }
}
