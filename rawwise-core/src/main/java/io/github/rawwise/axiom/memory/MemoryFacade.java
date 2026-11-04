package io.github.rawwise.axiom.memory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface MemoryFacade extends AutoCloseable {
    MemoryEntry store(
            String agentId, Map<String, Object> content, MemoryTier memoryTier, double importance);

    List<MemoryEntry> recall(
            String agentId, String query, int topK, MemoryTier memoryTier, Depth depth);

    void consolidate(String agentId, MemoryTier fromTier, MemoryTier toTier);

    void decay(String agentId, Duration age);

    Optional<MemoryEntry> reflect(String agentId, String query);

    void share(String fromAgentId, String toAgentId, String entryId, MemorySharePolicy policy);

    MemoryStatus status(String agentId);

    List<MemoryEntry> listEntries(String agentId, MemoryTier memoryTier, int offset, int limit);

    /**
     * 是否支持调度驱动的记忆生命周期管理（decay/consolidate）。 HTTP-backed facade 在缺少本地 ViewMetadataStore 时这两个操作为
     * no-op，AxiomScheduler 应据此跳过空转调度。
     */
    default boolean supportsScheduledLifecycle() {
        return true;
    }

    @Override
    default void close() {}
}
