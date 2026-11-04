package io.github.rawwise.axiom.memory;

import io.github.rawwise.axiom.memory.exception.AxiomException;
import io.github.rawwise.axiom.memory.exception.MemoryEntryNotFoundException;
import io.github.rawwise.axiom.memory.exception.MemoryOperationException;
import io.github.rawwise.axiom.memory.exception.MemoryRecallException;
import io.github.rawwise.axiom.memory.exception.MemoryShareException;
import io.github.rawwise.axiom.memory.exception.MemoryStoreException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AxiomMemoryFacade implements MemoryFacade {

    static final String TABLE_ID = "default.agent_memory";

    private final AxiomClient axiomClient;
    private final ViewMetadataStore viewMetadataStore;
    private final ViewMetadataDecayUpdater viewMetadataDecayUpdater;

    private volatile AgentIndexStore agentIndexStore;

    public AxiomMemoryFacade(AxiomClient axiomClient) {
        this(axiomClient, null, null);
    }

    public AxiomMemoryFacade(
            AxiomClient axiomClient, ViewMetadataStore viewMetadataStore, DecayPolicy decayPolicy) {
        this.axiomClient = axiomClient;
        this.viewMetadataStore = viewMetadataStore;
        this.viewMetadataDecayUpdater =
                viewMetadataStore != null
                        ? new ViewMetadataDecayUpdater(
                                viewMetadataStore,
                                decayPolicy != null ? decayPolicy : DecayPolicy.DEFAULT)
                        : null;
    }

    public void setAgentIndexStore(AgentIndexStore store) {
        this.agentIndexStore = store;
    }

    @Override
    public boolean supportsScheduledLifecycle() {
        return viewMetadataStore != null;
    }

    @Override
    public MemoryEntry store(
            String agentId, Map<String, Object> content, MemoryTier memoryTier, double importance) {
        try {
            String entryId = UUID.randomUUID().toString();
            String body = content != null ? content.toString() : "";
            Map<String, String> properties = new LinkedHashMap<>();
            properties.put("agent_id", agentId);
            properties.put("memory_tier", memoryTier.name());
            properties.put("importance", String.valueOf(importance));

            axiomClient.ingest(TABLE_ID, entryId, body, properties);

            long now = System.currentTimeMillis();
            if (viewMetadataStore != null) {
                ViewMetadata vm = new ViewMetadata(entryId, agentTableId(agentId));
                viewMetadataStore.put(vm);
            }

            if (agentIndexStore != null) {
                agentIndexStore.index(agentId, entryId);
            }

            return new MemoryEntry(
                    entryId, agentId, memoryTier, Depth.DETAIL, content, importance, 1.0, now, now);
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            throw new MemoryStoreException("Failed to store memory for agent " + agentId, e);
        }
    }

    @Override
    public List<MemoryEntry> recall(
            String agentId, String query, int topK, MemoryTier memoryTier, Depth depth) {
        try {
            Map<String, String> filter = new HashMap<>();
            filter.put("agent_id", agentId);
            filter.put("memory_tier", memoryTier.name());
            List<Map<String, Object>> results =
                    axiomClient.search(TABLE_ID, query, topK, agentId, filter);

            List<MemoryEntry> entries = new ArrayList<>(results.size());
            for (Map<String, Object> result : results) {
                String entryId =
                        String.valueOf(
                                result.getOrDefault(
                                        "entryKey",
                                        result.getOrDefault(
                                                "key", result.getOrDefault("viewId", ""))));
                String summary = String.valueOf(result.getOrDefault("summary", ""));
                String body = String.valueOf(result.getOrDefault("body", ""));
                double score = toDouble(result.get("score"));

                Map<String, Object> content = new LinkedHashMap<>();
                content.put("summary", summary);
                if (depth == Depth.DETAIL && !body.isBlank()) {
                    content.put("body", body);
                }

                entries.add(
                        new MemoryEntry(
                                entryId,
                                agentId,
                                memoryTier,
                                depth,
                                content,
                                5.0,
                                1.0,
                                0,
                                0,
                                score));
            }
            return entries;
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            throw new MemoryRecallException("Failed to recall memory for agent " + agentId, e);
        }
    }

    @Override
    public void consolidate(String agentId, MemoryTier fromTier, MemoryTier toTier) {
        log.debug(
                "consolidate is a no-op: tier migration abolished, derivation handled by AutoDerivationHook (agent={} fromTier={} toTier={})",
                agentId,
                fromTier,
                toTier);
    }

    @Override
    public void decay(String agentId, Duration age) {
        if (viewMetadataDecayUpdater == null) {
            log.debug("decay skipped: viewMetadataDecayUpdater not configured");
            return;
        }
        try {
            double halfLifeMs = age.toMillis();
            DecayPolicy policy = new DecayPolicy(halfLifeMs, 0.1, 100, 600_000L);
            int decayed = viewMetadataDecayUpdater.decayTable(agentTableId(agentId), policy);
            log.info("decay complete agentId={} age={} decayed={}", agentId, age, decayed);
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            throw new MemoryOperationException("Failed to decay memory for agent " + agentId, e);
        }
    }

    @Override
    public Optional<MemoryEntry> reflect(String agentId, String query) {
        try {
            Map<String, String> filter = new HashMap<>();
            filter.put("agent_id", agentId);
            List<Map<String, Object>> results =
                    axiomClient.search(TABLE_ID, query, 1, agentId, filter);
            if (results.isEmpty()) {
                return Optional.empty();
            }
            Map<String, Object> top = results.get(0);
            String entryId =
                    String.valueOf(
                            top.getOrDefault(
                                    "entryKey",
                                    top.getOrDefault("key", top.getOrDefault("viewId", ""))));
            String summary = String.valueOf(top.getOrDefault("summary", ""));
            double score = toDouble(top.get("score"));

            Map<String, Object> content = new LinkedHashMap<>();
            content.put("summary", summary);
            return Optional.of(
                    new MemoryEntry(
                            entryId,
                            agentId,
                            MemoryTier.EPISODIC,
                            Depth.ABSTRACT,
                            content,
                            5.0,
                            1.0,
                            0,
                            0,
                            score));
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            throw new MemoryRecallException("Failed to reflect memory for agent " + agentId, e);
        }
    }

    @Override
    public void share(
            String fromAgentId, String toAgentId, String entryId, MemorySharePolicy policy) {
        try {
            Optional<Map<String, Object>> entryOpt = axiomClient.getEntry(TABLE_ID, entryId);
            if (entryOpt.isEmpty()) {
                throw new MemoryEntryNotFoundException("Memory entry not found: " + entryId);
            }
            Map<String, Object> entry = entryOpt.get();
            @SuppressWarnings("unchecked")
            Map<String, String> properties =
                    (Map<String, String>) entry.getOrDefault("properties", Map.of());
            String ownerAgent = properties.get("agent_id");
            if (!fromAgentId.equals(ownerAgent)) {
                throw new MemoryShareException(
                        "Agent " + fromAgentId + " does not own entry " + entryId);
            }

            String body = String.valueOf(entry.getOrDefault("body", ""));
            Map<String, String> newProperties = new LinkedHashMap<>(properties);
            newProperties.put("agent_id", toAgentId);
            newProperties.put("shared_from", fromAgentId);
            newProperties.put("shared_timestamp", String.valueOf(System.currentTimeMillis()));

            String newEntryId = UUID.randomUUID().toString();
            axiomClient.ingest(TABLE_ID, newEntryId, body, newProperties);

            if (viewMetadataStore != null) {
                Optional<ViewMetadata> vmOpt =
                        viewMetadataStore.get(agentTableId(fromAgentId), entryId);
                if (vmOpt.isPresent()) {
                    ViewMetadata copy = new ViewMetadata(newEntryId, agentTableId(toAgentId));
                    viewMetadataStore.put(copy);
                }
            }

            if (agentIndexStore != null) {
                agentIndexStore.index(toAgentId, newEntryId);
            }

            log.info(
                    "share complete fromAgent={} toAgent={} entryId={} newEntryId={}",
                    fromAgentId,
                    toAgentId,
                    entryId,
                    newEntryId);
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            throw new MemoryShareException("Failed to share memory entry " + entryId, e);
        }
    }

    @Override
    public MemoryStatus status(String agentId) {
        try {
            List<Map<String, Object>> entries = axiomClient.scan(TABLE_ID, agentId);
            int working = 0, episodic = 0, semantic = 0, procedural = 0;
            for (Map<String, Object> entry : entries) {
                @SuppressWarnings("unchecked")
                Map<String, String> props =
                        (Map<String, String>) entry.getOrDefault("properties", Map.of());
                String tier = props.get("memory_tier");
                if (tier == null) continue;
                try {
                    switch (MemoryTier.valueOf(tier)) {
                        case WORKING -> working++;
                        case EPISODIC -> episodic++;
                        case SEMANTIC -> semantic++;
                        case PROCEDURAL -> procedural++;
                    }
                } catch (IllegalArgumentException e) {
                    log.debug("Unknown memory tier in status: {}", tier);
                }
            }
            int total = working + episodic + semantic + procedural;
            return new MemoryStatus(agentId, working, episodic, semantic, procedural, total);
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            throw new MemoryOperationException(
                    "Failed to get memory status for agent " + agentId, e);
        }
    }

    @Override
    public List<MemoryEntry> listEntries(
            String agentId, MemoryTier memoryTier, int offset, int limit) {
        try {
            List<Map<String, Object>> filtered = listEntriesRaw(agentId, memoryTier);

            if (offset >= filtered.size()) {
                return List.of();
            }
            int end = Math.min(offset + limit, filtered.size());
            List<Map<String, Object>> page = filtered.subList(offset, end);

            long now = System.currentTimeMillis();
            List<MemoryEntry> result = new ArrayList<>(page.size());
            for (Map<String, Object> entry : page) {
                String entryId = String.valueOf(entry.get("key"));
                String body = String.valueOf(entry.getOrDefault("body", ""));
                @SuppressWarnings("unchecked")
                Map<String, String> props =
                        (Map<String, String>) entry.getOrDefault("properties", Map.of());
                double importance = parseDouble(props.getOrDefault("importance", "5.0"), 5.0);

                Map<String, Object> content = new LinkedHashMap<>();
                content.put("body", body);

                double decayFactor = 1.0;
                if (viewMetadataStore != null) {
                    Optional<ViewMetadata> vm =
                            viewMetadataStore.get(agentTableId(agentId), entryId);
                    if (vm.isPresent()) {
                        decayFactor = vm.get().decayFactor();
                    }
                }

                result.add(
                        new MemoryEntry(
                                entryId,
                                agentId,
                                memoryTier,
                                Depth.DETAIL,
                                content,
                                importance,
                                decayFactor,
                                0,
                                now));
            }
            return result;
        } catch (AxiomException e) {
            throw e;
        } catch (Exception e) {
            throw new MemoryOperationException(
                    "Failed to list memory entries for agent " + agentId, e);
        }
    }

    private List<Map<String, Object>> listEntriesRaw(String agentId, MemoryTier memoryTier)
            throws IOException {
        if (agentIndexStore != null) {
            List<String> entryKeys = agentIndexStore.scanAgent(agentId, 10_000);
            List<Map<String, Object>> filtered = new ArrayList<>(entryKeys.size());
            for (String entryKey : entryKeys) {
                Optional<Map<String, Object>> entryOpt = axiomClient.getEntry(TABLE_ID, entryKey);
                if (entryOpt.isEmpty()) continue;
                Map<String, Object> entry = entryOpt.get();
                @SuppressWarnings("unchecked")
                Map<String, String> props =
                        (Map<String, String>) entry.getOrDefault("properties", Map.of());
                String tier = props.get("memory_tier");
                if (memoryTier == null || memoryTier.name().equals(tier)) {
                    filtered.add(entry);
                }
            }
            return filtered;
        }

        List<Map<String, Object>> entries = axiomClient.scan(TABLE_ID, agentId);
        List<Map<String, Object>> filtered = new ArrayList<>();
        for (Map<String, Object> entry : entries) {
            @SuppressWarnings("unchecked")
            Map<String, String> props =
                    (Map<String, String>) entry.getOrDefault("properties", Map.of());
            String tier = props.get("memory_tier");
            if (memoryTier == null || memoryTier.name().equals(tier)) {
                filtered.add(entry);
            }
        }
        return filtered;
    }

    private String agentTableId(String agentId) {
        return TABLE_ID + ":" + agentId;
    }

    private double toDouble(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value != null) {
            try {
                return Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
        return 0.0;
    }

    private double parseDouble(String value, double defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
