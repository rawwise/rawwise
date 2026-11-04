package io.github.rawwise.axiom.pipeline.state;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStateEngine implements StateEngine {

    private final Map<String, StateSpace> spaces = new ConcurrentHashMap<>();
    private final Map<String, Map<String, StateEntry>> entries = new ConcurrentHashMap<>();

    @Override
    public StateSpace createStateSpace(
            String agentId,
            String name,
            String initialState,
            List<String> states,
            List<StateTransition> transitions,
            Map<String, String> metadata) {
        String spaceId = "space:" + UUID.randomUUID().toString().substring(0, 8);
        long now = System.currentTimeMillis();
        var space =
                new StateSpace(
                        spaceId,
                        agentId,
                        name,
                        initialState,
                        List.copyOf(states),
                        List.copyOf(transitions),
                        metadata == null ? Map.of() : Map.copyOf(metadata),
                        now,
                        now);
        spaces.put(spaceId, space);
        entries.put(spaceId, new ConcurrentHashMap<>());
        return space;
    }

    @Override
    public Optional<StateSpace> getStateSpace(String spaceId) {
        return Optional.ofNullable(spaces.get(spaceId));
    }

    @Override
    public List<StateSpace> listStateSpacesByAgent(String agentId) {
        return spaces.values().stream().filter(s -> s.agentId().equals(agentId)).toList();
    }

    @Override
    public StateEntry read(String spaceId, String key) {
        Map<String, StateEntry> spaceEntries = entries.get(spaceId);
        if (spaceEntries == null) return null;
        return spaceEntries.get(key);
    }

    @Override
    public StateEntry write(
            String spaceId,
            String key,
            String value,
            String stateName,
            Map<String, String> metadata) {
        Map<String, StateEntry> spaceEntries =
                entries.computeIfAbsent(spaceId, k -> new ConcurrentHashMap<>());
        StateEntry existing = spaceEntries.get(key);
        long now = System.currentTimeMillis();
        long version = existing == null ? 1 : existing.version() + 1;
        long createdAt = existing == null ? now : existing.createdAt();
        StateEntry entry =
                new StateEntry(
                        key,
                        value,
                        stateName,
                        version,
                        metadata == null ? Map.of() : Map.copyOf(metadata),
                        createdAt,
                        now);
        spaceEntries.put(key, entry);
        return entry;
    }

    @Override
    public StateEntry transition(
            String spaceId, String key, String toState, Map<String, String> metadata) {
        Map<String, StateEntry> spaceEntries = entries.get(spaceId);
        if (spaceEntries == null) return null;
        StateEntry current = spaceEntries.get(key);
        if (current == null) return null;
        long now = System.currentTimeMillis();
        StateEntry updated =
                new StateEntry(
                        key,
                        current.value(),
                        toState,
                        current.version() + 1,
                        metadata == null ? current.metadata() : Map.copyOf(metadata),
                        current.createdAt(),
                        now);
        spaceEntries.put(key, updated);
        return updated;
    }

    @Override
    public List<StateEntry> scan(String spaceId, String prefix, int limit) {
        Map<String, StateEntry> spaceEntries = entries.get(spaceId);
        if (spaceEntries == null) return List.of();
        return spaceEntries.entrySet().stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .limit(limit)
                .map(Map.Entry::getValue)
                .toList();
    }

    @Override
    public boolean deleteStateSpace(String spaceId) {
        spaces.remove(spaceId);
        entries.remove(spaceId);
        return true;
    }
}
