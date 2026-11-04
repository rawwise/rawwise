package io.github.rawwise.axiom.pipeline.state;

import java.util.List;
import java.util.Map;

public record StateSpace(
        String spaceId,
        String agentId,
        String name,
        String initialState,
        List<String> states,
        List<StateTransition> transitions,
        Map<String, String> metadata,
        long createdAt,
        long updatedAt) {
    public StateSpace {
        states = List.copyOf(states);
        transitions = List.copyOf(transitions);
        metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
    }
}
