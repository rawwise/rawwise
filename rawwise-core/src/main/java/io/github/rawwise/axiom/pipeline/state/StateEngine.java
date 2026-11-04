package io.github.rawwise.axiom.pipeline.state;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface StateEngine {

    StateSpace createStateSpace(
            String agentId,
            String name,
            String initialState,
            List<String> states,
            List<StateTransition> transitions,
            Map<String, String> metadata)
            throws IOException;

    Optional<StateSpace> getStateSpace(String spaceId) throws IOException;

    List<StateSpace> listStateSpacesByAgent(String agentId) throws IOException;

    StateEntry read(String spaceId, String key) throws IOException;

    StateEntry write(
            String spaceId,
            String key,
            String value,
            String stateName,
            Map<String, String> metadata)
            throws IOException;

    StateEntry transition(String spaceId, String key, String toState, Map<String, String> metadata)
            throws IOException;

    List<StateEntry> scan(String spaceId, String prefix, int limit) throws IOException;

    boolean deleteStateSpace(String spaceId) throws IOException;
}
