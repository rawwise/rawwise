package io.github.rawwise.axiom.orchestrator;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public interface ElectionService {
    ElectionHandle campaign(String electionId, String agentId, Duration termTtl)
            throws CoordException;

    boolean resign(String electionId, String agentId) throws CoordException;

    boolean renewTerm(String electionId, String agentId, Duration termTtl) throws CoordException;

    Optional<ElectionHandle> getLeader(String electionId) throws CoordException;

    List<ElectionHandle> listElectionsByAgent(String agentId) throws CoordException;
}
