package io.github.rawwise.axiom.orchestrator;

import java.time.Duration;
import java.util.Optional;

public interface LockService {
    LockHandle acquire(String agentId, String resource, Duration ttl) throws CoordException;

    boolean release(String agentId, String resource, String fencingToken) throws CoordException;

    Optional<LockHandle> getLock(String resource) throws CoordException;

    boolean extend(String agentId, String resource, String fencingToken, Duration ttl)
            throws CoordException;
}
