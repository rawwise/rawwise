package io.github.rawwise.axiom.orchestrator;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public interface LeaseService {
    LeaseHandle acquireLease(String agentId, String resourceId, Duration ttl) throws CoordException;

    boolean renew(String agentId, String leaseId) throws CoordException;

    boolean release(String agentId, String leaseId) throws CoordException;

    Optional<LeaseHandle> getLease(String leaseId) throws CoordException;

    List<LeaseHandle> listLeasesByAgent(String agentId) throws CoordException;
}
