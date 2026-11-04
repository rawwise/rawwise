package io.github.rawwise.axiom.orchestrator;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface BarrierService {
    BarrierHandle create(String barrierId, int parties, Duration ttl) throws CoordException;

    boolean arrive(String barrierId, String agentId) throws CoordException;

    CompletableFuture<BarrierHandle> await(String barrierId, Duration timeout)
            throws CoordException;

    boolean awaitSync(String barrierId, Duration timeout) throws CoordException;

    Optional<BarrierHandle> getBarrier(String barrierId) throws CoordException;

    boolean reset(String barrierId) throws CoordException;
}
