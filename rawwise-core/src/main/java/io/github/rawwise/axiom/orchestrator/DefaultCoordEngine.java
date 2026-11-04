package io.github.rawwise.axiom.orchestrator;

import io.github.rawwise.governance.context.ContextJson;
import io.github.rawwise.metastore.KeyValue;
import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultCoordEngine implements CoordEngine, AutoCloseable {

    private static final String LOCK_PREFIX = "coord::lock::";
    private static final String LEASE_PREFIX = "coord::lease::";
    private static final String LEASE_AGENT_IDX = "coord::lease-idx::";
    private static final String BARRIER_PREFIX = "coord::barrier::";
    private static final String ELECTION_PREFIX = "coord::election::";
    private static final String ELECTION_AGENT_IDX = "coord::election-idx::";
    private static final String ELECTION_TOMBSTONE_PREFIX = "coord::election-term::";
    private static final String SEQ_PREFIX = "coord::seq::";
    private static final String PUBSUB_SUB_PREFIX = "coord::pubsub-sub::";
    private static final String PUBSUB_MSG_PREFIX = "coord::pubsub-msg::";
    private static final String PUBSUB_CURSOR_PREFIX = "coord::pubsub-cursor::";
    private static final String FENCING_TOKEN_KEY = "coord::fencing-token";
    private static final String SEP = "::";

    private final Metastore metastore;
    private final CoordEngineConfig config;
    private final CoordTrustAdapter trustAdapter;
    private final CoordGcService gcService;

    private final ConcurrentHashMap<String, CompletableFuture<BarrierHandle>> barrierFutures =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> subscriptionIdToTopic =
            new ConcurrentHashMap<>();

    public DefaultCoordEngine(CoordEngineConfig config) {
        this.config = config;
        this.metastore = config.metastore();
        this.trustAdapter = config.trustAdapter();
        initFencingToken();
        this.gcService =
                new CoordGcService(metastore, config.gcIntervalMs(), config.messageTtlMs());
    }

    private void initFencingToken() {
        byte[] key = FENCING_TOKEN_KEY.getBytes(StandardCharsets.UTF_8);
        try {
            metastore.compareAndSwap(
                    List.of(new Precondition.NotExists(key)),
                    List.of(new Mutation.Put(key, "0".getBytes(StandardCharsets.UTF_8))));
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize fencing token", e);
        }
    }

    private String nextFencingToken() throws CoordException {
        byte[] key = FENCING_TOKEN_KEY.getBytes(StandardCharsets.UTF_8);
        for (int attempt = 0; attempt < config.maxRetryCas(); attempt++) {
            try {
                Optional<byte[]> existing = metastore.get(key);
                if (existing.isEmpty()) {
                    throw new CoordException("fencing-token", "Fencing token not initialized");
                }
                long currentVal =
                        Long.parseLong(new String(existing.get(), StandardCharsets.UTF_8).trim());
                long nextVal = currentVal + 1;
                byte[] nextBytes = Long.toString(nextVal).getBytes(StandardCharsets.UTF_8);

                List<Precondition> preconditions =
                        List.of(new Precondition.ValueEquals(key, existing.get()));
                List<Mutation> mutations = List.of(new Mutation.Put(key, nextBytes));

                boolean success = metastore.compareAndSwap(preconditions, mutations);
                if (success) return Long.toString(nextVal);

                casRetrySleep(attempt);
            } catch (IOException e) {
                throw new CoordException("fencing-token", "Failed to generate fencing token", e);
            }
        }
        throw new CoordConflictException(
                "fencing-token", "CAS retries exhausted for fencing token");
    }

    private void casRetrySleep(int attempt) {
        try {
            long baseDelay = config.casRetryDelayMs();
            long maxDelay = config.maxCasRetryDelayMs();
            long expDelay = Math.min(baseDelay * (1L << Math.min(attempt, 30)), maxDelay);
            long jitter = (long) (expDelay * ThreadLocalRandom.current().nextDouble(-0.5, 0.5));
            long delay = Math.max(1, expDelay + jitter);
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public LockHandle acquire(String agentId, String resource, Duration ttl) throws CoordException {
        byte[] lockKey = lockKey(resource);
        long now = System.currentTimeMillis();
        long expiresAt = now + ttl.toMillis();

        for (int attempt = 0; attempt < config.maxRetryCas(); attempt++) {
            try {
                Optional<byte[]> existing = metastore.get(lockKey);

                if (existing.isPresent()) {
                    LockHandle existingLock = deserialize(existing.get(), LockHandle.class);
                    if (existingLock.expiresAt() > now) {
                        if (existingLock.agentId().equals(agentId)) {
                            String fencingToken = existingLock.fencingToken();
                            LockHandle reentrant =
                                    new LockHandle(
                                            resource,
                                            agentId,
                                            fencingToken,
                                            existingLock.acquiredAt(),
                                            existingLock.expiresAt(),
                                            existingLock.holdCount() + 1);

                            List<Precondition> preconditions =
                                    List.of(new Precondition.ValueEquals(lockKey, existing.get()));
                            List<Mutation> mutations =
                                    List.of(new Mutation.Put(lockKey, serialize(reentrant)));

                            boolean success = cas(resource, preconditions, mutations);
                            if (success) {
                                log.debug(
                                        "Lock re-acquired resource={} agent={} holdCount={}",
                                        resource,
                                        agentId,
                                        reentrant.holdCount());
                                return reentrant;
                            }
                            casRetrySleep(attempt);
                            continue;
                        }
                        throw new CoordAcquireTimeoutException(
                                resource,
                                "Resource locked by agent="
                                        + existingLock.agentId()
                                        + " until "
                                        + existingLock.expiresAt());
                    }
                }

                String fencingToken = nextFencingToken();
                LockHandle lock =
                        new LockHandle(resource, agentId, fencingToken, now, expiresAt, 1);

                List<Precondition> preconditions;
                if (existing.isPresent()) {
                    preconditions = List.of(new Precondition.ValueEquals(lockKey, existing.get()));
                } else {
                    preconditions = List.of(new Precondition.NotExists(lockKey));
                }

                List<Mutation> mutations = List.of(new Mutation.Put(lockKey, serialize(lock)));

                boolean success = cas(resource, preconditions, mutations);
                if (success) {
                    if (trustAdapter != null) {
                        trustAdapter.recordLockChange(resource, agentId, "acquire", now);
                    }
                    log.debug(
                            "Lock acquired resource={} agent={} fencingToken={}",
                            resource,
                            agentId,
                            fencingToken);
                    return lock;
                }

                casRetrySleep(attempt);
            } catch (IOException e) {
                throw new CoordException(resource, "Failed to acquire lock", e);
            }
        }
        throw new CoordConflictException(resource, "Lock CAS retries exhausted");
    }

    @Override
    public boolean release(String agentId, String resource, String fencingToken)
            throws CoordException {
        byte[] lockKey = lockKey(resource);
        try {
            Optional<byte[]> existing = metastore.get(lockKey);
            if (existing.isEmpty()) return false;

            LockHandle lock = deserialize(existing.get(), LockHandle.class);
            if (!lock.agentId().equals(agentId) || !lock.fencingToken().equals(fencingToken)) {
                throw new CoordConflictException(
                        resource, "Lock owner mismatch or fencing token invalid");
            }

            if (lock.holdCount() > 1) {
                LockHandle decremented =
                        new LockHandle(
                                resource,
                                agentId,
                                fencingToken,
                                lock.acquiredAt(),
                                lock.expiresAt(),
                                lock.holdCount() - 1);

                List<Precondition> preconditions =
                        List.of(new Precondition.ValueEquals(lockKey, existing.get()));
                List<Mutation> mutations =
                        List.of(new Mutation.Put(lockKey, serialize(decremented)));

                boolean success = cas(resource, preconditions, mutations);
                if (success) {
                    log.debug(
                            "Lock holdCount decremented resource={} agent={} holdCount={}",
                            resource,
                            agentId,
                            decremented.holdCount());
                }
                return success;
            }

            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(lockKey, existing.get()));
            List<Mutation> mutations = List.of(new Mutation.Delete(lockKey));

            boolean success = cas(resource, preconditions, mutations);
            if (success) {
                if (trustAdapter != null) {
                    trustAdapter.recordLockChange(
                            resource, agentId, "release", System.currentTimeMillis());
                }
                log.debug("Lock released resource={} agent={}", resource, agentId);
            }
            return success;
        } catch (IOException e) {
            throw new CoordException(resource, "Failed to release lock", e);
        }
    }

    @Override
    public Optional<LockHandle> getLock(String resource) throws CoordException {
        byte[] lockKey = lockKey(resource);
        try {
            Optional<byte[]> value = metastore.get(lockKey);
            if (value.isEmpty()) return Optional.empty();
            LockHandle lock = deserialize(value.get(), LockHandle.class);
            if (lock.expiresAt() <= System.currentTimeMillis()) {
                return Optional.empty();
            }
            return Optional.of(lock);
        } catch (IOException e) {
            throw new CoordException(resource, "Failed to read lock", e);
        }
    }

    @Override
    public boolean extend(String agentId, String resource, String fencingToken, Duration ttl)
            throws CoordException {
        byte[] lockKey = lockKey(resource);
        try {
            Optional<byte[]> existing = metastore.get(lockKey);
            if (existing.isEmpty()) return false;

            LockHandle lock = deserialize(existing.get(), LockHandle.class);
            if (!lock.agentId().equals(agentId) || !lock.fencingToken().equals(fencingToken)) {
                throw new CoordConflictException(
                        resource, "Lock owner mismatch or fencing token invalid");
            }
            if (lock.expiresAt() <= System.currentTimeMillis()) {
                throw new CoordExpiredException(resource, "Lock already expired");
            }

            long newExpiresAt = System.currentTimeMillis() + ttl.toMillis();
            LockHandle extended =
                    new LockHandle(
                            resource,
                            agentId,
                            fencingToken,
                            lock.acquiredAt(),
                            newExpiresAt,
                            lock.holdCount());

            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(lockKey, existing.get()));
            List<Mutation> mutations = List.of(new Mutation.Put(lockKey, serialize(extended)));

            boolean success = cas(resource, preconditions, mutations);
            if (success) {
                log.debug(
                        "Lock extended resource={} agent={} newExpiresAt={}",
                        resource,
                        agentId,
                        newExpiresAt);
            }
            return success;
        } catch (IOException e) {
            throw new CoordException(resource, "Failed to extend lock", e);
        }
    }

    @Override
    public LeaseHandle acquireLease(String agentId, String resourceId, Duration ttl)
            throws CoordException {
        long now = System.currentTimeMillis();
        String leaseId = "lease-" + UUID.randomUUID();
        long expiresAt = now + ttl.toMillis();
        String fencingToken = nextFencingToken();

        long ttlMs = ttl.toMillis();
        LeaseHandle lease =
                new LeaseHandle(
                        leaseId, agentId, resourceId, fencingToken, now, expiresAt, 0, ttlMs);

        byte[] leaseKey = leaseKey(leaseId);
        byte[] agentIdxKey = leaseAgentIdxKey(agentId, leaseId);

        List<Precondition> preconditions = List.of(new Precondition.NotExists(leaseKey));
        List<Mutation> mutations =
                List.of(
                        new Mutation.Put(leaseKey, serialize(lease)),
                        new Mutation.Put(agentIdxKey, leaseId.getBytes(StandardCharsets.UTF_8)));

        boolean success = cas(resourceId, preconditions, mutations);
        if (!success) {
            throw new CoordConflictException(leaseId, "Lease CAS conflict");
        }

        if (trustAdapter != null) {
            trustAdapter.recordLeaseChange(leaseId, agentId, "acquire", now);
        }
        log.debug("Lease acquired leaseId={} agent={} resource={}", leaseId, agentId, resourceId);
        return lease;
    }

    @Override
    public boolean renew(String agentId, String leaseId) throws CoordException {
        byte[] leaseKey = leaseKey(leaseId);
        try {
            Optional<byte[]> existing = metastore.get(leaseKey);
            if (existing.isEmpty()) return false;

            LeaseHandle lease = deserialize(existing.get(), LeaseHandle.class);
            if (!lease.agentId().equals(agentId)) {
                throw new CoordConflictException(leaseId, "Lease owner mismatch");
            }
            if (lease.expiresAt() <= System.currentTimeMillis()) {
                throw new CoordExpiredException(leaseId, "Lease already expired");
            }

            long now = System.currentTimeMillis();
            long originalTtl = lease.originalTtlMs();
            LeaseHandle renewed =
                    new LeaseHandle(
                            leaseId,
                            agentId,
                            lease.resourceId(),
                            lease.fencingToken(),
                            lease.acquiredAt(),
                            now + originalTtl,
                            lease.renewalCount() + 1,
                            lease.originalTtlMs());

            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(leaseKey, existing.get()));
            List<Mutation> mutations = List.of(new Mutation.Put(leaseKey, serialize(renewed)));

            boolean success = cas(leaseId, preconditions, mutations);
            if (success) {
                log.debug(
                        "Lease renewed leaseId={} agent={} renewalCount={}",
                        leaseId,
                        agentId,
                        renewed.renewalCount());
            }
            return success;
        } catch (IOException e) {
            throw new CoordException(leaseId, "Failed to renew lease", e);
        }
    }

    @Override
    public boolean release(String agentId, String leaseId) throws CoordException {
        byte[] leaseKey = leaseKey(leaseId);
        try {
            Optional<byte[]> existing = metastore.get(leaseKey);
            if (existing.isEmpty()) return false;

            LeaseHandle lease = deserialize(existing.get(), LeaseHandle.class);
            if (!lease.agentId().equals(agentId)) {
                throw new CoordConflictException(leaseId, "Lease owner mismatch");
            }

            byte[] agentIdxKey = leaseAgentIdxKey(agentId, leaseId);
            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(leaseKey, existing.get()));
            List<Mutation> mutations =
                    List.of(new Mutation.Delete(leaseKey), new Mutation.Delete(agentIdxKey));

            boolean success = cas(leaseId, preconditions, mutations);
            if (success) {
                if (trustAdapter != null) {
                    trustAdapter.recordLeaseChange(
                            leaseId, agentId, "release", System.currentTimeMillis());
                }
                log.debug("Lease released leaseId={} agent={}", leaseId, agentId);
            }
            return success;
        } catch (IOException e) {
            throw new CoordException(leaseId, "Failed to release lease", e);
        }
    }

    @Override
    public Optional<LeaseHandle> getLease(String leaseId) throws CoordException {
        byte[] leaseKey = leaseKey(leaseId);
        try {
            Optional<byte[]> value = metastore.get(leaseKey);
            if (value.isEmpty()) return Optional.empty();
            LeaseHandle lease = deserialize(value.get(), LeaseHandle.class);
            if (lease.expiresAt() <= System.currentTimeMillis()) {
                return Optional.empty();
            }
            return Optional.of(lease);
        } catch (IOException e) {
            throw new CoordException(leaseId, "Failed to read lease", e);
        }
    }

    @Override
    public List<LeaseHandle> listLeasesByAgent(String agentId) throws CoordException {
        byte[] prefix = (LEASE_AGENT_IDX + agentId + SEP).getBytes(StandardCharsets.UTF_8);
        try {
            List<KeyValue> entries = metastore.scanAll(prefix);
            List<LeaseHandle> leases = new ArrayList<>();
            for (KeyValue kv : entries) {
                String lid = new String(kv.value(), StandardCharsets.UTF_8);
                Optional<LeaseHandle> lease = getLease(lid);
                lease.ifPresent(leases::add);
            }
            return leases;
        } catch (IOException e) {
            throw new CoordException(agentId, "Failed to list leases", e);
        }
    }

    @Override
    public BarrierHandle create(String barrierId, int parties, Duration ttl) throws CoordException {
        long now = System.currentTimeMillis();
        long expiresAt = now + ttl.toMillis();
        BarrierHandle barrier =
                new BarrierHandle(barrierId, parties, List.of(), false, now, expiresAt);

        byte[] key = barrierKey(barrierId);
        List<Precondition> preconditions = List.of(new Precondition.NotExists(key));
        List<Mutation> mutations = List.of(new Mutation.Put(key, serialize(barrier)));

        boolean success = cas(barrierId, preconditions, mutations);
        if (!success) {
            throw new CoordConflictException(barrierId, "Barrier already exists");
        }

        log.debug("Barrier created barrierId={} parties={}", barrierId, parties);
        return barrier;
    }

    @Override
    public boolean arrive(String barrierId, String agentId) throws CoordException {
        byte[] key = barrierKey(barrierId);

        for (int attempt = 0; attempt < config.maxRetryCas(); attempt++) {
            try {
                Optional<byte[]> existing = metastore.get(key);
                if (existing.isEmpty()) {
                    throw new CoordNotFoundException(barrierId);
                }

                BarrierHandle barrier = deserialize(existing.get(), BarrierHandle.class);
                if (barrier.triggered()) return true;
                if (barrier.arrivedAgents().contains(agentId)) return true;
                if (barrier.expiresAt() > 0 && barrier.expiresAt() <= System.currentTimeMillis()) {
                    throw new CoordExpiredException(barrierId, "Barrier already expired");
                }

                List<String> newArrived = new ArrayList<>(barrier.arrivedAgents());
                newArrived.add(agentId);
                boolean triggered = newArrived.size() >= barrier.parties();
                BarrierHandle updated =
                        new BarrierHandle(
                                barrierId,
                                barrier.parties(),
                                newArrived,
                                triggered,
                                barrier.createdAt(),
                                barrier.expiresAt());

                List<Precondition> preconditions =
                        List.of(new Precondition.ValueEquals(key, existing.get()));
                List<Mutation> mutations = List.of(new Mutation.Put(key, serialize(updated)));

                boolean success = cas(barrierId, preconditions, mutations);
                if (success) {
                    if (triggered) {
                        CompletableFuture<BarrierHandle> future = barrierFutures.remove(barrierId);
                        if (future != null) future.complete(updated);
                        log.info(
                                "Barrier triggered barrierId={} parties={}",
                                barrierId,
                                barrier.parties());
                    }
                    return true;
                }

                casRetrySleep(attempt);
            } catch (IOException e) {
                throw new CoordException(barrierId, "Failed to arrive at barrier", e);
            }
        }
        throw new CoordConflictException(barrierId, "Barrier arrive CAS retries exhausted");
    }

    @Override
    public CompletableFuture<BarrierHandle> await(String barrierId, Duration timeout)
            throws CoordException {
        Optional<BarrierHandle> current = getBarrier(barrierId);
        if (current.isEmpty()) throw new CoordNotFoundException(barrierId);
        if (current.get().triggered()) {
            return CompletableFuture.completedFuture(current.get());
        }

        CompletableFuture<BarrierHandle> trigger =
                barrierFutures.computeIfAbsent(barrierId, k -> new CompletableFuture<>());

        CompletableFuture<BarrierHandle> personalFuture = new CompletableFuture<>();
        trigger.whenComplete(
                (result, ex) -> {
                    if (ex != null) personalFuture.completeExceptionally(ex);
                    else personalFuture.complete(result);
                });
        personalFuture.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);

        Optional<BarrierHandle> recheck = getBarrier(barrierId);
        if (recheck.isPresent() && recheck.get().triggered()) {
            trigger.complete(recheck.get());
            barrierFutures.remove(barrierId, trigger);
        }

        return personalFuture;
    }

    @Override
    public boolean awaitSync(String barrierId, Duration timeout) throws CoordException {
        try {
            CompletableFuture<BarrierHandle> future = await(barrierId, timeout);
            future.get();
            return true;
        } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof java.util.concurrent.TimeoutException) {
                return false;
            }
            throw new CoordException(barrierId, "Await failed", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CoordException(barrierId, "Interrupted while awaiting barrier", e);
        }
    }

    @Override
    public Optional<BarrierHandle> getBarrier(String barrierId) throws CoordException {
        byte[] key = barrierKey(barrierId);
        try {
            Optional<byte[]> value = metastore.get(key);
            return value.map(v -> deserialize(v, BarrierHandle.class));
        } catch (IOException e) {
            throw new CoordException(barrierId, "Failed to read barrier", e);
        }
    }

    @Override
    public boolean reset(String barrierId) throws CoordException {
        byte[] key = barrierKey(barrierId);
        try {
            Optional<byte[]> existing = metastore.get(key);
            if (existing.isEmpty()) return false;

            BarrierHandle barrier = deserialize(existing.get(), BarrierHandle.class);

            CompletableFuture<BarrierHandle> oldFuture = barrierFutures.remove(barrierId);
            if (oldFuture != null && !oldFuture.isDone()) {
                oldFuture.cancel(false);
            }

            BarrierHandle reset =
                    new BarrierHandle(
                            barrierId,
                            barrier.parties(),
                            List.of(),
                            false,
                            barrier.createdAt(),
                            barrier.expiresAt());

            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(key, existing.get()));
            List<Mutation> mutations = List.of(new Mutation.Put(key, serialize(reset)));

            return cas(barrierId, preconditions, mutations);
        } catch (IOException e) {
            throw new CoordException(barrierId, "Failed to reset barrier", e);
        }
    }

    @Override
    public ElectionHandle campaign(String electionId, String agentId, Duration termTtl)
            throws CoordException {
        byte[] electionKey = electionKey(electionId);
        byte[] termKey = electionTermKey(electionId);
        long now = System.currentTimeMillis();
        long expiresAt = now + termTtl.toMillis();

        for (int attempt = 0; attempt < config.maxRetryCas(); attempt++) {
            try {
                Optional<byte[]> existing = metastore.get(electionKey);
                if (existing.isPresent()) {
                    ElectionHandle current = deserialize(existing.get(), ElectionHandle.class);
                    if (current.expiresAt() > now && current.leaderAgentId().equals(agentId)) {
                        return current;
                    }
                    if (current.expiresAt() > now) {
                        throw new CoordAcquireTimeoutException(
                                electionId,
                                "Election already has leader="
                                        + current.leaderAgentId()
                                        + " until "
                                        + current.expiresAt());
                    }
                }

                long term = readTermFromTombstone(electionId) + 1;
                String fencingToken = nextFencingToken();
                ElectionHandle election =
                        new ElectionHandle(electionId, agentId, term, fencingToken, now, expiresAt);

                List<Precondition> preconditions;
                if (existing.isPresent()) {
                    preconditions =
                            List.of(new Precondition.ValueEquals(electionKey, existing.get()));
                } else {
                    preconditions = List.of(new Precondition.NotExists(electionKey));
                }

                byte[] agentIdxKey = electionAgentIdxKey(agentId, electionId);
                List<Mutation> mutations =
                        List.of(
                                new Mutation.Put(electionKey, serialize(election)),
                                new Mutation.Put(
                                        agentIdxKey, electionId.getBytes(StandardCharsets.UTF_8)),
                                new Mutation.Put(
                                        termKey,
                                        Long.toString(term).getBytes(StandardCharsets.UTF_8)));

                boolean success = cas(electionId, preconditions, mutations);
                if (success) {
                    if (trustAdapter != null) {
                        trustAdapter.recordElectionChange(electionId, agentId, "campaign", now);
                    }
                    log.info(
                            "Election won electionId={} agent={} term={}",
                            electionId,
                            agentId,
                            term);
                    return election;
                }

                casRetrySleep(attempt);
            } catch (IOException e) {
                throw new CoordException(electionId, "Failed to campaign for election", e);
            }
        }
        throw new CoordConflictException(electionId, "Election campaign CAS retries exhausted");
    }

    @Override
    public boolean resign(String electionId, String agentId) throws CoordException {
        byte[] electionKey = electionKey(electionId);
        try {
            Optional<byte[]> existing = metastore.get(electionKey);
            if (existing.isEmpty()) return false;

            ElectionHandle election = deserialize(existing.get(), ElectionHandle.class);
            if (!election.leaderAgentId().equals(agentId)) {
                throw new CoordConflictException(electionId, "Not the current leader");
            }

            ElectionHandle tombstone =
                    new ElectionHandle(
                            electionId, "", election.term(), election.fencingToken(), 0, 0);

            byte[] agentIdxKey = electionAgentIdxKey(agentId, electionId);
            byte[] termKey = electionTermKey(electionId);
            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(electionKey, existing.get()));
            List<Mutation> mutations =
                    List.of(
                            new Mutation.Put(electionKey, serialize(tombstone)),
                            new Mutation.Delete(agentIdxKey),
                            new Mutation.Put(
                                    termKey,
                                    Long.toString(election.term())
                                            .getBytes(StandardCharsets.UTF_8)));

            boolean success = cas(electionId, preconditions, mutations);
            if (success) {
                if (trustAdapter != null) {
                    trustAdapter.recordElectionChange(
                            electionId, agentId, "resign", System.currentTimeMillis());
                }
                log.info("Election resigned electionId={} agent={}", electionId, agentId);
            }
            return success;
        } catch (IOException e) {
            throw new CoordException(electionId, "Failed to resign from election", e);
        }
    }

    @Override
    public boolean renewTerm(String electionId, String agentId, Duration termTtl)
            throws CoordException {
        byte[] electionKey = electionKey(electionId);
        try {
            Optional<byte[]> existing = metastore.get(electionKey);
            if (existing.isEmpty()) return false;

            ElectionHandle election = deserialize(existing.get(), ElectionHandle.class);
            if (!election.leaderAgentId().equals(agentId)) {
                throw new CoordConflictException(electionId, "Not the current leader");
            }
            if (election.expiresAt() <= 0) {
                throw new CoordExpiredException(electionId, "Election already resigned");
            }

            long now = System.currentTimeMillis();
            long newExpiresAt = now + termTtl.toMillis();
            ElectionHandle renewed =
                    new ElectionHandle(
                            electionId,
                            agentId,
                            election.term(),
                            election.fencingToken(),
                            election.acquiredAt(),
                            newExpiresAt);

            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(electionKey, existing.get()));
            List<Mutation> mutations = List.of(new Mutation.Put(electionKey, serialize(renewed)));

            boolean success = cas(electionId, preconditions, mutations);
            if (success) {
                log.debug(
                        "Election term renewed electionId={} agent={} newExpiresAt={}",
                        electionId,
                        agentId,
                        newExpiresAt);
            }
            return success;
        } catch (IOException e) {
            throw new CoordException(electionId, "Failed to renew election term", e);
        }
    }

    @Override
    public Optional<ElectionHandle> getLeader(String electionId) throws CoordException {
        byte[] electionKey = electionKey(electionId);
        try {
            Optional<byte[]> value = metastore.get(electionKey);
            if (value.isEmpty()) return Optional.empty();
            ElectionHandle election = deserialize(value.get(), ElectionHandle.class);
            if (election.expiresAt() <= 0) return Optional.empty();
            if (election.expiresAt() <= System.currentTimeMillis()) {
                return Optional.empty();
            }
            return Optional.of(election);
        } catch (IOException e) {
            throw new CoordException(electionId, "Failed to read election", e);
        }
    }

    @Override
    public List<ElectionHandle> listElectionsByAgent(String agentId) throws CoordException {
        byte[] prefix = (ELECTION_AGENT_IDX + agentId + SEP).getBytes(StandardCharsets.UTF_8);
        try {
            List<KeyValue> entries = metastore.scanAll(prefix);
            List<ElectionHandle> elections = new ArrayList<>();
            for (KeyValue kv : entries) {
                String eid = new String(kv.value(), StandardCharsets.UTF_8);
                Optional<ElectionHandle> election = getLeader(eid);
                election.ifPresent(elections::add);
            }
            return elections;
        } catch (IOException e) {
            throw new CoordException(agentId, "Failed to list elections", e);
        }
    }

    private long readTermFromTombstone(String electionId) {
        byte[] termKey = electionTermKey(electionId);
        try {
            Optional<byte[]> existing = metastore.get(termKey);
            if (existing.isPresent()) {
                return Long.parseLong(new String(existing.get(), StandardCharsets.UTF_8).trim());
            }
        } catch (Exception e) {
            log.warn("Failed to read election term tombstone electionId={}", electionId, e);
        }
        try {
            byte[] electionKey = electionKey(electionId);
            Optional<byte[]> existing = metastore.get(electionKey);
            if (existing.isPresent()) {
                ElectionHandle handle = deserialize(existing.get(), ElectionHandle.class);
                return handle.term();
            }
        } catch (Exception e) {
            log.warn("Failed to read election for term fallback electionId={}", electionId, e);
        }
        return 0;
    }

    @Override
    public long nextVal(String seqId) throws CoordException {
        byte[] seqKey = seqKey(seqId);
        for (int attempt = 0; attempt < config.maxRetryCas(); attempt++) {
            try {
                Optional<byte[]> existing = metastore.get(seqKey);
                if (existing.isEmpty()) throw new CoordNotFoundException(seqId);

                SeqHandle currentHandle = deserialize(existing.get(), SeqHandle.class);
                long currentVal = currentHandle.currentValue();
                if (currentVal == Long.MAX_VALUE) {
                    throw new CoordException(seqId, "Sequence overflow: maxValue reached");
                }
                long nextVal = currentVal + 1;
                SeqHandle updated = new SeqHandle(seqId, nextVal, currentHandle.createdAt());

                List<Precondition> preconditions =
                        List.of(new Precondition.ValueEquals(seqKey, existing.get()));
                List<Mutation> mutations = List.of(new Mutation.Put(seqKey, serialize(updated)));

                boolean success = cas(seqId, preconditions, mutations);
                if (success) return nextVal;

                casRetrySleep(attempt);
            } catch (IOException e) {
                throw new CoordException(seqId, "Failed to get next sequence value", e);
            }
        }
        throw new CoordConflictException(seqId, "CAS retries exhausted for sequence increment");
    }

    @Override
    public long curVal(String seqId) throws CoordException {
        byte[] seqKey = seqKey(seqId);
        try {
            Optional<byte[]> value = metastore.get(seqKey);
            if (value.isEmpty()) {
                throw new CoordNotFoundException(seqId);
            }
            SeqHandle seq = deserialize(value.get(), SeqHandle.class);
            return seq.currentValue();
        } catch (IOException e) {
            throw new CoordException(seqId, "Failed to read sequence", e);
        }
    }

    @Override
    public SeqHandle create(String seqId, long initialValue) throws CoordException {
        long now = System.currentTimeMillis();
        SeqHandle seq = new SeqHandle(seqId, initialValue, now);

        byte[] seqKey = seqKey(seqId);
        List<Precondition> preconditions = List.of(new Precondition.NotExists(seqKey));
        List<Mutation> mutations = List.of(new Mutation.Put(seqKey, serialize(seq)));

        boolean success = cas(seqId, preconditions, mutations);
        if (!success) {
            throw new CoordConflictException(seqId, "Sequence already exists");
        }

        log.debug("Sequence created seqId={} initialValue={}", seqId, initialValue);
        return seq;
    }

    @Override
    public boolean reset(String seqId, long value) throws CoordException {
        byte[] seqKey = seqKey(seqId);
        try {
            Optional<byte[]> existing = metastore.get(seqKey);
            if (existing.isEmpty()) return false;

            SeqHandle current = deserialize(existing.get(), SeqHandle.class);
            SeqHandle reset = new SeqHandle(seqId, value, current.createdAt());

            List<Precondition> preconditions =
                    List.of(new Precondition.ValueEquals(seqKey, existing.get()));
            List<Mutation> mutations = List.of(new Mutation.Put(seqKey, serialize(reset)));

            return cas(seqId, preconditions, mutations);
        } catch (IOException e) {
            throw new CoordException(seqId, "Failed to reset sequence", e);
        }
    }

    @Override
    public void publish(String topic, String agentId, byte[] message) throws CoordException {
        String msgId = "msg-" + UUID.randomUUID();
        long now = System.currentTimeMillis();
        String timestampKey = String.format("%019d", now);

        byte[] msgKey =
                (PUBSUB_MSG_PREFIX + topic + SEP + timestampKey + SEP + msgId)
                        .getBytes(StandardCharsets.UTF_8);

        PubSubMessage pubSubMsg = new PubSubMessage(msgId, topic, agentId, now, message);

        try {
            metastore.put(msgKey, serialize(pubSubMsg));
        } catch (IOException e) {
            throw new CoordException(topic, "Failed to publish message", e);
        }

        log.debug("Message published topic={} msgId={}", topic, msgId);
    }

    @Override
    public SubscriptionHandle subscribe(String topic, String agentId) throws CoordException {
        String subscriptionId = "sub-" + UUID.randomUUID();
        long now = System.currentTimeMillis();

        SubscriptionHandle handle = new SubscriptionHandle(subscriptionId, topic, agentId, now);

        byte[] subKey =
                (PUBSUB_SUB_PREFIX + topic + SEP + subscriptionId).getBytes(StandardCharsets.UTF_8);
        byte[] cursorKey = (PUBSUB_CURSOR_PREFIX + subscriptionId).getBytes(StandardCharsets.UTF_8);
        String cursorValue = PUBSUB_MSG_PREFIX + topic + SEP + String.format("%019d", now);

        List<Precondition> preconditions = List.of(new Precondition.NotExists(subKey));
        List<Mutation> mutations =
                List.of(
                        new Mutation.Put(subKey, serialize(handle)),
                        new Mutation.Put(cursorKey, cursorValue.getBytes(StandardCharsets.UTF_8)));

        boolean success;
        try {
            success = metastore.compareAndSwap(preconditions, mutations);
        } catch (IOException e) {
            throw new CoordException(topic, "Failed to persist subscription", e);
        }
        if (!success) {
            throw new CoordConflictException(subscriptionId, "Subscription already exists");
        }

        subscriptionIdToTopic.put(subscriptionId, topic);

        log.debug(
                "Subscription created subscriptionId={} topic={} agent={}",
                subscriptionId,
                topic,
                agentId);
        return handle;
    }

    @Override
    public boolean unsubscribe(String subscriptionId) throws CoordException {
        String topic = subscriptionIdToTopic.remove(subscriptionId);
        if (topic == null) {
            try {
                topic = findTopicForSubscription(subscriptionId);
            } catch (IOException e) {
                log.warn("Failed to find topic for subscription subId={}", subscriptionId, e);
            }
        }
        if (topic == null) return false;

        byte[] subKey =
                (PUBSUB_SUB_PREFIX + topic + SEP + subscriptionId).getBytes(StandardCharsets.UTF_8);
        byte[] cursorKey = (PUBSUB_CURSOR_PREFIX + subscriptionId).getBytes(StandardCharsets.UTF_8);

        try {
            Optional<byte[]> existing = metastore.get(subKey);
            if (existing.isEmpty()) return false;

            boolean success =
                    metastore.compareAndSwap(
                            List.of(new Precondition.ValueEquals(subKey, existing.get())),
                            List.of(new Mutation.Delete(subKey), new Mutation.Delete(cursorKey)));
            return success;
        } catch (IOException e) {
            throw new CoordException(subscriptionId, "Failed to unsubscribe", e);
        }
    }

    @Override
    public Optional<SubscriptionHandle> getSubscription(String subscriptionId)
            throws CoordException {
        String topic = subscriptionIdToTopic.get(subscriptionId);
        if (topic == null) {
            try {
                topic = findTopicForSubscription(subscriptionId);
            } catch (IOException e) {
                log.warn("Failed to find topic for subscription subId={}", subscriptionId, e);
            }
        }
        if (topic == null) return Optional.empty();

        byte[] subKey =
                (PUBSUB_SUB_PREFIX + topic + SEP + subscriptionId).getBytes(StandardCharsets.UTF_8);
        try {
            Optional<byte[]> data = metastore.get(subKey);
            if (data.isEmpty()) return Optional.empty();
            return Optional.of(deserialize(data.get(), SubscriptionHandle.class));
        } catch (IOException e) {
            throw new CoordException(subscriptionId, "Failed to get subscription", e);
        }
    }

    @Override
    public List<SubscriptionHandle> listSubscriptions(String topic) throws CoordException {
        byte[] prefix = (PUBSUB_SUB_PREFIX + topic + SEP).getBytes(StandardCharsets.UTF_8);
        try {
            List<KeyValue> entries = metastore.scanAll(prefix);
            List<SubscriptionHandle> result = new ArrayList<>();
            for (KeyValue kv : entries) {
                result.add(deserialize(kv.value(), SubscriptionHandle.class));
            }
            return result;
        } catch (IOException e) {
            throw new CoordException(topic, "Failed to list subscriptions", e);
        }
    }

    @Override
    public List<PubSubMessage> pullMessages(String topic, String subscriptionId, int maxMessages)
            throws CoordException {
        byte[] cursorKey = (PUBSUB_CURSOR_PREFIX + subscriptionId).getBytes(StandardCharsets.UTF_8);

        String cursorValue;
        try {
            Optional<byte[]> cursorData = metastore.get(cursorKey);
            if (cursorData.isEmpty()) {
                return List.of();
            }
            cursorValue = new String(cursorData.get(), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            throw new CoordException(topic, "Failed to read subscription cursor", e);
        }

        byte[] msgPrefix = (PUBSUB_MSG_PREFIX + topic + SEP).getBytes(StandardCharsets.UTF_8);

        List<PubSubMessage> messages = new ArrayList<>();

        try {
            List<KeyValue> entries = metastore.scanAll(msgPrefix);
            String lastKeyStr = null;
            for (KeyValue kv : entries) {
                if (messages.size() >= maxMessages) break;

                String keyStr = new String(kv.key(), StandardCharsets.UTF_8);
                if (keyStr.compareTo(cursorValue) > 0) {
                    PubSubMessage msg = deserialize(kv.value(), PubSubMessage.class);
                    messages.add(msg);
                    lastKeyStr = keyStr;
                }
            }

            if (lastKeyStr != null) {
                for (int attempt = 0; attempt < config.maxRetryCas(); attempt++) {
                    try {
                        Optional<byte[]> currentCursor = metastore.get(cursorKey);
                        if (currentCursor.isEmpty()) break;
                        String currentStr =
                                new String(currentCursor.get(), StandardCharsets.UTF_8).trim();
                        if (lastKeyStr.compareTo(currentStr) <= 0) break;
                        boolean success =
                                metastore.compareAndSwap(
                                        List.of(
                                                new Precondition.ValueEquals(
                                                        cursorKey, currentCursor.get())),
                                        List.of(
                                                new Mutation.Put(
                                                        cursorKey,
                                                        lastKeyStr.getBytes(
                                                                StandardCharsets.UTF_8))));
                        if (success) break;
                    } catch (IOException e) {
                        throw new CoordException(topic, "Failed to update cursor", e);
                    }
                }
            }
        } catch (IOException e) {
            throw new CoordException(topic, "Failed to pull messages", e);
        }

        return messages;
    }

    private String findTopicForSubscription(String subscriptionId) throws IOException {
        byte[] prefix = PUBSUB_SUB_PREFIX.getBytes(StandardCharsets.UTF_8);
        List<KeyValue> entries = metastore.scanAll(prefix);
        for (KeyValue kv : entries) {
            String keyStr = new String(kv.key(), StandardCharsets.UTF_8);
            if (keyStr.endsWith(SEP + subscriptionId)) {
                String[] parts = keyStr.split(SEP);
                if (parts.length >= 3) {
                    subscriptionIdToTopic.put(subscriptionId, parts[2]);
                    return parts[2];
                }
            }
        }
        return null;
    }

    private byte[] lockKey(String resource) {
        return (LOCK_PREFIX + resource).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] leaseKey(String leaseId) {
        return (LEASE_PREFIX + leaseId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] leaseAgentIdxKey(String agentId, String leaseId) {
        return (LEASE_AGENT_IDX + agentId + SEP + leaseId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] barrierKey(String barrierId) {
        return (BARRIER_PREFIX + barrierId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] electionKey(String electionId) {
        return (ELECTION_PREFIX + electionId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] electionAgentIdxKey(String agentId, String electionId) {
        return (ELECTION_AGENT_IDX + agentId + SEP + electionId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] electionTermKey(String electionId) {
        return (ELECTION_TOMBSTONE_PREFIX + electionId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] seqKey(String seqId) {
        return (SEQ_PREFIX + seqId).getBytes(StandardCharsets.UTF_8);
    }

    private byte[] serialize(Object obj) {
        try {
            return ContextJson.writeValueAsBytes(obj);
        } catch (IOException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    private <T> T deserialize(byte[] data, Class<T> type) {
        try {
            return ContextJson.readValue(data, type);
        } catch (IOException e) {
            throw new RuntimeException("Deserialization failed for " + type.getSimpleName(), e);
        }
    }

    private boolean cas(
            String resourceId, List<Precondition> preconditions, List<Mutation> mutations)
            throws CoordException {
        try {
            return metastore.compareAndSwap(preconditions, mutations);
        } catch (IOException e) {
            throw new CoordException(resourceId, "CAS operation failed", e);
        }
    }

    @Override
    public void close() {
        gcService.close();
    }
}
