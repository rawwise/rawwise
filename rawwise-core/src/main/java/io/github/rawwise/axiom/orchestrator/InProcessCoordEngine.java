package io.github.rawwise.axiom.orchestrator;

import io.github.rawwise.axiom.pipeline.cognition.ActionExecutor;
import io.github.rawwise.axiom.pipeline.cognition.ActionPlan;
import io.github.rawwise.axiom.pipeline.cognition.CognitionEngine;
import io.github.rawwise.axiom.pipeline.cognition.ExecutionContext;
import io.github.rawwise.model.record.Action;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InProcessCoordEngine implements CoordEngine, AutoCloseable {

    private final AtomicLong fencingTokenSeq = new AtomicLong(0);

    private final ConcurrentHashMap<String, LockHandle> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LeaseHandle> leases = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, BarrierState> barriers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<BarrierHandle>> barrierWaiters =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ElectionHandle> elections = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SeqState> sequences = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<PubSubMessage>> topics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SubscriptionHandle> subscriptions =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> cursors = new ConcurrentHashMap<>();

    private String nextFencingToken() {
        return "fence-" + fencingTokenSeq.incrementAndGet();
    }

    // ===== Lock Service =====
    @Override
    public LockHandle acquire(String agentId, String resource, Duration ttl) throws CoordException {
        long now = System.currentTimeMillis();
        long expiresAt = now + ttl.toMillis();
        return locks.compute(
                resource,
                (k, cur) -> {
                    if (cur == null || cur.expiresAt() <= now) {
                        LockHandle fresh =
                                new LockHandle(
                                        resource, agentId, nextFencingToken(), now, expiresAt, 1);
                        log.debug(
                                "InProcess lock acquired resource={} agent={}", resource, agentId);
                        return fresh;
                    }
                    if (cur.agentId().equals(agentId)) {
                        LockHandle reentrant =
                                new LockHandle(
                                        resource,
                                        agentId,
                                        cur.fencingToken(),
                                        cur.acquiredAt(),
                                        cur.expiresAt(),
                                        cur.holdCount() + 1);
                        log.debug(
                                "InProcess lock re-acquired resource={} agent={} holdCount={}",
                                resource,
                                agentId,
                                reentrant.holdCount());
                        return reentrant;
                    }
                    throw new CoordAcquireTimeoutException(
                            resource,
                            "Resource locked by agent="
                                    + cur.agentId()
                                    + " until "
                                    + cur.expiresAt());
                });
    }

    @Override
    public boolean release(String agentId, String resource, String fencingToken)
            throws CoordException {
        LockHandle cur = locks.get(resource);
        if (cur == null) return false;
        if (!cur.agentId().equals(agentId) || !cur.fencingToken().equals(fencingToken)) {
            throw new CoordConflictException(
                    resource, "Lock owner mismatch or fencing token invalid");
        }
        if (cur.holdCount() > 1) {
            LockHandle decremented =
                    new LockHandle(
                            resource,
                            agentId,
                            fencingToken,
                            cur.acquiredAt(),
                            cur.expiresAt(),
                            cur.holdCount() - 1);
            return locks.replace(resource, cur, decremented);
        }
        boolean removed = locks.remove(resource, cur);
        if (removed) log.debug("InProcess lock released resource={} agent={}", resource, agentId);
        return removed;
    }

    @Override
    public Optional<LockHandle> getLock(String resource) throws CoordException {
        LockHandle cur = locks.get(resource);
        if (cur == null || cur.expiresAt() <= System.currentTimeMillis()) return Optional.empty();
        return Optional.of(cur);
    }

    @Override
    public boolean extend(String agentId, String resource, String fencingToken, Duration ttl)
            throws CoordException {
        LockHandle cur = locks.get(resource);
        if (cur == null) return false;
        if (!cur.agentId().equals(agentId) || !cur.fencingToken().equals(fencingToken)) {
            throw new CoordConflictException(
                    resource, "Lock owner mismatch or fencing token invalid");
        }
        if (cur.expiresAt() <= System.currentTimeMillis()) {
            throw new CoordExpiredException(resource, "Lock already expired");
        }
        long newExpiresAt = System.currentTimeMillis() + ttl.toMillis();
        LockHandle extended =
                new LockHandle(
                        resource,
                        agentId,
                        fencingToken,
                        cur.acquiredAt(),
                        newExpiresAt,
                        cur.holdCount());
        return locks.replace(resource, cur, extended);
    }

    // ===== Lease Service =====
    @Override
    public LeaseHandle acquireLease(String agentId, String resourceId, Duration ttl)
            throws CoordException {
        long now = System.currentTimeMillis();
        String leaseId = "lease-" + UUID.randomUUID();
        LeaseHandle lease =
                new LeaseHandle(
                        leaseId,
                        agentId,
                        resourceId,
                        nextFencingToken(),
                        now,
                        now + ttl.toMillis(),
                        0,
                        ttl.toMillis());
        leases.put(leaseId, lease);
        log.debug(
                "InProcess lease acquired leaseId={} agent={} resource={}",
                leaseId,
                agentId,
                resourceId);
        return lease;
    }

    @Override
    public boolean renew(String agentId, String leaseId) throws CoordException {
        LeaseHandle cur = leases.get(leaseId);
        if (cur == null) return false;
        if (!cur.agentId().equals(agentId)) {
            throw new CoordConflictException(leaseId, "Lease owner mismatch");
        }
        if (cur.expiresAt() <= System.currentTimeMillis()) {
            throw new CoordExpiredException(leaseId, "Lease already expired");
        }
        long now = System.currentTimeMillis();
        LeaseHandle renewed =
                new LeaseHandle(
                        leaseId,
                        agentId,
                        cur.resourceId(),
                        cur.fencingToken(),
                        cur.acquiredAt(),
                        now + cur.originalTtlMs(),
                        cur.renewalCount() + 1,
                        cur.originalTtlMs());
        return leases.replace(leaseId, cur, renewed);
    }

    @Override
    public boolean release(String agentId, String leaseId) throws CoordException {
        LeaseHandle cur = leases.get(leaseId);
        if (cur == null) return false;
        if (!cur.agentId().equals(agentId)) {
            throw new CoordConflictException(leaseId, "Lease owner mismatch");
        }
        boolean removed = leases.remove(leaseId, cur);
        if (removed) log.debug("InProcess lease released leaseId={} agent={}", leaseId, agentId);
        return removed;
    }

    @Override
    public Optional<LeaseHandle> getLease(String leaseId) throws CoordException {
        LeaseHandle cur = leases.get(leaseId);
        if (cur == null || cur.expiresAt() <= System.currentTimeMillis()) return Optional.empty();
        return Optional.of(cur);
    }

    @Override
    public List<LeaseHandle> listLeasesByAgent(String agentId) throws CoordException {
        long now = System.currentTimeMillis();
        List<LeaseHandle> result = new ArrayList<>();
        for (LeaseHandle l : leases.values()) {
            if (l.agentId().equals(agentId) && l.expiresAt() > now) result.add(l);
        }
        return result;
    }

    // ===== Barrier Service =====
    @Override
    public BarrierHandle create(String barrierId, int parties, Duration ttl) throws CoordException {
        BarrierState state = new BarrierState(parties, ttl.toMillis());
        if (barriers.putIfAbsent(barrierId, state) != null) {
            throw new CoordConflictException(barrierId, "Barrier already exists");
        }
        log.debug("InProcess barrier created barrierId={} parties={}", barrierId, parties);
        return new BarrierHandle(
                barrierId, parties, List.of(), false, state.createdAt, state.expiresAt);
    }

    @Override
    public boolean arrive(String barrierId, String agentId) throws CoordException {
        BarrierState state = barriers.get(barrierId);
        if (state == null) throw new CoordNotFoundException(barrierId);
        if (state.expiresAt > 0 && state.expiresAt <= System.currentTimeMillis()) {
            throw new CoordExpiredException(barrierId, "Barrier already expired");
        }
        synchronized (state) {
            if (state.triggered) return true;
            if (state.arrived.contains(agentId)) return true;
            state.arrived.add(agentId);
            if (state.arrived.size() >= state.parties) {
                state.triggered = true;
                CompletableFuture<BarrierHandle> f = barrierWaiters.remove(barrierId);
                if (f != null) f.complete(toHandle(barrierId, state));
                log.info(
                        "InProcess barrier triggered barrierId={} parties={}",
                        barrierId,
                        state.parties);
            }
            return true;
        }
    }

    @Override
    public CompletableFuture<BarrierHandle> await(String barrierId, Duration timeout)
            throws CoordException {
        BarrierState state = barriers.get(barrierId);
        if (state == null) throw new CoordNotFoundException(barrierId);
        synchronized (state) {
            if (state.triggered)
                return CompletableFuture.completedFuture(toHandle(barrierId, state));
        }
        CompletableFuture<BarrierHandle> trigger =
                barrierWaiters.computeIfAbsent(barrierId, k -> new CompletableFuture<>());
        CompletableFuture<BarrierHandle> personal = new CompletableFuture<>();
        trigger.whenComplete(
                (r, ex) -> {
                    if (ex != null) personal.completeExceptionally(ex);
                    else personal.complete(r);
                });
        personal.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS);
        synchronized (state) {
            if (state.triggered) {
                trigger.complete(toHandle(barrierId, state));
            }
        }
        return personal;
    }

    @Override
    public boolean awaitSync(String barrierId, Duration timeout) throws CoordException {
        try {
            await(barrierId, timeout).get();
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
        BarrierState state = barriers.get(barrierId);
        if (state == null) return Optional.empty();
        return Optional.of(toHandle(barrierId, state));
    }

    @Override
    public boolean reset(String barrierId) throws CoordException {
        BarrierState state = barriers.get(barrierId);
        if (state == null) return false;
        CompletableFuture<BarrierHandle> f = barrierWaiters.remove(barrierId);
        if (f != null && !f.isDone()) f.cancel(false);
        synchronized (state) {
            state.arrived.clear();
            state.triggered = false;
        }
        log.debug("InProcess barrier reset barrierId={}", barrierId);
        return true;
    }

    private BarrierHandle toHandle(String barrierId, BarrierState state) {
        return new BarrierHandle(
                barrierId,
                state.parties,
                List.copyOf(state.arrived),
                state.triggered,
                state.createdAt,
                state.expiresAt);
    }

    // ===== Election Service =====
    @Override
    public ElectionHandle campaign(String electionId, String agentId, Duration termTtl)
            throws CoordException {
        long now = System.currentTimeMillis();
        long expiresAt = now + termTtl.toMillis();
        return elections.compute(
                electionId,
                (k, cur) -> {
                    if (cur != null && cur.expiresAt() > now) {
                        if (cur.leaderAgentId().equals(agentId)) return cur;
                        throw new CoordAcquireTimeoutException(
                                electionId, "Election already has leader=" + cur.leaderAgentId());
                    }
                    long term = (cur != null ? cur.term() + 1 : 1);
                    ElectionHandle won =
                            new ElectionHandle(
                                    electionId, agentId, term, nextFencingToken(), now, expiresAt);
                    log.info(
                            "InProcess election won electionId={} agent={} term={}",
                            electionId,
                            agentId,
                            term);
                    return won;
                });
    }

    @Override
    public boolean resign(String electionId, String agentId) throws CoordException {
        ElectionHandle cur = elections.get(electionId);
        if (cur == null) return false;
        if (!cur.leaderAgentId().equals(agentId)) {
            throw new CoordConflictException(electionId, "Not the current leader");
        }
        ElectionHandle tombstone =
                new ElectionHandle(electionId, "", cur.term(), cur.fencingToken(), 0, 0);
        return elections.replace(electionId, cur, tombstone);
    }

    @Override
    public boolean renewTerm(String electionId, String agentId, Duration termTtl)
            throws CoordException {
        ElectionHandle cur = elections.get(electionId);
        if (cur == null) return false;
        if (!cur.leaderAgentId().equals(agentId)) {
            throw new CoordConflictException(electionId, "Not the current leader");
        }
        if (cur.expiresAt() <= 0) {
            throw new CoordExpiredException(electionId, "Election already resigned");
        }
        long newExpiresAt = System.currentTimeMillis() + termTtl.toMillis();
        ElectionHandle renewed =
                new ElectionHandle(
                        electionId,
                        agentId,
                        cur.term(),
                        cur.fencingToken(),
                        cur.acquiredAt(),
                        newExpiresAt);
        return elections.replace(electionId, cur, renewed);
    }

    @Override
    public Optional<ElectionHandle> getLeader(String electionId) throws CoordException {
        ElectionHandle cur = elections.get(electionId);
        if (cur == null || cur.expiresAt() <= 0) return Optional.empty();
        if (cur.expiresAt() <= System.currentTimeMillis()) return Optional.empty();
        return Optional.of(cur);
    }

    @Override
    public List<ElectionHandle> listElectionsByAgent(String agentId) throws CoordException {
        long now = System.currentTimeMillis();
        List<ElectionHandle> result = new ArrayList<>();
        for (ElectionHandle e : elections.values()) {
            if (e.leaderAgentId().equals(agentId) && e.expiresAt() > now) result.add(e);
        }
        return result;
    }

    // ===== Seq Service =====
    @Override
    public long nextVal(String seqId) throws CoordException {
        SeqState s = sequences.get(seqId);
        if (s == null) throw new CoordNotFoundException(seqId);
        long v = s.value.get();
        if (v == Long.MAX_VALUE)
            throw new CoordException(seqId, "Sequence overflow: maxValue reached");
        return s.value.incrementAndGet();
    }

    @Override
    public long curVal(String seqId) throws CoordException {
        SeqState s = sequences.get(seqId);
        if (s == null) throw new CoordNotFoundException(seqId);
        return s.value.get();
    }

    @Override
    public SeqHandle create(String seqId, long initialValue) throws CoordException {
        SeqState state = new SeqState(initialValue);
        if (sequences.putIfAbsent(seqId, state) != null) {
            throw new CoordConflictException(seqId, "Sequence already exists");
        }
        log.debug("InProcess sequence created seqId={} initialValue={}", seqId, initialValue);
        return new SeqHandle(seqId, initialValue, state.createdAt);
    }

    @Override
    public boolean reset(String seqId, long value) throws CoordException {
        SeqState s = sequences.get(seqId);
        if (s == null) return false;
        s.value.set(value);
        return true;
    }

    // ===== Pub Sub Service =====
    @Override
    public void publish(String topic, String agentId, byte[] message) throws CoordException {
        String msgId = "msg-" + UUID.randomUUID();
        long now = System.currentTimeMillis();
        topics.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(new PubSubMessage(msgId, topic, agentId, now, message));
        log.debug("InProcess message published topic={} msgId={}", topic, msgId);
    }

    @Override
    public SubscriptionHandle subscribe(String topic, String agentId) throws CoordException {
        String subId = "sub-" + UUID.randomUUID();
        SubscriptionHandle handle =
                new SubscriptionHandle(subId, topic, agentId, System.currentTimeMillis());
        subscriptions.put(subId, handle);
        List<PubSubMessage> msgs =
                topics.computeIfAbsent(topic, k -> Collections.synchronizedList(new ArrayList<>()));
        synchronized (msgs) {
            cursors.put(subId, msgs.size());
        }
        log.debug("InProcess subscription created subId={} topic={}", subId, topic);
        return handle;
    }

    @Override
    public boolean unsubscribe(String subscriptionId) throws CoordException {
        SubscriptionHandle h = subscriptions.remove(subscriptionId);
        if (h == null) return false;
        cursors.remove(subscriptionId);
        return true;
    }

    @Override
    public Optional<SubscriptionHandle> getSubscription(String subscriptionId)
            throws CoordException {
        return Optional.ofNullable(subscriptions.get(subscriptionId));
    }

    @Override
    public List<SubscriptionHandle> listSubscriptions(String topic) throws CoordException {
        List<SubscriptionHandle> result = new ArrayList<>();
        for (SubscriptionHandle h : subscriptions.values()) {
            if (h.topic().equals(topic)) result.add(h);
        }
        return result;
    }

    @Override
    public List<PubSubMessage> pullMessages(String topic, String subscriptionId, int maxMessages)
            throws CoordException {
        List<PubSubMessage> msgs = topics.get(topic);
        if (msgs == null) return List.of();
        Integer cur = cursors.get(subscriptionId);
        if (cur == null) return List.of();
        List<PubSubMessage> result = new ArrayList<>();
        synchronized (msgs) {
            int idx = cur;
            while (idx < msgs.size() && result.size() < maxMessages) {
                result.add(msgs.get(idx));
                idx++;
            }
            cursors.put(subscriptionId, idx);
        }
        return result;
    }

    // ===== Action Plan execution (CHAIN only, in-memory) =====
    public CognitionEngine.PlanResult executeActionPlan(
            ActionPlan plan, ActionExecutor executor, ExecutionContext context) {
        if (plan.planType() == CognitionEngine.PlanType.DAG) {
            throw new UnsupportedOperationException("InProcessCoordEngine does not support DAG");
        }
        long start = System.currentTimeMillis();
        List<CognitionEngine.ActionResult> results = new ArrayList<>();
        for (Action action : plan.actions()) {
            CognitionEngine.ActionResult result = executor.execute(action, context);
            results.add(result);
            if (!result.success()) {
                log.warn(
                        "InProcess CHAIN plan {} stopped at action {} due to failure",
                        plan.planId(),
                        action.actionId());
                break;
            }
        }
        long successCount = results.stream().filter(CognitionEngine.ActionResult::success).count();
        boolean success = results.stream().allMatch(CognitionEngine.ActionResult::success);
        String summary =
                String.format(
                        "InProcess plan %s: %d/%d actions succeeded in %dms",
                        plan.planId(),
                        successCount,
                        results.size(),
                        System.currentTimeMillis() - start);
        log.info(
                "InProcess CHAIN plan executed planId={} success={} actions={}",
                plan.planId(),
                success,
                results.size());
        return new CognitionEngine.PlanResult(
                plan.planId(), success, results, summary, System.currentTimeMillis());
    }

    @Override
    public void close() {
        locks.clear();
        leases.clear();
        barriers.clear();
        barrierWaiters.clear();
        elections.clear();
        sequences.clear();
        topics.clear();
        subscriptions.clear();
        cursors.clear();
        log.info("InProcessCoordEngine closed, all in-memory state cleared");
    }

    private static final class BarrierState {
        final int parties;
        final Set<String> arrived = new LinkedHashSet<>();
        boolean triggered = false;
        final long createdAt;
        final long expiresAt;

        BarrierState(int parties, long ttlMs) {
            this.parties = parties;
            this.createdAt = System.currentTimeMillis();
            this.expiresAt = createdAt + ttlMs;
        }
    }

    private static final class SeqState {
        final AtomicLong value;
        final long createdAt;

        SeqState(long initialValue) {
            this.value = new AtomicLong(initialValue);
            this.createdAt = System.currentTimeMillis();
        }
    }
}
