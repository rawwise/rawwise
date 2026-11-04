package io.github.rawwise.frontend.api.routes;

import io.github.rawwise.axiom.memory.exception.AxiomException;
import io.github.rawwise.axiom.orchestrator.BarrierHandle;
import io.github.rawwise.axiom.orchestrator.CoordEngine;
import io.github.rawwise.axiom.orchestrator.ElectionHandle;
import io.github.rawwise.axiom.orchestrator.LeaseHandle;
import io.github.rawwise.axiom.orchestrator.LockHandle;
import io.github.rawwise.axiom.orchestrator.PubSubMessage;
import io.github.rawwise.axiom.orchestrator.SeqHandle;
import io.github.rawwise.axiom.orchestrator.SubscriptionHandle;
import io.github.rawwise.governance.auth.Principal;
import io.javalin.Javalin;
import io.javalin.http.Handler;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CoordRoutes {

    private final CoordEngine coordEngine;

    public final Handler acquireLock;
    public final Handler releaseLock;
    public final Handler getLock;
    public final Handler extendLock;
    public final Handler acquireLease;
    public final Handler renewLease;
    public final Handler releaseLease;
    public final Handler getLease;
    public final Handler listLeases;
    public final Handler createBarrier;
    public final Handler arriveBarrier;
    public final Handler awaitBarrier;
    public final Handler getBarrier;
    public final Handler resetBarrier;
    public final Handler campaign;
    public final Handler resign;
    public final Handler renewTerm;
    public final Handler getLeader;
    public final Handler listElections;
    public final Handler createSeq;
    public final Handler nextVal;
    public final Handler curVal;
    public final Handler resetSeq;
    public final Handler publish;
    public final Handler subscribe;
    public final Handler unsubscribe;
    public final Handler listSubscriptions;
    public final Handler pullMessages;

    public CoordRoutes(CoordEngine coordEngine) {
        this.coordEngine = coordEngine;

        this.acquireLock =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(AcquireLockRequest.class);
                        LockHandle handle =
                                coordEngine.acquire(
                                        principal.id(),
                                        body.resource(),
                                        Duration.ofMillis(body.ttlMs()));
                        ctx.status(201).json(handle);
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.releaseLock =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(ReleaseLockRequest.class);
                        boolean released =
                                coordEngine.release(
                                        principal.id(), body.resource(), body.fencingToken());
                        ctx.json(Map.of("released", released));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.getLock =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String resource = ctx.pathParam("resource");
                        Optional<LockHandle> handle = coordEngine.getLock(resource);
                        if (handle.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_COORD_NOT_FOUND",
                                                            "message",
                                                            "Lock not found for resource: "
                                                                    + resource)));
                            return;
                        }
                        ctx.json(handle.get());
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.extendLock =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(ExtendLockRequest.class);
                        boolean extended =
                                coordEngine.extend(
                                        principal.id(),
                                        body.resource(),
                                        body.fencingToken(),
                                        Duration.ofMillis(body.ttlMs()));
                        ctx.json(Map.of("extended", extended));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.acquireLease =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(AcquireLeaseRequest.class);
                        LeaseHandle handle =
                                coordEngine.acquireLease(
                                        principal.id(),
                                        body.resourceId(),
                                        Duration.ofMillis(body.ttlMs()));
                        ctx.status(201).json(handle);
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.renewLease =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(LeaseIdRequest.class);
                        boolean renewed = coordEngine.renew(principal.id(), body.leaseId());
                        ctx.json(Map.of("renewed", renewed));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.releaseLease =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(LeaseIdRequest.class);
                        boolean released = coordEngine.release(principal.id(), body.leaseId());
                        ctx.json(Map.of("released", released));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.getLease =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String leaseId = ctx.pathParam("leaseId");
                        Optional<LeaseHandle> handle = coordEngine.getLease(leaseId);
                        if (handle.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_COORD_NOT_FOUND",
                                                            "message",
                                                            "Lease not found: " + leaseId)));
                            return;
                        }
                        ctx.json(handle.get());
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.listLeases =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        List<LeaseHandle> leases = coordEngine.listLeasesByAgent(principal.id());
                        ctx.json(Map.of("leases", leases));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.createBarrier =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(CreateBarrierRequest.class);
                        BarrierHandle handle =
                                coordEngine.create(
                                        body.barrierId(),
                                        body.parties(),
                                        Duration.ofMillis(body.ttlMs()));
                        ctx.status(201).json(handle);
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.arriveBarrier =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(BarrierIdRequest.class);
                        boolean arrived = coordEngine.arrive(body.barrierId(), principal.id());
                        ctx.json(Map.of("arrived", arrived));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.awaitBarrier =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(AwaitBarrierRequest.class);
                        boolean triggered =
                                coordEngine.awaitSync(
                                        body.barrierId(), Duration.ofMillis(body.timeoutMs()));
                        ctx.json(Map.of("triggered", triggered));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.getBarrier =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String barrierId = ctx.pathParam("barrierId");
                        Optional<BarrierHandle> handle = coordEngine.getBarrier(barrierId);
                        if (handle.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_COORD_NOT_FOUND",
                                                            "message",
                                                            "Barrier not found: " + barrierId)));
                            return;
                        }
                        ctx.json(handle.get());
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.resetBarrier =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String barrierId = ctx.pathParam("barrierId");
                        // TODO: ownership check - BarrierHandle lacks agentId
                        boolean reset = coordEngine.reset(barrierId);
                        ctx.json(Map.of("reset", reset));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.campaign =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(CampaignRequest.class);
                        ElectionHandle handle =
                                coordEngine.campaign(
                                        body.electionId(),
                                        principal.id(),
                                        Duration.ofMillis(body.termTtlMs()));
                        ctx.status(201).json(handle);
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.resign =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(ElectionIdRequest.class);
                        boolean resigned = coordEngine.resign(body.electionId(), principal.id());
                        ctx.json(Map.of("resigned", resigned));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.renewTerm =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(RenewTermRequest.class);
                        boolean renewed =
                                coordEngine.renewTerm(
                                        body.electionId(),
                                        principal.id(),
                                        Duration.ofMillis(body.termTtlMs()));
                        ctx.json(Map.of("renewed", renewed));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.getLeader =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String electionId = ctx.pathParam("electionId");
                        Optional<ElectionHandle> handle = coordEngine.getLeader(electionId);
                        if (handle.isEmpty()) {
                            ctx.status(404)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "AXIOM_COORD_NOT_FOUND",
                                                            "message",
                                                            "No leader for election: "
                                                                    + electionId)));
                            return;
                        }
                        ctx.json(handle.get());
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.listElections =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        List<ElectionHandle> elections =
                                coordEngine.listElectionsByAgent(principal.id());
                        ctx.json(Map.of("elections", elections));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.createSeq =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(CreateSeqRequest.class);
                        SeqHandle handle = coordEngine.create(body.seqId(), body.initialValue());
                        ctx.status(201).json(handle);
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.nextVal =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String seqId = ctx.pathParam("seqId");
                        long value = coordEngine.nextVal(seqId);
                        ctx.json(Map.of("value", value));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.curVal =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String seqId = ctx.pathParam("seqId");
                        long value = coordEngine.curVal(seqId);
                        ctx.json(Map.of("value", value));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.resetSeq =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String seqId = ctx.pathParam("seqId");
                        // TODO: ownership check - SeqHandle lacks agentId
                        var body = ctx.bodyAsClass(ResetSeqRequest.class);
                        boolean reset = coordEngine.reset(seqId, body.value());
                        ctx.json(Map.of("reset", reset));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.publish =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(PublishRequest.class);
                        byte[] message;
                        try {
                            message = Base64.getDecoder().decode(body.message());
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(
                                    "Invalid base64 encoding for message field");
                        }
                        coordEngine.publish(body.topic(), principal.id(), message);
                        ctx.json(Map.of("published", true));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.subscribe =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(SubscribeRequest.class);
                        SubscriptionHandle handle =
                                coordEngine.subscribe(body.topic(), principal.id());
                        ctx.status(201).json(handle);
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.unsubscribe =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(UnsubscribeRequest.class);
                        Optional<SubscriptionHandle> subscription =
                                coordEngine.getSubscription(body.subscriptionId());
                        if (subscription.isPresent()
                                && !subscription.get().agentId().equals(principal.id())) {
                            ctx.status(403)
                                    .json(
                                            Map.of(
                                                    "error",
                                                    Map.of(
                                                            "code",
                                                            "FORBIDDEN",
                                                            "message",
                                                            "Not authorized to unsubscribe this subscription")));
                            return;
                        }
                        boolean unsubscribed = coordEngine.unsubscribe(body.subscriptionId());
                        ctx.json(Map.of("unsubscribed", unsubscribed));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.listSubscriptions =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        String topic = ctx.pathParam("topic");
                        List<SubscriptionHandle> subscriptions =
                                coordEngine.listSubscriptions(topic);
                        ctx.json(Map.of("subscriptions", subscriptions));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };

        this.pullMessages =
                ctx -> {
                    Principal principal = ctx.attribute("principal");
                    if (principal == null) {
                        ctx.status(401)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "UNAUTHORIZED",
                                                        "message",
                                                        "Not authenticated")));
                        return;
                    }
                    try {
                        var body = ctx.bodyAsClass(PullMessagesRequest.class);
                        List<PubSubMessage> messages =
                                coordEngine.pullMessages(
                                        body.topic(), body.subscriptionId(), body.maxMessages());
                        ctx.json(Map.of("messages", messages));
                    } catch (AxiomException e) {
                        ctx.status(e.getHttpStatus())
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        e.getErrorCode(),
                                                        "message",
                                                        e.getMessage())));
                    } catch (IllegalArgumentException e) {
                        ctx.status(400)
                                .json(
                                        Map.of(
                                                "error",
                                                Map.of(
                                                        "code",
                                                        "INVALID_REQUEST",
                                                        "message",
                                                        e.getMessage())));
                    }
                };
    }

    public void register(Javalin app) {
        app.post("/api/v1/axiom/coord/locks/acquire", acquireLock)
                .post("/api/v1/axiom/coord/locks/release", releaseLock)
                .get("/api/v1/axiom/coord/locks/{resource}", getLock)
                .post("/api/v1/axiom/coord/locks/extend", extendLock)
                .post("/api/v1/axiom/coord/leases/acquire", acquireLease)
                .post("/api/v1/axiom/coord/leases/renew", renewLease)
                .post("/api/v1/axiom/coord/leases/release", releaseLease)
                .get("/api/v1/axiom/coord/leases/{leaseId}", getLease)
                .get("/api/v1/axiom/coord/leases", listLeases)
                .post("/api/v1/axiom/coord/barriers/create", createBarrier)
                .post("/api/v1/axiom/coord/barriers/arrive", arriveBarrier)
                .post("/api/v1/axiom/coord/barriers/await", awaitBarrier)
                .get("/api/v1/axiom/coord/barriers/{barrierId}", getBarrier)
                .post("/api/v1/axiom/coord/barriers/{barrierId}/reset", resetBarrier)
                .post("/api/v1/axiom/coord/elections/campaign", campaign)
                .post("/api/v1/axiom/coord/elections/resign", resign)
                .post("/api/v1/axiom/coord/elections/renew-term", renewTerm)
                .get("/api/v1/axiom/coord/elections/{electionId}/leader", getLeader)
                .get("/api/v1/axiom/coord/elections", listElections)
                .post("/api/v1/axiom/coord/seqs/create", createSeq)
                .get("/api/v1/axiom/coord/seqs/{seqId}/next", nextVal)
                .get("/api/v1/axiom/coord/seqs/{seqId}/current", curVal)
                .post("/api/v1/axiom/coord/seqs/{seqId}/reset", resetSeq)
                .post("/api/v1/axiom/coord/pubsub/publish", publish)
                .post("/api/v1/axiom/coord/pubsub/subscribe", subscribe)
                .post("/api/v1/axiom/coord/pubsub/unsubscribe", unsubscribe)
                .get("/api/v1/axiom/coord/pubsub/{topic}/subscriptions", listSubscriptions)
                .post("/api/v1/axiom/coord/pubsub/pull", pullMessages);
    }

    private record AcquireLockRequest(String resource, long ttlMs) {}

    private record ReleaseLockRequest(String resource, String fencingToken) {}

    private record ExtendLockRequest(String resource, String fencingToken, long ttlMs) {}

    private record AcquireLeaseRequest(String resourceId, long ttlMs) {}

    private record LeaseIdRequest(String leaseId) {}

    private record CreateBarrierRequest(String barrierId, int parties, long ttlMs) {}

    private record BarrierIdRequest(String barrierId) {}

    private record AwaitBarrierRequest(String barrierId, long timeoutMs) {}

    private record CampaignRequest(String electionId, long termTtlMs) {}

    private record ElectionIdRequest(String electionId) {}

    private record RenewTermRequest(String electionId, long termTtlMs) {}

    private record CreateSeqRequest(String seqId, long initialValue) {}

    private record ResetSeqRequest(long value) {}

    private record PublishRequest(String topic, String message) {}

    private record SubscribeRequest(String topic) {}

    private record UnsubscribeRequest(String subscriptionId) {}

    private record PullMessagesRequest(String topic, String subscriptionId, int maxMessages) {}
}
