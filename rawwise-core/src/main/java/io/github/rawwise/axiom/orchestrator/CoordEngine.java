package io.github.rawwise.axiom.orchestrator;

public interface CoordEngine
        extends LockService,
                LeaseService,
                BarrierService,
                ElectionService,
                SeqService,
                PubSubService {}
