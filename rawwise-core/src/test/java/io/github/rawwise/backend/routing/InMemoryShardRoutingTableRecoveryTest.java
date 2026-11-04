package io.github.rawwise.backend.routing;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryShardRoutingTableRecoveryTest {
    private InMemoryShardRoutingTable routingTable;

    @BeforeEach
    void setUp() {
        routingTable = new InMemoryShardRoutingTable();
    }

    private void registerAndReady(String executorId, NodeSlot slot) {
        routingTable.registerExecutor(executorId, slot);
        routingTable.markExecutorReady(executorId);
    }

    @Test
    void reassignShardForRecoverySetsRecoveringTrueAndStatusIsRecovering() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        routingTable.reassignShardForRecovery(new ShardId(0));

        ShardLookupResult result = routingTable.lookup(new ShardId(0));
        assertThat(result.status()).isEqualTo(RoutingStatus.RECOVERING);
        assertThat(result.assignment().recovering()).isTrue();
    }

    @Test
    void clearRecoveringFlagWithMatchingEpochReturnsTrueAndClearsFlag() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        routingTable.reassignShardForRecovery(new ShardId(0));

        ShardAssignment recoveringAssignment = routingTable.lookup(new ShardId(0)).assignment();
        RoutingEpoch epoch = recoveringAssignment.epoch();

        boolean cleared = routingTable.clearRecoveringFlag(new ShardId(0), epoch, null);

        assertThat(cleared).isTrue();
        ShardLookupResult afterClear = routingTable.lookup(new ShardId(0));
        assertThat(afterClear.status()).isEqualTo(RoutingStatus.ASSIGNED);
        assertThat(afterClear.assignment().recovering()).isFalse();
    }

    @Test
    void clearRecoveringFlagWithMismatchedEpochReturnsFalseAndDoesNotClearFlag() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        routingTable.reassignShardForRecovery(new ShardId(0));

        RoutingEpoch mismatchedEpoch = new RoutingEpoch(999);

        boolean cleared = routingTable.clearRecoveringFlag(new ShardId(0), mismatchedEpoch, null);

        assertThat(cleared).isFalse();
        ShardLookupResult afterAttempt = routingTable.lookup(new ShardId(0));
        assertThat(afterAttempt.status()).isEqualTo(RoutingStatus.RECOVERING);
        assertThat(afterAttempt.assignment().recovering()).isTrue();
    }

    @Test
    void clearRecoveringFlagOnNonRecoveringShardReturnsFalse() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        routingTable.assignShard(
                new ShardAssignment(new ShardId(0), slot, "exec-1", RoutingEpoch.initial()));

        ShardAssignment assignment = routingTable.lookup(new ShardId(0)).assignment();
        boolean cleared =
                routingTable.clearRecoveringFlag(new ShardId(0), assignment.epoch(), null);

        assertThat(cleared).isFalse();
    }

    @Test
    void clearRecoveringFlagWithOnClearedCallbackExecutesWithinWriteLock() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        routingTable.reassignShardForRecovery(new ShardId(0));

        ShardAssignment recoveringAssignment = routingTable.lookup(new ShardId(0)).assignment();
        RoutingEpoch epoch = recoveringAssignment.epoch();

        AtomicBoolean callbackExecuted = new AtomicBoolean(false);
        AtomicBoolean callbackSawAssignedStatus = new AtomicBoolean(false);

        boolean cleared =
                routingTable.clearRecoveringFlag(
                        new ShardId(0),
                        epoch,
                        () -> {
                            callbackExecuted.set(true);
                            ShardLookupResult insideCallback = routingTable.lookup(new ShardId(0));
                            callbackSawAssignedStatus.set(
                                    insideCallback.status() == RoutingStatus.ASSIGNED);
                        });

        assertThat(cleared).isTrue();
        assertThat(callbackExecuted).isTrue();
        assertThat(callbackSawAssignedStatus).isTrue();
    }

    @Test
    void unregisterExecutorAndCollectAssignmentsReturnsAffectedAssignmentsWithRecoveringTrue() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        registerAndReady("exec-1", slot);

        routingTable.reassignShardForRecovery(new ShardId(0));

        List<ShardAssignment> assignments =
                routingTable.unregisterExecutorAndCollectAssignments("exec-1");

        assertThat(assignments).hasSize(1);
        assertThat(assignments.get(0).shardId()).isEqualTo(new ShardId(0));
        assertThat(assignments.get(0).recovering()).isTrue();
    }

    @Test
    void markExecutorReadyDoesNotTransitionRecoveringToAssigned() {
        NodeSlot slot = new NodeSlot("node-a", 0);
        routingTable.registerExecutor("exec-1", slot);

        routingTable.reassignShardForRecovery(new ShardId(0));

        ShardLookupResult beforeReady = routingTable.lookup(new ShardId(0));
        assertThat(beforeReady.status()).isEqualTo(RoutingStatus.RECOVERING);

        routingTable.markExecutorReady("exec-1");

        ShardLookupResult afterReady = routingTable.lookup(new ShardId(0));
        assertThat(afterReady.status()).isEqualTo(RoutingStatus.RECOVERING);
    }
}
