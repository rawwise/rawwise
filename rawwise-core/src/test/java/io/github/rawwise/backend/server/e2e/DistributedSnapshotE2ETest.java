package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.snapshot.DistributedSnapshotManager;
import io.github.rawwise.backend.snapshot.NodeSnapshotCheckpoint;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Phase 6-T4: distributedsnapshotandconsistentrecovery E2E test.
 *
 * <p>validationallsnapshotcoordinate + crashrecoverycorelink:
 *
 * <ol>
 *   <li>S1: allsnapshot - two Node allpartsuccess -> COMPLETE
 *   <li>S2: allsnapshot - a Node failfail -> PARTIAL
 *   <li>S3: recovery - fromrecentsuccesssnapshotrecoveryspecified Node
 *   <li>S4: recovery - nosnapshotwhenreturn empty
 *   <li>S5: listSnapshots in descending orderreturn
 *   <li>S6: multicallssnapshotafterrecoveryfrommostnew
 * </ol>
 */
@DisplayName("Phase 6-T4: Distributed Snapshot E2E - Global checkpoint + recovery")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DistributedSnapshotE2ETest {

    @Test
    @Order(1)
    @DisplayName("S1: Global snapshot - both nodes succeed -> COMPLETE")
    void testGlobalSnapshotComplete() {
        DistributedSnapshotManager manager = new DistributedSnapshotManager();
        MockCheckpoint nodeA = new MockCheckpoint("exec-A", true);
        MockCheckpoint nodeB = new MockCheckpoint("exec-B", true);
        manager.registerNode("exec-A", nodeA);
        manager.registerNode("exec-B", nodeB);

        DistributedSnapshotManager.GlobalSnapshotMetadata meta = manager.beginGlobalSnapshot();
        assertThat(meta.state())
                .isEqualTo(
                        DistributedSnapshotManager.GlobalSnapshotMetadata.SnapshotState.COMPLETE);
        assertThat(meta.successNodeCount()).isEqualTo(2);
        assertThat(meta.nodeResults().get("exec-A").success()).isTrue();
        assertThat(meta.nodeResults().get("exec-B").success()).isTrue();
        assertThat(nodeA.checkpointCount.get()).isEqualTo(1);
        assertThat(nodeB.checkpointCount.get()).isEqualTo(1);
    }

    @Test
    @Order(2)
    @DisplayName("S2: Global snapshot - one node fails -> PARTIAL")
    void testGlobalSnapshotPartial() {
        DistributedSnapshotManager manager = new DistributedSnapshotManager();
        MockCheckpoint nodeA = new MockCheckpoint("exec-A", true);
        MockCheckpoint nodeB = new MockCheckpoint("exec-B", false);
        manager.registerNode("exec-A", nodeA);
        manager.registerNode("exec-B", nodeB);

        DistributedSnapshotManager.GlobalSnapshotMetadata meta = manager.beginGlobalSnapshot();
        assertThat(meta.state())
                .isEqualTo(DistributedSnapshotManager.GlobalSnapshotMetadata.SnapshotState.PARTIAL);
        assertThat(meta.successNodeCount()).isEqualTo(1);
        assertThat(meta.nodeResults().get("exec-A").success()).isTrue();
        assertThat(meta.nodeResults().get("exec-B").success()).isFalse();
    }

    @Test
    @Order(3)
    @DisplayName("S3: Recover node from latest successful snapshot")
    void testRecoverNode() {
        DistributedSnapshotManager manager = new DistributedSnapshotManager();
        MockCheckpoint nodeA = new MockCheckpoint("exec-A", true);
        manager.registerNode("exec-A", nodeA);
        manager.beginGlobalSnapshot();
        manager.beginGlobalSnapshot();

        Optional<NodeSnapshotCheckpoint.RecoveryResult> result = manager.recoverNode("exec-A");
        assertThat(result).as("recovery should return result").isPresent();
        assertThat(result.get().success()).as("recovery should succeed").isTrue();
        assertThat(nodeA.recoverCount.get()).as("recover should be called once").isEqualTo(1);
    }

    @Test
    @Order(4)
    @DisplayName("S4: Recover node with no snapshots -> empty")
    void testRecoverNoSnapshots() {
        DistributedSnapshotManager manager = new DistributedSnapshotManager();
        MockCheckpoint nodeA = new MockCheckpoint("exec-A", true);
        manager.registerNode("exec-A", nodeA);

        Optional<NodeSnapshotCheckpoint.RecoveryResult> result = manager.recoverNode("exec-A");
        assertThat(result).as("no snapshots -> empty").isEmpty();
    }

    @Test
    @Order(5)
    @DisplayName("S5: listSnapshots returns descending by snapshotId")
    void testListSnapshotsDescending() {
        DistributedSnapshotManager manager = new DistributedSnapshotManager();
        MockCheckpoint nodeA = new MockCheckpoint("exec-A", true);
        manager.registerNode("exec-A", nodeA);
        manager.beginGlobalSnapshot();
        manager.beginGlobalSnapshot();
        manager.beginGlobalSnapshot();

        List<DistributedSnapshotManager.GlobalSnapshotMetadata> list = manager.listSnapshots();
        assertThat(list).hasSize(3);
        assertThat(list.get(0).snapshotId())
                .as("first should be latest")
                .isGreaterThan(list.get(1).snapshotId());
        assertThat(list.get(1).snapshotId()).isGreaterThan(list.get(2).snapshotId());
    }

    @Test
    @Order(6)
    @DisplayName("S6: Multiple snapshots - recover uses the latest successful one")
    void testRecoverUsesLatest() {
        DistributedSnapshotManager manager = new DistributedSnapshotManager();
        MockCheckpoint nodeA = new MockCheckpoint("exec-A", true);
        manager.registerNode("exec-A", nodeA);
        long snap1 = manager.beginGlobalSnapshot().snapshotId();
        long snap2 = manager.beginGlobalSnapshot().snapshotId();
        assertThat(snap2).isGreaterThan(snap1);

        Optional<NodeSnapshotCheckpoint.RecoveryResult> result = manager.recoverNode("exec-A");
        assertThat(result).isPresent();
        assertThat(result.get().success()).isTrue();
        assertThat(nodeA.lastRecoveredSnapshotId)
                .as("should recover from latest snapshot")
                .isEqualTo(snap2);
    }

    @Test
    @Order(7)
    @DisplayName("S7: Recover skips failed snapshots, uses latest successful")
    void testRecoverSkipsFailedSnapshots() {
        DistributedSnapshotManager manager = new DistributedSnapshotManager();
        MockCheckpoint nodeA = new MockCheckpoint("exec-A", true);
        manager.registerNode("exec-A", nodeA);
        long snap1 = manager.beginGlobalSnapshot().snapshotId();
        nodeA.setSuccess(false);
        manager.beginGlobalSnapshot();
        nodeA.setSuccess(true);
        long snap3 = manager.beginGlobalSnapshot().snapshotId();

        Optional<NodeSnapshotCheckpoint.RecoveryResult> result = manager.recoverNode("exec-A");
        assertThat(result).isPresent();
        assertThat(result.get().success()).isTrue();
        assertThat(nodeA.lastRecoveredSnapshotId)
                .as("should recover from latest successful (snap3), skipping failed (snap2)")
                .isEqualTo(snap3);
    }

    @Test
    @Order(8)
    @DisplayName("S8: Unregistered node recovery -> empty")
    void testRecoverUnregisteredNode() {
        DistributedSnapshotManager manager = new DistributedSnapshotManager();
        Optional<NodeSnapshotCheckpoint.RecoveryResult> result =
                manager.recoverNode("unknown-exec");
        assertThat(result).as("unregistered node -> empty").isEmpty();
    }

    /** Mock checkpoint: canconfigsuccess/failfail, recordcallcallsandrecovery snapshotId. */
    static class MockCheckpoint implements NodeSnapshotCheckpoint {
        private final String executorId;
        private volatile boolean success;
        private final AtomicInteger checkpointCount = new AtomicInteger(0);
        private final AtomicInteger recoverCount = new AtomicInteger(0);
        private volatile long lastRecoveredSnapshotId = -1;
        private final ConcurrentHashMap<Long, Long> snapshotWalSeqs = new ConcurrentHashMap<>();

        MockCheckpoint(String executorId, boolean success) {
            this.executorId = executorId;
            this.success = success;
        }

        void setSuccess(boolean success) {
            this.success = success;
        }

        @Override
        public CheckpointResult checkpoint(long snapshotId) {
            checkpointCount.incrementAndGet();
            if (success) {
                long walSeq = snapshotId * 100;
                snapshotWalSeqs.put(snapshotId, walSeq);
                return CheckpointResult.success(walSeq, 1024L);
            }
            return CheckpointResult.failed("mock failure");
        }

        @Override
        public RecoveryResult recover(long snapshotId) {
            recoverCount.incrementAndGet();
            lastRecoveredSnapshotId = snapshotId;
            if (snapshotWalSeqs.containsKey(snapshotId)) {
                return RecoveryResult.success(42);
            }
            return RecoveryResult.failed("snapshot not found: " + snapshotId);
        }

        @Override
        public Optional<Long> getLatestSnapshotId() {
            return snapshotWalSeqs.keySet().stream().max(Long::compare);
        }
    }
}
