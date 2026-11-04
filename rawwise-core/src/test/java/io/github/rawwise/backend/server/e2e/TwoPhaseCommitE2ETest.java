package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.txn.twopc.TwoPhaseCommitCoordinator;
import io.github.rawwise.engine.txn.twopc.TwoPhaseCommitParticipant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Phase 5-T5: crosstabletwo-phasesegmentcommit E2E test.
 *
 * <p>validation 2PC corelink(§16.2):
 *
 * <ol>
 *   <li>S1: twotableallpart Prepare + Commit success
 *   <li>S2: secondtable Prepare conflict -> firstatable Rollback
 *   <li>S3: Commit phasesegmentsecondtablefailfail -> return commitFailed
 *   <li>S4: singletable 2PC exit -> correctcommit
 *   <li>S5: threetableallpartsuccess -> validationmultitableoriginalchild
 * </ol>
 */
@DisplayName("Phase 5-T5: Two-Phase Commit E2E - Cross-table atomicity")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TwoPhaseCommitE2ETest {

    @Test
    @Order(1)
    @DisplayName("S1: Two tables prepare + commit success")
    void testTwoTableCommitSuccess() {
        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator();
        MockParticipant tableA = new MockParticipant("table-A", true, true);
        MockParticipant tableB = new MockParticipant("table-B", true, true);
        coordinator.registerParticipant(tableA);
        coordinator.registerParticipant(tableB);

        Map<String, TwoPhaseCommitCoordinator.TableWrite> writes = new HashMap<>();
        writes.put("table-A", write(1));
        writes.put("table-B", write(2));

        TwoPhaseCommitCoordinator.TwoPhaseCommitResult result = coordinator.commit(writes);
        assertThat(result.success()).as("2PC should succeed").isTrue();
        assertThat(result.tableCount()).isEqualTo(2);
        assertThat(result.resultType())
                .isEqualTo(TwoPhaseCommitCoordinator.TwoPhaseCommitResult.ResultType.SUCCESS);
        assertThat(tableA.state).isEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
        assertThat(tableB.state).isEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
    }

    @Test
    @Order(2)
    @DisplayName("S2: Second table prepare conflict -> first table rollback")
    void testPrepareConflictTriggersRollback() {
        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator();
        MockParticipant tableA = new MockParticipant("table-A", true, true);
        MockParticipant tableB = new MockParticipant("table-B", false, true);
        coordinator.registerParticipant(tableA);
        coordinator.registerParticipant(tableB);

        Map<String, TwoPhaseCommitCoordinator.TableWrite> writes = new HashMap<>();
        writes.put("table-A", write(1));
        writes.put("table-B", write(2));

        TwoPhaseCommitCoordinator.TwoPhaseCommitResult result = coordinator.commit(writes);
        assertThat(result.success()).as("2PC should fail on conflict").isFalse();
        assertThat(result.resultType())
                .isEqualTo(TwoPhaseCommitCoordinator.TwoPhaseCommitResult.ResultType.CONFLICT);
        assertThat(tableA.state)
                .as("table-A (prepared) should be rolled back")
                .isEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.ABORTED);
        assertThat(tableB.state)
                .as("table-B (conflict) should not be committed")
                .isNotEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
    }

    @Test
    @Order(3)
    @DisplayName("S3: Commit phase failure on second table -> commitFailed")
    void testCommitPhaseFailure() {
        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator();
        MockParticipant tableA = new MockParticipant("table-A", true, true);
        MockParticipant tableB = new MockParticipant("table-B", true, false);
        coordinator.registerParticipant(tableA);
        coordinator.registerParticipant(tableB);

        Map<String, TwoPhaseCommitCoordinator.TableWrite> writes = new HashMap<>();
        writes.put("table-A", write(1));
        writes.put("table-B", write(2));

        TwoPhaseCommitCoordinator.TwoPhaseCommitResult result = coordinator.commit(writes);
        assertThat(result.success()).as("2PC should fail on commit error").isFalse();
        assertThat(result.resultType())
                .isEqualTo(TwoPhaseCommitCoordinator.TwoPhaseCommitResult.ResultType.COMMIT_FAILED);
        assertThat(tableA.state)
                .as("table-A should be committed (commit is serial)")
                .isEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
        assertThat(tableB.state)
                .as("table-B commit should have failed")
                .isNotEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
    }

    @Test
    @Order(4)
    @DisplayName("S4: Single table 2PC (degenerate case)")
    void testSingleTableCommit() {
        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator();
        MockParticipant tableA = new MockParticipant("table-A", true, true);
        coordinator.registerParticipant(tableA);

        Map<String, TwoPhaseCommitCoordinator.TableWrite> writes = new HashMap<>();
        writes.put("table-A", write(1));

        TwoPhaseCommitCoordinator.TwoPhaseCommitResult result = coordinator.commit(writes);
        assertThat(result.success()).as("single-table 2PC should succeed").isTrue();
        assertThat(result.tableCount()).isEqualTo(1);
        assertThat(tableA.state).isEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
    }

    @Test
    @Order(5)
    @DisplayName("S5: Three tables all succeed -> multi-table atomicity")
    void testThreeTableAtomicity() {
        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator();
        MockParticipant tableA = new MockParticipant("table-A", true, true);
        MockParticipant tableB = new MockParticipant("table-B", true, true);
        MockParticipant tableC = new MockParticipant("table-C", true, true);
        coordinator.registerParticipant(tableA);
        coordinator.registerParticipant(tableB);
        coordinator.registerParticipant(tableC);

        Map<String, TwoPhaseCommitCoordinator.TableWrite> writes = new HashMap<>();
        writes.put("table-A", write(1));
        writes.put("table-B", write(2));
        writes.put("table-C", write(3));

        TwoPhaseCommitCoordinator.TwoPhaseCommitResult result = coordinator.commit(writes);
        assertThat(result.success()).as("3-table 2PC should succeed").isTrue();
        assertThat(result.tableCount()).isEqualTo(3);
        assertThat(tableA.state).isEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
        assertThat(tableB.state).isEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
        assertThat(tableC.state).isEqualTo(TwoPhaseCommitParticipant.ParticipantTxnState.COMMITTED);
    }

    @Test
    @Order(6)
    @DisplayName("S6: Empty commit -> success with 0 tables")
    void testEmptyCommit() {
        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator();
        TwoPhaseCommitCoordinator.TwoPhaseCommitResult result = coordinator.commit(new HashMap<>());
        assertThat(result.success()).isTrue();
        assertThat(result.tableCount()).isEqualTo(0);
    }

    @Test
    @Order(7)
    @DisplayName("S7: Prepare order follows tableId lexicographic order")
    void testPrepareOrderIsLexicographic() {
        TwoPhaseCommitCoordinator coordinator = new TwoPhaseCommitCoordinator();
        OrderedMockParticipant tableC = new OrderedMockParticipant("table-C");
        OrderedMockParticipant tableA = new OrderedMockParticipant("table-A");
        OrderedMockParticipant tableB = new OrderedMockParticipant("table-B");
        coordinator.registerParticipant(tableC);
        coordinator.registerParticipant(tableA);
        coordinator.registerParticipant(tableB);

        Map<String, TwoPhaseCommitCoordinator.TableWrite> writes = new HashMap<>();
        writes.put("table-C", write(3));
        writes.put("table-A", write(1));
        writes.put("table-B", write(2));

        coordinator.commit(writes);
        List<String> prepareOrder = OrderedMockParticipant.globalPrepareOrder;
        assertThat(prepareOrder)
                .as("prepare order should be lexicographic: A, B, C")
                .containsExactly("table-A", "table-B", "table-C");
        OrderedMockParticipant.globalPrepareOrder.clear();
    }

    private static TwoPhaseCommitCoordinator.TableWrite write(int count) {
        List<byte[]> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            keys.add(("key-" + i).getBytes());
            values.add(("val-" + i).getBytes());
        }
        return new TwoPhaseCommitCoordinator.TableWrite(keys, values);
    }

    /** Mock Participant: canconfig prepare and commit isnosuccess. */
    static class MockParticipant implements TwoPhaseCommitParticipant {
        private final String tableId;
        private final boolean prepareSuccess;
        private final boolean commitSuccess;
        volatile ParticipantTxnState state = ParticipantTxnState.UNKNOWN;

        MockParticipant(String tableId, boolean prepareSuccess, boolean commitSuccess) {
            this.tableId = tableId;
            this.prepareSuccess = prepareSuccess;
            this.commitSuccess = commitSuccess;
        }

        @Override
        public String tableId() {
            return tableId;
        }

        @Override
        public boolean prepare(String txnId, List<byte[]> keys, List<byte[]> values) {
            if (prepareSuccess) {
                state = ParticipantTxnState.PREPARED;
                return true;
            }
            return false;
        }

        @Override
        public boolean commit(String txnId) {
            if (commitSuccess) {
                state = ParticipantTxnState.COMMITTED;
                return true;
            }
            return false;
        }

        @Override
        public boolean rollback(String txnId) {
            state = ParticipantTxnState.ABORTED;
            return true;
        }

        @Override
        public ParticipantTxnState queryState(String txnId) {
            return state;
        }
    }

    /** record prepare order Mock Participant. */
    static class OrderedMockParticipant extends MockParticipant {
        static final List<String> globalPrepareOrder = new ArrayList<>();

        OrderedMockParticipant(String tableId) {
            super(tableId, true, true);
        }

        @Override
        public boolean prepare(String txnId, List<byte[]> keys, List<byte[]> values) {
            globalPrepareOrder.add(tableId());
            return super.prepare(txnId, keys, values);
        }
    }
}
