package io.github.rawwise.engine.txn;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.txn.isolate.ReadSnapshot;
import io.github.rawwise.engine.txn.state.TxnContext;
import io.github.rawwise.engine.txn.state.TxnState;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("TxnStatus 7-state machine")
class TxnStatusTest {

    private TxnContext ctx;

    private TxnContext createContext() {
        return new TxnContext(1L, 100L, IsolationLevel.READ_COMMITTED, "testTable", 0);
    }

    private TxnContext createContextWithFields() {
        return new TxnContext(
                1L, 100L, IsolationLevel.READ_COMMITTED, "testTable", 0, 42L, 7L, "attempt_001");
    }

    @Nested
    @DisplayName("Valid commit path transitions")
    class ValidCommitPath {

        @BeforeEach
        void setUp() {
            ctx = createContext();
        }

        @Test
        @DisplayName("ACTIVE -> PREPARING -> PREPARED -> COMMITTING -> COMMITTED")
        void fullCommitPath() {
            assertThat(ctx.currentState()).isEqualTo(TxnState.BEGINNING);

            ctx.markPreparing();
            assertThat(ctx.currentState()).isEqualTo(TxnState.PREPARING);

            ctx.markPrepared();
            assertThat(ctx.currentState()).isEqualTo(TxnState.DATA_DURABLE);

            ctx.markCommitting();
            assertThat(ctx.currentState()).isEqualTo(TxnState.PUBLISHED);

            ctx.markCommitted();
            assertThat(ctx.currentState()).isEqualTo(TxnState.COMMITTED);
            assertThat(ctx.isCommitted()).isTrue();
        }

        @Test
        @DisplayName("isActive returns true only in ACTIVE")
        void isActiveOnlyInActive() {
            assertThat(ctx.isActive()).isTrue();
            ctx.markPreparing();
            assertThat(ctx.isActive()).isFalse();
        }

        @Test
        @DisplayName("isCommitted returns true only in COMMITTED")
        void isCommittedOnlyInCommitted() {
            assertThat(ctx.isCommitted()).isFalse();
            ctx.markPreparing();
            ctx.markPrepared();
            ctx.markCommitting();
            assertThat(ctx.isCommitted()).isFalse();
            ctx.markCommitted();
            assertThat(ctx.isCommitted()).isTrue();
        }

        @Test
        @DisplayName("isAborted returns true only in ABORTED")
        void isAbortedOnlyInAborted() {
            assertThat(ctx.isAborted()).isFalse();
            ctx.markAborting();
            assertThat(ctx.isAborted()).isFalse();
            ctx.markAborted();
            assertThat(ctx.isAborted()).isTrue();
        }
    }

    @Nested
    @DisplayName("Valid abort paths")
    class ValidAbortPaths {

        @Test
        @DisplayName("ACTIVE -> ABORTING -> ABORTED")
        void activeToAborted() {
            ctx = createContext();
            ctx.markAborting();
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTING);
            ctx.markAborted();
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTED);
            assertThat(ctx.isAborted()).isTrue();
        }

        @Test
        @DisplayName("PREPARING -> ABORTING -> ABORTED")
        void preparingToAborted() {
            ctx = createContext();
            ctx.markPreparing();
            ctx.markAborting();
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTING);
            ctx.markAborted();
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTED);
        }

        @Test
        @DisplayName("PREPARED -> ABORTING -> ABORTED")
        void preparedToAborted() {
            ctx = createContext();
            ctx.markPreparing();
            ctx.markPrepared();
            ctx.markAborting();
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTING);
            ctx.markAborted();
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTED);
        }

        @Test
        @DisplayName("COMMITTING -> ABORTING -> ABORTED")
        void committingToAborted() {
            ctx = createContext();
            ctx.markPreparing();
            ctx.markPrepared();
            ctx.markCommitting();
            ctx.markAborting();
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTING);
            ctx.markAborted();
            assertThat(ctx.currentState()).isEqualTo(TxnState.ABORTED);
        }
    }

    @Nested
    @DisplayName("Invalid transitions throw IllegalStateException")
    class InvalidTransitions {

        @Test
        @DisplayName("PREPARING from non-ACTIVE is invalid")
        void preparingFromNonActive() {
            ctx = createContext();
            ctx.markPreparing();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markPreparing());
        }

        @Test
        @DisplayName("PREPARED from non-PREPARING is invalid")
        void preparedFromNonPreparing() {
            ctx = createContext();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markPrepared());
        }

        @Test
        @DisplayName("COMMITTING from non-PREPARED is invalid")
        void committingFromNonPrepared() {
            ctx = createContext();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markCommitting());

            ctx.markPreparing();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markCommitting());
        }

        @Test
        @DisplayName("COMMITTED from non-COMMITTING is invalid")
        void committedFromNonCommitting() {
            ctx = createContext();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markCommitted());

            ctx.markPreparing();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markCommitted());

            ctx.markPrepared();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markCommitted());
        }

        @Test
        @DisplayName("ABORTING from COMMITTED is invalid")
        void abortingFromCommitted() {
            ctx = createContext();
            ctx.markPreparing();
            ctx.markPrepared();
            ctx.markCommitting();
            ctx.markCommitted();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markAborting());
        }

        @Test
        @DisplayName("ABORTED from non-ABORTING is invalid")
        void abortedFromNonAborting() {
            ctx = createContext();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markAborted());

            ctx.markPreparing();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markAborted());

            ctx.markPrepared();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markAborted());

            ctx.markCommitting();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markAborted());
        }

        @Test
        @DisplayName("markCommitted from ACTIVE is invalid (was valid before)")
        void committedFromActiveIsNowInvalid() {
            ctx = createContext();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markCommitted());
        }

        @Test
        @DisplayName("markAborted from ACTIVE is invalid (was valid before)")
        void abortedFromActiveIsNowInvalid() {
            ctx = createContext();
            assertThatIllegalStateException().isThrownBy(() -> ctx.markAborted());
        }
    }

    @Nested
    @DisplayName("WriteBuffer is transient")
    class WriteBufferTransient {

        @Test
        @DisplayName("WriteBuffer is not serialized with TxnContext")
        void writeBufferNotSerialized() throws Exception {
            ctx = createContext();
            ctx.writeBuffer().put("testTable", "key1".getBytes(), "value1".getBytes());
            assertThat(ctx.writeBuffer().get("testTable", "key1".getBytes()).exists()).isTrue();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(ctx);
            oos.close();

            ObjectInputStream ois =
                    new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            TxnContext deserialized = (TxnContext) ois.readObject();

            assertThat(deserialized.writeBuffer()).isNotNull();
            assertThat(deserialized.writeBuffer().get("testTable", "key1".getBytes()).exists())
                    .isFalse();
        }

        @Test
        @DisplayName("Status is preserved after serialization round-trip")
        void statusPreservedAfterSerialization() throws Exception {
            ctx = createContext();
            ctx.markPreparing();
            ctx.markPrepared();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(ctx);
            oos.close();

            ObjectInputStream ois =
                    new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            TxnContext deserialized = (TxnContext) ois.readObject();

            assertThat(deserialized.currentState()).isEqualTo(TxnState.DATA_DURABLE);
            assertThat(deserialized.txnId()).isEqualTo(1L);
            assertThat(deserialized.tableId()).isEqualTo("testTable");
        }

        @Test
        @DisplayName("WriteBuffer is empty after deserialization")
        void writeBufferEmptyAfterDeserialization() throws Exception {
            ctx = createContext();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(ctx);
            oos.close();

            ObjectInputStream ois =
                    new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            TxnContext deserialized = (TxnContext) ois.readObject();

            assertThat(deserialized.writeBuffer().currentBytes()).isEqualTo(0);
        }
    }

    @Nested
    @DisplayName("New fields serialization")
    class NewFieldsSerialization {

        @Test
        @DisplayName("New fields survive serialization round-trip")
        void newFieldsSurviveSerialization() throws Exception {
            ctx = createContextWithFields();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(ctx);
            oos.close();

            ObjectInputStream ois =
                    new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            TxnContext deserialized = (TxnContext) ois.readObject();

            assertThat(deserialized.baseSnapshotId()).isEqualTo(42L);
            assertThat(deserialized.routeTableVersion()).isEqualTo(7L);
            assertThat(deserialized.taskAttemptId()).isEqualTo("attempt_001");
        }
    }

    @Nested
    @DisplayName("Snapshot and ReadCache integration")
    class SnapshotAndReadCacheIntegration {

        @Test
        @DisplayName("TxnContext should have ReadSnapshot for REPEATABLE_READ")
        void txnContextShouldHaveReadSnapshotForRepeatableRead() {
            TxnContext ctx =
                    new TxnContext(
                            1L, 100L, IsolationLevel.REPEATABLE_READ, "table1", 0, 5L, 0L, null);

            assertThat(ctx.readSnapshot()).isNotNull();
            assertThat(ctx.readSnapshot().snapshotTs()).isEqualTo(100L);
            assertThat(ctx.readSnapshot().baseSnapshotId()).isEqualTo(5L);

            // setReadSnapshot: update with a new snapshot
            ReadSnapshot newSnapshot = new ReadSnapshot(200L, 10L);
            ctx.setReadSnapshot(newSnapshot);

            assertThat(ctx.readSnapshot()).isEqualTo(newSnapshot);
            assertThat(ctx.readSnapshot().snapshotTs()).isEqualTo(200L);
        }

        @Test
        @DisplayName("TxnContext should have TxnReadCache for REPEATABLE_READ")
        void txnContextShouldHaveTxnReadCacheForRepeatableRead() {
            TxnContext ctx = new TxnContext(1L, 100L, IsolationLevel.REPEATABLE_READ, "table1", 0);

            assertThat(ctx.readCache()).isNotNull();

            // Should be functional
            ctx.readCache().put("table1", "key".getBytes(), "value".getBytes());
            TxnReadCache.CachedRead result = ctx.readCache().get("table1", "key".getBytes());
            assertThat(result).isNotNull();
            assertThat(result.exists()).isTrue();
        }

        @Test
        @DisplayName("TxnContext should not have TxnReadCache for READ_COMMITTED")
        void txnContextShouldNotHaveTxnReadCacheForReadCommitted() {
            TxnContext ctx = new TxnContext(1L, 100L, IsolationLevel.READ_COMMITTED, "table1", 0);

            assertThat(ctx.readCache()).isNull();
            assertThat(ctx.readSnapshot()).isNull();
        }

        @Test
        @DisplayName("TxnContext ReadSnapshot should survive getter call")
        void txnContextReadSnapshotShouldSurviveGetterCall() {
            TxnContext ctx =
                    new TxnContext(
                            1L, 100L, IsolationLevel.REPEATABLE_READ, "table1", 0, 5L, 0L, null);

            ReadSnapshot first = ctx.readSnapshot();
            ReadSnapshot second = ctx.readSnapshot();

            assertThat(first).isSameAs(second);
            assertThat(first.snapshotTs()).isEqualTo(second.snapshotTs());
            assertThat(first.baseSnapshotId()).isEqualTo(second.baseSnapshotId());
        }
    }
}
