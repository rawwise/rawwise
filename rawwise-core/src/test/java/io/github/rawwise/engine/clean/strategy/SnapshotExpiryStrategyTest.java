package io.github.rawwise.engine.clean.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.engine.clean.CleanAction;
import io.github.rawwise.engine.clean.CleanContext;
import io.github.rawwise.engine.clean.ExpireSnapshotAction;
import io.github.rawwise.model.record.Snapshot;
import java.util.List;
import org.junit.jupiter.api.Test;

class SnapshotExpiryStrategyTest {

    @Test
    void expiresSnapshotsOlderThanRetentionMs() {
        long nowMs = 100000L;
        long retentionMs = 60000L;

        Snapshot latest = Snapshot.of(3L, null, nowMs, null, "append", null);
        Snapshot middle = Snapshot.of(2L, null, nowMs - 30000L, null, "append", null);
        Snapshot oldest = Snapshot.of(1L, null, nowMs - 80000L, null, "append", null);

        CleanContext context =
                new CleanContext(
                        "t1",
                        List.of(latest, middle, oldest),
                        List.of(),
                        List.of(),
                        List.of(),
                        nowMs);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(retentionMs, 1);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(1);
        ExpireSnapshotAction action = (ExpireSnapshotAction) actions.get(0);
        assertThat(action.snapshotId()).isEqualTo(1L);
    }

    @Test
    void keepsMinSnapshotsToKeep() {
        long nowMs = 100000L;
        long retentionMs = 60000L;

        Snapshot latest = Snapshot.of(3L, null, nowMs, null, "append", null);
        Snapshot middle = Snapshot.of(2L, null, nowMs - 80000L, null, "append", null);
        Snapshot oldest = Snapshot.of(1L, null, nowMs - 90000L, null, "append", null);

        CleanContext context =
                new CleanContext(
                        "t1",
                        List.of(latest, middle, oldest),
                        List.of(),
                        List.of(),
                        List.of(),
                        nowMs);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(retentionMs, 2);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(1);
        ExpireSnapshotAction action = (ExpireSnapshotAction) actions.get(0);
        assertThat(action.snapshotId()).isEqualTo(1L);
    }

    @Test
    void noExpiryWhenSnapshotCountEqualsMinSnapshotsToKeep() {
        long nowMs = 100000L;
        long retentionMs = 60000L;

        Snapshot latest = Snapshot.of(2L, null, nowMs, null, "append", null);
        Snapshot oldest = Snapshot.of(1L, null, nowMs - 80000L, null, "append", null);

        CleanContext context =
                new CleanContext(
                        "t1", List.of(latest, oldest), List.of(), List.of(), List.of(), nowMs);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(retentionMs, 2);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void noExpiryWhenSnapshotCountLessThanMinSnapshotsToKeep() {
        long nowMs = 100000L;
        long retentionMs = 60000L;

        Snapshot latest = Snapshot.of(1L, null, nowMs, null, "append", null);

        CleanContext context =
                new CleanContext("t1", List.of(latest), List.of(), List.of(), List.of(), nowMs);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(retentionMs, 2);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void noExpiryWhenAllSnapshotsWithinRetentionTime() {
        long nowMs = 100000L;
        long retentionMs = 60000L;

        Snapshot latest = Snapshot.of(3L, null, nowMs, null, "append", null);
        Snapshot middle = Snapshot.of(2L, null, nowMs - 30000L, null, "append", null);
        Snapshot oldest = Snapshot.of(1L, null, nowMs - 50000L, null, "append", null);

        CleanContext context =
                new CleanContext(
                        "t1",
                        List.of(latest, middle, oldest),
                        List.of(),
                        List.of(),
                        List.of(),
                        nowMs);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(retentionMs, 1);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void expiresMultipleOldSnapshots() {
        long nowMs = 100000L;
        long retentionMs = 60000L;

        Snapshot snap5 = Snapshot.of(5L, null, nowMs, null, "append", null);
        Snapshot snap4 = Snapshot.of(4L, null, nowMs - 30000L, null, "append", null);
        Snapshot snap3 = Snapshot.of(3L, null, nowMs - 70000L, null, "append", null);
        Snapshot snap2 = Snapshot.of(2L, null, nowMs - 80000L, null, "append", null);
        Snapshot snap1 = Snapshot.of(1L, null, nowMs - 90000L, null, "append", null);

        CleanContext context =
                new CleanContext(
                        "t1",
                        List.of(snap5, snap4, snap3, snap2, snap1),
                        List.of(),
                        List.of(),
                        List.of(),
                        nowMs);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(retentionMs, 1);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(3);
        List<Long> expiredIds =
                actions.stream().map(a -> ((ExpireSnapshotAction) a).snapshotId()).toList();
        assertThat(expiredIds).containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    void zeroRetentionMsExpiresAllBeyondMinSnapshotsToKeep() {
        long nowMs = 100000L;

        Snapshot snap3 = Snapshot.of(3L, null, nowMs, null, "append", null);
        Snapshot snap2 = Snapshot.of(2L, null, nowMs - 1000L, null, "append", null);
        Snapshot snap1 = Snapshot.of(1L, null, nowMs - 2000L, null, "append", null);

        CleanContext context =
                new CleanContext(
                        "t1", List.of(snap3, snap2, snap1), List.of(), List.of(), List.of(), nowMs);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(0L, 1);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(2);
        List<Long> expiredIds =
                actions.stream().map(a -> ((ExpireSnapshotAction) a).snapshotId()).toList();
        assertThat(expiredIds).containsExactlyInAnyOrder(1L, 2L);
    }

    @Test
    void emptySnapshotsReturnsEmpty() {
        CleanContext context =
                new CleanContext("t1", List.of(), List.of(), List.of(), List.of(), 100000L);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(60000L, 1);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void nullSnapshotsReturnsEmpty() {
        CleanContext context =
                new CleanContext("t1", null, List.of(), List.of(), List.of(), 100000L);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(60000L, 1);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void strategyTypeIsSnapshotExpiry() {
        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(60000L, 1);
        assertThat(strategy.strategyType()).isEqualTo(CleanStrategy.StrategyType.SNAPSHOT_EXPIRY);
    }

    @Test
    void negativeRetentionMsRejected() {
        assertThatThrownBy(() -> new SnapshotExpiryStrategy(-1L, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void zeroMinSnapshotsToKeepRejected() {
        assertThatThrownBy(() -> new SnapshotExpiryStrategy(60000L, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeMinSnapshotsToKeepRejected() {
        assertThatThrownBy(() -> new SnapshotExpiryStrategy(60000L, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void accessors() {
        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(86400000L, 5);
        assertThat(strategy.retentionMs()).isEqualTo(86400000L);
        assertThat(strategy.minSnapshotsToKeep()).isEqualTo(5);
    }

    @Test
    void sortsByTimestampMillisDescBeforeSelecting() {
        long nowMs = 100000L;
        long retentionMs = 60000L;

        Snapshot snap1 = Snapshot.of(1L, null, nowMs - 90000L, null, "append", null);
        Snapshot snap3 = Snapshot.of(3L, null, nowMs, null, "append", null);
        Snapshot snap2 = Snapshot.of(2L, null, nowMs - 30000L, null, "append", null);

        CleanContext context =
                new CleanContext(
                        "t1", List.of(snap1, snap3, snap2), List.of(), List.of(), List.of(), nowMs);

        SnapshotExpiryStrategy strategy = new SnapshotExpiryStrategy(retentionMs, 1);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(1);
        assertThat(((ExpireSnapshotAction) actions.get(0)).snapshotId()).isEqualTo(1L);
    }
}
