package io.github.rawwise.engine.clean.strategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.rawwise.backend.lazyupdate.UpdateLog;
import io.github.rawwise.engine.clean.CleanAction;
import io.github.rawwise.engine.clean.CleanContext;
import io.github.rawwise.engine.clean.DeleteFileAction;
import io.github.rawwise.engine.clean.StorageFileEntry;
import io.github.rawwise.engine.storage.ColumnStatistics;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.ParquetDataFile;
import io.github.rawwise.engine.storage.RawWiseFile;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OrphanFileStrategyTest {

    @Test
    void detectsOrphanFilesNotInManifest() {
        StorageFileEntry orphan = new StorageFileEntry("/data/orphan.parquet", 2048L, 1000L);
        StorageFileEntry referenced =
                new StorageFileEntry("/data/referenced.parquet", 1024L, 1000L);
        DataFile df = createDataFile("/data/referenced.parquet");

        CleanContext context =
                new CleanContext(
                        "t1",
                        List.of(),
                        List.of(df),
                        List.of(),
                        List.of(orphan, referenced),
                        100000L);

        OrphanFileStrategy strategy = new OrphanFileStrategy(0L);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(1);
        DeleteFileAction action = (DeleteFileAction) actions.get(0);
        assertThat(action.filePath()).isEqualTo("/data/orphan.parquet");
        assertThat(action.estimatedExpiredBytes()).isEqualTo(2048L);
    }

    @Test
    void detectsOrphanFilesNotInLogFileReferences() {
        StorageFileEntry orphan = new StorageFileEntry("/logs/orphan.nwl", 512L, 1000L);
        StorageFileEntry referenced = new StorageFileEntry("/logs/referenced.nwl", 256L, 1000L);
        LogFile lf = createLogFile("/logs/referenced.nwl");

        CleanContext context =
                new CleanContext(
                        "t1",
                        List.of(),
                        List.of(),
                        List.of(lf),
                        List.of(orphan, referenced),
                        100000L);

        OrphanFileStrategy strategy = new OrphanFileStrategy(0L);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(1);
        DeleteFileAction action = (DeleteFileAction) actions.get(0);
        assertThat(action.filePath()).isEqualTo("/logs/orphan.nwl");
    }

    @Test
    void filtersByMinOrphanAgeMs() {
        long nowMs = 100000L;
        long minAgeMs = 60000L;
        StorageFileEntry oldOrphan = new StorageFileEntry("/data/old.parquet", 1024L, 10000L);
        StorageFileEntry recentOrphan = new StorageFileEntry("/data/recent.parquet", 2048L, 80000L);

        CleanContext context =
                new CleanContext(
                        "t1",
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(oldOrphan, recentOrphan),
                        nowMs);

        OrphanFileStrategy strategy = new OrphanFileStrategy(minAgeMs);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(1);
        DeleteFileAction action = (DeleteFileAction) actions.get(0);
        assertThat(action.filePath()).isEqualTo("/data/old.parquet");
    }

    @Test
    void recentOrphanBelowAgeThresholdNotDeleted() {
        long nowMs = 100000L;
        long minAgeMs = 60000L;
        StorageFileEntry recentOrphan = new StorageFileEntry("/data/recent.parquet", 1024L, 80000L);

        CleanContext context =
                new CleanContext(
                        "t1", List.of(), List.of(), List.of(), List.of(recentOrphan), nowMs);

        OrphanFileStrategy strategy = new OrphanFileStrategy(minAgeMs);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void noOrphansWhenAllReferenced() {
        StorageFileEntry entry = new StorageFileEntry("/data/f1.parquet", 1024L, 1000L);
        DataFile df = createDataFile("/data/f1.parquet");

        CleanContext context =
                new CleanContext("t1", List.of(), List.of(df), List.of(), List.of(entry), 100000L);

        OrphanFileStrategy strategy = new OrphanFileStrategy(0L);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void emptyStorageFileEntriesReturnsEmpty() {
        DataFile df = createDataFile("/data/f1.parquet");

        CleanContext context =
                new CleanContext("t1", List.of(), List.of(df), List.of(), List.of(), 100000L);

        OrphanFileStrategy strategy = new OrphanFileStrategy(0L);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void strategyTypeIsOrphanFile() {
        OrphanFileStrategy strategy = new OrphanFileStrategy(1000L);
        assertThat(strategy.strategyType()).isEqualTo(CleanStrategy.StrategyType.ORPHAN_FILE);
    }

    @Test
    void negativeMinOrphanAgeMsRejected() {
        assertThatThrownBy(() -> new OrphanFileStrategy(-1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void minOrphanAgeMsAccessor() {
        OrphanFileStrategy strategy = new OrphanFileStrategy(5000L);
        assertThat(strategy.minOrphanAgeMs()).isEqualTo(5000L);
    }

    @Test
    void exactAgeBoundaryNotIncluded() {
        long nowMs = 100000L;
        long minAgeMs = 60000L;
        long exactBoundary = nowMs - minAgeMs;
        StorageFileEntry exactOrphan =
                new StorageFileEntry("/data/exact.parquet", 1024L, exactBoundary);

        CleanContext context =
                new CleanContext(
                        "t1", List.of(), List.of(), List.of(), List.of(exactOrphan), nowMs);

        OrphanFileStrategy strategy = new OrphanFileStrategy(minAgeMs);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).isEmpty();
    }

    @Test
    void justAboveAgeBoundaryIncluded() {
        long nowMs = 100000L;
        long minAgeMs = 60000L;
        long justBelow = nowMs - minAgeMs - 1;
        StorageFileEntry justAboveOrphan =
                new StorageFileEntry("/data/justabove.parquet", 1024L, justBelow);

        CleanContext context =
                new CleanContext(
                        "t1", List.of(), List.of(), List.of(), List.of(justAboveOrphan), nowMs);

        OrphanFileStrategy strategy = new OrphanFileStrategy(minAgeMs);
        List<CleanAction> actions = strategy.plan(context);

        assertThat(actions).hasSize(1);
        assertThat(((DeleteFileAction) actions.get(0)).filePath())
                .isEqualTo("/data/justabove.parquet");
    }

    private DataFile createDataFile(String path) {
        return new ParquetDataFile(
                path,
                1024L,
                System.currentTimeMillis(),
                "t1",
                1L,
                new KeyRange(new byte[] {1}, true, new byte[] {9}, true),
                100L,
                1L,
                10L,
                20L,
                Map.of(),
                new ColumnStatistics(Map.of()),
                0);
    }

    private LogFile createLogFile(String path) {
        return new UpdateLog(
                path,
                RawWiseFile.FileFormat.NWL,
                512L,
                System.currentTimeMillis(),
                "t1",
                1L,
                10L,
                20L);
    }
}
