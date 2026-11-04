package io.github.rawwise.engine.compaction.cleanup;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class CompactionGarbageCollectorTest {

    @Mock private Storage storage;

    @Mock private SnapshotManager snapshotManager;

    @TempDir Path tempDir;

    private FileGarbageCollector gc;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        gc =
                new FileGarbageCollector(
                        storage, snapshotManager, (io.github.rawwise.metastore.Metastore) null);
    }

    @Test
    void scheduleGcAddsEntriesToPendingQueue() throws IOException {
        when(snapshotManager.latestSnapshot(anyString())).thenReturn(Optional.empty());

        gc.scheduleGc("t1", List.of("t1/data/f1.parquet", "t1/data/f2.parquet"), 5L);

        when(snapshotManager.latestSnapshot("t1"))
                .thenReturn(Optional.of(Snapshot.of(10L, "append")));
        when(storage.exists(new Storage.StoragePath("file", "t1/data/f1.parquet")))
                .thenReturn(true);
        when(storage.exists(new Storage.StoragePath("file", "t1/data/f2.parquet")))
                .thenReturn(true);

        gc.runGcCycle("t1");

        verify(storage).delete(new Storage.StoragePath("file", "t1/data/f1.parquet"));
        verify(storage).delete(new Storage.StoragePath("file", "t1/data/f2.parquet"));
    }

    @Test
    void runGcCycleDoesNotDeleteWhenLatestSnapshotEqualsObsoletePlusOne() throws IOException {
        when(snapshotManager.latestSnapshot("t1"))
                .thenReturn(Optional.of(Snapshot.of(6L, "append")));

        gc.scheduleGc("t1", List.of("t1/data/f1.parquet"), 5L);
        gc.runGcCycle("t1");

        verify(storage, never()).delete(new Storage.StoragePath("file", "t1/data/f1.parquet"));
    }

    @Test
    void runGcCycleDeletesWhenLatestSnapshotGreaterThanObsoletePlusOne() throws IOException {
        when(snapshotManager.latestSnapshot("t1"))
                .thenReturn(Optional.of(Snapshot.of(7L, "append")));
        when(storage.exists(new Storage.StoragePath("file", "t1/data/f1.parquet")))
                .thenReturn(true);

        gc.scheduleGc("t1", List.of("t1/data/f1.parquet"), 5L);
        gc.runGcCycle("t1");

        verify(storage).delete(new Storage.StoragePath("file", "t1/data/f1.parquet"));
    }

    @Test
    void runGcCycleHandlesMissingSnapshotGracefully() throws IOException {
        when(snapshotManager.latestSnapshot("t1")).thenReturn(Optional.empty());

        gc.scheduleGc("t1", List.of("t1/data/f1.parquet"), 5L);
        gc.runGcCycle("t1");

        verify(storage, never()).delete(new Storage.StoragePath("file", "t1/data/f1.parquet"));
    }

    @Test
    void runGcCycleAllTablesProcessesAllDistinctTables() throws IOException {
        when(snapshotManager.latestSnapshot("t1"))
                .thenReturn(Optional.of(Snapshot.of(10L, "append")));
        when(snapshotManager.latestSnapshot("t2"))
                .thenReturn(Optional.of(Snapshot.of(10L, "append")));
        when(storage.exists(new Storage.StoragePath("file", "t1/data/f1.parquet")))
                .thenReturn(true);
        when(storage.exists(new Storage.StoragePath("file", "t2/data/f2.parquet")))
                .thenReturn(true);

        gc.scheduleGc("t1", List.of("t1/data/f1.parquet"), 5L);
        gc.scheduleGc("t2", List.of("t2/data/f2.parquet"), 5L);
        gc.runGcCycleAllTables();

        verify(storage).delete(new Storage.StoragePath("file", "t1/data/f1.parquet"));
        verify(storage).delete(new Storage.StoragePath("file", "t2/data/f2.parquet"));
    }

    @Test
    void pendingGcEntryFieldsAreCorrectlyStored() throws IOException {
        when(snapshotManager.latestSnapshot("t1"))
                .thenReturn(Optional.of(Snapshot.of(50L, "append")));
        when(storage.exists(new Storage.StoragePath("file", "t1/data/f1.parquet")))
                .thenReturn(true);

        gc.scheduleGc("t1", List.of("t1/data/f1.parquet"), 42L);
        gc.runGcCycle("t1");

        verify(storage).delete(new Storage.StoragePath("file", "t1/data/f1.parquet"));
    }
}
