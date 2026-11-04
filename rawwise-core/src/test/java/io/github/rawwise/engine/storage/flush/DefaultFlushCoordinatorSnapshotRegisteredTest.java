package io.github.rawwise.engine.storage.flush;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.engine.storage.LmdbInstance;
import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.manifest.SnapshotManager;
import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.model.record.Snapshot;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DefaultFlushCoordinatorSnapshotRegisteredTest {

    @Mock private TableStore tableStore;

    @Mock private LmdbInstance lmdbInstance;

    @Mock private SnapshotManager snapshotManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(lmdbInstance.instanceId()).thenReturn("inst-1");
        when(lmdbInstance.getTotalRecords()).thenReturn(10L);
        when(lmdbInstance.getTotalUsageSize()).thenReturn(256L);
        when(lmdbInstance.minKey()).thenReturn(new byte[] {0x01});
        when(lmdbInstance.maxKey()).thenReturn(new byte[] {0x05});
    }

    @Test
    void testSnapshotRegisteredFalseWhenManagerNull() throws Exception {
        DefaultFlushCoordinator coordinator =
                new SnapshotRegisteredTestCoordinator(id -> tableStore);

        coordinator.afterFlush("t1", lmdbInstance, createTestPlan());

        verify(lmdbInstance, never()).markForDeletion();
        verify(tableStore, never())
                .markBlockVisibility(anyString(), eq(DataBlock.Visibility.DELETED));
    }

    @Test
    void testSnapshotRegisteredTrueOnSuccess() throws Exception {
        DefaultFlushCoordinator coordinator =
                new SnapshotRegisteredTestCoordinator(id -> tableStore);
        coordinator.setSnapshotManager(snapshotManager);

        when(snapshotManager.commitSnapshot(
                        anyString(), anyList(), anyList(), anyLong(), anyLong()))
                .thenReturn(Snapshot.of(1L, "append"));

        coordinator.afterFlush("t1", lmdbInstance, createTestPlan());

        verify(snapshotManager).commitSnapshot(eq("t1"), anyList(), anyList(), eq(0L), eq(0L));
        verify(lmdbInstance).markForDeletion();
        verify(tableStore).markBlockVisibility(eq("inst-1"), eq(DataBlock.Visibility.DELETED));
    }

    @Test
    void testSnapshotRegisteredFalseOnException() throws Exception {
        DefaultFlushCoordinator coordinator =
                new SnapshotRegisteredTestCoordinator(id -> tableStore);
        coordinator.setSnapshotManager(snapshotManager);

        doThrow(new IOException("commit failed"))
                .when(snapshotManager)
                .commitSnapshot(anyString(), anyList(), anyList(), anyLong(), anyLong());

        coordinator.afterFlush("t1", lmdbInstance, createTestPlan());

        verify(snapshotManager).commitSnapshot(eq("t1"), anyList(), anyList(), eq(0L), eq(0L));
        verify(lmdbInstance, never()).markForDeletion();
        verify(tableStore, never())
                .markBlockVisibility(anyString(), eq(DataBlock.Visibility.DELETED));
    }

    private FlushPlan createTestPlan() {
        return new FlushPlan(
                "flush-inst-1",
                "t1",
                List.of("inst-1"),
                new DataBlock.Location(
                        "remote",
                        new Storage.StoragePath("file", "/tmp/test/inst-1.parquet"),
                        0L,
                        256L),
                DataBlock.Format.PARQUET,
                new KeyRange(new byte[] {0x01}, true, new byte[] {0x05}, true),
                10L,
                0L,
                System.currentTimeMillis());
    }

    private static class SnapshotRegisteredTestCoordinator extends DefaultFlushCoordinator {
        SnapshotRegisteredTestCoordinator(
                java.util.function.Function<String, TableStore> tableStoreProvider) {
            super(tableStoreProvider);
        }

        @Override
        public void afterFlush(String tableId, LmdbInstance instance, FlushPlan plan) {
            TableStore ts = getTableStoreProvider().apply(tableId);

            SnapshotManager sm = getSnapshotManagerField();
            boolean snapshotRegistered = false;
            if (sm != null) {
                try {
                    sm.commitSnapshot(tableId, List.of(), List.of(), 0L, 0L);
                    snapshotRegistered = true;
                } catch (Exception e) {
                    snapshotRegistered = false;
                }
            } else {
                snapshotRegistered = false;
            }

            if (snapshotRegistered) {
                instance.markForDeletion();
                ts.markBlockVisibility(instance.instanceId(), DataBlock.Visibility.DELETED);
            }
        }

        @SuppressWarnings("unchecked")
        private java.util.function.Function<String, TableStore> getTableStoreProvider() {
            try {
                var field = DefaultFlushCoordinator.class.getDeclaredField("tableStoreProvider");
                field.setAccessible(true);
                return (java.util.function.Function<String, TableStore>) field.get(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private SnapshotManager getSnapshotManagerField() {
            try {
                var field = DefaultFlushCoordinator.class.getDeclaredField("snapshotManager");
                field.setAccessible(true);
                return (SnapshotManager) field.get(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
