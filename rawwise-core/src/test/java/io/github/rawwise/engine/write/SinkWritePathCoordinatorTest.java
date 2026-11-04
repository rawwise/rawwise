package io.github.rawwise.engine.write;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.github.rawwise.engine.storage.ingestion.IngestionDataSink;
import io.github.rawwise.engine.storage.write.LmdbWritePathCoordinator;
import io.github.rawwise.engine.storage.write.SinkWritePathCoordinator;
import io.github.rawwise.engine.storage.write.WritePathCoordinator;
import java.util.function.BiFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SinkWritePathCoordinatorTest {

    @Test
    @DisplayName("PARQUET : hasIngestionDataSink ensureWritablereturns true")
    void ensureWritable_withValidSink_returnsTrue() {
        IngestionDataSink sink = mock(IngestionDataSink.class);
        WritePathCoordinator coordinator = new SinkWritePathCoordinator(sink);

        assertTrue(coordinator.ensureWritable("default.test_table", new byte[] {1, 2, 3}));
    }

    @Test
    @DisplayName("PARQUET : noIngestionDataSink ensureWritablereturns false")
    void ensureWritable_withNullSink_returnsFalse() {
        WritePathCoordinator coordinator = new SinkWritePathCoordinator(null);

        assertFalse(coordinator.ensureWritable("default.test_table", new byte[] {1, 2, 3}));
    }

    @Test
    @DisplayName("PARQUET : null key + has sink returns true(sinkcan is)")
    void ensureWritable_nullKey_validSink_returnsTrue() {
        IngestionDataSink sink = mock(IngestionDataSink.class);
        WritePathCoordinator coordinator = new SinkWritePathCoordinator(sink);

        assertTrue(coordinator.ensureWritable("default.test_table", null));
    }

    @Test
    @DisplayName("PARQUET : isLmdbBacked returns false")
    void isLmdbBacked_returnsFalse() {
        IngestionDataSink sink = mock(IngestionDataSink.class);
        WritePathCoordinator coordinator = new SinkWritePathCoordinator(sink);

        assertFalse(coordinator.isLmdbBacked());
    }

    @Test
    @DisplayName("PARQUET : multi-tableensureWritable via")
    void ensureWritable_multipleTables_allPass() {
        IngestionDataSink sink = mock(IngestionDataSink.class);
        WritePathCoordinator coordinator = new SinkWritePathCoordinator(sink);

        assertTrue(coordinator.ensureWritable("default.table_a", new byte[] {1}));
        assertTrue(coordinator.ensureWritable("default.table_b", new byte[] {2}));
        assertTrue(coordinator.ensureWritable("default.table_c", new byte[] {3}));
    }

    @Test
    @DisplayName("PARQUET : ensureWritabledoes not trigger IngestionDataSink call(can check)")
    void ensureWritable_doesNotInvokeSinkMethods() throws Exception {
        IngestionDataSink sink = mock(IngestionDataSink.class);
        WritePathCoordinator coordinator = new SinkWritePathCoordinator(sink);

        coordinator.ensureWritable("default.test_table", new byte[] {1, 2, 3});

        verify(sink, never())
                .flushBatch(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.anyList(),
                        org.mockito.ArgumentMatchers.anyList(),
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.anyLong());
        verify(sink, never())
                .prepare(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyList(),
                        org.mockito.ArgumentMatchers.anyLong());
        verify(sink, never())
                .commit(
                        org.mockito.ArgumentMatchers.anyString(),
                        org.mockito.ArgumentMatchers.anyList(),
                        org.mockito.ArgumentMatchers.anyLong(),
                        org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    @DisplayName(
            "for : LmdbWritePathCoordinator.isLmdbBacked=true, SinkWritePathCoordinator.isLmdbBacked=false")
    void contrast_isLmdbBacked_sinkVsLmdb() {
        IngestionDataSink sink = mock(IngestionDataSink.class);
        WritePathCoordinator sinkCoord = new SinkWritePathCoordinator(sink);

        BiFunction<String, byte[], Boolean> checker = (tid, key) -> true;
        WritePathCoordinator lmdbCoord = new LmdbWritePathCoordinator(checker);

        assertFalse(sinkCoord.isLmdbBacked(), "Sink coordinator should NOT be LMDB backed");
        assertTrue(lmdbCoord.isLmdbBacked(), "LMDB coordinator should be LMDB backed");
    }

    @Test
    @DisplayName("PARQUET : null sinkofisLmdbBacked returns false")
    void isLmdbBacked_nullSink_returnsFalse() {
        WritePathCoordinator coordinator = new SinkWritePathCoordinator(null);

        assertFalse(coordinator.isLmdbBacked());
    }
}
