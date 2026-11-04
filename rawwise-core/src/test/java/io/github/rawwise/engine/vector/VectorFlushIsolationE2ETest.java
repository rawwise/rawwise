package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName(".5-2: Vector flush isolation E2E")
class VectorFlushIsolationE2ETest {

    @Test
    @DisplayName("vector flush fail flush complete")
    void scalarFlushSucceedsOnVectorFlushFailure() {
        VectorIndex failingIndex = new FailingVectorIndex();
        VectorMetrics metrics = new VectorMetrics();
        VectorFlushListener listener = new VectorFlushListener(failingIndex, metrics);

        byte[] key = "test-key".getBytes();
        listener.onFlushCommitted("table1", "inst1", List.of(key));

        assertEquals(1, metrics.getFlushFailureCount());
        assertEquals(1, listener.pendingRetryCount());

        listener.close();
    }

    @Test
    @DisplayName("after retrysuccess")
    void backgroundRetrySucceeds() {
        VectorMetrics metrics = new VectorMetrics();
        VectorFlushListener listener = new VectorFlushListener(new NoOpVectorIndex(), metrics);

        byte[] key = "test-key".getBytes();
        listener.onFlushCommitted("table1", "inst1", List.of(key));

        assertEquals(0, metrics.getFlushFailureCount());
        assertEquals(0, listener.pendingRetryCount());

        listener.close();
    }

    private static class FailingVectorIndex implements VectorIndex {
        @Override
        public void index(String namespace, String entryKey, String l0Abstract) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void indexWithEmbedding(String namespace, String entryKey, float[] embedding) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK) {
            return List.of();
        }

        @Override
        public void delete(String namespace, String entryKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void flushToCold(String namespace, Collection<String> entryKeys) {
            throw new RuntimeException("Simulated vector flush failure");
        }

        @Override
        public void setColdStore(VectorColdStore coldStore) {}

        @Override
        public void close() {}
    }

    private static class NoOpVectorIndex implements VectorIndex {
        @Override
        public void index(String namespace, String entryKey, String l0Abstract) {}

        @Override
        public void indexWithEmbedding(String namespace, String entryKey, float[] embedding) {}

        @Override
        public List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK) {
            return List.of();
        }

        @Override
        public void delete(String namespace, String entryKey) {}

        @Override
        public void flushToCold(String namespace, Collection<String> entryKeys) {}

        @Override
        public void setColdStore(VectorColdStore coldStore) {}

        @Override
        public void close() {}
    }
}
