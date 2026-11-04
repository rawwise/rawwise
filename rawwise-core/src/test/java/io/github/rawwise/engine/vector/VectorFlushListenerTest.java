package io.github.rawwise.engine.vector;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

class VectorFlushListenerTest {

    @Test
    void onFlushCommittedCallsFlushToColdWithDecodedKeys() {
        VectorIndex vectorIndex = mock(VectorIndex.class);
        VectorFlushListener listener = new VectorFlushListener(vectorIndex);

        List<byte[]> flushedKeys =
                List.of(
                        "key1".getBytes(StandardCharsets.UTF_8),
                        "key2".getBytes(StandardCharsets.UTF_8));

        listener.onFlushCommitted("table1", "inst1", flushedKeys);

        verify(vectorIndex)
                .flushToCold(
                        eq("table1"),
                        argThat(
                                keys ->
                                        keys.stream()
                                                .map(String::new)
                                                .toList()
                                                .equals(List.of("key1", "key2"))));
    }

    @Test
    void onFlushCommittedWithEmptyKeysIsNoop() {
        VectorIndex vectorIndex = mock(VectorIndex.class);
        VectorFlushListener listener = new VectorFlushListener(vectorIndex);

        listener.onFlushCommitted("table1", "inst1", List.of());

        verifyNoInteractions(vectorIndex);
    }

    @Test
    void onFlushCommittedWithNullKeysIsNoop() {
        VectorIndex vectorIndex = mock(VectorIndex.class);
        VectorFlushListener listener = new VectorFlushListener(vectorIndex);

        listener.onFlushCommitted("table1", "inst1", null);

        verifyNoInteractions(vectorIndex);
    }

    @Test
    void onFlushCommittedPropagatesException() {
        VectorIndex vectorIndex = mock(VectorIndex.class);
        VectorFlushListener listener = new VectorFlushListener(vectorIndex);

        doThrow(new RuntimeException("flush failed"))
                .when(vectorIndex)
                .flushToCold(anyString(), anyCollection());

        List<byte[]> flushedKeys = List.of("key1".getBytes(StandardCharsets.UTF_8));

        assertThrows(
                RuntimeException.class,
                () -> listener.onFlushCommitted("table1", "inst1", flushedKeys),
                "Vector flush failed");
    }
}
