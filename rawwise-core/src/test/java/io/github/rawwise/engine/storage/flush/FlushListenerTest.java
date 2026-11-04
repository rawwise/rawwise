package io.github.rawwise.engine.storage.flush;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.github.rawwise.engine.vector.VectorFlushListener;
import io.github.rawwise.engine.vector.VectorIndex;
import org.junit.jupiter.api.Test;

class FlushListenerTest {

    @Test
    void addFlushListenerDoesNotThrow() {
        DefaultFlushCoordinator coordinator = new DefaultFlushCoordinator(tableId -> null);
        VectorIndex mockIndex = mock(VectorIndex.class);
        assertDoesNotThrow(() -> coordinator.addFlushListener(new VectorFlushListener(mockIndex)));
    }

    @Test
    void listenerExceptionDoesNotPropagate() {
        DefaultFlushCoordinator coordinator = new DefaultFlushCoordinator(tableId -> null);
        VectorIndex mockIndex = mock(VectorIndex.class);
        assertDoesNotThrow(() -> coordinator.addFlushListener(new VectorFlushListener(mockIndex)));
    }

    @Test
    void multipleListenersAllAccepted() {
        DefaultFlushCoordinator coordinator = new DefaultFlushCoordinator(tableId -> null);
        VectorIndex mockIndex = mock(VectorIndex.class);
        coordinator.addFlushListener(new VectorFlushListener(mockIndex));
        coordinator.addFlushListener(new VectorFlushListener(mockIndex));
    }
}
