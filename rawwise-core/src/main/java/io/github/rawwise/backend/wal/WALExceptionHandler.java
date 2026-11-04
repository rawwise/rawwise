package io.github.rawwise.backend.wal;

import com.lmax.disruptor.ExceptionHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WALExceptionHandler<T> implements ExceptionHandler<T> {

    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        log.error("Exception processing event at sequence: {}", sequence, ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.error("Exception during WAL startup", ex);
        throw new RuntimeException("WAL startup failed", ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.error("Exception during WAL shutdown", ex);
    }
}
