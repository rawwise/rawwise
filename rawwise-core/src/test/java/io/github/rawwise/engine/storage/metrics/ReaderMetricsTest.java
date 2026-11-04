package io.github.rawwise.engine.storage.metrics;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class ReaderMetricsTest {

    @Test
    void readerErrorTypeHasAllExpectedValues() {
        assertThat(ReaderErrorType.values())
                .containsExactlyInAnyOrder(
                        ReaderErrorType.THROTTLING,
                        ReaderErrorType.TIMEOUT,
                        ReaderErrorType.CORRUPTION,
                        ReaderErrorType.DECOMPRESSION,
                        ReaderErrorType.NETWORK,
                        ReaderErrorType.STORAGE_UNAVAILABLE,
                        ReaderErrorType.PERMISSION_DENIED,
                        ReaderErrorType.UNKNOWN);
    }

    @Test
    void noOpReaderMetricsDoesNotThrow() {
        ReaderMetrics metrics = NoOpReaderMetrics.INSTANCE;
        assertThatCode(
                        () -> {
                            metrics.recordRead("find", 100, 10, 1000);
                            metrics.recordSeek(50);
                            metrics.recordCacheHit(true);
                            metrics.recordError(
                                    ReaderErrorType.THROTTLING, new IOException("test"), "find");
                            metrics.recordRetry("find", 3);
                        })
                .doesNotThrowAnyException();
    }

    @Test
    void noOpReaderMetricsIsSingleton() {
        assertThat(NoOpReaderMetrics.INSTANCE).isSameAs(NoOpReaderMetrics.INSTANCE);
    }
}
