package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.engine.txn.orchestrate.TxnOrchestrator;
import java.util.concurrent.atomic.AtomicReference;

public final class LazyUpdateBridge {

    private static final AtomicReference<LazyUpdateWriteInterceptor> INTERCEPTOR =
            new AtomicReference<>();
    private static final AtomicReference<TxnOrchestrator> ORCHESTRATOR = new AtomicReference<>();

    private LazyUpdateBridge() {}

    public static void install(LazyUpdateWriteInterceptor interceptor) {
        INTERCEPTOR.set(interceptor);
    }

    public static LazyUpdateWriteInterceptor interceptor() {
        return INTERCEPTOR.get();
    }

    public static boolean isAvailable() {
        return INTERCEPTOR.get() != null;
    }

    public static void installOrchestrator(TxnOrchestrator orchestrator) {
        ORCHESTRATOR.set(orchestrator);
    }

    public static TxnOrchestrator orchestrator() {
        return ORCHESTRATOR.get();
    }
}
