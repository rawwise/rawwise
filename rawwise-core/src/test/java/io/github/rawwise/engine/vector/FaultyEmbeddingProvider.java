package io.github.rawwise.engine.vector;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FaultyEmbeddingProvider implements EmbeddingProvider {

    private final EmbeddingProvider delegate;
    private final AtomicInteger callCount = new AtomicInteger(0);
    private volatile int failUntilCall;
    private volatile boolean alwaysFail;

    public FaultyEmbeddingProvider(EmbeddingProvider delegate, int failUntilCall) {
        this.delegate = delegate;
        this.failUntilCall = failUntilCall;
    }

    @Override
    public float[] embed(String text) {
        int call = callCount.incrementAndGet();
        if (alwaysFail) throw new EmbeddingUnavailableException("Service down");
        if (call <= failUntilCall) throw new EmbeddingUnavailableException("Timeout #" + call);
        return delegate.embed(text);
    }

    @Override
    public float[][] embedBatch(List<String> texts) {
        return texts.stream().map(this::embed).toArray(float[][]::new);
    }

    @Override
    public String modelIdentifier() {
        return delegate.modelIdentifier();
    }

    public void setFailUntilCall(int count) {
        this.failUntilCall = count;
    }

    public void setAlwaysFail(boolean fail) {
        this.alwaysFail = fail;
    }

    public void reset() {
        callCount.set(0);
        failUntilCall = 0;
        alwaysFail = false;
    }

    public int getCallCount() {
        return callCount.get();
    }
}
