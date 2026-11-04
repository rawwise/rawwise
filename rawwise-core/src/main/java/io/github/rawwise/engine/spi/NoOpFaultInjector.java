package io.github.rawwise.engine.spi;

public final class NoOpFaultInjector implements FaultInjector {
    public static final NoOpFaultInjector INSTANCE = new NoOpFaultInjector();

    private NoOpFaultInjector() {}

    @Override
    public void inject(InjectionPoint point) {}

    @Override
    public boolean isEnabled(InjectionPoint point) {
        return false;
    }
}
