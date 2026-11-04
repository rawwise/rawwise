package io.github.rawwise.common;

import java.util.function.Supplier;
import lombok.Getter;

public class Lazy<T> {
    private final Supplier<T> supplier;
    @Getter private volatile boolean initialized;
    private T value;

    public Lazy(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    public T get() {
        if (!initialized) {
            synchronized (this) {
                if (!initialized) {
                    value = supplier.get();
                    initialized = true;
                }
            }
        }
        return value;
    }

    public void set(T value) {
        synchronized (this) {
            this.value = value;
            this.initialized = true;
        }
    }

    public void reset() {
        synchronized (this) {
            initialized = false;
            value = null;
        }
    }
}
