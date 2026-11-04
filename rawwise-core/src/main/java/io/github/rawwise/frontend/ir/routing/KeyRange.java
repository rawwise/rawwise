package io.github.rawwise.frontend.ir.routing;

public record KeyRange(long startInclusive, long endExclusive) {
    public boolean contains(long key) {
        return key >= startInclusive && key < endExclusive;
    }
}
