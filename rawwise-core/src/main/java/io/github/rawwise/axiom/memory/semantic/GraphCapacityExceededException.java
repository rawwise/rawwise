package io.github.rawwise.axiom.memory.semantic;

public final class GraphCapacityExceededException extends GraphException {
    private final String resource;
    private final long current;
    private final long max;

    public GraphCapacityExceededException(String graphId, String resource, long current, long max) {
        super(
                graphId,
                "AXIOM_GRAPH_CAPACITY_EXCEEDED",
                429,
                "Capacity exceeded for " + resource + ": " + current + "/" + max);
        this.resource = resource;
        this.current = current;
        this.max = max;
    }

    public String resource() {
        return resource;
    }

    public long current() {
        return current;
    }

    public long max() {
        return max;
    }
}
