package io.github.rawwise.frontend.ir.routing;

import java.io.Serializable;

public record RoutingStats(
        int totalShards,
        int selectedShards,
        int totalKeys,
        int matchedKeys,
        boolean fastPathHit,
        String fallbackReason)
        implements Serializable {
    private static final long serialVersionUID = 1L;

    public static RoutingStats empty() {
        return new RoutingStats(0, 0, 0, 0, false, null);
    }

    public String explain() {
        StringBuilder sb = new StringBuilder();
        sb.append("Shards: ").append(totalShards).append(" -> ").append(selectedShards);
        sb.append(", Keys: ").append(totalKeys).append(" -> ").append(matchedKeys);
        sb.append(", FastPath: ").append(fastPathHit ? "hit" : "miss");
        if (fallbackReason != null) {
            sb.append(", Fallback: ").append(fallbackReason);
        }
        return sb.toString();
    }
}
