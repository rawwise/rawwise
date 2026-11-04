package io.github.rawwise.axiom.pipeline.cognition;

import io.github.rawwise.axiom.pipeline.derivation.Layer;
import java.util.List;

@FunctionalInterface
public interface DerivationFeedbackCallback {
    void onDerive(String entityId, Layer layer, double selfScore);

    default void onDeriveBatch(List<String> entityIds, Layer layer, double defaultScore) {
        if (entityIds == null) return;
        for (String entityId : entityIds) {
            onDerive(entityId, layer, defaultScore);
        }
    }
}
