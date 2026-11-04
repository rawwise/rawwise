package io.github.rawwise.axiom.pipeline.entity;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class HybridEntityExtractor implements AutoCloseable {

    private final EntityProvider primary;
    private final EntityProvider fallback;
    private final boolean hybridEnabled;

    public HybridEntityExtractor(
            EntityProvider primary, EntityProvider fallback, boolean hybridEnabled) {
        this.primary = primary;
        this.fallback = fallback;
        this.hybridEnabled = hybridEnabled;
    }

    public EntityView extract(String text, EntityContext context) throws ExtractionException {
        EntityView primaryView = null;
        ExtractionException primaryError = null;
        try {
            primaryView = primary.extract(text, context);
        } catch (ExtractionException e) {
            primaryError = e;
            log.warn("Primary entity provider [{}] failed: {}", primary.name(), e.getMessage());
        }

        if (!hybridEnabled || fallback == null) {
            if (primaryView != null) return primaryView;
            throw primaryError == null
                    ? new ExtractionException("primary returned null and no fallback configured")
                    : primaryError;
        }

        EntityView fallbackView = null;
        ExtractionException fallbackError = null;
        try {
            fallbackView = fallback.extract(text, context);
        } catch (ExtractionException e) {
            fallbackError = e;
            log.warn("Fallback entity provider [{}] failed: {}", fallback.name(), e.getMessage());
        }

        if (primaryView == null && fallbackView == null) {
            throw new ExtractionException(
                    "both providers failed: primary="
                            + (primaryError == null ? "null" : primaryError.getMessage())
                            + ", fallback="
                            + (fallbackError == null ? "null" : fallbackError.getMessage()));
        }
        if (primaryView == null) {
            return relabel(fallbackView, "spacy-only");
        }
        if (fallbackView == null) {
            return relabel(primaryView, "llm-only");
        }
        return fuse(primaryView, fallbackView, context);
    }

    private EntityView relabel(EntityView view, String source) {
        return new EntityView(
                view.diaId(),
                view.sampleId(),
                view.entities(),
                view.relations(),
                source,
                view.extractedAt(),
                view.status(),
                view.failureReason());
    }

    private EntityView fuse(
            EntityView primaryView, EntityView fallbackView, EntityContext context) {
        Map<String, Entity> byCanon = new LinkedHashMap<>();
        for (Entity e : primaryView.entities()) {
            String canonKey = canonKey(e);
            byCanon.put(canonKey, e);
        }
        for (Entity e : fallbackView.entities()) {
            String canonKey = canonKey(e);
            Entity existing = byCanon.get(canonKey);
            if (existing == null) {
                byCanon.put(canonKey, withConfidence(e, 0.75));
                continue;
            }
            if (existing.type() == e.type()) {
                byCanon.put(canonKey, withConfidence(existing, 0.95));
            } else {
                Entity winner = existing.confidence() >= e.confidence() ? existing : e;
                byCanon.put(canonKey, winner);
            }
        }
        List<Entity> hybridPrimaryUpgraded = new ArrayList<>();
        for (Entity existing : byCanon.values()) {
            if (existing.confidence() < 0.85) {
                hybridPrimaryUpgraded.add(existing);
            } else {
                hybridPrimaryUpgraded.add(existing);
            }
        }
        List<Relation> mergedRelations = new ArrayList<>(primaryView.relations());
        mergedRelations.addAll(fallbackView.relations());

        String diaId = primaryView.diaId() != null ? primaryView.diaId() : fallbackView.diaId();
        String sampleId =
                primaryView.sampleId() != null ? primaryView.sampleId() : fallbackView.sampleId();
        return new EntityView(
                diaId,
                sampleId,
                hybridPrimaryUpgraded,
                mergedRelations,
                "hybrid",
                Instant.now(),
                ExtractionStatus.SUCCESS,
                null);
    }

    private static String canonKey(Entity e) {
        return e.type().name() + ":" + Entity.canonicalize(e.name());
    }

    private static Entity withConfidence(Entity e, double confidence) {
        return new Entity(e.id(), e.type(), e.name(), e.surface(), e.attrs(), confidence);
    }

    @Override
    public void close() {
        try {
            if (primary != null) primary.close();
        } catch (RuntimeException e) {
            log.debug("HybridEntityExtractor.close primary runtime failure: {}", e.getMessage());
        } catch (Exception e) {
            log.debug("HybridEntityExtractor.close primary failed: {}", e.getMessage());
        }
        try {
            if (fallback != null) fallback.close();
        } catch (RuntimeException e) {
            log.debug("HybridEntityExtractor.close fallback runtime failure: {}", e.getMessage());
        } catch (Exception e) {
            log.debug("HybridEntityExtractor.close fallback failed: {}", e.getMessage());
        }
    }
}
