package io.github.rawwise.axiom.pipeline.feedback.retrieval;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RerankerAbSwitcher {

    private record SwitcherState(
            RerankerModelMetadata currentModel,
            RerankerModelMetadata candidateModel,
            double abTrafficPercent) {}

    private final AtomicReference<SwitcherState> state;

    public RerankerAbSwitcher() {
        this(0.10);
    }

    public RerankerAbSwitcher(double abTrafficPercent) {
        this.state = new AtomicReference<>(new SwitcherState(null, null, clamp(abTrafficPercent)));
    }

    public boolean useNewModel(String requestId) {
        if (requestId == null) return false;
        double pct = state.get().abTrafficPercent();
        if (pct <= 0.0) return false;
        if (pct >= 1.0) return true;
        int bucket = Math.floorMod(stableHash(requestId), 100);
        double targetBucket = pct * 100;
        double switchMu =
                io.github.rawwise.axiom.pipeline.cognition.MembershipFunctions.sigmoid(
                        bucket, targetBucket, 2.0);
        return switchMu < 0.5;
    }

    public RerankerModelMetadata allocateTraffic(String requestId) {
        SwitcherState s = state.get();
        if (requestId == null || s.currentModel() == null) {
            return s.candidateModel() != null ? s.candidateModel() : null;
        }
        if (useNewModel(requestId)) {
            return s.candidateModel() != null ? s.candidateModel() : s.currentModel();
        }
        return s.currentModel();
    }

    public void setCurrentModel(RerankerModelMetadata model) {
        state.updateAndGet(s -> new SwitcherState(model, s.candidateModel(), s.abTrafficPercent()));
        log.info(
                "RerankerAbSwitcher.setCurrentModel: version={}",
                model == null ? "null" : model.version());
    }

    public void setCandidateModel(RerankerModelMetadata model) {
        state.updateAndGet(s -> new SwitcherState(s.currentModel(), model, s.abTrafficPercent()));
        log.info(
                "RerankerAbSwitcher.setCandidateModel: version={}",
                model == null ? "null" : model.version());
    }

    public Optional<RerankerModelMetadata> currentModel() {
        return Optional.ofNullable(state.get().currentModel());
    }

    public Optional<RerankerModelMetadata> candidateModel() {
        return Optional.ofNullable(state.get().candidateModel());
    }

    public void promoteCandidate() {
        state.getAndUpdate(
                s -> {
                    RerankerModelMetadata cand = s.candidateModel();
                    if (cand == null) {
                        log.warn(
                                "RerankerAbSwitcher.promoteCandidate: no candidate model to promote");
                        return s;
                    }
                    log.info(
                            "RerankerAbSwitcher.promoteCandidate: promoted version={} to current, traffic reset to 0",
                            cand.version());
                    return new SwitcherState(cand, null, 0.0);
                });
    }

    public double getAbTrafficPercent() {
        return state.get().abTrafficPercent();
    }

    public void promoteToFull() {
        state.updateAndGet(s -> new SwitcherState(s.currentModel(), s.candidateModel(), 1.0));
        log.info("RerankerAbSwitcher.promoteToFull: trafficPercent=1.0");
    }

    public void rollback() {
        state.updateAndGet(s -> new SwitcherState(s.currentModel(), null, 0.0));
        log.info("RerankerAbSwitcher.rollback: trafficPercent=0.0 candidate cleared");
    }

    public void setAbTrafficPercent(double newPct) {
        state.updateAndGet(
                s -> new SwitcherState(s.currentModel(), s.candidateModel(), clamp(newPct)));
    }

    private static double clamp(double v) {
        if (v < 0.0) return 0.0;
        if (v > 1.0) return 1.0;
        return v;
    }

    private static int stableHash(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        int h = 0;
        for (byte b : bytes) {
            h = 31 * h + (b & 0xff);
        }
        return h;
    }
}
