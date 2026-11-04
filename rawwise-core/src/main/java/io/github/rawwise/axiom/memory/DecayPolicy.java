package io.github.rawwise.axiom.memory;

public record DecayPolicy(
        double halfLifeMs, double minDecayFactor, int batchSize, long intervalMs) {
    public DecayPolicy {
        if (halfLifeMs <= 0) halfLifeMs = 86_400_000L;
        if (minDecayFactor < 0) minDecayFactor = 0.1;
        if (batchSize <= 0) batchSize = 100;
        if (intervalMs <= 0) intervalMs = 600_000L;
    }

    public static final DecayPolicy DEFAULT = new DecayPolicy(86_400_000L, 0.1, 100, 600_000L);
    public static final DecayPolicy ANNOTATION_DEFAULT =
            new DecayPolicy(604_800_000L, 0.1, 100, 600_000L);

    public double computeDecayFactor(long ageMs, int importance) {
        double importanceBoost = 1.0 + (importance / 10.0);
        double effectiveHalfLife = halfLifeMs * importanceBoost;
        double lambda = Math.log(2) / effectiveHalfLife;
        double factor = Math.exp(-lambda * ageMs);
        return Math.max(factor, minDecayFactor);
    }

    public double computeDecayFactor(long ageMs, double confidence) {
        double importanceBoost = 1.0 + (confidence / 10.0);
        double effectiveHalfLife = halfLifeMs * importanceBoost;
        double lambda = Math.log(2) / effectiveHalfLife;
        double factor = Math.exp(-lambda * ageMs);
        return Math.max(factor, minDecayFactor);
    }
}
