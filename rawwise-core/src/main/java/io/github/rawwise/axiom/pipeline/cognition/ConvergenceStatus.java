package io.github.rawwise.axiom.pipeline.cognition;

/** status - descriptionoptimization. */
public record ConvergenceStatus(
        State state,
        int roundsCompleted,
        double averageRegret,
        double confidenceLevel,
        String assessment) {

    public enum State {
        EXPLORING,
        CONVERGING,
        CONVERGED,
        DIVERGED,
        DISABLED
    }

    public static ConvergenceStatus exploring(int rounds, double regret) {
        return new ConvergenceStatus(
                State.EXPLORING, rounds, regret, 0.0, "Still exploring strategy space");
    }

    public static ConvergenceStatus converged(int rounds, double regret) {
        return new ConvergenceStatus(
                State.CONVERGED, rounds, regret, 1.0, "Regret bound satisfied: R_T/T < epsilon");
    }

    public static ConvergenceStatus diverged(int rounds, double regret) {
        return new ConvergenceStatus(
                State.DIVERGED,
                rounds,
                regret,
                0.0,
                "Regret not decreasing after threshold rounds");
    }

    public static ConvergenceStatus disabled() {
        return new ConvergenceStatus(State.DISABLED, 0, 0.0, 0.0, "Optimizer not available");
    }
}
