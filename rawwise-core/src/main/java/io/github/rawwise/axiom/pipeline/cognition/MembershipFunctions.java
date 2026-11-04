package io.github.rawwise.axiom.pipeline.cognition;

public final class MembershipFunctions {

    private MembershipFunctions() {}

    /**
     * Sigmoid (S-shaped) membership function centered at {@code center} with steepness {@code k}.
     * Returns a value in (0, 1).
     *
     * <p>μ(x) = 1 / (1 + e^(-k*(x - center)))
     *
     * <p>At x = center, μ = 0.5 (transition zone). For k = 2, at x = center + 1, μ ≈ 0.88; at x =
     * center - 1, μ ≈ 0.12.
     *
     * @param x input value
     * @param center inflection point (where μ = 0.5)
     * @param k steepness (higher = sharper transition)
     * @return membership degree in (0, 1)
     */
    public static double sigmoid(double x, double center, double k) {
        return 1.0 / (1.0 + Math.exp(-k * (x - center)));
    }

    /**
     * Triangular membership function with peak at {@code peak}, left base at {@code left}, right
     * base at {@code right}.
     *
     * <p>Returns 0 outside [left, right], 1 at peak, linear in between.
     *
     * @param x input value
     * @param left left base (μ = 0)
     * @param peak peak (μ = 1)
     * @param right right base (μ = 0)
     * @return membership degree in [0, 1]
     */
    public static double triangular(double x, double left, double peak, double right) {
        if (x <= left || x >= right) return 0.0;
        if (Math.abs(x - peak) < 1e-15) return 1.0;
        if (x < peak) return (x - left) / (peak - left);
        return (right - x) / (right - peak);
    }
}
