package io.github.rawwise.benchmark.ycsb;

public enum YcsbWorkload {
    A(0.50, 0.50, 0.00),
    B(0.95, 0.05, 0.00),
    C(1.00, 0.00, 0.00),
    D(0.95, 0.00, 0.05),
    E(0.95, 0.00, 0.05);

    private final double readRatio;
    private final double writeRatio;
    private final double scanRatio;

    YcsbWorkload(double readRatio, double writeRatio, double scanRatio) {
        this.readRatio = readRatio;
        this.writeRatio = writeRatio;
        this.scanRatio = scanRatio;
    }

    public double readRatio() {
        return readRatio;
    }

    public double writeRatio() {
        return writeRatio;
    }

    public double scanRatio() {
        return scanRatio;
    }

    public double insertRatio() {
        return 1.0 - readRatio - writeRatio - scanRatio;
    }
}
