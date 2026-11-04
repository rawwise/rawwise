package io.github.rawwise.backend.server.load;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class BackendLoadInfo {
    private int queueLength;
    private double cpuUsage;
    private double memoryUsage;
    private double diskUsage;
    private double networkUsage;
    private int sparkActiveTasks;
    private int sparkPendingTasks;
    private double s3GetQps;
    private double s3FirstByteLatencyP99;
    private double s3ErrorRate;
    private double nvmeCacheHitRatio;
    private double lmdbPageFaultRate;
    private boolean hdfsAvailable = true;
    private boolean metastoreAvailable = true;
    private boolean embeddingProviderAvailable = true;
    private long lastUpdated;

    public double calculateLoadScore(
            double queueWeight,
            double cpuWeight,
            double memoryWeight,
            double diskWeight,
            double networkWeight,
            double sparkActiveWeight,
            double sparkPendingWeight) {
        double normalizedQueue = Math.min(queueLength / 100.0, 1.0);
        double normalizedCpu = Math.min(cpuUsage, 1.0);
        double normalizedMemory = Math.min(memoryUsage, 1.0);
        double normalizedDisk = Math.min(diskUsage, 1.0);
        double normalizedNetwork = Math.min(networkUsage, 1.0);
        double normalizedSparkActive = Math.min(sparkActiveTasks / 100.0, 1.0);
        double normalizedSparkPending = Math.min(sparkPendingTasks / 100.0, 1.0);
        double baseScore =
                normalizedQueue * queueWeight
                        + normalizedCpu * cpuWeight
                        + normalizedMemory * memoryWeight
                        + normalizedDisk * diskWeight
                        + normalizedNetwork * networkWeight
                        + normalizedSparkActive * sparkActiveWeight
                        + normalizedSparkPending * sparkPendingWeight;
        double downstreamPenalty = 0.0;
        if (!hdfsAvailable) downstreamPenalty += 0.3;
        if (!metastoreAvailable) downstreamPenalty += 0.3;
        if (!embeddingProviderAvailable) downstreamPenalty += 0.1;
        return Math.min(baseScore + downstreamPenalty, 1.0);
    }

    public void update(
            int queueLength,
            double cpuUsage,
            double memoryUsage,
            double diskUsage,
            double networkUsage) {
        this.queueLength = queueLength;
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
        this.diskUsage = diskUsage;
        this.networkUsage = networkUsage;
        this.lastUpdated = System.currentTimeMillis();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int queueLength;
        private double cpuUsage;
        private double memoryUsage;
        private double diskUsage;
        private double networkUsage;
        private int sparkActiveTasks;
        private int sparkPendingTasks;
        private double s3GetQps;
        private double s3FirstByteLatencyP99;
        private double s3ErrorRate;
        private double nvmeCacheHitRatio;
        private double lmdbPageFaultRate;
        private boolean hdfsAvailable = true;
        private boolean metastoreAvailable = true;
        private boolean embeddingProviderAvailable = true;

        public Builder queueLength(int queueLength) {
            this.queueLength = queueLength;
            return this;
        }

        public Builder cpuUsage(double cpuUsage) {
            this.cpuUsage = cpuUsage;
            return this;
        }

        public Builder memoryUsage(double memoryUsage) {
            this.memoryUsage = memoryUsage;
            return this;
        }

        public Builder diskUsage(double diskUsage) {
            this.diskUsage = diskUsage;
            return this;
        }

        public Builder networkUsage(double networkUsage) {
            this.networkUsage = networkUsage;
            return this;
        }

        public Builder sparkActiveTasks(int sparkActiveTasks) {
            this.sparkActiveTasks = sparkActiveTasks;
            return this;
        }

        public Builder sparkPendingTasks(int sparkPendingTasks) {
            this.sparkPendingTasks = sparkPendingTasks;
            return this;
        }

        public Builder s3GetQps(double s3GetQps) {
            this.s3GetQps = s3GetQps;
            return this;
        }

        public Builder s3FirstByteLatencyP99(double s3FirstByteLatencyP99) {
            this.s3FirstByteLatencyP99 = s3FirstByteLatencyP99;
            return this;
        }

        public Builder s3ErrorRate(double s3ErrorRate) {
            this.s3ErrorRate = s3ErrorRate;
            return this;
        }

        public Builder nvmeCacheHitRatio(double nvmeCacheHitRatio) {
            this.nvmeCacheHitRatio = nvmeCacheHitRatio;
            return this;
        }

        public Builder lmdbPageFaultRate(double lmdbPageFaultRate) {
            this.lmdbPageFaultRate = lmdbPageFaultRate;
            return this;
        }

        public Builder hdfsAvailable(boolean hdfsAvailable) {
            this.hdfsAvailable = hdfsAvailable;
            return this;
        }

        public Builder metastoreAvailable(boolean metastoreAvailable) {
            this.metastoreAvailable = metastoreAvailable;
            return this;
        }

        public Builder embeddingProviderAvailable(boolean embeddingProviderAvailable) {
            this.embeddingProviderAvailable = embeddingProviderAvailable;
            return this;
        }

        public BackendLoadInfo build() {
            BackendLoadInfo info = new BackendLoadInfo();
            info.queueLength = this.queueLength;
            info.cpuUsage = this.cpuUsage;
            info.memoryUsage = this.memoryUsage;
            info.diskUsage = this.diskUsage;
            info.networkUsage = this.networkUsage;
            info.sparkActiveTasks = this.sparkActiveTasks;
            info.sparkPendingTasks = this.sparkPendingTasks;
            info.s3GetQps = this.s3GetQps;
            info.s3FirstByteLatencyP99 = this.s3FirstByteLatencyP99;
            info.s3ErrorRate = this.s3ErrorRate;
            info.nvmeCacheHitRatio = this.nvmeCacheHitRatio;
            info.lmdbPageFaultRate = this.lmdbPageFaultRate;
            info.hdfsAvailable = this.hdfsAvailable;
            info.metastoreAvailable = this.metastoreAvailable;
            info.embeddingProviderAvailable = this.embeddingProviderAvailable;
            info.lastUpdated = System.currentTimeMillis();
            return info;
        }
    }
}
