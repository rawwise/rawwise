package io.github.rawwise.backend.server.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.server.load.BackendLoadInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("LoadScore E2E Test")
class LoadScoreE2ETest {

    private static final double QUEUE_WEIGHT = 0.45;
    private static final double CPU_WEIGHT = 0.15;
    private static final double MEMORY_WEIGHT = 0.15;
    private static final double DISK_WEIGHT = 0.1;
    private static final double NETWORK_WEIGHT = 0.1;
    private static final double SPARK_ACTIVE_WEIGHT = 0.05;
    private static final double SPARK_PENDING_WEIGHT = 0.05;

    private BackendLoadInfo createLoadInfo(
            int queueLength,
            double cpuUsage,
            double memoryUsage,
            double diskUsage,
            double networkUsage,
            int sparkActiveTasks,
            int sparkPendingTasks) {
        return BackendLoadInfo.builder()
                .queueLength(queueLength)
                .cpuUsage(cpuUsage)
                .memoryUsage(memoryUsage)
                .diskUsage(diskUsage)
                .networkUsage(networkUsage)
                .sparkActiveTasks(sparkActiveTasks)
                .sparkPendingTasks(sparkPendingTasks)
                .build();
    }

    @Nested
    @DisplayName("sparkPendingTasks affects load score")
    class SparkPendingTasksAffectsLoadScore {

        @Test
        @DisplayName("higher sparkPendingTasks yields higher load score")
        void sparkPendingTasksAffectsLoadScore() {
            BackendLoadInfo lowPending = createLoadInfo(50, 0.5, 0.5, 0.5, 0.5, 50, 0);
            BackendLoadInfo highPending = createLoadInfo(50, 0.5, 0.5, 0.5, 0.5, 50, 50);

            double lowScore =
                    lowPending.calculateLoadScore(
                            QUEUE_WEIGHT,
                            CPU_WEIGHT,
                            MEMORY_WEIGHT,
                            DISK_WEIGHT,
                            NETWORK_WEIGHT,
                            SPARK_ACTIVE_WEIGHT,
                            SPARK_PENDING_WEIGHT);
            double highScore =
                    highPending.calculateLoadScore(
                            QUEUE_WEIGHT,
                            CPU_WEIGHT,
                            MEMORY_WEIGHT,
                            DISK_WEIGHT,
                            NETWORK_WEIGHT,
                            SPARK_ACTIVE_WEIGHT,
                            SPARK_PENDING_WEIGHT);

            assertThat(highScore).isGreaterThan(lowScore);
        }
    }

    @Nested
    @DisplayName("diskUsage naming consistency")
    class DiskUsageNamingConsistency {

        @Test
        @DisplayName("builder diskUsage maps to getDiskUsage")
        void diskUsageNamingConsistency() {
            BackendLoadInfo info = BackendLoadInfo.builder().diskUsage(0.5).build();

            assertThat(info.getDiskUsage()).isEqualTo(0.5);
        }
    }

    @Nested
    @DisplayName("load score weights sum to one")
    class LoadScoreWeightsSumToOne {

        @Test
        @DisplayName("all metrics at max with weights summing to 1.0 yields score 1.0")
        void loadScoreWeightsSumToOne() {
            BackendLoadInfo maxLoad = createLoadInfo(100, 1.0, 1.0, 1.0, 1.0, 100, 100);

            double queueW = 0.40;
            double cpuW = 0.15;
            double memoryW = 0.15;
            double diskW = 0.10;
            double networkW = 0.10;
            double sparkActiveW = 0.05;
            double sparkPendingW = 0.05;

            double score =
                    maxLoad.calculateLoadScore(
                            queueW, cpuW, memoryW, diskW, networkW, sparkActiveW, sparkPendingW);

            assertThat(score).isEqualTo(1.0);
        }
    }

    @Nested
    @DisplayName("sparkPendingWeight is parameterized")
    class SparkPendingWeightIsParameterized {

        @Test
        @DisplayName("different sparkPendingWeight values produce different scores")
        void sparkPendingWeightIsParameterized() {
            BackendLoadInfo info = createLoadInfo(0, 0.0, 0.0, 0.0, 0.0, 0, 50);

            double scoreWithZeroWeight =
                    info.calculateLoadScore(
                            QUEUE_WEIGHT,
                            CPU_WEIGHT,
                            MEMORY_WEIGHT,
                            DISK_WEIGHT,
                            NETWORK_WEIGHT,
                            SPARK_ACTIVE_WEIGHT,
                            0.0);
            double scoreWithNonZeroWeight =
                    info.calculateLoadScore(
                            QUEUE_WEIGHT,
                            CPU_WEIGHT,
                            MEMORY_WEIGHT,
                            DISK_WEIGHT,
                            NETWORK_WEIGHT,
                            SPARK_ACTIVE_WEIGHT,
                            0.1);

            assertThat(scoreWithNonZeroWeight).isGreaterThan(scoreWithZeroWeight);
        }
    }
}
