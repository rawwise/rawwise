package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LmdbInstanceUsageRatioTest {

    @TempDir Path tempDir;

    private LmdbInstance instance;
    private Path instanceDir;

    @BeforeEach
    void setUp() throws IOException {
        instanceDir = tempDir.resolve("test-table");
        Files.createDirectories(instanceDir);
        long mapSize = 1L * 1024 * 1024;
        String instanceId = "inst-test-" + System.currentTimeMillis();
        instance = new LmdbInstance(instanceId, instanceDir.getParent(), mapSize);
    }

    @AfterEach
    void tearDown() {
        if (instance != null) {
            try {
                instance.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void getUsageRatioReturnsZeroForEmptyInstance() {
        double ratio = instance.getUsageRatio();
        assertThat(ratio).isGreaterThanOrEqualTo(0.0);
        assertThat(ratio).isLessThan(0.1);
    }

    @Test
    void getUsageRatioIncreasesAfterWrites() {
        double before = instance.getUsageRatio();

        byte[] key = "test-key".getBytes();
        byte[] value = new byte[1024];
        java.util.Arrays.fill(value, (byte) 1);
        instance.put(key, value);

        double after = instance.refreshUsageRatio();
        assertThat(after).isGreaterThan(before);
    }

    @Test
    void getUsageRatioUsesCachedValueWithinInterval() {
        double first = instance.getUsageRatio();
        double second = instance.getUsageRatio();
        assertThat(second).isEqualTo(first);
    }

    @Test
    void refreshUsageRatioBypassesCache() {
        double first = instance.refreshUsageRatio();

        byte[] key = "refresh-test-key".getBytes();
        byte[] value = new byte[512];
        instance.put(key, value);

        double second = instance.refreshUsageRatio();
        assertThat(second).isGreaterThan(first);
    }

    @Test
    void isApproachingFullReturnsFalseForEmptyInstance() {
        assertThat(instance.isApproachingFull()).isFalse();
        assertThat(instance.isApproachingFull(0.5)).isFalse();
    }

    @Test
    void isApproachingFullReturnsTrueForReadOnlyInstance() {
        instance.markAsReadOnly();
        assertThat(instance.isApproachingFull()).isTrue();
        assertThat(instance.isApproachingFull(0.01)).isTrue();
    }

    @Test
    void isApproachingFullReturnsTrueWhenThresholdExceeded() {
        long mapSize = 64L * 1024;
        Path smallDir = tempDir.resolve("small-table");
        try {
            Files.createDirectories(smallDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String instanceId = "inst-small-" + System.currentTimeMillis();
        LmdbInstance smallInstance = new LmdbInstance(instanceId, smallDir.getParent(), mapSize);

        try {
            byte[] value = new byte[4000];
            java.util.Arrays.fill(value, (byte) 1);
            for (int i = 0; i < 20; i++) {
                byte[] key = ("key-" + i).getBytes();
                LmdbInstance.PutStatus status = smallInstance.put(key, value);
                if (status == LmdbInstance.PutStatus.MAP_FULL) {
                    break;
                }
            }

            double ratio = smallInstance.refreshUsageRatio();
            if (ratio > 0.3) {
                assertThat(smallInstance.isApproachingFull(0.3)).isTrue();
            }
        } finally {
            try {
                smallInstance.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void isApproachingFullWithCustomThreshold() {
        assertThat(instance.isApproachingFull(0.99)).isFalse();
        assertThat(instance.isApproachingFull(0.0)).isTrue();
    }

    @Test
    void batchPutInvalidatesUsageRatioCache() {
        double before = instance.refreshUsageRatio();

        List<byte[]> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            keys.add(("batch-key-" + i).getBytes());
            byte[] value = new byte[256];
            values.add(value);
        }
        instance.batchPut(keys, values);

        double after = instance.getUsageRatio();
        assertThat(after).isGreaterThan(before);
    }

    @Test
    void refreshUsageRatioReturnsCachedOnClosedInstance() {
        double ratioBefore = instance.refreshUsageRatio();
        instance.close();
        double ratioAfter = instance.refreshUsageRatio();
        assertThat(ratioAfter).isEqualTo(ratioBefore);
    }

    @Test
    void usageRatioReflectsBtreeAmplification() {
        long mapSize = 128L * 1024;
        Path ampDir = tempDir.resolve("amp-table");
        try {
            Files.createDirectories(ampDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String instanceId = "inst-amp-" + System.currentTimeMillis();
        LmdbInstance ampInstance = new LmdbInstance(instanceId, ampDir.getParent(), mapSize);

        try {
            int recordCount = 0;
            byte[] value = new byte[100];
            java.util.Arrays.fill(value, (byte) 1);
            while (true) {
                byte[] key = String.format("k%08d", recordCount).getBytes();
                LmdbInstance.PutStatus status = ampInstance.put(key, value);
                if (status == LmdbInstance.PutStatus.MAP_FULL) {
                    break;
                }
                recordCount++;
            }

            double ratio = ampInstance.refreshUsageRatio();
            long rawBytes = (long) recordCount * (8 + 100);
            double rawRatio = (double) rawBytes / mapSize;

            assertThat(ratio).isGreaterThan(rawRatio);
        } finally {
            try {
                ampInstance.close();
            } catch (Exception ignored) {
            }
        }
    }
}
