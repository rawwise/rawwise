package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LmdbReadonlyStateTransitionTest {

    @TempDir Path tempDir;

    private LmdbInstance instance;
    private Path instanceDir;

    @BeforeEach
    void setUp() throws IOException {
        instanceDir = tempDir.resolve("readonly-test");
        Files.createDirectories(instanceDir);
        long mapSize = 16L * 1024 * 1024;
        String instanceId = "inst-ro-" + System.nanoTime();
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
    void newInstanceIsWritable() {
        assertThat(instance.isReadOnly()).isFalse();
        assertThat(instance.isMarkedForDeletion()).isFalse();
    }

    @Test
    void markAsReadOnlyChangesState() {
        assertThat(instance.isReadOnly()).isFalse();
        instance.markAsReadOnly();
        assertThat(instance.isReadOnly()).isTrue();
    }

    @Test
    void readOnlyInstanceRejectsWrites() {
        instance.markAsReadOnly();
        byte[] key = "test-key".getBytes();
        byte[] value = "test-value".getBytes();
        LmdbInstance.PutStatus status = instance.put(key, value);
        assertThat(status).isEqualTo(LmdbInstance.PutStatus.READ_ONLY);
    }

    @Test
    void readOnlyInstanceAllowsReads() {
        byte[] key = "read-test-key".getBytes();
        byte[] value = "read-test-value".getBytes();
        instance.put(key, value);

        instance.markAsReadOnly();

        byte[] readValue = instance.get(key);
        assertThat(readValue).isNotNull();
        assertThat(new String(readValue)).isEqualTo("read-test-value");
    }

    @Test
    void isApproachingFullReturnsTrueForReadOnlyInstance() {
        assertThat(instance.isApproachingFull(0.5)).isFalse();
        instance.markAsReadOnly();
        assertThat(instance.isApproachingFull(0.5)).isTrue();
        assertThat(instance.isApproachingFull(0.01)).isTrue();
        assertThat(instance.isApproachingFull()).isTrue();
    }

    @Test
    void usageRatioIsCachedAfterReadonlyTransition() {
        double ratioBefore = instance.refreshUsageRatio();
        instance.markAsReadOnly();
        double ratioAfter = instance.getUsageRatio();
        assertThat(ratioAfter).isEqualTo(ratioBefore);
    }

    @Test
    void stateTransitionsFollowCorrectOrder() {
        assertThat(instance.isReadOnly()).isFalse();
        assertThat(instance.isMarkedForDeletion()).isFalse();

        instance.markAsReadOnly();
        assertThat(instance.isReadOnly()).isTrue();
        assertThat(instance.isMarkedForDeletion()).isFalse();

        instance.markForDeletion();
        assertThat(instance.isMarkedForDeletion()).isTrue();
    }

    @Test
    void readonlyInstanceCanStillBeClosed() {
        instance.markAsReadOnly();
        assertThat(instance.isReadOnly()).isTrue();
        instance.close();
    }

    @Test
    void usageRatioReflectsRealPageUsageNotRawBytes() {
        byte[] value = new byte[100];
        java.util.Arrays.fill(value, (byte) 1);
        int recordCount = 0;
        for (int i = 0; i < 100; i++) {
            byte[] key = String.format("k%08d", i).getBytes();
            LmdbInstance.PutStatus status = instance.put(key, value);
            if (status == LmdbInstance.PutStatus.MAP_FULL) break;
            recordCount++;
        }

        double ratio = instance.refreshUsageRatio();
        long rawBytes = (long) recordCount * (8 + 100);
        double rawRatio = (double) rawBytes / instance.maxBytes();
        assertThat(ratio).isGreaterThan(rawRatio);

        instance.markAsReadOnly();
        assertThat(instance.isApproachingFull(0.3)).isTrue();
    }

    @Test
    void proactiveSealBeforeMapFull() {
        byte[] value = new byte[1024];
        java.util.Arrays.fill(value, (byte) 1);
        int writeCount = 0;
        boolean sealed = false;
        for (int i = 0; i < 10000; i++) {
            byte[] key = ("key-" + writeCount).getBytes();
            LmdbInstance.PutStatus status = instance.put(key, value);
            if (status == LmdbInstance.PutStatus.MAP_FULL) {
                break;
            }
            writeCount++;

            double ratio = instance.refreshUsageRatio();
            if (ratio > 0.5) {
                instance.markAsReadOnly();
                sealed = true;
                break;
            }
        }

        assertThat(sealed).isTrue();
        LmdbInstance.PutStatus status = instance.put("after-seal".getBytes(), new byte[10]);
        assertThat(status).isEqualTo(LmdbInstance.PutStatus.READ_ONLY);
    }
}
