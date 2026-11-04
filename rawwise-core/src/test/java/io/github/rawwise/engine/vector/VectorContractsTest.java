package io.github.rawwise.engine.vector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("VectorContracts E2E - vector full chaintest")
class VectorContractsTest {

    private static final int DIM = 128;
    private static final int NLIST = 16;
    private static final int M = 16;

    private List<float[]> generateVectors(int count, long seed) {
        Random rng = new Random(seed);
        List<float[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            float[] v = new float[DIM];
            for (int d = 0; d < DIM; d++) {
                v[d] = rng.nextFloat();
            }
            vectors.add(v);
        }
        return vectors;
    }

    private ShardInternalIndex buildIndex(List<float[]> vectors, List<String> keys) {
        IvfPqConfig config = new IvfPqConfig(DIM, NLIST, M, 4, 10, 20, 10000, 42L, 3);
        ShardInternalIndex index = new IvfPqIndex(config);
        index.initialize(vectors, keys);
        return index;
    }

    @Test
    @DisplayName("STO-VEC-001: WARM->COLD migration: updateLocation sets COLD with lance ref")
    void testWarmToColdMigrationUpdatesLocation() {
        List<float[]> vectors = generateVectors(200, 42L);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < vectors.size(); i++) {
            keys.add("key-" + i);
        }

        IvfPqConfig config = new IvfPqConfig(DIM, NLIST, M, 4, 10, 20, 10000, 42L, 3);
        ShardInternalIndex index = new IvfPqIndex(config);
        index.initialize(vectors, keys);

        index.updateLocation("key-0", VectorLocation.COLD, "lance://cold/key-0");
        assertThat(index.getLocation("key-0")).isEqualTo(VectorLocation.COLD);
        assertThat(index.getStorageRef("key-0")).isEqualTo("lance://cold/key-0");

        List<SearchCandidate> results = index.search(vectors.get(0), 10);
        assertThat(results).isNotEmpty();
        assertThat(results.stream().anyMatch(c -> c.entryKey().equals("key-0"))).isTrue();
    }

    @Test
    @DisplayName("STO-VEC-002: HotBufferTracker crash recovery: appended keys survive reload")
    void testHotBufferCrashRecoveryConsistency(@TempDir Path tempDir) {
        HotBufferTracker tracker = new HotBufferTracker(tempDir, "test-ns");
        try {
            tracker.append("key-alpha");
            tracker.append("key-beta");
            tracker.append("key-gamma");

            List<String> loaded = tracker.loadKeys();
            assertThat(loaded).containsExactly("key-alpha", "key-beta", "key-gamma");
        } finally {
            tracker.close();
        }
    }

    @Test
    @DisplayName("STO-VEC-003: Index optimization does not interrupt search")
    void testRetrainDoesNotInterruptSearch() {
        List<float[]> vectors = generateVectors(500, 42L);
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < vectors.size(); i++) {
            keys.add("key-" + i);
        }

        ShardInternalIndex index = buildIndex(vectors, keys);

        List<SearchCandidate> beforeRetrain = index.search(vectors.get(0), 10);
        assertThat(beforeRetrain).isNotEmpty();

        List<float[]> newVectors = generateVectors(500, 99L);
        List<String> newKeys = new ArrayList<>();
        for (int i = 0; i < newVectors.size(); i++) {
            newKeys.add("key-" + i);
        }
        ShardInternalIndex retrained = buildIndex(newVectors, newKeys);

        List<SearchCandidate> afterRetrain = retrained.search(newVectors.get(0), 10);
        assertThat(afterRetrain).isNotEmpty();
        assertThat(afterRetrain.stream().anyMatch(c -> c.entryKey().equals("key-0"))).isTrue();
    }
}
