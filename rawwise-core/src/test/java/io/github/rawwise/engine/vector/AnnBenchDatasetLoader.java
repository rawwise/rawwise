package io.github.rawwise.engine.vector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads ann-benchmarks standard datasets in RawWise binary format (converted by
 * bin/benchmark/download_annbench.py).
 *
 * <p>Format:
 *
 * <ul>
 *   <li>{@code meta.json} - dataset metadata (dim, counts, metric)
 *   <li>{@code train.bin} - N * D * float32 (little-endian)
 *   <li>{@code test.bin} - M * D * float32 (little-endian)
 *   <li>{@code truth.bin} - M * K * int32 (ground-truth indices)
 * </ul>
 *
 * <p>Datasets: SIFT-128-euclidean, GLoVe-25-angular, Fashion-MNIST-784-cosine, MNIST-784-euclidean.
 * Source: <a href="http://ann-benchmarks.com">ann-benchmarks.com</a>.
 */
public final class AnnBenchDatasetLoader {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                    .configure(
                            com.fasterxml.jackson.databind.DeserializationFeature
                                    .FAIL_ON_UNKNOWN_PROPERTIES,
                            false);

    public record DatasetMeta(
            String dataset,
            int dim,
            int trainCount,
            int testCount,
            int truthK,
            String metric,
            String sourceUrl) {}

    public record Dataset(
            DatasetMeta meta, List<float[]> train, List<float[]> test, int[][] groundTruth) {
        public int dimension() {
            return meta.dim();
        }

        public int trainSize() {
            return meta.trainCount();
        }

        public int queryCount() {
            return meta.testCount();
        }

        public int truthK() {
            return meta.truthK();
        }
    }

    private AnnBenchDatasetLoader() {}

    public static Dataset load(Path datasetDir) throws IOException {
        if (!Files.isDirectory(datasetDir)) {
            throw new IOException(
                    "Dataset directory not found: "
                            + datasetDir
                            + ". Run bin/benchmark/download_annbench.py first.");
        }
        Path metaPath = datasetDir.resolve("meta.json");
        if (!Files.exists(metaPath)) {
            throw new IOException(
                    "meta.json not found in "
                            + datasetDir
                            + ". Run bin/benchmark/download_annbench.py first.");
        }
        DatasetMeta meta = MAPPER.readValue(Files.readAllBytes(metaPath), DatasetMeta.class);

        List<float[]> train =
                readFloatMatrix(datasetDir.resolve("train.bin"), meta.trainCount(), meta.dim());
        List<float[]> test =
                readFloatMatrix(datasetDir.resolve("test.bin"), meta.testCount(), meta.dim());
        int[][] truth =
                readIntMatrix(datasetDir.resolve("truth.bin"), meta.testCount(), meta.truthK());

        return new Dataset(meta, train, test, truth);
    }

    public static boolean isAvailable(Path datasetDir) {
        return Files.isDirectory(datasetDir)
                && Files.exists(datasetDir.resolve("meta.json"))
                && Files.exists(datasetDir.resolve("train.bin"))
                && Files.exists(datasetDir.resolve("test.bin"))
                && Files.exists(datasetDir.resolve("truth.bin"));
    }

    /**
     * Compute Recall@K: fraction of ground-truth top-K neighbors found in the approximate top-K
     * results.
     *
     * <p>For angular (cosine) datasets, ann-benchmarks ground truth is computed with cosine
     * similarity. JVector uses cosine similarity when vectors are normalized, so results are
     * directly comparable.
     */
    public static double recallAtK(int[] groundTruthTopK, List<String> resultKeys, int k) {
        int hits = 0;
        int effectiveK = Math.min(k, groundTruthTopK.length);
        java.util.Set<Integer> gtSet = new java.util.HashSet<>();
        for (int i = 0; i < effectiveK; i++) {
            gtSet.add(groundTruthTopK[i]);
        }
        for (String key : resultKeys) {
            int idx = parseTrainIndex(key);
            if (gtSet.contains(idx)) {
                hits++;
            }
        }
        return (double) hits / effectiveK;
    }

    /**
     * Compute Recall@K with separate K values for ground truth and results. Standard ann-benchmarks
     * reports recall@10 with ground-truth@100: recall@10 = |approx_top10 ∩ gt_top100| / 10
     *
     * <p>The denominator is {@code resultK} (number of results returned), not {@code gtK}. This
     * matches the ann-benchmarks convention where recall@10 measures "of the 10 results returned,
     * how many are in the top-100 ground truth".
     *
     * @param groundTruth ground-truth neighbor indices (length >= gtK)
     * @param gtK how many ground-truth entries to consider (e.g. 100)
     * @param resultKeys approximate search result keys (length <= resultK)
     * @param resultK the K in "recall@K" (e.g. 10); denominator
     * @return recall in [0, 1]; 1.0 means all resultK results are in gt top-gtK
     */
    public static double recallAtK(
            int[] groundTruth, int gtK, List<String> resultKeys, int resultK) {
        int effectiveGtK = Math.min(gtK, groundTruth.length);
        java.util.Set<Integer> gtSet = new java.util.HashSet<>();
        for (int i = 0; i < effectiveGtK; i++) {
            gtSet.add(groundTruth[i]);
        }
        int hits = 0;
        int effectiveResultK = Math.min(resultK, resultKeys.size());
        for (int i = 0; i < effectiveResultK && i < resultKeys.size(); i++) {
            int idx = parseTrainIndex(resultKeys.get(i));
            if (gtSet.contains(idx)) {
                hits++;
            }
        }
        return effectiveResultK > 0 ? (double) hits / effectiveResultK : 0.0;
    }

    private static int parseTrainIndex(String key) {
        if (key == null || key.isBlank()) return -1;
        try {
            if (key.startsWith("train-")) {
                return Integer.parseInt(key.substring(6));
            }
            return Integer.parseInt(key);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static List<float[]> readFloatMatrix(Path file, int rows, int cols) throws IOException {
        long expectedBytes = (long) rows * cols * Float.BYTES;
        long actualBytes = Files.size(file);
        if (actualBytes < expectedBytes) {
            throw new IOException(
                    "File "
                            + file
                            + " too small: expected "
                            + expectedBytes
                            + " bytes, got "
                            + actualBytes);
        }
        List<float[]> matrix = new ArrayList<>(rows);
        try (java.io.DataInputStream dis =
                new java.io.DataInputStream(
                        new java.io.BufferedInputStream(Files.newInputStream(file)))) {
            byte[] buf = new byte[Float.BYTES];
            for (int r = 0; r < rows; r++) {
                float[] row = new float[cols];
                for (int c = 0; c < cols; c++) {
                    dis.readFully(buf);
                    int bits =
                            ((buf[3] & 0xFF) << 24)
                                    | ((buf[2] & 0xFF) << 16)
                                    | ((buf[1] & 0xFF) << 8)
                                    | (buf[0] & 0xFF);
                    row[c] = Float.intBitsToFloat(bits);
                }
                matrix.add(row);
            }
        }
        return matrix;
    }

    private static int[][] readIntMatrix(Path file, int rows, int cols) throws IOException {
        long expectedBytes = (long) rows * cols * Integer.BYTES;
        long actualBytes = Files.size(file);
        if (actualBytes < expectedBytes) {
            throw new IOException(
                    "File "
                            + file
                            + " too small: expected "
                            + expectedBytes
                            + " bytes, got "
                            + actualBytes);
        }
        int[][] matrix = new int[rows][cols];
        try (java.io.DataInputStream dis =
                new java.io.DataInputStream(
                        new java.io.BufferedInputStream(Files.newInputStream(file)))) {
            byte[] buf = new byte[Integer.BYTES];
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    dis.readFully(buf);
                    matrix[r][c] =
                            ((buf[3] & 0xFF) << 24)
                                    | ((buf[2] & 0xFF) << 16)
                                    | ((buf[1] & 0xFF) << 8)
                                    | (buf[0] & 0xFF);
                }
            }
        }
        return matrix;
    }
}
