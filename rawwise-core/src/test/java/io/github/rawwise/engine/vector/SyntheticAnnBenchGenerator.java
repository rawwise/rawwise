package io.github.rawwise.engine.vector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

/**
 * Generates a small synthetic dataset in RawWise binary format (same format as ann-benchmarks
 * converted data) for smoke-testing {@link AnnBenchDatasetLoader} and {@link
 * AnnBenchStandardBenchmarkTest} without downloading real datasets.
 *
 * <p>This is NOT a replacement for real ann-benchmarks data. It only validates the plumbing (file
 * format parsing, recall computation, latency measurement). Real recall numbers require real
 * datasets from {@code bin/benchmark/download_annbench.py}.
 *
 * <p>The generated data has cluster structure (K Gaussian centers) so recall is meaningful (unlike
 * uniform random which gives ~0 recall for ANN).
 */
public final class SyntheticAnnBenchGenerator {

    private static final ObjectMapper MAPPER =
            new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    public record GenConfig(
            String datasetName,
            int dim,
            int trainCount,
            int queryCount,
            int truthK,
            int clusterCount,
            long seed) {
        public static GenConfig small() {
            return new GenConfig("synthetic-smoke", 128, 10_000, 200, 100, 50, 42L);
        }
    }

    private SyntheticAnnBenchGenerator() {}

    public static void generate(Path outDir, GenConfig cfg) throws IOException {
        Files.createDirectories(outDir);
        Random rng = new Random(cfg.seed());

        int dim = cfg.dim();
        int trainN = cfg.trainCount();
        int queryN = cfg.queryCount();
        int truthK = cfg.truthK();

        float[][] centers = new float[cfg.clusterCount()][dim];
        for (int c = 0; c < cfg.clusterCount(); c++) {
            for (int d = 0; d < dim; d++) {
                centers[c][d] = (float) rng.nextGaussian();
            }
            normalize(centers[c]);
        }

        float[][] train = new float[trainN][dim];
        for (int i = 0; i < trainN; i++) {
            int c = rng.nextInt(cfg.clusterCount());
            for (int d = 0; d < dim; d++) {
                train[i][d] = centers[c][d] + (float) (rng.nextGaussian() * 0.1);
            }
            normalize(train[i]);
        }

        float[][] queries = new float[queryN][dim];
        for (int i = 0; i < queryN; i++) {
            int c = rng.nextInt(cfg.clusterCount());
            for (int d = 0; d < dim; d++) {
                queries[i][d] = centers[c][d] + (float) (rng.nextGaussian() * 0.1);
            }
            normalize(queries[i]);
        }

        int[][] truth = new int[queryN][truthK];
        for (int q = 0; q < queryN; q++) {
            final float[] query = queries[q];
            Integer[] indices = new Integer[trainN];
            double[] dists = new double[trainN];
            for (int i = 0; i < trainN; i++) {
                indices[i] = i;
                double sum = 0;
                for (int d = 0; d < dim; d++) {
                    double diff = query[d] - train[i][d];
                    sum += diff * diff;
                }
                dists[i] = sum;
            }
            java.util.Arrays.sort(indices, java.util.Comparator.comparingDouble(a -> dists[a]));
            for (int k = 0; k < truthK && k < trainN; k++) {
                truth[q][k] = indices[k];
            }
        }

        AnnBenchDatasetLoader.DatasetMeta meta =
                new AnnBenchDatasetLoader.DatasetMeta(
                        cfg.datasetName(),
                        dim,
                        trainN,
                        queryN,
                        truthK,
                        "euclidean",
                        "synthetic://smoke-test");
        MAPPER.writeValue(outDir.resolve("meta.json").toFile(), meta);

        writeFloatMatrix(outDir.resolve("train.bin"), train);
        writeFloatMatrix(outDir.resolve("test.bin"), queries);
        writeIntMatrix(outDir.resolve("truth.bin"), truth);

        System.out.println(
                "[generated] "
                        + cfg.datasetName()
                        + " at "
                        + outDir
                        + " (train="
                        + trainN
                        + "x"
                        + dim
                        + ", query="
                        + queryN
                        + ", truthK="
                        + truthK
                        + ", clusters="
                        + cfg.clusterCount()
                        + ")");
    }

    private static void normalize(float[] v) {
        double sumSq = 0;
        for (float x : v) sumSq += x * x;
        float norm = (float) Math.sqrt(sumSq);
        if (norm > 0) {
            for (int i = 0; i < v.length; i++) v[i] /= norm;
        } else {
            v[0] = 1.0f;
        }
    }

    private static void writeFloatMatrix(Path file, float[][] matrix) throws IOException {
        int rows = matrix.length;
        int cols = matrix[0].length;
        try (FileChannel ch =
                FileChannel.open(
                        file,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocate(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    while (buf.remaining() < Float.BYTES) {
                        buf.flip();
                        ch.write(buf);
                        buf.clear();
                    }
                    buf.putFloat(matrix[r][c]);
                }
            }
            buf.flip();
            if (buf.hasRemaining()) ch.write(buf);
        }
    }

    private static void writeIntMatrix(Path file, int[][] matrix) throws IOException {
        int rows = matrix.length;
        int cols = matrix[0].length;
        try (FileChannel ch =
                FileChannel.open(
                        file,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.allocate(64 * 1024).order(ByteOrder.LITTLE_ENDIAN);
            for (int r = 0; r < rows; r++) {
                for (int c = 0; c < cols; c++) {
                    while (buf.remaining() < Integer.BYTES) {
                        buf.flip();
                        ch.write(buf);
                        buf.clear();
                    }
                    buf.putInt(matrix[r][c]);
                }
            }
            buf.flip();
            if (buf.hasRemaining()) ch.write(buf);
        }
    }
}
