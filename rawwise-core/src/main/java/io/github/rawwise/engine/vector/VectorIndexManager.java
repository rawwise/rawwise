package io.github.rawwise.engine.vector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VectorIndexManager {

    static final int SMALL_SHARD_THRESHOLD = 10_000;
    static final int LARGE_SHARD_THRESHOLD = 100_000;
    static final int SMALL_GRAPH_DEGREE = 32;
    static final int LARGE_GRAPH_DEGREE = 48;
    static final int DEFAULT_EF_SEARCH = 50;
    static final int LARGE_EF_SEARCH = 100;

    private final ConcurrentHashMap<String, ManagedIndex> indices = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, StatsAccumulator> searchStats =
            new ConcurrentHashMap<>();
    private final IvfPqConfig defaultIvfPqConfig;
    private final JVectorConfig defaultJVectorConfig;
    private final boolean tableFormatMode;
    private final Path basePath;
    private final boolean adaptiveRoutingEnabled;
    private final LmdbIoBackend sharedLmdbIoBackend;

    public VectorIndexManager(
            IvfPqConfig defaultIvfPqConfig, boolean tableFormatMode, Path basePath) {
        this(defaultIvfPqConfig, null, tableFormatMode, basePath, true);
    }

    public VectorIndexManager(
            IvfPqConfig defaultIvfPqConfig,
            JVectorConfig defaultJVectorConfig,
            boolean tableFormatMode,
            Path basePath) {
        this(defaultIvfPqConfig, defaultJVectorConfig, tableFormatMode, basePath, true);
    }

    public VectorIndexManager(
            IvfPqConfig defaultIvfPqConfig,
            JVectorConfig defaultJVectorConfig,
            boolean tableFormatMode,
            Path basePath,
            boolean adaptiveRoutingEnabled) {
        this(
                defaultIvfPqConfig,
                defaultJVectorConfig,
                tableFormatMode,
                basePath,
                adaptiveRoutingEnabled,
                null);
    }

    public VectorIndexManager(
            IvfPqConfig defaultIvfPqConfig,
            JVectorConfig defaultJVectorConfig,
            boolean tableFormatMode,
            Path basePath,
            boolean adaptiveRoutingEnabled,
            LmdbIoBackend sharedLmdbIoBackend) {
        this.defaultIvfPqConfig = defaultIvfPqConfig;
        this.defaultJVectorConfig = defaultJVectorConfig;
        this.tableFormatMode = tableFormatMode;
        this.basePath = basePath;
        this.adaptiveRoutingEnabled = adaptiveRoutingEnabled;
        this.sharedLmdbIoBackend = sharedLmdbIoBackend;
    }

    public ShardInternalIndex createIndex(String tableId, String columnName, int dimension) {
        return createIndex(tableId, columnName, dimension, 0);
    }

    public ShardInternalIndex createIndex(
            String tableId, String columnName, int dimension, long estimatedVectorCount) {
        String key = tableId + "." + columnName;
        ShardInternalIndex index;
        int graphDegree = SMALL_GRAPH_DEGREE;
        int efSearch = DEFAULT_EF_SEARCH;
        if (tableFormatMode) {
            IvfPqConfig config =
                    new IvfPqConfig(
                            dimension,
                            defaultIvfPqConfig.nlist(),
                            defaultIvfPqConfig.m(),
                            defaultIvfPqConfig.nprobe(),
                            defaultIvfPqConfig.retrainThreshold(),
                            defaultIvfPqConfig.kmeansMaxIter(),
                            defaultIvfPqConfig.mergeThreshold(),
                            defaultIvfPqConfig.seed(),
                            defaultIvfPqConfig.restarts());
            index = new IvfPqIndex(config);
        } else {
            if (adaptiveRoutingEnabled) {
                graphDegree = targetGraphDegree(estimatedVectorCount);
                efSearch = targetEfSearch(estimatedVectorCount);
            } else if (defaultJVectorConfig != null) {
                graphDegree = defaultJVectorConfig.graphDegree();
                efSearch = defaultJVectorConfig.efSearch();
            }
            JVectorConfig config = buildJVectorConfig(dimension, graphDegree, efSearch);
            Path indexPath = basePath.resolve("vector-index").resolve(key);
            if (sharedLmdbIoBackend != null) {
                index = new JVectorDiskIndex(config, indexPath, null, sharedLmdbIoBackend);
            } else {
                index = new JVectorDiskIndex(config, indexPath);
            }
        }
        indices.put(key, new ManagedIndex(index, dimension, graphDegree));
        searchStats.put(key, new StatsAccumulator(dimension));
        log.info(
                "Created {} index for table={}, column={}, dimension={}, graphDegree={}, efSearch={}, adaptive={}, lmdbIo={}",
                tableFormatMode ? "IvfPq" : "JVectorDisk",
                tableId,
                columnName,
                dimension,
                graphDegree,
                efSearch,
                adaptiveRoutingEnabled,
                sharedLmdbIoBackend != null);
        return index;
    }

    private JVectorConfig buildJVectorConfig(int dimension, int graphDegree, int efSearch) {
        if (defaultJVectorConfig != null) {
            return new JVectorConfig(
                    defaultJVectorConfig.mode(),
                    dimension,
                    graphDegree,
                    defaultJVectorConfig.searchTopK(),
                    efSearch,
                    defaultJVectorConfig.basePath(),
                    defaultJVectorConfig.buildThreshold(),
                    defaultJVectorConfig.mergeThreshold(),
                    defaultJVectorConfig.maxVectors());
        }
        return new JVectorConfig(
                "disk",
                dimension,
                graphDegree,
                10,
                efSearch,
                "/tmp/rawwise/vector",
                10000,
                10000,
                500000);
    }

    public VectorIndex createVectorIndex(String tableId, String columnName, int dimension) {
        ShardInternalIndex internal = createIndex(tableId, columnName, dimension);
        return new ShardInternalIndexVectorAdapter(internal, tableId + "." + columnName);
    }

    public void rebuildVectorIndex(String tableId, String columnName, VectorSchemaDiff diff) {
        rebuildIndex(tableId, columnName, List.of(diff));
    }

    public void dropIndex(String tableId, String columnName) {
        String key = tableId + "." + columnName;
        ManagedIndex managed = indices.remove(key);
        searchStats.remove(key);
        if (managed != null) {
            try {
                managed.index.close();
            } catch (Exception e) {
                log.warn("Failed to close index", e);
            }
        }
        log.info("Dropped index for table={}, column={}", tableId, columnName);
    }

    public void rebuildIndex(String tableId, String columnName, List<VectorSchemaDiff> diffs) {
        String key = tableId + "." + columnName;
        ManagedIndex managed = indices.get(key);
        if (managed == null) {
            log.warn(
                    "No index found for table={}, column={}, skipping rebuild",
                    tableId,
                    columnName);
            return;
        }

        for (VectorSchemaDiff diff : diffs) {
            log.info(
                    "Rebuilding index for table={}, column={}, reason={}",
                    tableId,
                    columnName,
                    diff.type());

            switch (diff.type()) {
                case VECTOR_ADDED -> {
                    if (diff instanceof VectorSchemaDiff.VectorAdded va) {
                        try {
                            managed.index.close();
                        } catch (Exception e) {
                            log.warn("Failed to close old index", e);
                        }
                        ShardInternalIndex newIndex =
                                createIndex(tableId, columnName, va.dimension());
                        managed.replace(newIndex, va.dimension());
                    }
                }
                case VECTOR_REMOVED -> dropIndex(tableId, columnName);
                case VECTOR_DIMENSION_CHANGED -> {
                    if (diff instanceof VectorSchemaDiff.DimensionChanged dc) {
                        try {
                            managed.index.close();
                        } catch (Exception e) {
                            log.warn("Failed to close old index", e);
                        }
                        ShardInternalIndex newIndex =
                                createIndex(tableId, columnName, dc.newDimension());
                        managed.replace(newIndex, dc.newDimension());
                    }
                }
                case VECTOR_MODEL_CHANGED -> {
                    try {
                        managed.index.close();
                    } catch (Exception e) {
                        log.warn("Failed to close old index", e);
                    }
                    ShardInternalIndex newIndex =
                            createIndex(tableId, columnName, managed.dimension);
                    managed.replace(newIndex, managed.dimension);
                }
                default -> log.info("No rebuild needed for diff type={}", diff.type());
            }
        }
    }

    public ShardInternalIndex getIndex(String tableId, String columnName) {
        ManagedIndex managed = indices.get(tableId + "." + columnName);
        return managed != null ? managed.index : null;
    }

    /**
     * 彻底移除一个索引：从内存 map 摘除、关闭旧索引并删除磁盘文件。用于就地重建（src==dest）前 清空目标索引，否则新 JVectorDiskIndex
     * 构造时会自动加载磁盘旧索引导致 initialize 失败。
     */
    public void removeIndex(String tableId, String columnName) {
        dropIndex(tableId, columnName);
        Path indexPath = basePath.resolve("vector-index").resolve(tableId + "." + columnName);
        try {
            deleteRecursively(indexPath);
        } catch (IOException e) {
            log.warn(
                    "Failed to delete index files during removeIndex table={} column={}",
                    tableId,
                    columnName,
                    e);
        }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) return;
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(
                            p -> {
                                try {
                                    Files.deleteIfExists(p);
                                } catch (IOException e) {
                                    log.warn("Failed to delete index file {}", p, e);
                                }
                            });
        }
    }

    public boolean isTableFormatMode() {
        return tableFormatMode;
    }

    public void recordSearch(String tableId, String columnName, int resultCount) {
        String key = tableId + "." + columnName;
        searchStats.compute(
                key,
                (k, acc) -> {
                    StatsAccumulator s = acc;
                    if (s == null) {
                        ManagedIndex managed = indices.get(key);
                        int dim = managed != null ? managed.dimension : 0;
                        s = new StatsAccumulator(dim);
                    }
                    s.searchCount++;
                    s.totalResultsReturned += Math.max(0, resultCount);
                    s.lastSearchedMs = System.currentTimeMillis();
                    return s;
                });
    }

    public IndexStats getStats(String tableId, String columnName) {
        String key = tableId + "." + columnName;
        StatsAccumulator s = searchStats.get(key);
        if (s == null) {
            ManagedIndex managed = indices.get(key);
            int dim = managed != null ? managed.dimension : 0;
            return new IndexStats(tableId, columnName, dim, 0L, 0L, 0L);
        }
        return new IndexStats(
                tableId,
                columnName,
                s.dimension,
                s.searchCount,
                s.totalResultsReturned,
                s.lastSearchedMs);
    }

    public List<IndexStats> getAllStats() {
        List<IndexStats> result = new ArrayList<>();
        for (String key : indices.keySet()) {
            int dot = key.indexOf('.');
            String tableId = dot > 0 ? key.substring(0, dot) : key;
            String col = dot > 0 ? key.substring(dot + 1) : "";
            result.add(getStats(tableId, col));
        }
        return result;
    }

    public int targetGraphDegree(long vectorCount) {
        if (vectorCount < SMALL_SHARD_THRESHOLD) return SMALL_GRAPH_DEGREE;
        return LARGE_GRAPH_DEGREE;
    }

    public int targetEfSearch(long vectorCount) {
        if (vectorCount >= LARGE_SHARD_THRESHOLD) return LARGE_EF_SEARCH;
        return DEFAULT_EF_SEARCH;
    }

    public boolean shouldRebuildIndex(long currentCount, int currentGraphDegree) {
        if (!adaptiveRoutingEnabled) return false;
        return targetGraphDegree(currentCount) != currentGraphDegree;
    }

    public boolean isAdaptiveRoutingEnabled() {
        return adaptiveRoutingEnabled;
    }

    public int getCurrentGraphDegree(String tableId, String columnName) {
        ManagedIndex managed = indices.get(tableId + "." + columnName);
        return managed != null ? managed.graphDegree : 0;
    }

    public void rebuildIndexWithData(
            String tableId,
            String columnName,
            int newDimension,
            List<float[]> vectors,
            List<String> keys) {
        String key = tableId + "." + columnName;
        ManagedIndex managed = indices.get(key);
        ShardInternalIndex oldIndex = managed != null ? managed.index : null;

        int graphDegree = managed != null ? managed.graphDegree : SMALL_GRAPH_DEGREE;
        int efSearch = adaptiveRoutingEnabled ? targetEfSearch(vectors.size()) : DEFAULT_EF_SEARCH;
        if (managed != null && managed.dimension == newDimension) {
            graphDegree = adaptiveRoutingEnabled ? targetGraphDegree(vectors.size()) : graphDegree;
        }

        JVectorConfig config = buildJVectorConfig(newDimension, graphDegree, efSearch);
        Path indexPath = basePath.resolve("vector-index").resolve(key);

        // 就地重建（同目录）：先关闭旧索引并清除磁盘文件，否则新 JVectorDiskIndex 构造时会自动
        // 加载磁盘旧索引导致 initialize 失败
        if (oldIndex != null && oldIndex.isInitialized()) {
            try {
                oldIndex.close();
            } catch (Exception e) {
                log.warn("Failed to close old index before rebuild", e);
            }
            try {
                deleteRecursively(indexPath);
            } catch (IOException e) {
                log.warn("Failed to delete stale index files before rebuild", e);
            }
        }

        ShardInternalIndex newIndex =
                sharedLmdbIoBackend != null
                        ? new JVectorDiskIndex(config, indexPath, null, sharedLmdbIoBackend)
                        : new JVectorDiskIndex(config, indexPath);

        if (!vectors.isEmpty() && !keys.isEmpty()) {
            newIndex.initialize(vectors, keys);
        }

        if (managed != null) {
            managed.replace(newIndex, newDimension, graphDegree);
        } else {
            indices.put(key, new ManagedIndex(newIndex, newDimension, graphDegree));
        }
        log.info(
                "Rebuilt index with data: table={}, column={}, dimension={}, graphDegree={}, efSearch={}, vectors={}",
                tableId,
                columnName,
                newDimension,
                graphDegree,
                efSearch,
                vectors.size());
    }

    public boolean rebuildWithAdaptiveParams(String tableId, String columnName) {
        if (!adaptiveRoutingEnabled) return false;
        String key = tableId + "." + columnName;
        ManagedIndex managed = indices.get(key);
        if (managed == null) {
            log.debug(
                    "rebuildWithAdaptiveParams: no index for table={} column={}",
                    tableId,
                    columnName);
            return false;
        }
        long currentCount = managed.index.vectorCount();
        if (!shouldRebuildIndex(currentCount, managed.graphDegree)) {
            return false;
        }

        int targetDegree = targetGraphDegree(currentCount);
        int targetEf = targetEfSearch(currentCount);
        log.info(
                "rebuildWithAdaptiveParams: table={} count={} graphDegree {}->{} efSearch {}->{}",
                tableId,
                currentCount,
                managed.graphDegree,
                targetDegree,
                DEFAULT_EF_SEARCH,
                targetEf);

        Set<String> entryKeys = managed.index.entryKeys();
        List<float[]> vectors = new ArrayList<>(entryKeys.size());
        List<String> keys = new ArrayList<>(entryKeys.size());
        for (String entryKey : entryKeys) {
            float[] vec = managed.index.getVector(entryKey);
            if (vec != null) {
                vectors.add(vec);
                keys.add(entryKey);
            }
        }

        JVectorConfig config = buildJVectorConfig(managed.dimension, targetDegree, targetEf);
        Path indexPath = basePath.resolve("vector-index").resolve(key);
        ShardInternalIndex newIndex =
                sharedLmdbIoBackend != null
                        ? new JVectorDiskIndex(config, indexPath, null, sharedLmdbIoBackend)
                        : new JVectorDiskIndex(config, indexPath);
        if (!vectors.isEmpty()) {
            newIndex.initialize(vectors, keys);
        }

        ShardInternalIndex oldIndex = managed.index;
        managed.replace(newIndex, managed.dimension, targetDegree);
        try {
            oldIndex.close();
        } catch (Exception e) {
            log.warn("Failed to close old index during adaptive rebuild", e);
        }

        log.info(
                "rebuildWithAdaptiveParams complete: table={} count={} newGraphDegree={}",
                tableId,
                vectors.size(),
                targetDegree);
        return true;
    }

    public record IndexStats(
            String tableId,
            String columnName,
            int dimension,
            long searchCount,
            long totalResultsReturned,
            long lastSearchedMs) {}

    private static final class StatsAccumulator {
        volatile int dimension;
        volatile long searchCount;
        volatile long totalResultsReturned;
        volatile long lastSearchedMs;

        StatsAccumulator(int dimension) {
            this.dimension = dimension;
        }
    }

    public void close() {
        for (ManagedIndex managed : indices.values()) {
            try {
                managed.index.close();
            } catch (Exception e) {
                log.warn("Failed to close index", e);
            }
        }
        indices.clear();
    }

    private static class ManagedIndex {
        volatile ShardInternalIndex index;
        volatile int dimension;
        volatile int graphDegree;

        ManagedIndex(ShardInternalIndex index, int dimension) {
            this(index, dimension, SMALL_GRAPH_DEGREE);
        }

        ManagedIndex(ShardInternalIndex index, int dimension, int graphDegree) {
            this.index = index;
            this.dimension = dimension;
            this.graphDegree = graphDegree;
        }

        synchronized void replace(ShardInternalIndex newIndex, int newDimension) {
            this.index = newIndex;
            this.dimension = newDimension;
        }

        synchronized void replace(
                ShardInternalIndex newIndex, int newDimension, int newGraphDegree) {
            this.index = newIndex;
            this.dimension = newDimension;
            this.graphDegree = newGraphDegree;
        }
    }
}
