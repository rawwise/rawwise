package io.github.rawwise.engine.vector;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.rawwise.engine.txn.wal.TxnWALWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JVectorVectorIndex implements VectorIndex {

    private static final int DEFAULT_NLIST = 16;
    private static final int CENTROID_MAX_ITER = 20;
    private static final int CENTROID_SAMPLE_SIZE = 1000;

    private final ConcurrentHashMap<String, NamespaceVectorIndex> namespaceIndices =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ShardCentroids> namespaceCentroids =
            new ConcurrentHashMap<>();
    private final JVectorConfig config;
    private volatile IvfPqConfig ivfPqConfig;
    private final EmbeddingProvider embeddingProvider;
    private final Path basePath;
    private final boolean tableFormatMode;
    private volatile VectorColdStore coldStore;
    private volatile boolean closed = false;
    private final Object closeLock = new Object();
    private final VectorMetrics metrics = new VectorMetrics();
    private final AtomicLong snapshotCounter = new AtomicLong(0);
    private volatile Path versionBasePath;
    private volatile TxnWALWriter walWriter;
    private final LmdbIoBackend lmdbIoBackend;

    public JVectorVectorIndex(JVectorConfig config, EmbeddingProvider embeddingProvider) {
        this(config, embeddingProvider, false);
    }

    public JVectorVectorIndex(
            JVectorConfig config, EmbeddingProvider embeddingProvider, boolean tableFormatMode) {
        this.config = config;
        this.embeddingProvider = embeddingProvider;
        this.tableFormatMode = tableFormatMode;
        this.lmdbIoBackend = null;
        try {
            this.basePath = Path.of(config.basePath());
            Files.createDirectories(basePath);
            this.versionBasePath = basePath.resolve("vector-index-version");
            Files.createDirectories(versionBasePath);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to create vector index base path: " + config.basePath(), e);
        }
    }

    public JVectorVectorIndex(
            JVectorConfig config,
            EmbeddingProvider embeddingProvider,
            boolean tableFormatMode,
            LmdbIoBackend lmdbIoBackend) {
        this.config = config;
        this.embeddingProvider = embeddingProvider;
        this.tableFormatMode = tableFormatMode;
        this.lmdbIoBackend = lmdbIoBackend;
        try {
            this.basePath = Path.of(config.basePath());
            Files.createDirectories(basePath);
            this.versionBasePath = basePath.resolve("vector-index-version");
            Files.createDirectories(versionBasePath);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to create vector index base path: " + config.basePath(), e);
        }
    }

    public void setIvfPqConfig(IvfPqConfig ivfPqConfig) {
        this.ivfPqConfig = ivfPqConfig;
    }

    public void setWalWriter(TxnWALWriter walWriter) {
        this.walWriter = walWriter;
    }

    public boolean isTableFormatMode() {
        return tableFormatMode;
    }

    public VectorMetrics getMetrics() {
        return metrics;
    }

    @Override
    public void index(String namespace, String entryKey, String l0Abstract) {
        ensureOpen();
        if (l0Abstract == null || l0Abstract.isBlank()) return;
        float[] embedding = embeddingProvider.embed(l0Abstract);
        indexWithEmbedding(namespace, entryKey, embedding, l0Abstract);
    }

    @Override
    public void indexWithEmbedding(String namespace, String entryKey, float[] embedding) {
        ensureOpen();
        if (embedding == null || embedding.length == 0) return;
        indexWithEmbedding(namespace, entryKey, embedding, "");
    }

    @Override
    public void indexWithEmbedding(
            String namespace, String entryKey, float[] embedding, String l0Abstract) {
        NamespaceVectorIndex idx = getOrCreateNamespaceIndex(namespace);
        idx.writeLock.lock();
        try {
            idx.ensureOpen();
            idx.hotBuffer.add(entryKey, embedding, l0Abstract);
            idx.hotTracker.append(entryKey);
            metrics.recordInsert();
            int mergeThreshold =
                    tableFormatMode ? ivfPqConfig().mergeThreshold() : config.mergeThreshold();
            if (idx.hotBuffer.size() >= mergeThreshold) {
                mergeHotBuffer(idx, namespace);
            }
        } finally {
            idx.writeLock.unlock();
        }
        writeVectorPreparedToWAL(namespace, entryKey, embedding.length);
    }

    private void writeVectorPreparedToWAL(String namespace, String entryKey, int dimension) {
        TxnWALWriter writer = this.walWriter;
        if (writer == null) return;
        try {
            writer.writeVectorPrepared(
                    new io.github.rawwise.engine.txn.wal.TxnWALRecord.TxnVectorPrepared(
                            System.nanoTime(),
                            namespace,
                            entryKey,
                            embeddingProvider != null
                                    ? embeddingProvider.modelIdentifier()
                                    : "unknown",
                            dimension,
                            System.currentTimeMillis()));
        } catch (Exception e) {
            log.debug("Failed to write TXN_VECTOR_PREPARED to WAL: {}", e.getMessage());
        }
    }

    @Override
    public List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK) {
        ensureOpen();
        if (queryEmbedding.length != config.dimension()) {
            throw new IllegalArgumentException(
                    "Query dimension mismatch: expected "
                            + config.dimension()
                            + ", got "
                            + queryEmbedding.length);
        }
        long start = System.nanoTime();
        try {
            NamespaceVectorIndex idx = getOrCreateNamespaceIndex(namespace);
            if (idx == null) return List.of();

            Map<String, VectorSearchResult> merged = new HashMap<>();
            idx.readLock.lock();
            try {
                idx.ensureOpen();
                VectorFloat<?> queryVector =
                        BuildScoreProvider.vts.createFloatVector(queryEmbedding);
                List<VectorSearchResult> hotResults = idx.hotBuffer.search(queryVector, topK);
                for (VectorSearchResult r : hotResults) {
                    merged.put(r.entryKey(), r);
                }

                List<SearchCandidate> mainCandidates =
                        idx.mainIndex.search(queryEmbedding, topK * 2);
                for (SearchCandidate candidate : mainCandidates) {
                    if (merged.containsKey(candidate.entryKey())) continue;
                    float[] originalVector = readOriginalVector(idx, candidate.entryKey());
                    if (originalVector == null) continue;
                    float score = cosineSimilarity(queryEmbedding, originalVector);
                    String l0 = idx.hotBuffer.getL0Abstract(candidate.entryKey());
                    merged.put(
                            candidate.entryKey(),
                            new VectorSearchResult(
                                    candidate.entryKey(), l0 != null ? l0 : "", score));
                }
            } finally {
                idx.readLock.unlock();
            }

            List<VectorSearchResult> mergedResults =
                    merged.values().stream()
                            .sorted(
                                    Comparator.comparingDouble(VectorSearchResult::score)
                                            .reversed())
                            .limit(topK)
                            .toList();
            metrics.recordSearch(System.nanoTime() - start);
            return mergedResults;
        } catch (Exception e) {
            metrics.recordSearchError();
            throw e;
        }
    }

    @Override
    public List<VectorSearchResult> search(
            String namespace, float[] queryEmbedding, int topK, long snapshotId) {
        if (snapshotId <= 0) {
            return search(namespace, queryEmbedding, topK);
        }
        NamespaceVectorIndex idx = getOrCreateNamespaceIndex(namespace);
        if (idx != null) {
            Path versionPath = null;
            synchronized (idx.versionLock) {
                for (NamespaceVectorIndex.VersionIndexEntry entry : idx.versionIndices) {
                    if (entry.snapshotId() <= snapshotId) {
                        versionPath = entry.indexPath();
                        break;
                    }
                }
            }
            if (versionPath != null) {
                try {
                    JVectorDiskIndex versionIndex = new JVectorDiskIndex(config, versionPath);
                    if (versionIndex.isInitialized()) {
                        List<SearchCandidate> candidates =
                                versionIndex.search(queryEmbedding, topK);
                        List<VectorSearchResult> results = new ArrayList<>();
                        for (SearchCandidate candidate : candidates) {
                            float[] v = versionIndex.getVector(candidate.entryKey());
                            if (v != null) {
                                float score = cosineSimilarity(queryEmbedding, v);
                                results.add(
                                        new VectorSearchResult(candidate.entryKey(), "", score));
                            }
                        }
                        results.sort(
                                Comparator.comparingDouble(VectorSearchResult::score).reversed());
                        log.info(
                                "Time travel search: namespace={}, snapshotId={}, results={}",
                                namespace,
                                snapshotId,
                                results.size());
                        return results;
                    }
                } catch (Exception e) {
                    log.warn(
                            "Time travel search failed for snapshotId={}, falling back to current",
                            snapshotId,
                            e);
                }
            }
        }
        return search(namespace, queryEmbedding, topK);
    }

    @Override
    public void delete(String namespace, String entryKey) {
        ensureOpen();
        NamespaceVectorIndex idx = getOrCreateNamespaceIndex(namespace);
        if (idx == null) return;
        idx.writeLock.lock();
        try {
            idx.ensureOpen();
            idx.hotBuffer.remove(entryKey);
            idx.mainIndex.markTombstone(entryKey);
        } finally {
            idx.writeLock.unlock();
        }
        VectorColdStore cs = this.coldStore;
        if (cs != null) cs.delete(namespace, List.of(entryKey));
    }

    @Override
    public void flushToCold(String namespace, Collection<String> entryKeys) {
        ensureOpen();
        VectorColdStore cs = this.coldStore;
        if (cs == null)
            throw new IllegalStateException("coldStore not set, call setColdStore() first");

        NamespaceVectorIndex idx = getOrCreateNamespaceIndex(namespace);
        if (idx == null) return;

        Map<String, ColdVectorEntry> deduped = new LinkedHashMap<>();
        Map<String, ColdVectorEntry> hotExtracted = new LinkedHashMap<>();
        idx.writeLock.lock();
        try {
            idx.ensureOpen();
            for (String key : entryKeys) {
                if (deduped.containsKey(key)) continue;
                ColdVectorEntry hotEntry = idx.hotBuffer.extractEntry(key);
                if (hotEntry != null) {
                    deduped.put(key, hotEntry);
                    hotExtracted.put(key, hotEntry);
                    continue;
                }
                float[] vector = readOriginalVector(idx, key);
                String l0 = idx.hotBuffer.getL0Abstract(key);
                if (vector != null) {
                    deduped.put(key, new ColdVectorEntry(key, vector, l0 != null ? l0 : ""));
                }
            }

            if (deduped.isEmpty()) return;

            String fragmentId = UUID.randomUUID().toString();
            cs.write(namespace, fragmentId, new ArrayList<>(deduped.values()));

            for (String key : hotExtracted.keySet()) {
                idx.hotBuffer.remove(key);
            }
            for (String key : deduped.keySet()) {
                idx.mainIndex.updateLocation(key, VectorLocation.COLD, "lance://" + fragmentId);
            }
        } finally {
            idx.writeLock.unlock();
        }
        metrics.recordFlush(deduped.size());
        log.info(
                "Flushed {} vector entries to cold storage for namespace={}",
                deduped.size(),
                namespace);
    }

    @Override
    public void setColdStore(VectorColdStore coldStore) {
        this.coldStore = coldStore;
    }

    @Override
    public void persist() {
        flushHotBuffersToDisk();
        for (NamespaceVectorIndex idx : namespaceIndices.values()) {
            try {
                if (idx.mainIndex instanceof JVectorDiskIndex jvdi) {
                    jvdi.persist();
                }
            } catch (Exception e) {
                log.warn("Failed to persist namespace index", e);
            }
        }
    }

    private void flushHotBuffersToDisk() {
        for (var entry : namespaceIndices.entrySet()) {
            String namespace = entry.getKey();
            NamespaceVectorIndex idx = entry.getValue();
            try {
                if (idx.hotBuffer.size() > 0) {
                    log.info(
                            "Flushing {} hot buffer entries to disk for namespace={}",
                            idx.hotBuffer.size(),
                            namespace);
                    mergeHotBuffer(idx, namespace);
                }
            } catch (Exception e) {
                log.warn("Failed to flush hot buffer to disk for namespace={}", namespace, e);
            }
        }
    }

    @Override
    public void close() {
        synchronized (closeLock) {
            if (closed) return;
            closed = true;
            flushHotBuffersToDisk();
            for (NamespaceVectorIndex idx : namespaceIndices.values()) {
                try {
                    idx.close();
                } catch (Exception e) {
                    log.warn("Failed to close namespace index", e);
                }
            }
            namespaceIndices.clear();
            if (embeddingProvider instanceof AutoCloseable ac) {
                try {
                    ac.close();
                } catch (Exception e) {
                    log.warn("Failed to close embedding provider", e);
                }
            }
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("JVectorVectorIndex is closed");
    }

    private IvfPqConfig ivfPqConfig() {
        IvfPqConfig cfg = this.ivfPqConfig;
        if (cfg == null) {
            throw new IllegalStateException("IvfPqConfig not set, call setIvfPqConfig() first");
        }
        return cfg;
    }

    private ShardInternalIndex createMainIndex(String namespace) {
        if (tableFormatMode) {
            return new IvfPqIndex(ivfPqConfig());
        } else {
            Path indexDir = basePath.resolve("vector-index").resolve("ns-" + namespace);
            if (lmdbIoBackend != null) {
                return new JVectorDiskIndex(config, indexDir, null, lmdbIoBackend);
            }
            return new JVectorDiskIndex(config, indexDir);
        }
    }

    private void mergeHotBuffer(NamespaceVectorIndex idx, String namespace) {
        // Keys must be aligned with vectors by ordinal — keyEntrySet() (HashMap order)
        // does NOT match allVectors() ordinal order, so derive both from ordinal order.
        List<String> trainingKeys = idx.hotBuffer.allKeysInOrdinalOrder();
        List<float[]> trainingVectors = new ArrayList<>(trainingKeys.size());
        for (String key : trainingKeys) {
            trainingVectors.add(idx.hotBuffer.getVectorArray(idx.hotBuffer.keyToOrd.get(key)));
        }

        if (!idx.mainIndex.isInitialized()) {
            if (trainingVectors.size() < 2) {
                log.info(
                        "Not enough vectors to initialize main index for namespace={}, have {}",
                        namespace,
                        trainingVectors.size());
                return;
            }
            idx.mainIndex.initialize(trainingVectors, trainingKeys);
        } else {
            for (var entry : idx.hotBuffer.keyEntrySet()) {
                String entryKey = entry.getKey();
                float[] vector = idx.hotBuffer.getVectorArray(entry.getValue());
                if (vector == null) continue;
                idx.mainIndex.insert(entryKey, vector);
            }
        }

        try {
            idx.hotBuffer.clear();
            metrics.recordMerge();
            createIndexVersion(idx, namespace);
        } finally {
            idx.hotTracker.clear();
        }

        ShardCentroids sc = buildCentroids(namespace, idx, "shard-0");
        if (sc != null) {
            namespaceCentroids.put(namespace, sc);
        }

        log.info(
                "Merged hot buffer into main index for namespace={}, mainIndex count={}",
                namespace,
                idx.mainIndex.vectorCount());
    }

    private void createIndexVersion(NamespaceVectorIndex idx, String namespace) {
        if (!(idx.mainIndex instanceof JVectorDiskIndex jvdi)) return;
        long snapshotId = snapshotCounter.incrementAndGet();
        Path versionDir = versionBasePath.resolve(namespace + "-" + snapshotId);
        try {
            Files.createDirectories(versionDir);
            Path versionGraph = versionDir.resolve("graph.bin");
            Path versionMeta = versionDir.resolve("metadata.bin");
            if (lmdbIoBackend != null) {
                jvdi.exportGraphToFile(versionGraph);
            } else {
                Path srcGraph = jvdi.resolveIndexPath();
                if (Files.exists(srcGraph)) {
                    Files.copy(srcGraph, versionGraph, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            Path srcMeta = jvdi.resolveMetaPath();
            if (Files.exists(srcMeta)) {
                Files.copy(srcMeta, versionMeta, StandardCopyOption.REPLACE_EXISTING);
            }
            synchronized (idx.versionLock) {
                idx.versionIndices.add(
                        new NamespaceVectorIndex.VersionIndexEntry(snapshotId, versionDir));
            }
            log.info("Created index version snapshotId={} for namespace={}", snapshotId, namespace);
        } catch (IOException e) {
            log.warn(
                    "Failed to create index version for snapshotId={}, namespace={}",
                    snapshotId,
                    namespace,
                    e);
        }
    }

    public ShardCentroids getLatestCentroids(String namespace) {
        return namespaceCentroids.get(namespace);
    }

    public int totalVectorCount(String namespace) {
        NamespaceVectorIndex idx = namespaceIndices.get(namespace);
        if (idx == null) return 0;
        return idx.hotBuffer.size() + idx.mainIndex.vectorCount();
    }

    public ShardCentroids buildCentroids(
            String namespace, NamespaceVectorIndex idx, String shardId) {
        if (idx.mainIndex instanceof IvfPqIndex ivf) {
            float[][] ivfCentroids = ivf.centroids();
            if (ivfCentroids != null && ivfCentroids.length > 0) {
                return new ShardCentroids(
                        shardId,
                        ivfCentroids.length,
                        ivfCentroids,
                        idx.mainIndex.vectorCount(),
                        System.currentTimeMillis(),
                        System.currentTimeMillis());
            }
        }

        List<float[]> samples = sampleVectors(idx, CENTROID_SAMPLE_SIZE);
        if (samples.size() < DEFAULT_NLIST) {
            log.debug(
                    "Not enough sample vectors for centroids: namespace={}, samples={}",
                    namespace,
                    samples.size());
            return null;
        }
        float[][] centroids = kmeansPlusPlus(samples, DEFAULT_NLIST, CENTROID_MAX_ITER);
        return new ShardCentroids(
                shardId,
                centroids.length,
                centroids,
                idx.mainIndex.vectorCount(),
                System.currentTimeMillis(),
                System.currentTimeMillis());
    }

    public ShardCentroids buildAndPersistCentroids(String namespace, String shardId) {
        NamespaceVectorIndex idx = namespaceIndices.get(namespace);
        if (idx == null) return null;
        ShardCentroids sc = buildCentroids(namespace, idx, shardId);
        if (sc != null) {
            namespaceCentroids.put(namespace, sc);
        }
        return sc;
    }

    private List<float[]> sampleVectors(NamespaceVectorIndex idx, int maxSamples) {
        List<float[]> allVectors = idx.hotBuffer.allVectors();
        List<float[]> samples = new ArrayList<>();
        if (allVectors.isEmpty()) {
            int count = idx.mainIndex.vectorCount();
            if (count == 0) return samples;
            int step = Math.max(1, count / maxSamples);
            List<String> keys = new ArrayList<>(idx.mainIndex.entryKeys());
            for (int i = 0; i < keys.size() && samples.size() < maxSamples; i += step) {
                float[] v = idx.mainIndex.getVector(keys.get(i));
                if (v != null) samples.add(v);
            }
        } else {
            int step = Math.max(1, allVectors.size() / maxSamples);
            for (int i = 0; i < allVectors.size() && samples.size() < maxSamples; i += step) {
                samples.add(allVectors.get(i));
            }
        }
        return samples;
    }

    static float[][] kmeansPlusPlus(List<float[]> vectors, int k, int maxIter) {
        if (vectors.isEmpty() || k <= 0 || vectors.size() < k) {
            return vectors.toArray(new float[0][]);
        }

        int dim = vectors.get(0).length;
        float[][] centroids = new float[k][dim];
        centroids[0] = vectors.get(0).clone();

        for (int c = 1; c < k; c++) {
            double[] distances = new double[vectors.size()];
            double totalDist = 0;
            for (int i = 0; i < vectors.size(); i++) {
                double minDist = Double.MAX_VALUE;
                for (int j = 0; j < c; j++) {
                    double d = cosineDistanceSquared(vectors.get(i), centroids[j]);
                    if (d < minDist) minDist = d;
                }
                distances[i] = minDist * minDist;
                totalDist += distances[i];
            }

            double r = Math.random() * totalDist;
            double cumulative = 0;
            int selected = 0;
            for (int i = 0; i < vectors.size(); i++) {
                cumulative += distances[i];
                if (cumulative >= r) {
                    selected = i;
                    break;
                }
            }
            centroids[c] = vectors.get(selected).clone();
        }

        int[] assignments = new int[vectors.size()];
        for (int iter = 0; iter < maxIter; iter++) {
            boolean changed = false;
            for (int i = 0; i < vectors.size(); i++) {
                double minDist = Double.MAX_VALUE;
                int best = 0;
                for (int j = 0; j < k; j++) {
                    double d = cosineDistanceSquared(vectors.get(i), centroids[j]);
                    if (d < minDist) {
                        minDist = d;
                        best = j;
                    }
                }
                if (assignments[i] != best) {
                    assignments[i] = best;
                    changed = true;
                }
            }
            if (!changed) break;

            float[][] newCentroids = new float[k][dim];
            int[] counts = new int[k];
            for (int i = 0; i < vectors.size(); i++) {
                int cluster = assignments[i];
                float[] v = vectors.get(i);
                for (int d = 0; d < dim; d++) {
                    newCentroids[cluster][d] += v[d];
                }
                counts[cluster]++;
            }
            for (int j = 0; j < k; j++) {
                if (counts[j] > 0) {
                    for (int d = 0; d < dim; d++) {
                        newCentroids[j][d] /= counts[j];
                    }
                    centroids[j] = newCentroids[j];
                }
            }
        }

        return centroids;
    }

    private static double cosineDistanceSquared(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0 || normB == 0) return 4.0;
        return 2.0 * (1.0 - dot / (Math.sqrt(normA) * Math.sqrt(normB)));
    }

    private float[] readOriginalVector(NamespaceVectorIndex idx, String entryKey) {
        Integer ordinal = idx.hotBuffer.keyToOrd.get(entryKey);
        if (ordinal != null) {
            return idx.hotBuffer.getVectorArray(ordinal);
        }
        float[] v = idx.mainIndex.getVector(entryKey);
        if (v != null) return v;
        VectorColdStore cs = this.coldStore;
        if (cs != null) {
            metrics.recordColdSearch();
            return cs.readVector(idx.namespace, entryKey);
        }
        return null;
    }

    private static float cosineSimilarity(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0 || normB == 0) return 0;
        return dot / (float) (Math.sqrt(normA) * Math.sqrt(normB));
    }

    private NamespaceVectorIndex getOrCreateNamespaceIndex(String namespace) {
        NamespaceVectorIndex idx = namespaceIndices.get(namespace);
        if (idx != null) return idx;

        synchronized (closeLock) {
            ensureOpen();
            idx = namespaceIndices.get(namespace);
            if (idx != null) return idx;
            idx =
                    new NamespaceVectorIndex(
                            config,
                            namespace,
                            basePath.resolve("ns-" + namespace),
                            tableFormatMode);

            if (idx.hotTracker != null && idx.hotTracker.loadKeys().size() > 0) {
                recoverHotBuffer(idx);
            }

            namespaceIndices.put(namespace, idx);
            return idx;
        }
    }

    private void recoverHotBuffer(NamespaceVectorIndex idx) {
        List<String> pendingKeys = idx.hotTracker.loadKeys();
        int recovered = 0;
        for (String key : pendingKeys) {
            float[] vector = readOriginalVector(idx, key);
            if (vector != null) {
                idx.hotBuffer.add(key, vector, "");
                recovered++;
            }
        }
        if (recovered == 0 && !pendingKeys.isEmpty()) {
            log.warn(
                    "Recovered 0/{} hot buffer entries for namespace={} — vectors were lost (likely service was killed before flush). Re-ingest required.",
                    pendingKeys.size(),
                    idx.namespace);
            idx.hotTracker.clear();
        } else {
            log.info(
                    "Recovered {}/{} hot buffer entries for namespace={}",
                    recovered,
                    pendingKeys.size(),
                    idx.namespace);
        }
    }

    class NamespaceVectorIndex implements AutoCloseable {
        volatile ShardInternalIndex mainIndex;
        final SubIndex hotBuffer;
        HotBufferTracker hotTracker;
        final String namespace;
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
        final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();
        private volatile boolean closed = false;
        private final Object versionLock = new Object();
        private final List<VersionIndexEntry> versionIndices =
                Collections.synchronizedList(new ArrayList<>());

        record VersionIndexEntry(long snapshotId, Path indexPath) {}

        NamespaceVectorIndex(
                JVectorConfig config, String namespace, Path nsPath, boolean tableFormatMode) {
            this.namespace = namespace;
            this.mainIndex = createMainIndex(namespace);
            this.hotBuffer = new SubIndex(config);
            this.hotTracker = new HotBufferTracker(nsPath, namespace);
        }

        void ensureOpen() {
            if (closed) throw new IllegalStateException("NamespaceVectorIndex is closed");
        }

        @Override
        public void close() {
            writeLock.lock();
            try {
                if (closed) return;
                closed = true;
                hotBuffer.close();
                hotTracker.close();
                try {
                    mainIndex.close();
                } catch (Exception e) {
                    log.warn("Failed to close main index", e);
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    static class SubIndex implements AutoCloseable {
        private GraphIndexBuilder writer;
        private final List<VectorFloat<?>> vectors = new ArrayList<>();
        final Map<String, Integer> keyToOrd = new HashMap<>();
        private final Map<Integer, String> ordToKey = new HashMap<>();
        private final Map<Integer, String> ordToL0 = new HashMap<>();
        int nextOrdinal = 0;
        private final int dimension;
        private final int graphDegree;
        private final int efSearch;

        SubIndex(JVectorConfig config) {
            this.dimension = config.dimension();
            this.graphDegree = config.graphDegree();
            this.efSearch = config.efSearch();
            this.writer = createWriter();
        }

        private GraphIndexBuilder createWriter() {
            var ravv = new ListRandomAccessVectorValues(vectors, dimension);
            var scoreProvider =
                    BuildScoreProvider.randomAccessScoreProvider(
                            ravv, VectorSimilarityFunction.COSINE);
            return new GraphIndexBuilder(
                    scoreProvider, graphDegree, graphDegree * 2, graphDegree * 2, 1.0f, 1.0f, true);
        }

        void add(String entryKey, float[] embedding, String l0Abstract) {
            if (embedding.length != dimension) {
                throw new IllegalArgumentException(
                        "Dimension mismatch: expected " + dimension + ", got " + embedding.length);
            }
            int ordinal = nextOrdinal++;
            Integer oldOrdinal = keyToOrd.put(entryKey, ordinal);
            if (oldOrdinal != null) {
                ordToKey.remove(oldOrdinal);
                ordToL0.remove(oldOrdinal);
            }
            VectorFloat<?> vector = BuildScoreProvider.vts.createFloatVector(embedding);
            vectors.add(vector);
            writer.addGraphNode(ordinal, vector);
            ordToKey.put(ordinal, entryKey);
            ordToL0.put(ordinal, l0Abstract);
        }

        List<VectorSearchResult> search(VectorFloat<?> queryVector, int topK) {
            var ravv = new ListRandomAccessVectorValues(vectors, dimension);
            int searchWidth = Math.max(topK, efSearch);
            SearchResult result =
                    GraphSearcher.search(
                            queryVector,
                            searchWidth,
                            ravv,
                            VectorSimilarityFunction.COSINE,
                            writer.getGraph(),
                            Bits.ALL);
            List<VectorSearchResult> results = new ArrayList<>();
            for (SearchResult.NodeScore ns : result.getNodes()) {
                if (results.size() >= topK) break;
                String key = ordToKey.get(ns.node);
                if (key != null) {
                    results.add(
                            new VectorSearchResult(
                                    key, ordToL0.getOrDefault(ns.node, ""), ns.score));
                }
            }
            return results;
        }

        void remove(String entryKey) {
            Integer ordinal = keyToOrd.remove(entryKey);
            if (ordinal != null) {
                ordToKey.remove(ordinal);
                ordToL0.remove(ordinal);
            }
        }

        ColdVectorEntry extractEntry(String entryKey) {
            Integer ordinal = keyToOrd.get(entryKey);
            if (ordinal == null) return null;
            VectorFloat<?> vf = null;
            if (ordinal >= 0 && ordinal < vectors.size()) {
                vf = vectors.get(ordinal);
            }
            if (vf == null) return null;
            float[] embedding = new float[dimension];
            for (int i = 0; i < dimension; i++) {
                embedding[i] = vf.get(i);
            }
            return new ColdVectorEntry(entryKey, embedding, ordToL0.getOrDefault(ordinal, ""));
        }

        String getL0Abstract(String entryKey) {
            Integer ordinal = keyToOrd.get(entryKey);
            return ordinal != null ? ordToL0.get(ordinal) : null;
        }

        float[] getVectorArray(int ordinal) {
            if (ordinal >= 0 && ordinal < vectors.size()) {
                VectorFloat<?> vf = vectors.get(ordinal);
                float[] arr = new float[dimension];
                for (int i = 0; i < dimension; i++) {
                    arr[i] = vf.get(i);
                }
                return arr;
            }
            return null;
        }

        List<float[]> allVectors() {
            List<float[]> result = new ArrayList<>(vectors.size());
            for (VectorFloat<?> vf : vectors) {
                float[] arr = new float[dimension];
                for (int i = 0; i < dimension; i++) {
                    arr[i] = vf.get(i);
                }
                result.add(arr);
            }
            return result;
        }

        void clear() {
            try {
                writer.close();
            } catch (Exception e) {
                log.warn("Failed to close SubIndex writer during clear", e);
            }
            keyToOrd.clear();
            ordToKey.clear();
            ordToL0.clear();
            vectors.clear();
            nextOrdinal = 0;
            writer = createWriter();
        }

        @Override
        public void close() {
            try {
                writer.close();
            } catch (Exception e) {
                log.warn("Failed to close SubIndex writer", e);
            }
            keyToOrd.clear();
            ordToKey.clear();
            ordToL0.clear();
            vectors.clear();
        }

        int size() {
            return keyToOrd.size();
        }

        Set<Map.Entry<String, Integer>> keyEntrySet() {
            return keyToOrd.entrySet();
        }

        /** Keys sorted by ascending ordinal — aligned with the vector ordinal space. */
        List<String> allKeysInOrdinalOrder() {
            return keyToOrd.entrySet().stream()
                    .sorted(Comparator.comparingInt(Map.Entry::getValue))
                    .map(Map.Entry::getKey)
                    .toList();
        }
    }
}
