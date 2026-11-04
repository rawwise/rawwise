package io.github.rawwise.engine.vector;

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.disk.ReaderSupplierFactory;
import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.rawwise.engine.spi.FaultInjector;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public non-sealed class JVectorDiskIndex implements ShardInternalIndex {

    private static final int META_MAGIC = 0x4A564449;
    private static final int META_VERSION = 1;
    private static final int AUTO_PERSIST_THRESHOLD = 1000;

    private final JVectorConfig config;
    private final Path indexPath;

    private final List<VectorFloat<?>> vectors = new ArrayList<>();
    private final ConcurrentHashMap<String, Integer> keyToOrd = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> ordToKey = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, VectorLocation> ordToLocation =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> ordToStorageRef = new ConcurrentHashMap<>();
    private final AtomicLong nextOrdinal = new AtomicLong(0);
    private final AtomicLong insertsSincePersist = new AtomicLong(0);

    private GraphIndexBuilder graphBuilder;
    private ListRandomAccessVectorValues ravv;
    private BuildScoreProvider scoreProvider;
    private volatile boolean initialized = false;
    private volatile boolean dirty = false;

    private ReaderSupplier cachedReaderSupplier;
    private OnDiskGraphIndex cachedOnDiskIndex;

    private final ExecutorService persistExecutor =
            Executors.newSingleThreadExecutor(
                    r -> {
                        Thread t = new Thread(r, "jvector-disk-index-persist");
                        t.setDaemon(false);
                        return t;
                    });

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();
    private final FaultInjector faultInjector;
    private volatile LmdbIoBackend lmdbIoBackend;

    public JVectorDiskIndex(JVectorConfig config, Path indexPath) {
        this(config, indexPath, null);
    }

    public JVectorDiskIndex(JVectorConfig config, Path indexPath, FaultInjector faultInjector) {
        this.config = config;
        this.indexPath = indexPath;
        this.faultInjector = faultInjector != null ? faultInjector : FaultInjector.NOOP;
        this.lmdbIoBackend = null;
        try {
            Files.createDirectories(indexPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create index directory: " + indexPath, e);
        }

        tryLoadFromDisk();
    }

    public JVectorDiskIndex(
            JVectorConfig config,
            Path indexPath,
            FaultInjector faultInjector,
            LmdbIoBackend lmdbIoBackend) {
        this.config = config;
        this.indexPath = indexPath;
        this.faultInjector = faultInjector != null ? faultInjector : FaultInjector.NOOP;
        this.lmdbIoBackend = lmdbIoBackend;
        try {
            Files.createDirectories(indexPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create index directory: " + indexPath, e);
        }

        tryLoadFromDisk();
    }

    private void tryLoadFromDisk() {
        Path metaPath = resolveMetaPath();
        if (!Files.exists(metaPath)) return;
        boolean hasGraph =
                lmdbIoBackend != null ? lmdbIoBackend.hasData() : Files.exists(resolveIndexPath());
        if (!hasGraph) return;
        writeLock.lock();
        try {
            loadFromDisk();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void initialize(List<float[]> vectors, List<String> keys) {
        writeLock.lock();
        try {
            if (initialized)
                throw new IllegalStateException("JVectorDiskIndex already initialized");
            log.info(
                    "Initializing JVector disk index: N={}, dimension={}, graphDegree={}, path={}",
                    vectors.size(),
                    config.dimension(),
                    config.graphDegree(),
                    indexPath);

            this.ravv = new ListRandomAccessVectorValues(this.vectors, config.dimension());
            this.scoreProvider =
                    BuildScoreProvider.randomAccessScoreProvider(
                            ravv, VectorSimilarityFunction.COSINE);
            this.graphBuilder =
                    new GraphIndexBuilder(
                            scoreProvider,
                            config.graphDegree(),
                            config.graphDegree() * 2,
                            config.graphDegree() * 2,
                            1.0f,
                            1.0f,
                            true);

            for (int i = 0; i < keys.size(); i++) {
                int ord = i;
                VectorFloat<?> vf = BuildScoreProvider.vts.createFloatVector(vectors.get(i));
                this.vectors.add(vf);
                graphBuilder.addGraphNode(ord, vf);
                keyToOrd.put(keys.get(i), ord);
                ordToKey.put(ord, keys.get(i));
                ordToLocation.put(ord, VectorLocation.WARM);
                ordToStorageRef.put(ord, "lmdb-key-" + keys.get(i));
            }
            nextOrdinal.set(keys.size());
            graphBuilder.cleanup();
            persistAll();
            initialized = true;
            dirty = false;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize JVector disk index", e);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void insert(String entryKey, float[] vector) {
        writeLock.lock();
        try {
            if (!initialized) throw new IllegalStateException("JVectorDiskIndex not initialized");
            if (vectorCount() >= config.maxVectors()) {
                throw new IllegalStateException(
                        "JVectorDiskIndex reached maxVectors limit: " + config.maxVectors());
            }
            int ord = (int) nextOrdinal.getAndIncrement();
            VectorFloat<?> vf = BuildScoreProvider.vts.createFloatVector(vector);
            vectors.add(vf);
            graphBuilder.addGraphNode(ord, vf);
            keyToOrd.put(entryKey, ord);
            ordToKey.put(ord, entryKey);
            ordToLocation.put(ord, VectorLocation.WARM);
            ordToStorageRef.put(ord, "lmdb-key-" + entryKey);
            dirty = true;

            if (vectors.size() >= config.buildThreshold()) {
                persistExecutor.submit(
                        () -> {
                            try {
                                ensurePersisted();
                            } catch (IOException e) {
                                log.error(
                                        "Failed to persist index at build threshold {}",
                                        config.buildThreshold(),
                                        e);
                            }
                        });
            } else if (insertsSincePersist.incrementAndGet() >= AUTO_PERSIST_THRESHOLD) {
                persistExecutor.submit(
                        () -> {
                            try {
                                ensurePersisted();
                            } catch (IOException e) {
                                log.warn("Async persist failed", e);
                            }
                        });
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public List<SearchCandidate> search(float[] query, int topK) {
        if (!initialized) return List.of();

        VectorFloat<?> queryVector = BuildScoreProvider.vts.createFloatVector(query);

        readLock.lock();
        try {
            if (dirty) {
                return inMemoryGraphSearch(queryVector, topK);
            }

            OnDiskGraphIndex onDisk;
            try {
                onDisk = getOrLoadOnDiskIndex();
            } catch (IOException e) {
                log.warn("Failed to load disk index, falling back to brute force", e);
                return inMemoryBruteForceSearch(queryVector);
            }
            if (onDisk == null) return List.of();

            OnDiskGraphIndex.View view = onDisk.getView();
            var reranker = view.rerankerFor(queryVector, VectorSimilarityFunction.COSINE);
            var scoreProvider = new DefaultSearchScoreProvider(reranker, reranker);
            int searchWidth = Math.max(topK, config.efSearch());
            SearchResult result =
                    new GraphSearcher(onDisk).search(scoreProvider, searchWidth, Bits.ALL);

            List<SearchCandidate> candidates = new ArrayList<>();
            for (SearchResult.NodeScore ns : result.getNodes()) {
                String key = ordToKey.get(ns.node);
                if (key != null) {
                    candidates.add(new SearchCandidate(key, 1.0f - ns.score));
                }
            }
            return candidates;
        } finally {
            readLock.unlock();
        }
    }

    private List<SearchCandidate> inMemoryGraphSearch(VectorFloat<?> queryVector, int topK) {
        if (graphBuilder == null) return inMemoryBruteForceSearch(queryVector);

        var ravv = new ListRandomAccessVectorValues(vectors, config.dimension());
        int searchWidth = Math.max(topK, config.efSearch());
        SearchResult result =
                GraphSearcher.search(
                        queryVector,
                        searchWidth,
                        ravv,
                        VectorSimilarityFunction.COSINE,
                        graphBuilder.getGraph(),
                        Bits.ALL);

        List<SearchCandidate> candidates = new ArrayList<>();
        for (SearchResult.NodeScore ns : result.getNodes()) {
            String key = ordToKey.get(ns.node);
            if (key != null) {
                candidates.add(new SearchCandidate(key, 1.0f - ns.score));
            }
        }
        return candidates;
    }

    @Override
    public void updateLocation(String entryKey, VectorLocation newLocation, String storageRef) {
        Integer ord = keyToOrd.get(entryKey);
        if (ord == null) return;
        ordToLocation.put(ord, newLocation);
        ordToStorageRef.put(ord, storageRef);
    }

    @Override
    public VectorLocation getLocation(String entryKey) {
        Integer ord = keyToOrd.get(entryKey);
        return ord == null ? null : ordToLocation.get(ord);
    }

    @Override
    public String getStorageRef(String entryKey) {
        Integer ord = keyToOrd.get(entryKey);
        return ord == null ? null : ordToStorageRef.get(ord);
    }

    @Override
    public void markTombstone(String entryKey) {
        Integer ord = keyToOrd.remove(entryKey);
        if (ord != null) {
            ordToKey.remove(ord);
            ordToLocation.remove(ord);
            ordToStorageRef.remove(ord);
        }
    }

    @Override
    public int vectorCount() {
        return keyToOrd.size();
    }

    @Override
    public java.util.Set<String> entryKeys() {
        return java.util.Set.copyOf(keyToOrd.keySet());
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public float[] getVector(String entryKey) {
        Integer ord = keyToOrd.get(entryKey);
        if (ord == null) return null;
        if (ord >= 0 && ord < vectors.size()) {
            VectorFloat<?> vf = vectors.get(ord);
            float[] arr = new float[config.dimension()];
            for (int i = 0; i < config.dimension(); i++) {
                arr[i] = vf.get(i);
            }
            return arr;
        }
        return null;
    }

    @Override
    public void close() {
        writeLock.lock();
        try {
            if (!initialized) return;
            persistExecutor.shutdown();
            try {
                if (!persistExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    persistExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                persistExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            try {
                persistAll();
            } catch (IOException e) {
                log.warn("Failed to persist index on close", e);
            }
            closeCache();
            try {
                if (graphBuilder != null) graphBuilder.close();
            } catch (Exception e) {
                log.warn("Failed to close graph builder", e);
            }
            keyToOrd.clear();
            ordToKey.clear();
            ordToLocation.clear();
            ordToStorageRef.clear();
            vectors.clear();
            initialized = false;
        } finally {
            if (lmdbIoBackend != null) {
                try {
                    lmdbIoBackend.close();
                } catch (Exception e) {
                    log.warn("Failed to close LmdbIoBackend", e);
                }
            }
            writeLock.unlock();
        }
    }

    public void persist() {
        writeLock.lock();
        try {
            if (!initialized) return;
            try {
                faultInjector.inject(FaultInjector.InjectionPoint.DURING_VECTOR_INDEX_PERSIST);
                persistAll();
                dirty = false;
                insertsSincePersist.set(0);
            } catch (IOException e) {
                log.warn("Failed to persist JVectorDiskIndex", e);
            }
        } finally {
            writeLock.unlock();
        }
    }

    // ==================== Disk Recovery ====================

    private void loadFromDisk() {
        Path metaPath = resolveMetaPath();
        if (!Files.exists(metaPath)) return;
        boolean hasGraph =
                lmdbIoBackend != null ? lmdbIoBackend.hasData() : Files.exists(resolveIndexPath());
        if (!hasGraph) return;

        try {
            loadMetadata(new DataInputStream(Files.newInputStream(metaPath)));
            if (lmdbIoBackend != null) {
                loadGraphFromLmdb();
            } else {
                loadGraphFromDisk(resolveIndexPath());
            }
            initialized = true;
            dirty = false;
            log.info(
                    "Loaded JVector disk index from disk: {} entries, dimension={}, path={}",
                    keyToOrd.size(),
                    config.dimension(),
                    indexPath);
        } catch (IOException e) {
            log.warn(
                    "Failed to load index from disk at {}, will need re-initialization",
                    indexPath,
                    e);
            clearAllMappings();
        }
    }

    private void loadMetadata(DataInputStream in) throws IOException {
        try (in) {
            int magic = in.readInt();
            if (magic != META_MAGIC)
                throw new IOException("Invalid metadata magic: " + Integer.toHexString(magic));
            int version = in.readInt();
            if (version != META_VERSION)
                throw new IOException("Unsupported metadata version: " + version);
            int storedDim = in.readInt();
            if (storedDim != config.dimension()) {
                throw new IOException(
                        "Dimension mismatch: stored="
                                + storedDim
                                + ", config="
                                + config.dimension());
            }
            long storedNextOrd = in.readLong();
            int entryCount = in.readInt();

            for (int i = 0; i < entryCount; i++) {
                int keyLen = in.readInt();
                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);
                String key = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
                int ord = in.readInt();
                byte locByte = in.readByte();
                int refLen = in.readInt();
                byte[] refBytes = new byte[refLen];
                in.readFully(refBytes);
                String storageRef = new String(refBytes, java.nio.charset.StandardCharsets.UTF_8);

                keyToOrd.put(key, ord);
                ordToKey.put(ord, key);
                ordToLocation.put(ord, locByte == 0 ? VectorLocation.WARM : VectorLocation.COLD);
                ordToStorageRef.put(ord, storageRef);
            }

            nextOrdinal.set(storedNextOrd);
        }
    }

    private void loadGraphFromDisk(Path graphPath) throws IOException {
        ReaderSupplier rs = ReaderSupplierFactory.open(graphPath);
        try {
            cachedOnDiskIndex = OnDiskGraphIndex.load(rs);
            cachedReaderSupplier = rs;

            OnDiskGraphIndex.View view = cachedOnDiskIndex.getView();
            int numVectors = keyToOrd.size();
            for (int i = 0; i < numVectors; i++) {
                VectorFloat<?> vf = view.getVector(i);
                if (vf != null) {
                    vectors.add(vf);
                }
            }

            this.ravv = new ListRandomAccessVectorValues(this.vectors, config.dimension());
            this.scoreProvider =
                    BuildScoreProvider.randomAccessScoreProvider(
                            ravv, VectorSimilarityFunction.COSINE);
            this.graphBuilder =
                    new GraphIndexBuilder(
                            scoreProvider,
                            config.graphDegree(),
                            config.graphDegree() * 2,
                            config.graphDegree() * 2,
                            1.0f,
                            1.0f,
                            true);
            for (int i = 0; i < numVectors; i++) {
                if (i < vectors.size()) {
                    graphBuilder.addGraphNode(i, vectors.get(i));
                }
            }
        } catch (Exception e) {
            if (cachedReaderSupplier != null) {
                try {
                    cachedReaderSupplier.close();
                } catch (Exception ex) {
                }
                cachedReaderSupplier = null;
            }
            cachedOnDiskIndex = null;
            throw new IOException("Failed to load graph from disk", e);
        }
    }

    private void loadGraphFromLmdb() throws IOException {
        LmdbReaderSupplier rs = lmdbIoBackend.createReaderSupplier();
        try {
            cachedOnDiskIndex = OnDiskGraphIndex.load(rs);
            cachedReaderSupplier = rs;

            OnDiskGraphIndex.View view = cachedOnDiskIndex.getView();
            int numVectors = keyToOrd.size();
            for (int i = 0; i < numVectors; i++) {
                VectorFloat<?> vf = view.getVector(i);
                if (vf != null) {
                    vectors.add(vf);
                }
            }

            this.ravv = new ListRandomAccessVectorValues(this.vectors, config.dimension());
            this.scoreProvider =
                    BuildScoreProvider.randomAccessScoreProvider(
                            ravv, VectorSimilarityFunction.COSINE);
            this.graphBuilder =
                    new GraphIndexBuilder(
                            scoreProvider,
                            config.graphDegree(),
                            config.graphDegree() * 2,
                            config.graphDegree() * 2,
                            1.0f,
                            1.0f,
                            true);
            for (int i = 0; i < numVectors; i++) {
                if (i < vectors.size()) {
                    graphBuilder.addGraphNode(i, vectors.get(i));
                }
            }
        } catch (Exception e) {
            try {
                rs.close();
            } catch (Exception ex) {
            }
            cachedOnDiskIndex = null;
            cachedReaderSupplier = null;
            throw new IOException("Failed to load graph from LMDB", e);
        }
    }

    private void persistMetadata() throws IOException {
        Path metaPath = resolveMetaPath();
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(metaPath))) {
            out.writeInt(META_MAGIC);
            out.writeInt(META_VERSION);
            out.writeInt(config.dimension());
            out.writeLong(nextOrdinal.get());
            out.writeInt(keyToOrd.size());

            for (var entry : keyToOrd.entrySet()) {
                String key = entry.getKey();
                int ord = entry.getValue();
                byte[] keyBytes = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(keyBytes.length);
                out.write(keyBytes);
                out.writeInt(ord);
                VectorLocation loc = ordToLocation.getOrDefault(ord, VectorLocation.WARM);
                out.writeByte(loc == VectorLocation.WARM ? (byte) 0 : (byte) 1);
                String storageRef = ordToStorageRef.getOrDefault(ord, "");
                byte[] refBytes = storageRef.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(refBytes.length);
                out.write(refBytes);
            }
        }
    }

    // ==================== OnDiskGraphIndex Cache ====================

    private OnDiskGraphIndex getOrLoadOnDiskIndex() throws IOException {
        if (cachedOnDiskIndex != null) return cachedOnDiskIndex;

        if (lmdbIoBackend != null) {
            if (!lmdbIoBackend.hasData()) return null;
            LmdbReaderSupplier rs = lmdbIoBackend.createReaderSupplier();
            try {
                cachedOnDiskIndex = OnDiskGraphIndex.load(rs);
                cachedReaderSupplier = rs;
                return cachedOnDiskIndex;
            } catch (Exception e) {
                try {
                    rs.close();
                } catch (Exception ex) {
                }
                throw new IOException("Failed to load graph from LMDB", e);
            }
        }

        Path graphPath = resolveIndexPath();
        if (!Files.exists(graphPath)) return null;

        if (cachedReaderSupplier != null) {
            try {
                cachedReaderSupplier.close();
            } catch (Exception e) {
            }
            cachedReaderSupplier = null;
        }
        cachedReaderSupplier = ReaderSupplierFactory.open(graphPath);
        cachedOnDiskIndex = OnDiskGraphIndex.load(cachedReaderSupplier);
        return cachedOnDiskIndex;
    }

    private void invalidateCache() {
        if (cachedOnDiskIndex != null) {
            try {
                cachedOnDiskIndex.close();
            } catch (Exception e) {
            }
            cachedOnDiskIndex = null;
        }
        if (cachedReaderSupplier != null) {
            try {
                cachedReaderSupplier.close();
            } catch (Exception e) {
            }
            cachedReaderSupplier = null;
        }
    }

    private void closeCache() {
        invalidateCache();
    }

    // ==================== Internal ====================

    Path resolveIndexPath() {
        return indexPath.resolve("graph.bin");
    }

    Path resolveMetaPath() {
        return indexPath.resolve("metadata.bin");
    }

    private void persistAll() throws IOException {
        persistToDisk();
        persistMetadata();
    }

    private void ensurePersisted() throws IOException {
        if (dirty && graphBuilder != null) {
            writeLock.lock();
            try {
                if (dirty && graphBuilder != null) {
                    graphBuilder.cleanup();
                    persistAll();
                    dirty = false;
                    insertsSincePersist.set(0);
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    private void persistToDisk() throws IOException {
        if (graphBuilder == null || vectors.isEmpty()) return;
        if (lmdbIoBackend != null) {
            persistToLmdb();
        } else {
            Path graphPath = resolveIndexPath();
            OnDiskGraphIndex.write(graphBuilder.getGraph(), ravv, graphPath);
            log.debug("Persisted {} vectors to {}", vectors.size(), graphPath);
        }
        invalidateCache();
    }

    private void persistToLmdb() throws IOException {
        Path tempFile = indexPath.resolve("graph.tmp");
        try {
            OnDiskGraphIndex.write(graphBuilder.getGraph(), ravv, tempFile);
            try (LmdbRandomAccessWriter writer = lmdbIoBackend.createWriter()) {
                writer.seek(0);
                byte[] buffer = new byte[65536];
                try (var in = Files.newInputStream(tempFile)) {
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        writer.write(buffer, 0, read);
                    }
                }
                writer.flush();
            }
            log.debug("Persisted {} vectors to LMDB via {}", vectors.size(), indexPath);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    void exportGraphToFile(Path targetPath) throws IOException {
        if (graphBuilder == null || vectors.isEmpty()) return;
        graphBuilder.cleanup();
        OnDiskGraphIndex.write(graphBuilder.getGraph(), ravv, targetPath);
    }

    private void clearAllMappings() {
        keyToOrd.clear();
        ordToKey.clear();
        ordToLocation.clear();
        ordToStorageRef.clear();
        vectors.clear();
        nextOrdinal.set(0);
    }

    private List<SearchCandidate> inMemoryBruteForceSearch(VectorFloat<?> queryVector) {
        List<SearchCandidate> all = new ArrayList<>();
        for (var entry : keyToOrd.entrySet()) {
            String entryKey = entry.getKey();
            Integer ord = entry.getValue();
            if (ord >= 0 && ord < vectors.size()) {
                VectorFloat<?> vf = vectors.get(ord);
                float dist = cosineDistance(queryVector, vf);
                all.add(new SearchCandidate(entryKey, dist));
            }
        }
        all.sort(Comparator.comparingDouble(SearchCandidate::approximateDistance));
        return all;
    }

    private float cosineDistance(VectorFloat<?> queryVector, VectorFloat<?> vector) {
        float dot = 0, normQ = 0, normV = 0;
        for (int i = 0; i < config.dimension(); i++) {
            float q = queryVector.get(i);
            float v = vector.get(i);
            dot += q * v;
            normQ += q * q;
            normV += v * v;
        }
        if (normQ == 0 || normV == 0) return 1.0f;
        return 1.0f - dot / (float) (Math.sqrt(normQ) * Math.sqrt(normV));
    }
}
