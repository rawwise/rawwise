package io.github.rawwise.engine.vector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LanceVectorColdStore implements VectorColdStore {

    private final Path basePath;
    private final int dimension;
    private final int maxEntriesPerNamespace;
    private final ConcurrentHashMap<String, NamespaceState> namespaces = new ConcurrentHashMap<>();
    private static final List<String> DEFAULT_COLUMNS =
            List.of("entryKey", "embedding", "l0Abstract");

    private final boolean approximateEnabled;
    private final int ivfBuildThreshold;
    private final int ivfNprobe;

    public LanceVectorColdStore(Path basePath, int dimension) {
        this(basePath, dimension, 500_000);
    }

    public LanceVectorColdStore(Path basePath, int dimension, int maxEntriesPerNamespace) {
        this(
                basePath,
                dimension,
                maxEntriesPerNamespace,
                Boolean.parseBoolean(
                        System.getProperty("rawwise.vector.cold-storage.approximate", "true")),
                Integer.getInteger("rawwise.vector.cold-storage.ivf-build-threshold", 1000),
                Integer.getInteger("rawwise.vector.cold-storage.ivf-nprobe", 8));
    }

    public LanceVectorColdStore(
            Path basePath,
            int dimension,
            int maxEntriesPerNamespace,
            boolean approximateEnabled,
            int ivfBuildThreshold,
            int ivfNprobe) {
        this.basePath = basePath;
        this.dimension = dimension;
        this.maxEntriesPerNamespace = maxEntriesPerNamespace;
        this.approximateEnabled = approximateEnabled;
        this.ivfBuildThreshold = ivfBuildThreshold;
        this.ivfNprobe = Math.max(1, ivfNprobe);
        try {
            Files.createDirectories(basePath);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to create Lance cold store base path: " + basePath, e);
        }
        recoverFromDisk();
        log.info(
                "LanceVectorColdStore initialized: approximate={}, ivfBuildThreshold={}, ivfNprobe={}",
                approximateEnabled,
                ivfBuildThreshold,
                this.ivfNprobe);
    }

    @Override
    public void write(String namespace, String blockId, List<ColdVectorEntry> entries) {
        if (entries == null || entries.isEmpty()) return;

        NamespaceState nsState =
                namespaces.computeIfAbsent(
                        namespace, k -> new NamespaceState(basePath, dimension, k));
        if (nsState.totalEntryCount() >= maxEntriesPerNamespace) {
            log.warn(
                    "Lance cold store namespace={} reached maxEntries={}, dropping {} new entries",
                    namespace,
                    maxEntriesPerNamespace,
                    entries.size());
            return;
        }

        Path dataDir = basePath.resolve("ns-" + namespace).resolve("data");
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create data dir: " + dataDir, e);
        }

        String fragmentId = blockId;
        Path fragmentPath = dataDir.resolve(fragmentId + ".lance");

        LanceFragment fragment = LanceFragment.write(fragmentPath, dimension, entries);
        nsState.addFragment(fragmentId, fragment, entries);
        nsState.markIvfDirty();

        log.info(
                "Wrote {} cold vector entries to Lance fragment for namespace={}, fragmentId={}",
                entries.size(),
                namespace,
                fragmentId);
    }

    @Override
    public List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK) {
        NamespaceState nsState = namespaces.get(namespace);
        if (nsState == null) return List.of();

        if (approximateEnabled) {
            ColdIvfIndex ivf = nsState.getOrCreateIvfIndex(dimension, ivfNprobe);
            if (nsState.isIvfDirty() && nsState.totalEntryCount() >= ivfBuildThreshold) {
                synchronized (ivf) {
                    if (nsState.isIvfDirty() && nsState.totalEntryCount() >= ivfBuildThreshold) {
                        try {
                            rebuildIvf(namespace, nsState, ivf);
                            nsState.clearIvfDirty();
                        } catch (Exception e) {
                            log.warn(
                                    "Cold IVF index build failed for namespace={}, falling back to brute-force",
                                    namespace,
                                    e);
                        }
                    }
                }
            }
            if (ivf.isBuilt()) {
                try {
                    List<VectorSearchResult> approx = ivf.search(queryEmbedding, topK, nsState);
                    if (approx != null) {
                        return approx;
                    }
                } catch (Exception e) {
                    log.warn(
                            "Cold IVF search failed for namespace={}, falling back to brute-force",
                            namespace,
                            e);
                }
            }
        }

        return bruteForceSearch(nsState, queryEmbedding, topK);
    }

    private List<VectorSearchResult> bruteForceSearch(
            NamespaceState nsState, float[] queryEmbedding, int topK) {
        Set<String> tombstoneSet = nsState.getTombstones();
        PriorityQueue<VectorSearchResult> heap =
                new PriorityQueue<>(Comparator.comparingDouble(VectorSearchResult::score));

        for (var entry : nsState.getKeyIndex().entrySet()) {
            String key = entry.getKey();
            if (tombstoneSet.contains(key)) continue;

            FragmentLocation loc = entry.getValue();
            LanceFragment fragment = nsState.getFragments().get(loc.fragmentId());
            if (fragment == null) continue;

            float[] vector = fragment.getVector(loc.rowIndex());
            if (vector == null) continue;

            float score = cosineSimilarity(queryEmbedding, vector);
            VectorSearchResult result =
                    new VectorSearchResult(key, fragment.getL0Abstract(loc.rowIndex()), score);

            if (heap.size() < topK) {
                heap.offer(result);
            } else if (!heap.isEmpty() && score > heap.peek().score()) {
                heap.poll();
                heap.offer(result);
            }
        }

        List<VectorSearchResult> results = new ArrayList<>(heap);
        results.sort(Comparator.comparingDouble(VectorSearchResult::score).reversed());
        return results;
    }

    private void rebuildIvf(String namespace, NamespaceState nsState, ColdIvfIndex ivf) {
        Set<String> tombstones = nsState.getTombstones();
        List<float[]> vectors = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (var entry : nsState.getKeyIndex().entrySet()) {
            String key = entry.getKey();
            if (tombstones.contains(key)) continue;
            FragmentLocation loc = entry.getValue();
            LanceFragment fragment = nsState.getFragments().get(loc.fragmentId());
            if (fragment == null) continue;
            float[] vector = fragment.getVector(loc.rowIndex());
            if (vector == null) continue;
            vectors.add(vector);
            keys.add(key);
        }
        ivf.build(vectors, keys);
        log.info(
                "Cold IVF index rebuilt for namespace={}, vectors={}, nlist={}",
                namespace,
                vectors.size(),
                Math.max(1, (int) Math.sqrt(Math.max(1, vectors.size()))));
    }

    @Override
    public float[] readVector(String namespace, String entryKey) {
        NamespaceState nsState = namespaces.get(namespace);
        if (nsState == null) return null;
        if (nsState.getTombstones().contains(entryKey)) return null;

        FragmentLocation loc = nsState.getKeyIndex().get(entryKey);
        if (loc == null) return null;

        LanceFragment fragment = nsState.getFragments().get(loc.fragmentId());
        if (fragment == null) return null;

        return fragment.getVector(loc.rowIndex());
    }

    @Override
    public String readL0Abstract(String namespace, String entryKey) {
        NamespaceState nsState = namespaces.get(namespace);
        if (nsState == null) return null;
        if (nsState.getTombstones().contains(entryKey)) return null;

        FragmentLocation loc = nsState.getKeyIndex().get(entryKey);
        if (loc == null) return null;

        LanceFragment fragment = nsState.getFragments().get(loc.fragmentId());
        if (fragment == null) return null;

        return fragment.getL0Abstract(loc.rowIndex());
    }

    @Override
    public void delete(String namespace, Collection<String> entryKeys) {
        NamespaceState nsState = namespaces.get(namespace);
        if (nsState == null) return;
        Set<String> keys = new HashSet<>(entryKeys);
        nsState.addTombstones(keys);
        nsState.markIvfDirty();
    }

    @Override
    public boolean hasColdData(String namespace) {
        NamespaceState nsState = namespaces.get(namespace);
        return nsState != null && nsState.totalEntryCount() > 0;
    }

    public Set<String> getAllNamespaces() {
        return Set.copyOf(namespaces.keySet());
    }

    public List<ColdVectorEntry> scan(String namespace) {
        NamespaceState nsState = namespaces.get(namespace);
        if (nsState == null) return List.of();
        Set<String> tombstoneSet = nsState.getTombstones();
        List<ColdVectorEntry> result = new ArrayList<>();
        for (var entry : nsState.getKeyIndex().entrySet()) {
            String key = entry.getKey();
            if (tombstoneSet.contains(key)) continue;
            FragmentLocation loc = entry.getValue();
            LanceFragment fragment = nsState.getFragments().get(loc.fragmentId());
            if (fragment == null) continue;
            float[] vector = fragment.getVector(loc.rowIndex());
            if (vector == null) continue;
            String l0 = fragment.getL0Abstract(loc.rowIndex());
            result.add(new ColdVectorEntry(key, vector, l0));
        }
        return result;
    }

    public LanceManifest getManifest(String namespace) {
        NamespaceState nsState = namespaces.get(namespace);
        return nsState == null ? null : nsState.manifest;
    }

    @Override
    public void close() {
        for (NamespaceState nsState : namespaces.values()) {
            nsState.close();
        }
        namespaces.clear();
    }

    private void recoverFromDisk() {
        if (!Files.isDirectory(basePath)) return;
        try (var stream = Files.list(basePath)) {
            for (Path nsDir : stream.toList()) {
                if (!Files.isDirectory(nsDir)) continue;
                String dirName = nsDir.getFileName().toString();
                if (!dirName.startsWith("ns-")) continue;
                String namespace = dirName.substring(3);

                Path manifestPath = nsDir.resolve("manifest.bin");
                LanceManifest manifest = LanceManifest.recover(manifestPath);

                NamespaceState nsState =
                        new NamespaceState(basePath, dimension, namespace, manifest);
                Path dataDir = nsDir.resolve("data");
                if (Files.isDirectory(dataDir)) {
                    for (String fragmentId : manifest.getFragmentIds()) {
                        Path fragmentPath = dataDir.resolve(fragmentId + ".lance");
                        if (Files.exists(fragmentPath)) {
                            try {
                                LanceFragment fragment = new LanceFragment(fragmentPath, dimension);
                                fragment.openForRead();
                                nsState.addFragmentRecovered(fragmentId, fragment);
                            } catch (Exception e) {
                                log.warn(
                                        "Failed to recover Lance fragment {}: {}",
                                        fragmentPath,
                                        e.getMessage());
                            }
                        }
                    }
                }
                namespaces.put(namespace, nsState);
                log.info(
                        "Recovered Lance namespace={}, fragments={}, entries={}",
                        namespace,
                        manifest.getFragmentIds().size(),
                        nsState.getKeyIndex().size());
            }
        } catch (IOException e) {
            log.warn("Failed to recover Lance cold store from disk", e);
        }
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

    record FragmentLocation(String fragmentId, int rowIndex) {}

    static class NamespaceState implements AutoCloseable {
        private final LanceManifest manifest;
        private final ConcurrentHashMap<String, LanceFragment> fragments =
                new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, FragmentLocation> keyIndex =
                new ConcurrentHashMap<>();
        private final int dimension;
        private final Path nsDir;

        private volatile ColdIvfIndex ivfIndex;
        private volatile boolean ivfDirty = true;

        NamespaceState(Path basePath, int dimension, String namespace) {
            this(basePath, dimension, namespace, initNewManifest(dimension));
        }

        private static LanceManifest initNewManifest(int dimension) {
            LanceManifest m = new LanceManifest();
            m.setSchema(dimension, DEFAULT_COLUMNS);
            return m;
        }

        NamespaceState(Path basePath, int dimension, String namespace, LanceManifest manifest) {
            this.dimension = dimension;
            this.nsDir = basePath.resolve("ns-" + namespace);
            this.manifest = manifest;
            if (manifest.getDimension() == 0) {
                manifest.setSchema(dimension, DEFAULT_COLUMNS);
            }
        }

        void addFragment(String fragmentId, LanceFragment fragment, List<ColdVectorEntry> entries) {
            fragments.put(fragmentId, fragment);
            Set<String> rewrittenKeys = new HashSet<>();
            for (int i = 0; i < entries.size(); i++) {
                String key = entries.get(i).entryKey();
                if (key != null && !key.isEmpty()) {
                    keyIndex.put(key, new FragmentLocation(fragmentId, i));
                    rewrittenKeys.add(key);
                }
            }
            if (!rewrittenKeys.isEmpty()) {
                manifest.removeTombstones(rewrittenKeys);
            }
            if (manifest.getFragmentIds().contains(fragmentId)) return;
            manifest.addFragment(fragmentId, entries.size());
            persistManifest();
        }

        void addFragmentRecovered(String fragmentId, LanceFragment fragment) {
            fragments.put(fragmentId, fragment);
            for (int i = 0; i < fragment.getNumRows(); i++) {
                String key = fragment.getEntryKey(i);
                if (key != null && !key.isEmpty()) {
                    keyIndex.put(key, new FragmentLocation(fragmentId, i));
                }
            }
        }

        void addTombstones(Set<String> keys) {
            manifest.addTombstones(keys);
            persistManifest();
        }

        Set<String> getTombstones() {
            return manifest.getTombstones();
        }

        ConcurrentHashMap<String, LanceFragment> getFragments() {
            return fragments;
        }

        ConcurrentHashMap<String, FragmentLocation> getKeyIndex() {
            return keyIndex;
        }

        int totalEntryCount() {
            return Math.max(0, keyIndex.size() - manifest.getTombstones().size());
        }

        ColdIvfIndex getOrCreateIvfIndex(int dimension, int nprobe) {
            ColdIvfIndex idx = ivfIndex;
            if (idx == null) {
                synchronized (this) {
                    idx = ivfIndex;
                    if (idx == null) {
                        idx = new ColdIvfIndex(dimension, nprobe);
                        ivfIndex = idx;
                    }
                }
            }
            return idx;
        }

        void markIvfDirty() {
            ivfDirty = true;
        }

        boolean isIvfDirty() {
            return ivfDirty;
        }

        void clearIvfDirty() {
            ivfDirty = false;
        }

        void persistManifest() {
            Path manifestPath = nsDir.resolve("manifest.bin");
            try {
                Files.createDirectories(nsDir);
            } catch (IOException e) {
                log.warn("Failed to create namespace dir: {}", nsDir, e);
                return;
            }
            manifest.persist(manifestPath);
        }

        @Override
        public void close() {
            for (LanceFragment fragment : fragments.values()) {
                try {
                    fragment.close();
                } catch (Exception e) {
                    log.debug("Failed to close Lance fragment", e);
                }
            }
        }
    }

    private record IvfSnapshot(float[][] centroids, List<List<String>> clusters, int vectorCount) {}

    static final class ColdIvfIndex {

        private static final long KMEANS_SEED = 42L;
        private static final int KMEANS_RESTARTS = 2;
        private static final int KMEANS_MAX_ITER = 12;

        private final int dimension;
        private final int nprobe;
        private volatile IvfSnapshot snapshot;

        ColdIvfIndex(int dimension, int nprobe) {
            this.dimension = dimension;
            this.nprobe = Math.max(1, nprobe);
        }

        boolean isBuilt() {
            return snapshot != null;
        }

        int builtVectorCount() {
            IvfSnapshot snap = snapshot;
            return snap == null ? 0 : snap.vectorCount();
        }

        synchronized void build(List<float[]> vectors, List<String> keys) {
            int n = vectors.size();
            if (n == 0) {
                snapshot = null;
                return;
            }
            int nlist = Math.max(1, (int) Math.sqrt(n));
            if (nlist > n) nlist = n;

            float[][] centroids = kmeans(vectors, nlist);
            List<List<String>> clusters = new ArrayList<>(nlist);
            for (int i = 0; i < nlist; i++) clusters.add(new ArrayList<>());
            for (int i = 0; i < n; i++) {
                int c = nearestCentroid(vectors.get(i), centroids);
                clusters.get(c).add(keys.get(i));
            }
            snapshot = new IvfSnapshot(centroids, clusters, n);
        }

        List<VectorSearchResult> search(float[] query, int topK, NamespaceState ns) {
            IvfSnapshot snap = snapshot;
            if (snap == null) return null;
            float[][] centroids = snap.centroids();
            List<List<String>> clusters = snap.clusters();
            if (centroids == null || clusters == null) return null;

            Set<String> tombstones = ns.getTombstones();
            int probeCount = Math.min(nprobe, centroids.length);
            int[] probeIdx = nearestCentroids(query, centroids, probeCount);

            PriorityQueue<VectorSearchResult> heap =
                    new PriorityQueue<>(Comparator.comparingDouble(VectorSearchResult::score));

            for (int ci : probeIdx) {
                List<String> cluster = clusters.get(ci);
                for (String key : cluster) {
                    if (tombstones.contains(key)) continue;
                    FragmentLocation loc = ns.getKeyIndex().get(key);
                    if (loc == null) continue;
                    LanceFragment fragment = ns.getFragments().get(loc.fragmentId());
                    if (fragment == null) continue;
                    float[] vector = fragment.getVector(loc.rowIndex());
                    if (vector == null) continue;

                    float score = cosineSimilarity(query, vector);
                    VectorSearchResult result =
                            new VectorSearchResult(
                                    key, fragment.getL0Abstract(loc.rowIndex()), score);

                    if (heap.size() < topK) {
                        heap.offer(result);
                    } else if (!heap.isEmpty() && score > heap.peek().score()) {
                        heap.poll();
                        heap.offer(result);
                    }
                }
            }

            List<VectorSearchResult> results = new ArrayList<>(heap);
            results.sort(Comparator.comparingDouble(VectorSearchResult::score).reversed());
            return results;
        }

        private float[][] kmeans(List<float[]> vectors, int k) {
            float[][] best = null;
            double bestInertia = Double.MAX_VALUE;
            for (int restart = 0; restart < KMEANS_RESTARTS; restart++) {
                float[][] centroids =
                        initCentroidsKmeansPlusPlus(
                                vectors, k, new Random(KMEANS_SEED * 31L + restart));
                double inertia = runLloydIterations(vectors, k, centroids);
                if (inertia < bestInertia) {
                    bestInertia = inertia;
                    best = centroids;
                }
            }
            return best;
        }

        private float[][] initCentroidsKmeansPlusPlus(List<float[]> vectors, int k, Random rng) {
            int dim = dimension;
            float[][] centroids = new float[k][dim];
            centroids[0] = vectors.get(rng.nextInt(vectors.size())).clone();

            float[] minDistSq = new float[vectors.size()];
            Arrays.fill(minDistSq, Float.MAX_VALUE);

            for (int c = 1; c < k; c++) {
                double totalDist = 0;
                for (int i = 0; i < vectors.size(); i++) {
                    float d = distanceSq(vectors.get(i), centroids[c - 1]);
                    if (d < minDistSq[i]) minDistSq[i] = d;
                    totalDist += minDistSq[i];
                }
                double threshold = rng.nextDouble() * totalDist;
                double cumulative = 0;
                int nextIdx = 0;
                for (int i = 0; i < vectors.size(); i++) {
                    cumulative += minDistSq[i];
                    if (cumulative >= threshold) {
                        nextIdx = i;
                        break;
                    }
                }
                centroids[c] = vectors.get(nextIdx).clone();
            }
            return centroids;
        }

        private double runLloydIterations(List<float[]> vectors, int k, float[][] centroids) {
            int dim = dimension;
            int[] assignments = new int[vectors.size()];

            for (int iter = 0; iter < KMEANS_MAX_ITER; iter++) {
                boolean changed = false;
                for (int i = 0; i < vectors.size(); i++) {
                    int nearest = nearestCentroid(vectors.get(i), centroids);
                    if (nearest != assignments[i]) {
                        assignments[i] = nearest;
                        changed = true;
                    }
                }
                if (!changed) break;

                float[][] sums = new float[k][dim];
                int[] counts = new int[k];
                for (int i = 0; i < vectors.size(); i++) {
                    int ci = assignments[i];
                    counts[ci]++;
                    float[] v = vectors.get(i);
                    for (int d = 0; d < dim; d++) sums[ci][d] += v[d];
                }
                for (int ci = 0; ci < k; ci++) {
                    if (counts[ci] > 0) {
                        for (int d = 0; d < dim; d++) centroids[ci][d] = sums[ci][d] / counts[ci];
                    }
                }
            }

            double inertia = 0;
            for (int i = 0; i < vectors.size(); i++) {
                inertia += distanceSq(vectors.get(i), centroids[assignments[i]]);
            }
            return inertia;
        }

        private static int nearestCentroid(float[] vector, float[][] centroids) {
            int nearest = 0;
            float minD = Float.MAX_VALUE;
            for (int i = 0; i < centroids.length; i++) {
                float d = distanceSq(vector, centroids[i]);
                if (d < minD) {
                    minD = d;
                    nearest = i;
                }
            }
            return nearest;
        }

        private static int[] nearestCentroids(float[] query, float[][] centroids, int nprobe) {
            record CentroidDist(int index, float dist) {}
            PriorityQueue<CentroidDist> heap =
                    new PriorityQueue<>(Comparator.comparingDouble(CentroidDist::dist).reversed());
            for (int i = 0; i < centroids.length; i++) {
                float d = distanceSq(query, centroids[i]);
                CentroidDist cd = new CentroidDist(i, d);
                if (heap.size() < nprobe) {
                    heap.offer(cd);
                } else if (d < heap.peek().dist()) {
                    heap.poll();
                    heap.offer(cd);
                }
            }
            int[] result = new int[heap.size()];
            int idx = 0;
            for (CentroidDist cd : heap) result[idx++] = cd.index();
            return result;
        }

        private static float distanceSq(float[] a, float[] b) {
            float sum = 0;
            for (int i = 0; i < a.length; i++) {
                float diff = a[i] - b[i];
                sum += diff * diff;
            }
            return sum;
        }
    }
}
