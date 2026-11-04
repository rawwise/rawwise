package io.github.rawwise.engine.vector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

@Slf4j
@Getter
public non-sealed class IvfPqIndex implements ShardInternalIndex {

    private final IvfPqConfig config;
    private float[][] centroids;
    private float[][][] codebooks;
    private final List<List<IvfPqEntry>> invertedLists;
    private int vectorCount;
    private final Set<String> tombstones = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<String, VectorLocation> keyToLocation =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> keyToStorageRef = new ConcurrentHashMap<>();
    private volatile boolean initialized = false;

    public IvfPqIndex(IvfPqConfig config) {
        this.config = config;
        this.invertedLists = new ArrayList<>(config.nlist());
        for (int i = 0; i < config.nlist(); i++) {
            invertedLists.add(new ArrayList<>());
        }
    }

    @Override
    public int vectorCount() {
        return vectorCount;
    }

    public java.util.Set<String> entryKeys() {
        return java.util.Set.copyOf(keyToLocation.keySet());
    }

    public float[][] centroids() {
        if (centroids == null) return null;
        float[][] copy = new float[centroids.length][];
        for (int i = 0; i < centroids.length; i++) {
            copy[i] = centroids[i].clone();
        }
        return copy;
    }

    public float[][][] codebooks() {
        if (codebooks == null) return null;
        float[][][] copy = new float[codebooks.length][][];
        for (int i = 0; i < codebooks.length; i++) {
            copy[i] = new float[codebooks[i].length][];
            for (int j = 0; j < codebooks[i].length; j++) {
                copy[i][j] = codebooks[i][j].clone();
            }
        }
        return copy;
    }

    @Override
    public synchronized void initialize(List<float[]> vectors, List<String> keys) {
        if (initialized) throw new IllegalStateException("IvfPqIndex already initialized");
        if (vectors.isEmpty()) {
            initialized = true;
            return;
        }
        kmeans(vectors, config.nlist(), config.kmeansMaxIter());
        trainCodebooks(vectors, config.m());
        for (int i = 0; i < keys.size(); i++) {
            byte[] pqCode = encode(vectors.get(i));
            IvfPqEntry entry = new IvfPqEntry(keys.get(i), pqCode, VectorLocation.WARM, "");
            addEntry(entry);
            keyToLocation.put(keys.get(i), VectorLocation.WARM);
            keyToStorageRef.put(keys.get(i), "");
        }
        initialized = true;
    }

    @Override
    public synchronized void insert(String entryKey, float[] vector) {
        if (codebooks == null) return;
        byte[] pqCode = encode(vector);
        IvfPqEntry entry =
                new IvfPqEntry(
                        entryKey,
                        pqCode,
                        keyToLocation.getOrDefault(entryKey, VectorLocation.WARM),
                        keyToStorageRef.getOrDefault(entryKey, ""));
        addEntry(entry);
        keyToLocation.putIfAbsent(entryKey, VectorLocation.WARM);
        keyToStorageRef.putIfAbsent(entryKey, "");
    }

    @Override
    public List<SearchCandidate> search(float[] query, int topK) {
        if (centroids == null || codebooks == null) return List.of();

        int m = config.m();
        int subDim = config.dimension() / m;
        int nprobe = config.nprobe();

        float[][] distTable = new float[m][256];
        for (int sub = 0; sub < m; sub++) {
            int offset = sub * subDim;
            for (int ci = 0; ci < 256; ci++) {
                float d = 0;
                for (int sd = 0; sd < subDim; sd++) {
                    float diff = query[offset + sd] - codebooks[sub][ci][sd];
                    d += diff * diff;
                }
                distTable[sub][ci] = d;
            }
        }

        int[] nearestCentroids = findTopNprobeCentroids(query, nprobe);

        PriorityQueue<SearchCandidate> heap =
                new PriorityQueue<>(
                        Comparator.comparingDouble(SearchCandidate::approximateDistance)
                                .reversed());

        for (int centroidIdx : nearestCentroids) {
            for (IvfPqEntry entry : invertedLists.get(centroidIdx)) {
                if (tombstones.contains(entry.entryKey())) continue;

                float dist = 0;
                byte[] pqCode = entry.pqCode();
                for (int sub = 0; sub < m; sub++) {
                    dist += distTable[sub][pqCode[sub] & 0xFF];
                }

                SearchCandidate candidate = new SearchCandidate(entry.entryKey(), dist);
                if (heap.size() < topK) {
                    heap.offer(candidate);
                } else if (dist < heap.peek().approximateDistance()) {
                    heap.poll();
                    heap.offer(candidate);
                }
            }
        }

        List<SearchCandidate> results = new ArrayList<>(heap);
        results.sort(Comparator.comparingDouble(SearchCandidate::approximateDistance));
        return results;
    }

    @Override
    public void updateLocation(String entryKey, VectorLocation location, String storageRef) {
        keyToLocation.put(entryKey, location);
        keyToStorageRef.put(entryKey, storageRef);
    }

    @Override
    public VectorLocation getLocation(String entryKey) {
        return keyToLocation.get(entryKey);
    }

    @Override
    public String getStorageRef(String entryKey) {
        return keyToStorageRef.get(entryKey);
    }

    @Override
    public synchronized void markTombstone(String entryKey) {
        tombstones.add(entryKey);
        for (List<IvfPqEntry> list : invertedLists) {
            list.removeIf(e -> entryKey.equals(e.entryKey()));
        }
        vectorCount = Math.max(0, vectorCount - 1);
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public float[] getVector(String entryKey) {
        if (codebooks == null) return null;
        for (List<IvfPqEntry> list : invertedLists) {
            for (IvfPqEntry entry : list) {
                if (entryKey.equals(entry.entryKey()) && !tombstones.contains(entryKey)) {
                    return decodePq(entry.pqCode());
                }
            }
        }
        return null;
    }

    private float[][] initCentroidsKmeansPP(List<float[]> vectors, int k, Random rng) {
        int dim = config.dimension();
        float[][] init = new float[k][dim];

        int firstIdx = rng.nextInt(vectors.size());
        init[0] = vectors.get(firstIdx).clone();

        float[] minDistSq = new float[vectors.size()];
        Arrays.fill(minDistSq, Float.MAX_VALUE);

        for (int c = 1; c < k; c++) {
            double totalDist = 0;
            for (int i = 0; i < vectors.size(); i++) {
                float d = distanceSq(vectors.get(i), init[c - 1]);
                if (d < minDistSq[i]) {
                    minDistSq[i] = d;
                }
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
            init[c] = vectors.get(nextIdx).clone();
        }

        return init;
    }

    public void kmeans(List<float[]> vectors, int k, int maxIter) {
        if (vectors.isEmpty()) return;

        int restarts = config.restarts();
        float[][] bestCentroids = null;
        double bestInertia = Double.MAX_VALUE;

        for (int restart = 0; restart < restarts; restart++) {
            Random rng = new Random(config.seed() * 31 + restart);
            float[][] c = initCentroidsKmeansPP(vectors, k, rng);
            double inertia = runKmeansIterations(vectors, k, maxIter, c);
            if (inertia < bestInertia) {
                bestInertia = inertia;
                bestCentroids = c;
            }
        }

        this.centroids = bestCentroids;
        log.info("K-Means trained: k={}, restarts={}, bestInertia={}", k, restarts, bestInertia);
    }

    private double runKmeansIterations(List<float[]> vectors, int k, int maxIter, float[][] c) {
        int dim = config.dimension();
        int[] assignments = new int[vectors.size()];

        for (int iter = 0; iter < maxIter; iter++) {
            boolean changed = false;

            for (int i = 0; i < vectors.size(); i++) {
                int nearest = findNearestIn(vectors.get(i), c);
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
                for (int d = 0; d < dim; d++) {
                    sums[ci][d] += vectors.get(i)[d];
                }
            }

            for (int ci = 0; ci < k; ci++) {
                if (counts[ci] > 0) {
                    for (int d = 0; d < dim; d++) {
                        c[ci][d] = sums[ci][d] / counts[ci];
                    }
                }
            }
        }

        double inertia = 0;
        for (int i = 0; i < vectors.size(); i++) {
            inertia += distanceSq(vectors.get(i), c[assignments[i]]);
        }
        return inertia;
    }

    public void trainCodebooks(List<float[]> vectors, int m) {
        if (vectors.isEmpty()) return;

        int dim = config.dimension();
        int subDim = dim / m;
        this.codebooks = new float[m][256][subDim];

        for (int sub = 0; sub < m; sub++) {
            int offset = sub * subDim;
            List<float[]> subVectors = new ArrayList<>(vectors.size());
            for (float[] v : vectors) {
                float[] sv = new float[subDim];
                System.arraycopy(v, offset, sv, 0, subDim);
                subVectors.add(sv);
            }
            codebooks[sub] =
                    trainSubKmeans(
                            subVectors,
                            256,
                            config.kmeansMaxIter(),
                            subDim,
                            config.seed() * 37 + sub);
        }
    }

    private float[][] trainSubKmeans(
            List<float[]> vectors, int k, int maxIter, int subDim, long subSeed) {
        Random rng = new Random(subSeed);
        float[][] c = new float[k][subDim];
        int firstIdx = rng.nextInt(vectors.size());
        c[0] = vectors.get(firstIdx).clone();

        float[] minDistSq = new float[vectors.size()];
        Arrays.fill(minDistSq, Float.MAX_VALUE);

        for (int ci = 1; ci < k; ci++) {
            double totalDist = 0;
            for (int i = 0; i < vectors.size(); i++) {
                float d = distanceSq(vectors.get(i), c[ci - 1]);
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
            c[ci] = vectors.get(nextIdx).clone();
        }

        int[] assignments = new int[vectors.size()];
        for (int iter = 0; iter < maxIter; iter++) {
            boolean changed = false;
            for (int i = 0; i < vectors.size(); i++) {
                int nearest = findNearestIn(vectors.get(i), c);
                if (nearest != assignments[i]) {
                    assignments[i] = nearest;
                    changed = true;
                }
            }
            if (!changed) break;

            float[][] sums = new float[k][subDim];
            int[] counts = new int[k];
            for (int i = 0; i < vectors.size(); i++) {
                int ci = assignments[i];
                counts[ci]++;
                for (int d = 0; d < subDim; d++) {
                    sums[ci][d] += vectors.get(i)[d];
                }
            }
            for (int ci = 0; ci < k; ci++) {
                if (counts[ci] > 0) {
                    for (int d = 0; d < subDim; d++) {
                        c[ci][d] = sums[ci][d] / counts[ci];
                    }
                }
            }
        }

        return c;
    }

    public byte[] encode(float[] vector) {
        int m = config.m();
        int subDim = config.dimension() / m;
        byte[] code = new byte[m];

        for (int sub = 0; sub < m; sub++) {
            int offset = sub * subDim;
            int nearest = 0;
            float minD = Float.MAX_VALUE;
            for (int ci = 0; ci < 256; ci++) {
                float dist = 0;
                for (int sd = 0; sd < subDim; sd++) {
                    float diff = vector[offset + sd] - codebooks[sub][ci][sd];
                    dist += diff * diff;
                }
                if (dist < minD) {
                    minD = dist;
                    nearest = ci;
                }
            }
            code[sub] = (byte) nearest;
        }

        return code;
    }

    private int[] findTopNprobeCentroids(float[] query, int nprobe) {
        int limit = Math.min(nprobe, centroids.length);
        record CentroidDist(int index, float dist) {}

        PriorityQueue<CentroidDist> heap =
                new PriorityQueue<>(Comparator.comparingDouble(CentroidDist::dist).reversed());

        for (int i = 0; i < centroids.length; i++) {
            float d = distanceSq(query, centroids[i]);
            CentroidDist cd = new CentroidDist(i, d);
            if (heap.size() < limit) {
                heap.offer(cd);
            } else if (d < heap.peek().dist()) {
                heap.poll();
                heap.offer(cd);
            }
        }

        int[] result = new int[heap.size()];
        int idx = 0;
        for (CentroidDist cd : heap) {
            result[idx++] = cd.index();
        }
        return result;
    }

    public void addEntry(IvfPqEntry entry) {
        float[] approx = decodePq(entry.pqCode());
        int centroidIdx = findNearestCentroid(approx);
        invertedLists.get(centroidIdx).add(entry);
        vectorCount++;
    }

    public int findNearestCentroid(float[] vector) {
        return findNearestIn(vector, centroids);
    }

    private int findNearestIn(float[] vector, float[][] c) {
        int nearest = 0;
        float minD = Float.MAX_VALUE;
        for (int i = 0; i < c.length; i++) {
            float d = distanceSq(vector, c[i]);
            if (d < minD) {
                minD = d;
                nearest = i;
            }
        }
        return nearest;
    }

    public float[] decodePq(byte[] pqCode) {
        int m = config.m();
        int subDim = config.dimension() / m;
        float[] reconstructed = new float[config.dimension()];
        for (int sub = 0; sub < m; sub++) {
            int codeIdx = pqCode[sub] & 0xFF;
            System.arraycopy(codebooks[sub][codeIdx], 0, reconstructed, sub * subDim, subDim);
        }
        return reconstructed;
    }

    private IvfPqIndex(
            IvfPqConfig config,
            float[][] centroids,
            float[][][] codebooks,
            List<List<IvfPqEntry>> invertedLists,
            int vectorCount,
            Set<String> tombstones) {
        this.config = config;
        this.centroids = centroids;
        this.codebooks = codebooks;
        this.invertedLists = invertedLists;
        this.vectorCount = vectorCount;
        this.tombstones.addAll(tombstones);
    }

    public void persistToLmdb(Path indexPath) throws IOException {
        Files.createDirectories(indexPath);
        Env<ByteBuffer> env =
                Env.create()
                        .setMapSize(256L * 1024L * 1024L)
                        .setMaxDbs(8)
                        .setMaxReaders(126)
                        .open(indexPath.toFile(), EnvFlags.MDB_NOLOCK);

        try {
            Dbi<ByteBuffer> metaDbi = env.openDbi("ivf_meta", DbiFlags.MDB_CREATE);
            Dbi<ByteBuffer> centroidsDbi = env.openDbi("ivf_centroids", DbiFlags.MDB_CREATE);
            Dbi<ByteBuffer> codebooksDbi = env.openDbi("ivf_codebooks", DbiFlags.MDB_CREATE);
            Dbi<ByteBuffer> tombstonesDbi = env.openDbi("ivf_tombstones", DbiFlags.MDB_CREATE);

            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                persistMeta(metaDbi, txn);
                persistCentroids(centroidsDbi, txn);
                persistCodebooks(codebooksDbi, txn);
                persistClusters(env, txn);
                persistTombstones(tombstonesDbi, txn);
                txn.commit();
            }
        } finally {
            env.close();
        }
    }

    public static IvfPqIndex loadFromLmdb(Path indexPath) throws IOException {
        Env<ByteBuffer> env =
                Env.create()
                        .setMapSize(256L * 1024L * 1024L)
                        .setMaxDbs(8)
                        .setMaxReaders(126)
                        .open(indexPath.toFile(), EnvFlags.MDB_NOLOCK);

        try {
            Dbi<ByteBuffer> metaDbi = env.openDbi("ivf_meta");
            Dbi<ByteBuffer> centroidsDbi = env.openDbi("ivf_centroids");
            Dbi<ByteBuffer> codebooksDbi = env.openDbi("ivf_codebooks");
            Dbi<ByteBuffer> tombstonesDbi = env.openDbi("ivf_tombstones");

            try (Txn<ByteBuffer> txn = env.txnRead()) {
                IvfPqConfig config = loadMeta(metaDbi, txn);
                float[][] centroids =
                        loadCentroids(centroidsDbi, txn, config.nlist(), config.dimension());
                float[][][] codebooks =
                        loadCodebooks(codebooksDbi, txn, config.m(), config.dimension());
                List<List<IvfPqEntry>> invertedLists = loadClusters(env, txn, config.nlist());
                int vectorCount = loadVectorCount(metaDbi, txn);
                Set<String> tombstones = loadTombstones(tombstonesDbi, txn);

                IvfPqIndex index =
                        new IvfPqIndex(
                                config,
                                centroids,
                                codebooks,
                                invertedLists,
                                vectorCount,
                                tombstones);
                index.initialized = true;
                return index;
            }
        } finally {
            env.close();
        }
    }

    private void persistMeta(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(config.dimension());
            dos.writeInt(config.nlist());
            dos.writeInt(config.m());
            dos.writeInt(config.nprobe());
            dos.writeInt(config.retrainThreshold());
            dos.writeInt(config.kmeansMaxIter());
            dos.writeInt(config.mergeThreshold());
            dos.writeLong(config.seed());
            dos.writeInt(config.restarts());
            dos.writeInt(vectorCount);
        }
        byte[] data = baos.toByteArray();
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf = ByteBuffer.allocateDirect(data.length).order(ByteOrder.LITTLE_ENDIAN);
        valBuf.put(data).flip();
        dbi.put(txn, keyBuf, valBuf);
    }

    private static IvfPqConfig loadMeta(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn)
            throws IOException {
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf = dbi.get(txn, keyBuf);
        byte[] data = new byte[valBuf.remaining()];
        valBuf.get(data);
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            return new IvfPqConfig(
                    dis.readInt(),
                    dis.readInt(),
                    dis.readInt(),
                    dis.readInt(),
                    dis.readInt(),
                    dis.readInt(),
                    dis.readInt(),
                    dis.readLong(),
                    dis.readInt());
        }
    }

    private static int loadVectorCount(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn)
            throws IOException {
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf = dbi.get(txn, keyBuf);
        byte[] data = new byte[valBuf.remaining()];
        valBuf.get(data);
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            dis.readInt();
            dis.readInt();
            dis.readInt();
            dis.readInt();
            dis.readInt();
            dis.readInt();
            dis.readInt();
            dis.readLong();
            dis.readInt();
            return dis.readInt();
        }
    }

    private void persistCentroids(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn) {
        int totalFloats = config.nlist() * config.dimension();
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf =
                ByteBuffer.allocateDirect(totalFloats * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < config.nlist(); i++) {
            for (int d = 0; d < config.dimension(); d++) {
                valBuf.putFloat(centroids[i][d]);
            }
        }
        valBuf.flip();
        dbi.put(txn, keyBuf, valBuf);
    }

    private static float[][] loadCentroids(
            Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn, int nlist, int dimension) {
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf = dbi.get(txn, keyBuf).order(ByteOrder.LITTLE_ENDIAN);
        float[][] centroids = new float[nlist][dimension];
        for (int i = 0; i < nlist; i++) {
            for (int d = 0; d < dimension; d++) {
                centroids[i][d] = valBuf.getFloat();
            }
        }
        return centroids;
    }

    private void persistCodebooks(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn) {
        int m = config.m();
        int subDim = config.dimension() / m;
        int totalFloats = m * 256 * subDim;
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf =
                ByteBuffer.allocateDirect(totalFloats * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int sub = 0; sub < m; sub++) {
            for (int ci = 0; ci < 256; ci++) {
                for (int sd = 0; sd < subDim; sd++) {
                    valBuf.putFloat(codebooks[sub][ci][sd]);
                }
            }
        }
        valBuf.flip();
        dbi.put(txn, keyBuf, valBuf);
    }

    private static float[][][] loadCodebooks(
            Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn, int m, int dimension) {
        int subDim = dimension / m;
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf = dbi.get(txn, keyBuf).order(ByteOrder.LITTLE_ENDIAN);
        float[][][] codebooks = new float[m][256][subDim];
        for (int sub = 0; sub < m; sub++) {
            for (int ci = 0; ci < 256; ci++) {
                for (int sd = 0; sd < subDim; sd++) {
                    codebooks[sub][ci][sd] = valBuf.getFloat();
                }
            }
        }
        return codebooks;
    }

    private void persistClusters(Env<ByteBuffer> env, Txn<ByteBuffer> txn) throws IOException {
        for (int n = 0; n < config.nlist(); n++) {
            String dbName = "ivf_cluster_" + n;
            Dbi<ByteBuffer> dbi = env.openDbi(dbName, DbiFlags.MDB_CREATE);
            List<IvfPqEntry> cluster = invertedLists.get(n);
            for (int i = 0; i < cluster.size(); i++) {
                IvfPqEntry entry = cluster.get(i);
                ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
                keyBuf.putInt(i).flip();

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (DataOutputStream dos = new DataOutputStream(baos)) {
                    dos.writeUTF(entry.entryKey());
                    dos.writeInt(entry.pqCode().length);
                    dos.write(entry.pqCode());
                    dos.writeUTF(entry.location().name());
                    dos.writeUTF(entry.storageRef());
                }
                byte[] data = baos.toByteArray();
                ByteBuffer valBuf =
                        ByteBuffer.allocateDirect(data.length).order(ByteOrder.LITTLE_ENDIAN);
                valBuf.put(data).flip();
                dbi.put(txn, keyBuf, valBuf);
            }
        }
    }

    private static List<List<IvfPqEntry>> loadClusters(
            Env<ByteBuffer> env, Txn<ByteBuffer> txn, int nlist) throws IOException {
        List<List<IvfPqEntry>> lists = new ArrayList<>(nlist);
        for (int n = 0; n < nlist; n++) {
            String dbName = "ivf_cluster_" + n;
            Dbi<ByteBuffer> dbi = env.openDbi(dbName);
            List<IvfPqEntry> cluster = new ArrayList<>();
            ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
            for (int i = 0; ; i++) {
                keyBuf.clear().putInt(i).flip();
                ByteBuffer valBuf = dbi.get(txn, keyBuf);
                if (valBuf == null) break;
                byte[] data = new byte[valBuf.remaining()];
                valBuf.get(data);
                try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
                    String entryKey = dis.readUTF();
                    int pqLen = dis.readInt();
                    byte[] pqCode = new byte[pqLen];
                    dis.readFully(pqCode);
                    VectorLocation location = VectorLocation.valueOf(dis.readUTF());
                    String storageRef = dis.readUTF();
                    cluster.add(new IvfPqEntry(entryKey, pqCode, location, storageRef));
                }
            }
            lists.add(cluster);
        }
        return lists;
    }

    private void persistTombstones(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(tombstones.size());
            for (String key : tombstones) {
                dos.writeUTF(key);
            }
        }
        byte[] data = baos.toByteArray();
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf = ByteBuffer.allocateDirect(data.length).order(ByteOrder.LITTLE_ENDIAN);
        valBuf.put(data).flip();
        dbi.put(txn, keyBuf, valBuf);
    }

    private static Set<String> loadTombstones(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn)
            throws IOException {
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
        keyBuf.putInt(0).flip();
        ByteBuffer valBuf = dbi.get(txn, keyBuf);
        if (valBuf == null) return Set.of();
        byte[] data = new byte[valBuf.remaining()];
        valBuf.get(data);
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data))) {
            int count = dis.readInt();
            Set<String> tombstones = new HashSet<>(count);
            for (int i = 0; i < count; i++) {
                tombstones.add(dis.readUTF());
            }
            return tombstones;
        }
    }

    private float distanceSq(float[] a, float[] b) {
        float sum = 0;
        for (int i = 0; i < a.length; i++) {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }
}
