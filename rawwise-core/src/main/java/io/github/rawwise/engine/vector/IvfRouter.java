package io.github.rawwise.engine.vector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IvfRouter {

    private final ConcurrentHashMap<String, ShardCentroids> shardCentroidsMap =
            new ConcurrentHashMap<>();
    private volatile int defaultNprobe = 4;

    public void registerShard(ShardCentroids centroids) {
        shardCentroidsMap.put(centroids.shardId(), centroids);
        log.info(
                "Registered shard centroids: shardId={}, nlist={}, vectorCount={}, version={}",
                centroids.shardId(),
                centroids.nlist(),
                centroids.vectorCount(),
                centroids.version());
    }

    public void unregisterShard(String shardId) {
        shardCentroidsMap.remove(shardId);
        log.info("Unregistered shard centroids: shardId={}", shardId);
    }

    public List<String> route(float[] queryVector, int topK) {
        return route(queryVector, topK, defaultNprobe);
    }

    public List<String> route(float[] queryVector, int topK, int nprobe) {
        Map<String, ShardCentroids> snapshot = Map.copyOf(shardCentroidsMap);
        if (snapshot.isEmpty()) return List.of();

        return routeInSnapshot(queryVector, nprobe, snapshot);
    }

    public List<String> routeActive(
            float[] queryVector, int topK, int nprobe, List<String> activeShards) {
        Map<String, ShardCentroids> snapshot = Map.copyOf(shardCentroidsMap);
        if (snapshot.isEmpty()) return List.of();

        Map<String, ShardCentroids> active = new java.util.HashMap<>();
        for (String shardId : activeShards) {
            ShardCentroids sc = snapshot.get(shardId);
            if (sc != null) active.put(shardId, sc);
        }
        if (active.isEmpty()) return List.of();

        return routeInSnapshot(queryVector, nprobe, active);
    }

    private List<String> routeInSnapshot(
            float[] queryVector, int nprobe, Map<String, ShardCentroids> snapshot) {

        record ShardDistance(String shardId, float minDist) {}

        List<ShardDistance> distances = new ArrayList<>();
        for (ShardCentroids sc : snapshot.values()) {
            float minDist = Float.MAX_VALUE;
            for (float[] centroid : sc.centroids()) {
                float d = cosineDistance(queryVector, centroid);
                if (d < minDist) minDist = d;
            }
            distances.add(new ShardDistance(sc.shardId(), minDist));
        }

        distances.sort(Comparator.comparingDouble(ShardDistance::minDist));

        int shardCount = Math.min(nprobe, distances.size());
        List<String> targetShards = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            targetShards.add(distances.get(i).shardId());
        }
        return targetShards;
    }

    public ShardCentroids getShardCentroids(String shardId) {
        return shardCentroidsMap.get(shardId);
    }

    public List<ShardCentroids> allShardCentroids() {
        return List.copyOf(shardCentroidsMap.values());
    }

    public int shardCount() {
        return shardCentroidsMap.size();
    }

    public void setDefaultNprobe(int nprobe) {
        if (nprobe <= 0) throw new IllegalArgumentException("nprobe must be positive");
        this.defaultNprobe = nprobe;
    }

    public int defaultNprobe() {
        return defaultNprobe;
    }

    public List<String> mergeShards(List<String> shardIds, String newShardId) {
        List<float[]> allCentroids = new ArrayList<>();
        long totalVectors = 0;
        int maxNlist = 0;

        for (String shardId : shardIds) {
            ShardCentroids sc = shardCentroidsMap.get(shardId);
            if (sc == null) continue;
            for (float[] c : sc.centroids()) {
                allCentroids.add(c);
            }
            totalVectors += sc.vectorCount();
            maxNlist = Math.max(maxNlist, sc.nlist());
        }

        if (allCentroids.isEmpty()) return List.of();

        float[][] mergedCentroids = selectTopCentroids(allCentroids, maxNlist);
        long newVersion = System.currentTimeMillis();

        ShardCentroids newSc =
                new ShardCentroids(
                        newShardId,
                        mergedCentroids.length,
                        mergedCentroids,
                        totalVectors,
                        newVersion,
                        System.currentTimeMillis());
        shardCentroidsMap.put(newShardId, newSc);

        for (String shardId : shardIds) {
            shardCentroidsMap.remove(shardId);
        }

        log.info(
                "Merged {} shards into newShardId={}, totalVectors={}",
                shardIds.size(),
                newShardId,
                totalVectors);
        return shardIds;
    }

    public List<String> splitShard(String shardId, String newShardId1, String newShardId2) {
        ShardCentroids original = shardCentroidsMap.remove(shardId);
        if (original == null) return List.of();

        float[][] centroids = original.centroids();
        int half = centroids.length / 2;

        float[][] centroids1 = new float[half][];
        System.arraycopy(centroids, 0, centroids1, 0, half);
        float[][] centroids2 = new float[centroids.length - half][];
        System.arraycopy(centroids, half, centroids2, 0, centroids.length - half);

        long halfVectors = original.vectorCount() / 2;
        long newVersion = System.currentTimeMillis();

        ShardCentroids sc1 =
                new ShardCentroids(
                        newShardId1,
                        centroids1.length,
                        centroids1,
                        halfVectors,
                        newVersion,
                        System.currentTimeMillis());
        ShardCentroids sc2 =
                new ShardCentroids(
                        newShardId2,
                        centroids2.length,
                        centroids2,
                        original.vectorCount() - halfVectors,
                        newVersion,
                        System.currentTimeMillis());

        shardCentroidsMap.put(newShardId1, sc1);
        shardCentroidsMap.put(newShardId2, sc2);

        log.info("Split shard {} into {} and {}", shardId, newShardId1, newShardId2);
        return List.of(newShardId1, newShardId2);
    }

    private float[][] selectTopCentroids(List<float[]> allCentroids, int maxCount) {
        if (allCentroids.size() <= maxCount) {
            return allCentroids.toArray(new float[0][]);
        }

        float[] centroid = new float[allCentroids.get(0).length];
        for (float[] c : allCentroids) {
            for (int i = 0; i < c.length; i++) {
                centroid[i] += c[i];
            }
        }
        for (int i = 0; i < centroid.length; i++) {
            centroid[i] /= allCentroids.size();
        }

        allCentroids.sort(Comparator.comparingDouble(c -> cosineDistance(c, centroid)));
        return allCentroids.subList(0, maxCount).toArray(new float[0][]);
    }

    private static float cosineDistance(float[] a, float[] b) {
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        if (normA == 0 || normB == 0) return 1.0f;
        return 1.0f - dot / (float) (Math.sqrt(normA) * Math.sqrt(normB));
    }
}
