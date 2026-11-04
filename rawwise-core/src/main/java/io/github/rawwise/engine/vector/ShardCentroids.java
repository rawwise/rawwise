package io.github.rawwise.engine.vector;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public record ShardCentroids(
        String shardId,
        int nlist,
        float[][] centroids,
        long vectorCount,
        long version,
        long timestampMillis)
        implements Serializable {
    public ShardCentroids {
        if (shardId == null || shardId.isBlank())
            throw new IllegalArgumentException("shardId must not be blank");
        if (nlist <= 0) throw new IllegalArgumentException("nlist must be positive");
        if (centroids == null || centroids.length != nlist)
            throw new IllegalArgumentException("centroids length must equal nlist");
        if (version < 0) version = 0;
        if (timestampMillis <= 0) timestampMillis = System.currentTimeMillis();
    }

    public byte[] toBytes() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            byte[] shardBytes = shardId.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(shardBytes.length);
            dos.write(shardBytes);
            dos.writeInt(nlist);
            int dim = centroids.length > 0 ? centroids[0].length : 0;
            dos.writeInt(dim);
            for (float[] c : centroids) {
                for (float v : c) {
                    dos.writeFloat(v);
                }
            }
            dos.writeLong(vectorCount);
            dos.writeLong(version);
            dos.writeLong(timestampMillis);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize ShardCentroids", e);
        }
    }

    public static ShardCentroids fromBytes(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            int shardLen = dis.readInt();
            byte[] shardBytes = new byte[shardLen];
            dis.readFully(shardBytes);
            String shardId = new String(shardBytes, StandardCharsets.UTF_8);
            int nlist = dis.readInt();
            int dim = dis.readInt();
            float[][] centroids = new float[nlist][dim];
            for (int i = 0; i < nlist; i++) {
                for (int j = 0; j < dim; j++) {
                    centroids[i][j] = dis.readFloat();
                }
            }
            long vectorCount = dis.readLong();
            long version = dis.readLong();
            long timestampMillis = dis.readLong();
            return new ShardCentroids(
                    shardId, nlist, centroids, vectorCount, version, timestampMillis);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize ShardCentroids", e);
        }
    }

    public static final byte[] METASTORE_PREFIX =
            "shard-centroids/".getBytes(StandardCharsets.UTF_8);

    public static byte[] metastoreKey(String shardId) {
        byte[] prefix = METASTORE_PREFIX;
        byte[] suffix = shardId.getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[prefix.length + suffix.length];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        System.arraycopy(suffix, 0, key, prefix.length, suffix.length);
        return key;
    }
}
