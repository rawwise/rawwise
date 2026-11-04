package io.github.rawwise.engine.vector;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FlatVectorColdStore implements VectorColdStore {

    private static final int MAGIC = 0x4E564331;
    private static final int VERSION = 2;

    private final Path basePath;
    private final int dimension;
    private final int maxEntriesPerNamespace;
    private final ConcurrentHashMap<String, NamespaceColdData> coldData = new ConcurrentHashMap<>();

    public FlatVectorColdStore(Path basePath, int dimension) {
        this(basePath, dimension, 100_000);
    }

    public FlatVectorColdStore(Path basePath, int dimension, int maxEntriesPerNamespace) {
        this.basePath = basePath;
        this.dimension = dimension;
        this.maxEntriesPerNamespace = maxEntriesPerNamespace;
        try {
            Files.createDirectories(basePath);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to create vector cold store base path: " + basePath, e);
        }
        recoverFromDisk();
    }

    @Override
    public void write(String namespace, String blockId, List<ColdVectorEntry> entries) {
        if (entries == null || entries.isEmpty()) return;

        NamespaceColdData nsData =
                coldData.computeIfAbsent(namespace, k -> new NamespaceColdData());
        if (nsData.entries.size() >= maxEntriesPerNamespace) {
            log.warn(
                    "Cold store namespace={} reached maxEntries={}, dropping {} new entries",
                    namespace,
                    maxEntriesPerNamespace,
                    entries.size());
            return;
        }

        Path nsDir = basePath.resolve("ns-" + namespace);
        try {
            Files.createDirectories(nsDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create namespace cold dir: " + nsDir, e);
        }

        Path filePath = nsDir.resolve(blockId + ".nvc");
        try (DataOutputStream dos =
                new DataOutputStream(
                        new BufferedOutputStream(Files.newOutputStream(filePath), 64 * 1024))) {
            dos.writeInt(MAGIC);
            dos.writeInt(VERSION);
            dos.writeInt(dimension);
            dos.writeInt(entries.size());
            for (ColdVectorEntry entry : entries) {
                writeLengthPrefixedString(dos, entry.entryKey());
                float[] emb = entry.embedding();
                for (float v : emb) {
                    dos.writeFloat(v);
                }
                writeLengthPrefixedString(
                        dos, entry.l0Abstract() != null ? entry.l0Abstract() : "");
            }
        } catch (IOException e) {
            try {
                Files.deleteIfExists(filePath);
            } catch (IOException ignored) {
            }
            throw new RuntimeException("Failed to write cold vector file: " + filePath, e);
        }

        nsData.addBlock(blockId, filePath);
        loadBlockData(nsData, blockId, filePath);

        log.info(
                "Wrote {} cold vector entries for namespace={}, blockId={}",
                entries.size(),
                namespace,
                blockId);
    }

    @Override
    public List<VectorSearchResult> search(String namespace, float[] queryEmbedding, int topK) {
        NamespaceColdData nsData = coldData.get(namespace);
        if (nsData == null || nsData.entries.isEmpty()) return List.of();

        List<VectorSearchResult> results = new ArrayList<>(nsData.entries.size());
        for (ColdVectorEntry entry : nsData.entries.values()) {
            float score = cosineSimilarity(queryEmbedding, entry.embedding());
            results.add(new VectorSearchResult(entry.entryKey(), entry.l0Abstract(), score));
        }
        results.sort(Comparator.comparingDouble(VectorSearchResult::score).reversed());
        return results.stream().limit(topK).toList();
    }

    @Override
    public void delete(String namespace, Collection<String> entryKeys) {
        NamespaceColdData nsData = coldData.get(namespace);
        if (nsData == null) return;
        for (String key : entryKeys) {
            nsData.entries.remove(key);
            nsData.tombstones.add(key);
        }
        persistTombstones(namespace, nsData);
    }

    @Override
    public boolean hasColdData(String namespace) {
        NamespaceColdData nsData = coldData.get(namespace);
        return nsData != null && !nsData.entries.isEmpty();
    }

    @Override
    public void close() {
        for (Map.Entry<String, NamespaceColdData> entry : coldData.entrySet()) {
            if (!entry.getValue().tombstones.isEmpty()) {
                persistTombstones(entry.getKey(), entry.getValue());
            }
        }
        coldData.clear();
    }

    /** Get all namespace names with persisted data. */
    public Set<String> getAllNamespaces() {
        return Set.copyOf(coldData.keySet());
    }

    /** Get all non-tombstoned entries for a namespace. */
    public List<ColdVectorEntry> getAllEntries(String namespace) {
        NamespaceColdData nsData = coldData.get(namespace);
        if (nsData == null) return null;
        return List.copyOf(nsData.entries.values());
    }

    private void recoverFromDisk() {
        if (!Files.isDirectory(basePath)) return;
        try (var stream = Files.list(basePath)) {
            for (Path nsDir : stream.toList()) {
                if (!Files.isDirectory(nsDir)) continue;
                String dirName = nsDir.getFileName().toString();
                if (!dirName.startsWith("ns-")) continue;
                String namespace = dirName.substring(3);
                NamespaceColdData nsData =
                        coldData.computeIfAbsent(namespace, k -> new NamespaceColdData());
                loadTombstones(nsDir, nsData);
                try (var files = Files.list(nsDir)) {
                    for (Path file : files.toList()) {
                        String fileName = file.getFileName().toString();
                        if (!fileName.endsWith(".nvc")) continue;
                        String blockId = fileName.substring(0, fileName.length() - 4);
                        nsData.addBlock(blockId, file);
                        loadBlockData(nsData, blockId, file);
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to recover cold vector data from disk", e);
        }
    }

    private void loadBlockData(NamespaceColdData nsData, String blockId, Path filePath) {
        try (DataInputStream dis =
                new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(filePath), 64 * 1024))) {
            int magic = dis.readInt();
            if (magic != MAGIC) throw new IOException("Invalid magic: " + magic);
            int version = dis.readInt();
            if (version != VERSION && version != 1)
                throw new IOException("Unsupported version: " + version);
            int dim = dis.readInt();
            if (dim != dimension)
                throw new IOException("Dimension mismatch: expected " + dimension + ", got " + dim);
            int count = dis.readInt();
            for (int i = 0; i < count; i++) {
                String entryKey = (version == 1) ? dis.readUTF() : readLengthPrefixedString(dis);
                float[] embedding = new float[dimension];
                for (int j = 0; j < dimension; j++) {
                    embedding[j] = dis.readFloat();
                }
                String l0Abstract = (version == 1) ? dis.readUTF() : readLengthPrefixedString(dis);
                if (!nsData.tombstones.contains(entryKey)) {
                    nsData.entries.put(
                            entryKey, new ColdVectorEntry(entryKey, embedding.clone(), l0Abstract));
                }
            }
            log.debug("Loaded {} cold vector entries from block={}", count, blockId);
        } catch (IOException e) {
            log.warn("Failed to load cold vector block {}: {}", blockId, e.getMessage());
        }
    }

    private void loadTombstones(Path nsDir, NamespaceColdData nsData) {
        Path tombstoneFile = nsDir.resolve("tombstones.bin");
        if (!Files.exists(tombstoneFile)) return;
        try (DataInputStream dis =
                new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(tombstoneFile), 64 * 1024))) {
            int count = dis.readInt();
            for (int i = 0; i < count; i++) {
                nsData.tombstones.add(readLengthPrefixedString(dis));
            }
            log.debug("Loaded {} tombstones for namespace", count);
        } catch (IOException e) {
            log.warn("Failed to load tombstones: {}", e.getMessage());
        }
    }

    private void persistTombstones(String namespace, NamespaceColdData nsData) {
        Path nsDir = basePath.resolve("ns-" + namespace);
        Path tombstoneFile = nsDir.resolve("tombstones.bin");
        try {
            Files.createDirectories(nsDir);
            Path tmpFile = nsDir.resolve("tombstones.bin.tmp");
            try (DataOutputStream dos =
                    new DataOutputStream(
                            new BufferedOutputStream(Files.newOutputStream(tmpFile), 64 * 1024))) {
                dos.writeInt(nsData.tombstones.size());
                for (String key : nsData.tombstones) {
                    writeLengthPrefixedString(dos, key);
                }
            }
            Files.move(
                    tmpFile,
                    tombstoneFile,
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                    java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.warn(
                    "Failed to persist tombstones for namespace={}: {}", namespace, e.getMessage());
        }
    }

    private void writeLengthPrefixedString(DataOutputStream dos, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private String readLengthPrefixedString(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private float cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) return 0f;
        float dot = 0, normA = 0, normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        float denom = (float) (Math.sqrt(normA) * Math.sqrt(normB));
        return denom == 0 ? 0f : dot / denom;
    }

    static class NamespaceColdData {
        final Map<String, ColdVectorEntry> entries = new ConcurrentHashMap<>();
        final Set<String> blockIds = ConcurrentHashMap.newKeySet();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();

        void addBlock(String blockId, Path filePath) {
            blockIds.add(blockId);
        }
    }
}
