package io.github.rawwise.engine.vector;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LanceManifest {

    private static final int MAGIC = 0x4C4D4E46;

    @Getter private volatile int version;
    private final List<String> fragmentIds = new ArrayList<>();
    private final Set<String> tombstones = ConcurrentHashMap.newKeySet();
    @Getter private volatile int dimension;
    @Getter private volatile int totalRowCount;
    private final List<String> columnNames = new ArrayList<>();

    public LanceManifest() {
        this.version = 0;
    }

    public synchronized int addFragment(String fragmentId, int rowCount) {
        fragmentIds.add(fragmentId);
        if (rowCount > 0) totalRowCount += rowCount;
        return ++version;
    }

    public synchronized void setSchema(int dimension, List<String> columns) {
        this.dimension = dimension;
        this.columnNames.clear();
        if (columns != null) this.columnNames.addAll(columns);
    }

    public synchronized List<String> getColumnNames() {
        return List.copyOf(columnNames);
    }

    public synchronized int addTombstones(Set<String> keys) {
        tombstones.addAll(keys);
        return ++version;
    }

    public synchronized int removeTombstones(Set<String> keys) {
        int removed = 0;
        for (String key : keys) {
            if (tombstones.remove(key)) removed++;
        }
        if (removed > 0) return ++version;
        return version;
    }

    public synchronized List<String> getFragmentIds() {
        return List.copyOf(fragmentIds);
    }

    public Set<String> getTombstones() {
        return Set.copyOf(tombstones);
    }

    public void persist(Path manifestPath) {
        try {
            Files.createDirectories(manifestPath.getParent());
            Path tmpFile = manifestPath.resolveSibling(manifestPath.getFileName() + ".tmp");
            try (DataOutputStream dos =
                    new DataOutputStream(
                            new BufferedOutputStream(Files.newOutputStream(tmpFile), 64 * 1024))) {
                dos.writeInt(MAGIC);
                List<String> snapshotFragmentIds;
                Set<String> snapshotTombstones;
                List<String> snapshotColumnNames;
                int snapshotDimension;
                int snapshotTotalRowCount;
                synchronized (this) {
                    dos.writeInt(version);
                    snapshotFragmentIds = List.copyOf(fragmentIds);
                    snapshotTombstones = Set.copyOf(tombstones);
                    snapshotColumnNames = List.copyOf(columnNames);
                    snapshotDimension = dimension;
                    snapshotTotalRowCount = totalRowCount;
                }
                dos.writeInt(snapshotFragmentIds.size());
                for (String id : snapshotFragmentIds) {
                    writeLengthPrefixedString(dos, id);
                }
                dos.writeInt(snapshotTombstones.size());
                for (String key : snapshotTombstones) {
                    writeLengthPrefixedString(dos, key);
                }
                dos.writeInt(snapshotDimension);
                dos.writeInt(snapshotTotalRowCount);
                dos.writeInt(snapshotColumnNames.size());
                for (String col : snapshotColumnNames) {
                    writeLengthPrefixedString(dos, col);
                }
            }
            try {
                Files.move(
                        tmpFile,
                        manifestPath,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicMoveFailed) {
                Files.move(tmpFile, manifestPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to persist Lance manifest: " + manifestPath, e);
        }
    }

    public static LanceManifest recover(Path manifestPath) {
        LanceManifest manifest = new LanceManifest();
        if (!Files.exists(manifestPath)) {
            return manifest;
        }
        try (DataInputStream dis =
                new DataInputStream(
                        new BufferedInputStream(Files.newInputStream(manifestPath), 64 * 1024))) {
            int magic = dis.readInt();
            if (magic != MAGIC) {
                throw new IOException("Invalid manifest magic: 0x" + Integer.toHexString(magic));
            }
            manifest.version = dis.readInt();
            int numFragments = dis.readInt();
            for (int i = 0; i < numFragments; i++) {
                manifest.fragmentIds.add(readLengthPrefixedString(dis));
            }
            int numTombstones = dis.readInt();
            for (int i = 0; i < numTombstones; i++) {
                manifest.tombstones.add(readLengthPrefixedString(dis));
            }
            try {
                manifest.dimension = dis.readInt();
                manifest.totalRowCount = dis.readInt();
                int numColumns = dis.readInt();
                for (int i = 0; i < numColumns; i++) {
                    manifest.columnNames.add(readLengthPrefixedString(dis));
                }
            } catch (java.io.EOFException eof) {
                // legacy manifest without schema/rowCount fields
            }
            log.debug(
                    "Recovered Lance manifest: version={}, fragments={}, tombstones={}, dimension={}, rowCount={}",
                    manifest.version,
                    numFragments,
                    numTombstones,
                    manifest.dimension,
                    manifest.totalRowCount);
        } catch (IOException e) {
            log.warn("Failed to recover Lance manifest from {}: {}", manifestPath, e.getMessage());
        }
        return manifest;
    }

    private static void writeLengthPrefixedString(DataOutputStream dos, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    private static String readLengthPrefixedString(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
