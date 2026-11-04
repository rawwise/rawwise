package io.github.rawwise.engine.storage;

import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.governance.config.RawWiseConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageManager {

    private final Storage localStorage;
    private Storage hdfsStorage;
    private final String basePath;
    private final String hdfsBasePath;

    public StorageManager(String basePath, String hdfsBasePath, RawWiseConfig config) {
        this.basePath = basePath;
        this.hdfsBasePath = hdfsBasePath;
        this.localStorage =
                StorageRegistry.instance()
                        .getStorage(new Storage.StoragePath("file", basePath), config);
        if (hdfsBasePath != null && !hdfsBasePath.isBlank()) {
            try {
                this.hdfsStorage =
                        StorageRegistry.instance()
                                .getStorage(new Storage.StoragePath("hdfs", hdfsBasePath), config);
            } catch (Exception e) {
                log.warn(
                        "Failed to initialize HDFS storage, HDFS operations will be unavailable: {}",
                        e.getMessage());
            }
        }
    }

    public Storage localStorage() {
        return localStorage;
    }

    public Storage hdfsStorage() {
        return hdfsStorage;
    }

    public String basePath() {
        return basePath;
    }

    public String hdfsBasePath() {
        return hdfsBasePath;
    }

    public boolean hdfsAvailable() {
        return hdfsStorage != null;
    }

    public boolean localExists(String relativePath) throws IOException {
        return localStorage.exists(toLocalPath(relativePath));
    }

    public boolean localDirectoryExists(String absolutePath) throws IOException {
        try {
            Storage.StorageStatus status =
                    localStorage.status(new Storage.StoragePath("file", absolutePath));
            return status.exists() && status.directory();
        } catch (IOException e) {
            return false;
        }
    }

    public boolean hdfsExists(String relativePath) throws IOException {
        if (hdfsStorage == null) {
            log.debug("HDFS storage not available, hdfsExists returns false for: {}", relativePath);
            return false;
        }
        return hdfsStorage.exists(toHdfsPath(relativePath));
    }

    public void createLocalDirectories(String relativePath) throws IOException {
        localStorage.createDirectories(toLocalPath(relativePath));
    }

    public void createHdfsDirectories(String relativePath) throws IOException {
        if (hdfsStorage == null) {
            log.warn("HDFS storage not available, skipping directory creation: {}", relativePath);
            return;
        }
        hdfsStorage.createDirectories(toHdfsPath(relativePath));
    }

    public List<Storage.StoragePath> listLocal(String relativePath) throws IOException {
        return localStorage.list(toLocalPath(relativePath));
    }

    public List<Storage.StoragePath> listHdfs(String relativePath) throws IOException {
        if (hdfsStorage == null) {
            return List.of();
        }
        return hdfsStorage.list(toHdfsPath(relativePath));
    }

    public void deleteLocal(String relativePath) throws IOException {
        localStorage.delete(toLocalPath(relativePath));
    }

    public void deleteLocalRecursively(String relativePath) throws IOException {
        localStorage.deleteRecursively(toLocalPath(relativePath));
    }

    public void deleteHdfs(String relativePath) throws IOException {
        if (hdfsStorage == null) {
            log.warn("HDFS storage not available, skipping delete: {}", relativePath);
            return;
        }
        hdfsStorage.delete(toHdfsPath(relativePath));
    }

    public void deleteHdfsRecursively(String relativePath) throws IOException {
        if (hdfsStorage == null) {
            log.warn("HDFS storage not available, skipping recursive delete: {}", relativePath);
            return;
        }
        hdfsStorage.deleteRecursively(toHdfsPath(relativePath));
    }

    public List<String> listInstanceDirs(String tablePath) throws IOException {
        List<Storage.StoragePath> entries = localStorage.list(toLocalPath(tablePath));
        List<String> result = new ArrayList<>();
        for (Storage.StoragePath p : entries) {
            String name = extractFileName(p.location());
            if (!name.startsWith("inst-")) {
                continue;
            }
            if (name.endsWith(".lease") || name.endsWith(".lock")) {
                continue;
            }
            try {
                Storage.StorageStatus status = localStorage.status(p);
                if (!status.directory()) {
                    continue;
                }
            } catch (IOException e) {
                log.warn("Failed to stat entry {} under {}, skipping", name, tablePath, e);
                continue;
            }
            result.add(name);
        }
        result.sort(String::compareTo);
        return result;
    }

    public List<String> listAllInstanceDirs(String absoluteBasePath) throws IOException {
        List<Storage.StoragePath> topEntries =
                localStorage.list(new Storage.StoragePath("file", absoluteBasePath));
        List<String> allInstanceDirs = new ArrayList<>();
        for (Storage.StoragePath tableEntry : topEntries) {
            String tableName = extractFileName(tableEntry.location());
            if (tableName.startsWith(".") || tableName.equals("wal")) {
                continue;
            }
            try {
                Storage.StorageStatus status = localStorage.status(tableEntry);
                if (!status.directory()) {
                    continue;
                }
                List<Storage.StoragePath> subEntries = localStorage.list(tableEntry);
                for (Storage.StoragePath subEntry : subEntries) {
                    String name = extractFileName(subEntry.location());
                    if (!name.startsWith("inst-")) {
                        continue;
                    }
                    if (name.endsWith(".lease") || name.endsWith(".lock")) {
                        continue;
                    }
                    try {
                        Storage.StorageStatus subStatus = localStorage.status(subEntry);
                        if (!subStatus.directory()) {
                            continue;
                        }
                    } catch (IOException e) {
                        log.warn("Failed to stat entry {} under {}, skipping", name, tableName, e);
                        continue;
                    }
                    allInstanceDirs.add(tableName + "/" + name);
                }
            } catch (IOException e) {
                log.warn("Failed to list instances under {}", tableName, e);
            }
        }
        allInstanceDirs.sort(String::compareTo);
        return allInstanceDirs;
    }

    public List<String> listInstanceDirsForTable(String absoluteBasePath, String tableIdentifier)
            throws IOException {
        String tablePath = absoluteBasePath + "/" + tableIdentifier;
        if (!localDirectoryExists(tablePath)) {
            return List.of();
        }
        return listInstanceDirs(tableIdentifier);
    }

    public List<String> listWALFiles(String walPath) throws IOException {
        List<Storage.StoragePath> entries = localStorage.list(toLocalPath(walPath));
        return entries.stream()
                .filter(
                        p -> {
                            String name = extractFileName(p.location());
                            return name.endsWith(".parquet") || name.endsWith(".nwl");
                        })
                .map(p -> extractFileName(p.location()))
                .collect(Collectors.toList());
    }

    public Set<String> discoverLocalTables(String localBasePath) {
        Set<String> tables = new java.util.HashSet<>();
        try {
            List<Storage.StoragePath> entries =
                    localStorage.list(new Storage.StoragePath("file", localBasePath));
            for (Storage.StoragePath entry : entries) {
                String name = extractFileName(entry.location());
                if (!name.startsWith(".") && !name.equals("wal")) {
                    tables.add(name);
                }
            }
        } catch (IOException e) {
            log.warn("Failed to discover local tables from {}", localBasePath, e);
        }
        return tables;
    }

    public Set<String> discoverHdfsWALTables(String walHdfsDir) {
        return Set.of();
    }

    public void cleanupWALFiles(String walPath) {
        try {
            List<Storage.StoragePath> entries = localStorage.list(toLocalPath(walPath));
            for (Storage.StoragePath entry : entries) {
                String name = extractFileName(entry.location());
                if (name.endsWith(".parquet")
                        || name.endsWith(".nwl")
                        || name.endsWith(".nwl.tmp")) {
                    try {
                        localStorage.delete(entry);
                        log.debug("Deleted WAL file: {}", name);
                    } catch (IOException e) {
                        log.warn("Failed to delete WAL file: {}", name, e);
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to cleanup WAL files", e);
        }
    }

    public void downloadFromHdfs(String hdfsRelativePath, String localRelativePath)
            throws IOException {
        if (hdfsStorage == null) {
            log.warn(
                    "HDFS storage not available, skipping download from HDFS: {} -> {}",
                    hdfsRelativePath,
                    localRelativePath);
            return;
        }
        Storage.StoragePath srcPath = toHdfsPath(hdfsRelativePath);
        Storage.StoragePath dstPath = toLocalPath(localRelativePath);

        Storage.InputHandle in = hdfsStorage.openRead(srcPath);
        Storage.OutputHandle out = localStorage.openWrite(dstPath, Storage.WriteMode.CREATE);
        try {
            in.stream().transferTo(out.stream());
            out.flush();
        } finally {
            closeQuietly(in);
            closeQuietly(out);
        }
    }

    public void uploadToHdfs(String localRelativePath, String hdfsRelativePath) throws IOException {
        if (hdfsStorage == null) {
            log.warn(
                    "HDFS storage not available, skipping upload to HDFS: {} -> {}",
                    localRelativePath,
                    hdfsRelativePath);
            return;
        }
        Storage.StoragePath srcPath = toLocalPath(localRelativePath);
        Storage.StoragePath dstPath = toHdfsPath(hdfsRelativePath);

        hdfsStorage.createDirectories(
                new Storage.StoragePath(
                        "hdfs",
                        hdfsRelativePath.substring(
                                0, Math.max(hdfsRelativePath.lastIndexOf('/'), 0))));

        Storage.InputHandle in = localStorage.openRead(srcPath);
        Storage.OutputHandle out = hdfsStorage.openWrite(dstPath, Storage.WriteMode.CREATE);
        try {
            in.stream().transferTo(out.stream());
            out.flush();
        } finally {
            closeQuietly(in);
            closeQuietly(out);
        }
    }

    public List<Storage.StorageStatus> listHdfsFilesWithFilter(
            String hdfsRelativePath, java.util.function.Predicate<String> nameFilter)
            throws IOException {
        if (hdfsStorage == null) {
            return List.of();
        }
        Storage.StoragePath dirPath = toHdfsPath(hdfsRelativePath);
        if (!hdfsStorage.exists(dirPath)) {
            return List.of();
        }
        List<Storage.StoragePath> entries = hdfsStorage.list(dirPath);
        List<Storage.StorageStatus> result = new ArrayList<>();
        for (Storage.StoragePath entry : entries) {
            String name = extractFileName(entry.location());
            if (nameFilter.test(name)) {
                result.add(hdfsStorage.status(entry));
            }
        }
        return result;
    }

    public void close() {
        try {
            localStorage.close();
        } catch (Exception e) {
            log.warn("Failed to close local storage", e);
        }
        if (hdfsStorage != null) {
            try {
                hdfsStorage.close();
            } catch (Exception e) {
                log.warn("Failed to close HDFS storage", e);
            }
        }
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                log.warn("Failed to close handle", e);
            }
        }
    }

    private Storage.StoragePath toLocalPath(String relativePath) {
        return new Storage.StoragePath("file", basePath + "/" + relativePath);
    }

    private Storage.StoragePath toHdfsPath(String relativePath) {
        return new Storage.StoragePath("hdfs", hdfsBasePath + "/" + relativePath);
    }

    private String extractFileName(String path) {
        int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    }
}
