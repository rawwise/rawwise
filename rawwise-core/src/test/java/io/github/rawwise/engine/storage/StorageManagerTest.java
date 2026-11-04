package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.governance.config.RawWiseConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/**
 * StorageManager unit test.
 *
 * <p>teststrategy: - local storage operations @TempDir implement real filesystem interaction,
 * ensure test independence - HDFS related operationsvia null hdfsBasePath simulate unavailable
 * scenario - does not introduce real HDFS cluster dependency maintain unit test fastness
 */
class StorageManagerTest {

    @TempDir Path tempDir;

    private StorageManager storageManager;
    private String basePath;

    @BeforeAll
    static void registerLocalStorage() {
        // LocalStorageProvider alreadyregister
        StorageRegistry.registerDefaults();
    }

    @BeforeEach
    void setUp() {
        basePath = tempDir.toString();
        // not providing hdfsBasePath HDFS unavailable
        storageManager = new StorageManager(basePath, null, RawWiseConfig.empty());
    }

    @AfterEach
    void tearDown() {
        if (storageManager != null) {
            storageManager.close();
        }
    }

    // ==================== ====================

    @Test
    void localStorageShouldNotBeNull() {
        assertThat(storageManager.localStorage()).isNotNull();
    }

    @Test
    void hdfsStorageShouldBeNullWhenNoHdfsBasePath() {
        assertThat(storageManager.hdfsStorage()).isNull();
    }

    @Test
    void hdfsAvailableShouldBeFalseWhenNoHdfsBasePath() {
        assertThat(storageManager.hdfsAvailable()).isFalse();
    }

    @Test
    void hdfsAvailableShouldBeFalseWhenHdfsBasePathBlank() {
        StorageManager mgr = new StorageManager(basePath, "  ", RawWiseConfig.empty());
        try {
            assertThat(mgr.hdfsAvailable()).isFalse();
        } finally {
            mgr.close();
        }
    }

    @Test
    void hdfsAvailableShouldBeFalseWhenHdfsProviderNotRegistered() {
        // hdfsBasePath non- no HdfsStorageProvider register, construct should catch exception
        StorageManager mgr = new StorageManager(basePath, "/tmp/hdfs-fake", RawWiseConfig.empty());
        try {
            assertThat(mgr.hdfsAvailable()).isFalse();
        } finally {
            mgr.close();
        }
    }

    // ==================== local storage operations ====================

    @Nested
    class LocalStorageOperations {

        @Test
        void localExistsReturnsFalseForNonExistentPath() throws IOException {
            assertThat(storageManager.localExists("nonexistent/path")).isFalse();
        }

        @Test
        void localExistsReturnsTrueAfterCreateDirectories() throws IOException {
            storageManager.createLocalDirectories("test-dir/sub");
            assertThat(storageManager.localExists("test-dir/sub")).isTrue();
        }

        @Test
        void createLocalDirectoriesCreatesNestedDirs() throws IOException {
            String relativePath = "a/b/c/d";
            storageManager.createLocalDirectories(relativePath);

            Path created = tempDir.resolve(relativePath);
            assertThat(Files.exists(created)).isTrue();
            assertThat(Files.isDirectory(created)).isTrue();
        }

        @Test
        void localDirectoryExistsReturnsTrueForExistingDir() throws IOException {
            Path dir = tempDir.resolve("mydir");
            Files.createDirectories(dir);
            assertThat(storageManager.localDirectoryExists(dir.toString())).isTrue();
        }

        @Test
        void localDirectoryExistsReturnsFalseForNonExistentDir() throws IOException {
            assertThat(storageManager.localDirectoryExists(tempDir.resolve("nope").toString()))
                    .isFalse();
        }

        @Test
        void localDirectoryExistsReturnsFalseForFile() throws IOException {
            Path file = tempDir.resolve("afile.txt");
            Files.writeString(file, "hello");
            assertThat(storageManager.localDirectoryExists(file.toString())).isFalse();
        }

        @Test
        void listLocalReturnsEmptyForNonExistentDir() throws IOException {
            assertThat(storageManager.listLocal("nonexistent")).isEmpty();
        }

        @Test
        void listLocalReturnsCreatedEntries() throws IOException {
            storageManager.createLocalDirectories("list-test");
            // create and
            Files.createDirectories(tempDir.resolve("list-test/child1"));
            Files.createDirectories(tempDir.resolve("list-test/child2"));
            Files.writeString(tempDir.resolve("list-test/file1.txt"), "data");

            List<Storage.StoragePath> entries = storageManager.listLocal("list-test");
            assertThat(entries).hasSize(3);
            assertThat(entries.stream().map(p -> p.location())).anyMatch(s -> s.endsWith("child1"));
            assertThat(entries.stream().map(p -> p.location())).anyMatch(s -> s.endsWith("child2"));
            assertThat(entries.stream().map(p -> p.location()))
                    .anyMatch(s -> s.endsWith("file1.txt"));
        }

        @Test
        void deleteLocalRemovesFile() throws IOException {
            storageManager.createLocalDirectories("del-test");
            Path file = tempDir.resolve("del-test/target.txt");
            Files.writeString(file, "bye");

            assertThat(storageManager.localExists("del-test/target.txt")).isTrue();
            storageManager.deleteLocal("del-test/target.txt");
            assertThat(storageManager.localExists("del-test/target.txt")).isFalse();
        }

        @Test
        void deleteLocalRecursivelyRemovesDirTree() throws IOException {
            storageManager.createLocalDirectories("del-rec/a/b");
            Files.writeString(tempDir.resolve("del-rec/a/b/data.txt"), "x");

            assertThat(storageManager.localExists("del-rec")).isTrue();
            storageManager.deleteLocalRecursively("del-rec");
            assertThat(storageManager.localExists("del-rec")).isFalse();
        }

        @Test
        void deleteLocalThrowsForNonExistentPath() {
            assertThatThrownBy(() -> storageManager.deleteLocal("no-such-file"))
                    .isInstanceOf(IOException.class);
        }
    }

    // ==================== HDFS operations(unavailable scenario) ====================

    @Nested
    class HdfsOperationsWhenUnavailable {

        @Test
        void hdfsExistsReturnsFalseWhenUnavailable() throws IOException {
            assertThat(storageManager.hdfsExists("any/path")).isFalse();
        }

        @Test
        void listHdfsReturnsEmptyListWhenUnavailable() throws IOException {
            assertThat(storageManager.listHdfs("any/path")).isEmpty();
        }

        @Test
        void createHdfsDirectoriesDoesNotThrowWhenUnavailable() {
            assertThatCode(() -> storageManager.createHdfsDirectories("any/path"))
                    .doesNotThrowAnyException();
        }

        @Test
        void deleteHdfsDoesNotThrowWhenUnavailable() {
            assertThatCode(() -> storageManager.deleteHdfs("any/path")).doesNotThrowAnyException();
        }

        @Test
        void deleteHdfsRecursivelyDoesNotThrowWhenUnavailable() {
            assertThatCode(() -> storageManager.deleteHdfsRecursively("any/path"))
                    .doesNotThrowAnyException();
        }

        @Test
        void downloadFromHdfsDoesNotThrowWhenUnavailable() {
            assertThatCode(() -> storageManager.downloadFromHdfs("src", "dst"))
                    .doesNotThrowAnyException();
        }

        @Test
        void uploadToHdfsDoesNotThrowWhenUnavailable() {
            assertThatCode(() -> storageManager.uploadToHdfs("src", "dst"))
                    .doesNotThrowAnyException();
        }

        @Test
        void listHdfsFilesWithFilterReturnsEmptyWhenUnavailable() throws IOException {
            assertThat(storageManager.listHdfsFilesWithFilter("any", s -> true)).isEmpty();
        }
    }

    // ==================== listInstanceDirs ====================

    @Nested
    class ListInstanceDirsTests {

        @Test
        void listInstanceDirsReturnsOnlyInstPrefixDirs() throws IOException {
            storageManager.createLocalDirectories("mytable/inst-001");
            storageManager.createLocalDirectories("mytable/inst-002");
            storageManager.createLocalDirectories("mytable/other-dir");
            Files.writeString(tempDir.resolve("mytable/somefile.txt"), "x");

            List<String> dirs = storageManager.listInstanceDirs("mytable");
            assertThat(dirs).containsExactly("inst-001", "inst-002");
        }

        @Test
        void listInstanceDirsReturnsSortedResults() throws IOException {
            storageManager.createLocalDirectories("tbl/inst-003");
            storageManager.createLocalDirectories("tbl/inst-001");
            storageManager.createLocalDirectories("tbl/inst-002");

            List<String> dirs = storageManager.listInstanceDirs("tbl");
            assertThat(dirs).containsExactly("inst-001", "inst-002", "inst-003");
        }

        @Test
        void listInstanceDirsReturnsEmptyForEmptyDir() throws IOException {
            storageManager.createLocalDirectories("empty-table");
            List<String> dirs = storageManager.listInstanceDirs("empty-table");
            assertThat(dirs).isEmpty();
        }

        @Test
        void listInstanceDirsReturnsEmptyForNonExistentDir() throws IOException {
            List<String> dirs = storageManager.listInstanceDirs("no-such-table");
            assertThat(dirs).isEmpty();
        }
    }

    // ==================== listAllInstanceDirs ====================

    @Nested
    class ListAllInstanceDirsTests {

        @Test
        void listAllInstanceDirsDiscoversAcrossTables() throws IOException {
            storageManager.createLocalDirectories("db.table1/inst-100");
            storageManager.createLocalDirectories("db.table1/inst-101");
            storageManager.createLocalDirectories("db.table2/inst-200");

            List<String> all = storageManager.listAllInstanceDirs(basePath);
            assertThat(all)
                    .containsExactly(
                            "db.table1/inst-100", "db.table1/inst-101", "db.table2/inst-200");
        }

        @Test
        void listAllInstanceDirsSkipsDotDirsAndWal() throws IOException {
            storageManager.createLocalDirectories(".hidden/inst-001");
            storageManager.createLocalDirectories("wal/inst-002");
            storageManager.createLocalDirectories("real-table/inst-003");

            List<String> all = storageManager.listAllInstanceDirs(basePath);
            assertThat(all).containsExactly("real-table/inst-003");
        }

        @Test
        void listAllInstanceDirsSkipsNonDirectoryEntries() throws IOException {
            Files.writeString(tempDir.resolve("afile.txt"), "not a dir");
            storageManager.createLocalDirectories("good-table/inst-010");

            List<String> all = storageManager.listAllInstanceDirs(basePath);
            assertThat(all).containsExactly("good-table/inst-010");
        }

        @Test
        void listAllInstanceDirsReturnsSortedResults() throws IOException {
            storageManager.createLocalDirectories("z-table/inst-001");
            storageManager.createLocalDirectories("a-table/inst-002");

            List<String> all = storageManager.listAllInstanceDirs(basePath);
            assertThat(all).containsExactly("a-table/inst-002", "z-table/inst-001");
        }
    }

    // ==================== listInstanceDirsForTable ====================

    @Nested
    class ListInstanceDirsForTableTests {

        @Test
        void listInstanceDirsForTableReturnsInstances() throws IOException {
            storageManager.createLocalDirectories("mytbl/inst-001");
            storageManager.createLocalDirectories("mytbl/inst-002");

            List<String> dirs = storageManager.listInstanceDirsForTable(basePath, "mytbl");
            assertThat(dirs).containsExactly("inst-001", "inst-002");
        }

        @Test
        void listInstanceDirsForTableReturnsEmptyWhenTableDirMissing() throws IOException {
            List<String> dirs = storageManager.listInstanceDirsForTable(basePath, "no-table");
            assertThat(dirs).isEmpty();
        }
    }

    // ==================== listWALFiles ====================

    @Nested
    class ListWALFilesTests {

        @Test
        void listWALFilesReturnsParquetAndNwlFiles() throws IOException {
            storageManager.createLocalDirectories("wal");
            Files.writeString(tempDir.resolve("wal/segment1.parquet"), "p");
            Files.writeString(tempDir.resolve("wal/segment2.nwl"), "n");
            Files.writeString(tempDir.resolve("wal/other.txt"), "t");

            List<String> files = storageManager.listWALFiles("wal");
            assertThat(files).containsExactlyInAnyOrder("segment1.parquet", "segment2.nwl");
        }

        @Test
        void listWALFilesReturnsEmptyForNonExistentDir() throws IOException {
            assertThat(storageManager.listWALFiles("no-wal")).isEmpty();
        }
    }

    // ==================== discoverLocalTables ====================

    @Nested
    class DiscoverLocalTablesTests {

        @Test
        void discoverLocalTablesReturnsTableNames() throws IOException {
            storageManager.createLocalDirectories("table_a/inst-001");
            storageManager.createLocalDirectories("table_b/inst-001");

            Set<String> tables = storageManager.discoverLocalTables(basePath);
            assertThat(tables).containsExactlyInAnyOrder("table_a", "table_b");
        }

        @Test
        void discoverLocalTablesExcludesDotDirsAndWal() throws IOException {
            storageManager.createLocalDirectories(".snapshot/inst-001");
            storageManager.createLocalDirectories("wal/inst-001");
            storageManager.createLocalDirectories("real_table/inst-001");

            Set<String> tables = storageManager.discoverLocalTables(basePath);
            assertThat(tables).containsExactly("real_table");
        }

        @Test
        void discoverLocalTablesReturnsEmptyForNonExistentPath() {
            Set<String> tables = storageManager.discoverLocalTables("/nonexistent/path/xyz");
            assertThat(tables).isEmpty();
        }
    }

    // ==================== discoverHdfsWALTables ====================

    @Test
    void discoverHdfsWALTablesReturnsEmptySet() {
        // whenbefore returns empty
        assertThat(storageManager.discoverHdfsWALTables("/any/path")).isEmpty();
    }

    // ==================== cleanupWALFiles ====================

    @Nested
    class CleanupWALFilesTests {

        @Test
        void cleanupWALFilesDeletesParquetAndNwlFiles() throws IOException {
            storageManager.createLocalDirectories("wal-cleanup");
            Path parquetFile = tempDir.resolve("wal-cleanup/seg.parquet");
            Path nwlFile = tempDir.resolve("wal-cleanup/seg.nwl");
            Path txtFile = tempDir.resolve("wal-cleanup/readme.txt");
            Files.writeString(parquetFile, "p");
            Files.writeString(nwlFile, "n");
            Files.writeString(txtFile, "t");

            storageManager.cleanupWALFiles("wal-cleanup");

            assertThat(Files.exists(parquetFile)).isFalse();
            assertThat(Files.exists(nwlFile)).isFalse();
            assertThat(Files.exists(txtFile)).isTrue(); // non- WAL should
        }

        @Test
        void cleanupWALFilesDoesNotThrowForNonExistentDir() {
            assertThatCode(() -> storageManager.cleanupWALFiles("no-such-dir"))
                    .doesNotThrowAnyException();
        }
    }

    // ==================== pathparse ====================

    @Nested
    class PathResolutionTests {

        @Test
        void localExistsResolvesAgainstBasePath() throws IOException {
            // localExists toLocalPath(relativePath) = "file", basePath + "/" + relativePath
            storageManager.createLocalDirectories("sub/dir");
            assertThat(storageManager.localExists("sub/dir")).isTrue();
            // verify in basePath create
            assertThat(Files.exists(tempDir.resolve("sub/dir"))).isTrue();
        }

        @Test
        void createLocalDirectoriesResolvesAgainstBasePath() throws IOException {
            storageManager.createLocalDirectories("deep/nested/path");
            Path expected = tempDir.resolve("deep/nested/path");
            assertThat(Files.exists(expected)).isTrue();
        }
    }

    // ==================== downloadFromHdfs / uploadToHdfs ====================

    @Nested
    class TransferOperationsTests {

        @Test
        void downloadFromHdfsDoesNotThrowWhenHdfsUnavailable() {
            assertThatCode(() -> storageManager.downloadFromHdfs("src", "dst"))
                    .doesNotThrowAnyException();
        }

        @Test
        void uploadToHdfsDoesNotThrowWhenHdfsUnavailable() {
            assertThatCode(() -> storageManager.uploadToHdfs("src", "dst"))
                    .doesNotThrowAnyException();
        }
    }

    // ==================== close ====================

    @Nested
    class CloseTests {

        @Test
        void closeDoesNotThrowWhenCalledOnce() {
            assertThatCode(() -> storageManager.close()).doesNotThrowAnyException();
        }

        @Test
        void closeCanBeCalledMultipleTimesWithoutError() {
            storageManager.close();
            assertThatCode(() -> storageManager.close()).doesNotThrowAnyException();
        }
    }

    // ==================== listHdfsFilesWithFilter ====================

    @Test
    void listHdfsFilesWithFilterReturnsEmptyWhenHdfsUnavailable() throws IOException {
        assertThat(storageManager.listHdfsFilesWithFilter("any", name -> true)).isEmpty();
    }
}
