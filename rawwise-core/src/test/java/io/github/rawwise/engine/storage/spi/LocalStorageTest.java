package io.github.rawwise.engine.storage.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalStorageTest {

    @TempDir Path tempDir;

    private LocalStorage storage;

    @BeforeEach
    void setUp() {
        storage = new LocalStorage("test-local");
    }

    private Storage.StoragePath file(String location) {
        return new Storage.StoragePath("file", location);
    }

    private Storage.StoragePath badScheme(String location) {
        return new Storage.StoragePath("hdfs", location);
    }

    @Test
    void openReadReturnsInputHandleWithCorrectPath() throws Exception {
        var filePath = tempDir.resolve("read-test.txt");
        Files.writeString(filePath, "hello world");
        var handle = storage.openRead(file(filePath.toString()));
        try (handle) {
            assertThat(handle.path()).isEqualTo(file(filePath.toString()));
        }
    }

    @Test
    void inputHandleReadsFileContent() throws Exception {
        var filePath = tempDir.resolve("content.txt");
        Files.writeString(filePath, "hello");
        var handle = storage.openRead(file(filePath.toString()));
        try (handle) {
            var stream = handle.stream();
            var bytes = stream.readAllBytes();
            assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("hello");
        }
    }

    @Test
    void inputHandleSeekAndPosition() throws Exception {
        var filePath = tempDir.resolve("seek.txt");
        Files.writeString(filePath, "0123456789");
        var handle = storage.openRead(file(filePath.toString()));
        try (handle) {
            assertThat(handle.position()).isEqualTo(0);
            handle.seek(5);
            assertThat(handle.position()).isEqualTo(5);
            var stream = handle.stream();
            var bytes = stream.readAllBytes();
            assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("56789");
        }
    }

    @Test
    void inputHandleLength() throws Exception {
        var filePath = tempDir.resolve("length.txt");
        Files.writeString(filePath, "abcde");
        var handle = storage.openRead(file(filePath.toString()));
        try (handle) {
            assertThat(handle.length()).isEqualTo(5);
        }
    }

    @Test
    void openWriteCreateMode() throws Exception {
        var filePath = tempDir.resolve("create.txt");
        var handle = storage.openWrite(file(filePath.toString()), Storage.WriteMode.CREATE);
        try (handle) {
            handle.stream().write("new".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(Files.readString(filePath)).isEqualTo("new");
    }

    @Test
    void openWriteCreateModeFailsOnExisting() throws Exception {
        var filePath = tempDir.resolve("create-exists.txt");
        Files.writeString(filePath, "old");
        assertThatThrownBy(
                        () ->
                                storage.openWrite(
                                        file(filePath.toString()), Storage.WriteMode.CREATE))
                .isInstanceOf(IOException.class);
    }

    @Test
    void openWriteOverwriteMode() throws Exception {
        var filePath = tempDir.resolve("overwrite.txt");
        Files.writeString(filePath, "old content");
        var handle = storage.openWrite(file(filePath.toString()), Storage.WriteMode.OVERWRITE);
        try (handle) {
            handle.stream().write("new".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(Files.readString(filePath)).isEqualTo("new");
    }

    @Test
    void openWriteOverwriteModeCreatesIfMissing() throws Exception {
        var filePath = tempDir.resolve("overwrite-new.txt");
        var handle = storage.openWrite(file(filePath.toString()), Storage.WriteMode.OVERWRITE);
        try (handle) {
            handle.stream().write("created".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(Files.readString(filePath)).isEqualTo("created");
    }

    @Test
    void openWriteAppendMode() throws Exception {
        var filePath = tempDir.resolve("append.txt");
        Files.writeString(filePath, "hello");
        var handle = storage.openWrite(file(filePath.toString()), Storage.WriteMode.APPEND);
        try (handle) {
            handle.stream().write(" world".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(Files.readString(filePath)).isEqualTo("hello world");
    }

    @Test
    void openWriteAppendModeCreatesIfMissing() throws Exception {
        var filePath = tempDir.resolve("append-new.txt");
        var handle = storage.openWrite(file(filePath.toString()), Storage.WriteMode.APPEND);
        try (handle) {
            handle.stream().write("created".getBytes(StandardCharsets.UTF_8));
        }
        assertThat(Files.readString(filePath)).isEqualTo("created");
    }

    @Test
    void outputHandlePositionAdvancesAfterWrite() throws Exception {
        var filePath = tempDir.resolve("position.txt");
        Files.createFile(filePath);
        var handle = storage.openWrite(file(filePath.toString()), Storage.WriteMode.OVERWRITE);
        try (handle) {
            assertThat(handle.position()).isEqualTo(0);
            handle.stream().write("abc".getBytes(StandardCharsets.UTF_8));
            assertThat(handle.position()).isEqualTo(3);
        }
    }

    @Test
    void statusForExistingFile() throws Exception {
        var filePath = tempDir.resolve("status-file.txt");
        Files.writeString(filePath, "content");
        var status = storage.status(file(filePath.toString()));
        assertThat(status.exists()).isTrue();
        assertThat(status.directory()).isFalse();
        assertThat(status.length()).isEqualTo(7);
        assertThat(status.lastModifiedMillis()).isGreaterThan(0);
    }

    @Test
    void statusForExistingDirectory() throws Exception {
        var dirPath = tempDir.resolve("status-dir");
        Files.createDirectory(dirPath);
        var status = storage.status(file(dirPath.toString()));
        assertThat(status.exists()).isTrue();
        assertThat(status.directory()).isTrue();
    }

    @Test
    void statusForNonExistingPath() throws Exception {
        var missing = tempDir.resolve("nonexistent");
        var status = storage.status(file(missing.toString()));
        assertThat(status.exists()).isFalse();
        assertThat(status.directory()).isFalse();
        assertThat(status.length()).isEqualTo(0);
        assertThat(status.lastModifiedMillis()).isEqualTo(0);
    }

    @Test
    void existsReturnsTrueForExistingFile() throws Exception {
        var filePath = tempDir.resolve("exists-yes.txt");
        Files.writeString(filePath, "data");
        assertThat(storage.exists(file(filePath.toString()))).isTrue();
    }

    @Test
    void existsReturnsFalseForNonExistingFile() throws Exception {
        var missing = tempDir.resolve("no-such-file");
        assertThat(storage.exists(file(missing.toString()))).isFalse();
    }

    @Test
    void createDirectoriesCreatesNestedDirs() throws Exception {
        var nested = tempDir.resolve("a").resolve("b").resolve("c");
        storage.createDirectories(file(nested.toString()));
        assertThat(Files.isDirectory(nested)).isTrue();
    }

    @Test
    void listReturnsChildren() throws Exception {
        Files.createFile(tempDir.resolve("child1.txt"));
        Files.createFile(tempDir.resolve("child2.txt"));
        Files.createDirectory(tempDir.resolve("subdir"));
        var children = storage.list(file(tempDir.toString()));
        assertThat(children).hasSize(3);
        assertThat(children.stream().map(Storage.StoragePath::scheme).distinct().toList())
                .containsExactly("file");
    }

    @Test
    void listReturnsEmptyForNonExistingDir() throws Exception {
        var missing = tempDir.resolve("no-dir");
        var children = storage.list(file(missing.toString()));
        assertThat(children).isEmpty();
    }

    @Test
    void deleteRemovesFile() throws Exception {
        var filePath = tempDir.resolve("delete-me.txt");
        Files.writeString(filePath, "bye");
        storage.delete(file(filePath.toString()));
        assertThat(Files.exists(filePath)).isFalse();
    }

    @Test
    void deleteThrowsForNonExistingFile() throws Exception {
        var missing = tempDir.resolve("no-file");
        assertThatThrownBy(() -> storage.delete(file(missing.toString())))
                .isInstanceOf(IOException.class);
    }

    @Test
    void deleteRecursivelyRemovesDirectoryTree() throws Exception {
        var dir = tempDir.resolve("tree");
        Files.createDirectories(dir.resolve("sub1").resolve("sub2"));
        Files.writeString(dir.resolve("sub1").resolve("sub2").resolve("f.txt"), "data");
        Files.writeString(dir.resolve("sub1").resolve("g.txt"), "more");
        storage.deleteRecursively(file(dir.toString()));
        assertThat(Files.exists(dir)).isFalse();
    }

    @Test
    void deleteRecursivelyIsNoOpForNonExistingPath() throws Exception {
        var missing = tempDir.resolve("ghost");
        storage.deleteRecursively(file(missing.toString()));
    }

    @Test
    void renameMovesFile() throws Exception {
        var src = tempDir.resolve("src.txt");
        var dst = tempDir.resolve("dst.txt");
        Files.writeString(src, "moved");
        storage.rename(file(src.toString()), file(dst.toString()));
        assertThat(Files.exists(src)).isFalse();
        assertThat(Files.readString(dst)).isEqualTo("moved");
    }

    @Test
    void mapReadOnlyReturnsMappedRegion() throws Exception {
        var filePath = tempDir.resolve("mmap-read.txt");
        Files.writeString(filePath, "mapped-data");
        var region = storage.map(file(filePath.toString()), 0, 11, Storage.AccessMode.READ_ONLY);
        try (region) {
            assertThat(region.path()).isEqualTo(file(filePath.toString()));
            assertThat(region.offset()).isEqualTo(0);
            assertThat(region.length()).isEqualTo(11);
            assertThat(region.mode()).isEqualTo(Storage.AccessMode.READ_ONLY);
            var buf = region.asByteBuffer();
            var bytes = new byte[buf.remaining()];
            buf.get(bytes);
            assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("mapped-data");
        }
    }

    @Test
    void mapReadWriteModifiesFile() throws Exception {
        var filePath = tempDir.resolve("mmap-rw.txt");
        Files.writeString(filePath, "AAAAAAAAAA");
        var region = storage.map(file(filePath.toString()), 0, 10, Storage.AccessMode.READ_WRITE);
        try (region) {
            assertThat(region.mode()).isEqualTo(Storage.AccessMode.READ_WRITE);
            var buf = region.asByteBuffer();
            buf.put(0, (byte) 'B');
            buf.put(1, (byte) 'B');
        }
        assertThat(Files.readString(filePath)).startsWith("BB");
    }

    @Test
    void mapWithOffset() throws Exception {
        var filePath = tempDir.resolve("mmap-offset.txt");
        Files.writeString(filePath, "0123456789");
        var region = storage.map(file(filePath.toString()), 5, 5, Storage.AccessMode.READ_ONLY);
        try (region) {
            assertThat(region.offset()).isEqualTo(5);
            var buf = region.asByteBuffer();
            var bytes = new byte[buf.remaining()];
            buf.get(bytes);
            assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("56789");
        }
    }

    @Test
    void mapRejectsNegativeOffset() throws Exception {
        var filePath = tempDir.resolve("mmap-neg.txt");
        Files.writeString(filePath, "data");
        assertThatThrownBy(
                        () ->
                                storage.map(
                                        file(filePath.toString()),
                                        -1,
                                        4,
                                        Storage.AccessMode.READ_ONLY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offset");
    }

    @Test
    void mapRejectsZeroLength() throws Exception {
        var filePath = tempDir.resolve("mmap-zero.txt");
        Files.writeString(filePath, "data");
        assertThatThrownBy(
                        () ->
                                storage.map(
                                        file(filePath.toString()),
                                        0,
                                        0,
                                        Storage.AccessMode.READ_ONLY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length");
    }

    @Test
    void toJavaPathRejectsNonFileScheme() throws Exception {
        var badPath = badScheme("/some/path");
        assertThatThrownBy(() -> storage.openRead(badPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.openWrite(badPath, Storage.WriteMode.CREATE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.status(badPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.exists(badPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.createDirectories(badPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.list(badPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.delete(badPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.deleteRecursively(badPath))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.rename(badPath, badScheme("/b")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
        assertThatThrownBy(() -> storage.map(badPath, 0, 4, Storage.AccessMode.READ_ONLY))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file");
    }
}
