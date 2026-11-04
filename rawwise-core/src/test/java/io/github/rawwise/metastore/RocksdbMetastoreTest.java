package io.github.rawwise.metastore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class RocksdbMetastoreTest extends MetastoreContractTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("rocksdb-test");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (tempDir != null) {
            try (var walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(
                                p -> {
                                    try {
                                        Files.delete(p);
                                    } catch (IOException ignored) {
                                    }
                                });
            }
        }
    }

    @Override
    protected Metastore createStore() throws IOException {
        return new RocksdbMetastore(tempDir.toString());
    }
}
