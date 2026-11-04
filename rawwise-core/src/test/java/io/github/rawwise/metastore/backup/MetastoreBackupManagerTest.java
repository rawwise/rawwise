package io.github.rawwise.metastore.backup;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.metastore.RocksdbMetastore;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetastoreBackupManagerTest {

    @Test
    void performBackupCreatesCheckpoint(@TempDir Path tempDir) throws IOException {
        String dbPath = tempDir.resolve("metastore").toString();
        String backupPath = tempDir.resolve("backups").toString();
        RocksdbMetastore metastore = new RocksdbMetastore(dbPath);
        try {
            metastore.put("test-key".getBytes(), "test-value".getBytes());

            MetastoreBackupManager manager = new MetastoreBackupManager(metastore, backupPath, 6);
            manager.performBackup();

            Path checkpointDir = tempDir.resolve("backups");
            assertTrue(checkpointDir.toFile().exists());
            assertTrue(
                    checkpointDir.toFile().listFiles() != null
                            && checkpointDir.toFile().listFiles().length > 0);

            manager.close();
        } finally {
            metastore.close();
        }
    }

    @Test
    void startWithNullPathDoesNotSchedule() throws IOException {
        String dbPath = Path.of("/tmp/rawwise-test-backup-null").toString();
        RocksdbMetastore metastore = new RocksdbMetastore(dbPath);
        try {
            MetastoreBackupManager manager = new MetastoreBackupManager(metastore, null, 6);
            assertDoesNotThrow(manager::start);
            manager.close();
        } finally {
            metastore.close();
        }
    }

    @Test
    void startWithBlankPathDoesNotSchedule() throws IOException {
        String dbPath = Path.of("/tmp/rawwise-test-backup-blank").toString();
        RocksdbMetastore metastore = new RocksdbMetastore(dbPath);
        try {
            MetastoreBackupManager manager = new MetastoreBackupManager(metastore, "  ", 6);
            assertDoesNotThrow(manager::start);
            manager.close();
        } finally {
            metastore.close();
        }
    }

    @Test
    void closeTerminatesScheduler() throws IOException {
        String dbPath = Path.of("/tmp/rawwise-test-backup-close").toString();
        String backupPath = Path.of("/tmp/rawwise-test-backup-close-dir").toString();
        RocksdbMetastore metastore = new RocksdbMetastore(dbPath);
        try {
            MetastoreBackupManager manager = new MetastoreBackupManager(metastore, backupPath, 6);
            manager.start();
            assertDoesNotThrow(manager::close);
        } finally {
            metastore.close();
        }
    }
}
