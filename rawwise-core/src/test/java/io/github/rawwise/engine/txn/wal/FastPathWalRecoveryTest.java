package io.github.rawwise.engine.txn.wal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.backend.wal.DefaultWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecordCodec;
import io.github.rawwise.backend.wal.WAL;
import io.github.rawwise.backend.wal.WALConfig;
import io.github.rawwise.backend.wal.WALRecord;
import io.github.rawwise.engine.storage.LmdbInstance;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("FastPath WAL recovery test")
class FastPathWalRecoveryTest {

    @TempDir Path tempDir;

    @Test
    @DisplayName("construct FastPath INSERT+COMMIT WAL afterrecoveryto LMDB, replayed datareadable")
    void testFastPathWalReplayToLmdb() throws Exception {
        Path walDir = tempDir.resolve("wal/fastpath");
        Path lmdbBase = tempDir.resolve("lmdb");
        java.nio.file.Files.createDirectories(walDir);
        java.nio.file.Files.createDirectories(lmdbBase);

        WALConfig walConfig =
                new WALConfig(
                        walDir.toString(),
                        "fastpath-wal",
                        64L * 1024L * 1024L,
                        true,
                        8192,
                        "NONE",
                        "fastpath",
                        "test-executor",
                        "",
                        1000L,
                        30000L,
                        true,
                        32,
                        10L);
        WAL wal = new WAL(walConfig);

        long commitTs1 = 1001L;
        long commitTs2 = 1002L;
        long commitTs3 = 1003L;
        String tableId = "default.test_table";

        byte[] key1 = "key-001".getBytes(StandardCharsets.UTF_8);
        byte[] value1 = "value-001".getBytes(StandardCharsets.UTF_8);
        byte[] key2 = "key-002".getBytes(StandardCharsets.UTF_8);
        byte[] value2 = "value-002".getBytes(StandardCharsets.UTF_8);
        byte[] key3 = "key-003".getBytes(StandardCharsets.UTF_8);
        byte[] value3 = "value-003".getBytes(StandardCharsets.UTF_8);

        appendFastPathWriteRecord(wal, commitTs1, tableId, key1, value1, "INSERT");
        appendFastPathWriteRecord(wal, commitTs2, tableId, key2, value2, "INSERT");
        appendFastPathCommitMarker(wal, 1L, tableId);

        appendFastPathWriteRecord(wal, commitTs3, tableId, key3, value3, "UPDATE");

        wal.close();

        NwlBackedFastPathWalReader reader = new NwlBackedFastPathWalReader(walDir.toString());
        List<FastPathWalReader.FastPathReplayEntry> entries = reader.scanReplayEntries();

        assertThat(entries).hasSize(3);
        assertThat(entries.get(0).commitTs()).isEqualTo(commitTs1);
        assertThat(entries.get(0).tableId()).isEqualTo(tableId);
        assertThat(entries.get(0).writeType()).isEqualTo("INSERT");
        assertThat(new String(entries.get(0).keyBytes(), StandardCharsets.UTF_8))
                .isEqualTo("key-001");
        assertThat(new String(entries.get(0).valueBytes(), StandardCharsets.UTF_8))
                .isEqualTo("value-001");

        assertThat(entries.get(1).commitTs()).isEqualTo(commitTs2);
        assertThat(entries.get(2).commitTs()).isEqualTo(commitTs3);
        assertThat(entries.get(2).writeType()).isEqualTo("UPDATE");

        LmdbInstance instance = new LmdbInstance("test-instance", lmdbBase, 64L * 1024L * 1024L);

        int successCount = 0;
        for (FastPathWalReader.FastPathReplayEntry entry : entries) {
            if ("DELETE".equals(entry.writeType()) || entry.valueBytes() == null) {
                continue;
            }
            LmdbInstance.PutStatus status = instance.put(entry.keyBytes(), entry.valueBytes());
            if (status == LmdbInstance.PutStatus.SUCCESS) {
                successCount++;
            }
        }

        assertThat(successCount).isEqualTo(3);

        byte[] readValue1 = instance.get(key1);
        byte[] readValue2 = instance.get(key2);
        byte[] readValue3 = instance.get(key3);

        assertThat(readValue1).isNotNull();
        assertThat(new String(readValue1, StandardCharsets.UTF_8)).isEqualTo("value-001");

        assertThat(readValue2).isNotNull();
        assertThat(new String(readValue2, StandardCharsets.UTF_8)).isEqualTo("value-002");

        assertThat(readValue3).isNotNull();
        assertThat(new String(readValue3, StandardCharsets.UTF_8)).isEqualTo("value-003");

        instance.close();

        System.out.println("=== FastPath WAL 恢复测试通过 ===");
        System.out.println("WAL 文件路径: " + walDir);
        System.out.println("LMDB 路径: " + lmdbBase);
        System.out.println("Replay entries: " + entries.size());
        System.out.println("成功 replay 到 LMDB: " + successCount + " 条");
        System.out.println("验证读取:");
        System.out.println("  key-001 -> " + new String(readValue1, StandardCharsets.UTF_8));
        System.out.println("  key-002 -> " + new String(readValue2, StandardCharsets.UTF_8));
        System.out.println("  key-003 -> " + new String(readValue3, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("WAL when not exists scanReplayEntries returns empty list")
    void testScanNonExistentDirectory() {
        NwlBackedFastPathWalReader reader =
                new NwlBackedFastPathWalReader(tempDir.resolve("nonexistent").toString());
        List<FastPathWalReader.FastPathReplayEntry> entries = reader.scanReplayEntries();
        assertThat(entries).isEmpty();
    }

    @Test
    @DisplayName("DELETE typerecordin replay beskip")
    void testDeleteRecordsSkippedDuringReplay() throws Exception {
        Path walDir = tempDir.resolve("wal/fastpath");
        Path lmdbBase = tempDir.resolve("lmdb");
        java.nio.file.Files.createDirectories(walDir);
        java.nio.file.Files.createDirectories(lmdbBase);

        WALConfig walConfig =
                new WALConfig(
                        walDir.toString(),
                        "fastpath-wal",
                        64L * 1024L * 1024L,
                        true,
                        8192,
                        "NONE",
                        "fastpath",
                        "test-executor",
                        "",
                        1000L,
                        30000L,
                        true,
                        32,
                        10L);
        WAL wal = new WAL(walConfig);

        String tableId = "default.test_table";
        byte[] key1 = "del-key".getBytes(StandardCharsets.UTF_8);
        byte[] value1 = "del-value".getBytes(StandardCharsets.UTF_8);

        appendFastPathWriteRecord(wal, 2001L, tableId, key1, value1, "INSERT");
        appendFastPathWriteRecord(wal, 2002L, tableId, key1, null, "DELETE");
        appendFastPathCommitMarker(wal, 2L, tableId);

        wal.close();

        NwlBackedFastPathWalReader reader = new NwlBackedFastPathWalReader(walDir.toString());
        List<FastPathWalReader.FastPathReplayEntry> entries = reader.scanReplayEntries();

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).writeType()).isEqualTo("INSERT");
        assertThat(entries.get(0).valueBytes()).isNotNull();
        assertThat(entries.get(1).writeType()).isEqualTo("DELETE");
        assertThat(entries.get(1).valueBytes()).isNull();

        LmdbInstance instance = new LmdbInstance("del-test", lmdbBase, 64L * 1024L * 1024L);

        int replayedCount = 0;
        for (FastPathWalReader.FastPathReplayEntry entry : entries) {
            if ("DELETE".equals(entry.writeType()) || entry.valueBytes() == null) {
                continue;
            }
            LmdbInstance.PutStatus status = instance.put(entry.keyBytes(), entry.valueBytes());
            if (status == LmdbInstance.PutStatus.SUCCESS) {
                replayedCount++;
            }
        }

        assertThat(replayedCount).isEqualTo(1);
        assertThat(instance.get(key1)).isNotNull();
        assertThat(new String(instance.get(key1), StandardCharsets.UTF_8)).isEqualTo("del-value");

        instance.close();
    }

    private void appendFastPathWriteRecord(
            WAL wal, long commitTs, String tableId, byte[] key, byte[] value, String writeType) {
        TxnWALRecord record = TxnWALRecord.fastpathWrite(commitTs, tableId, key, value, writeType);
        byte[] encoded = TxnWALRecordCodec.encode(record);
        WALRecord walRecord =
                new DefaultWALRecord(
                        System.nanoTime(),
                        encoded,
                        System.currentTimeMillis(),
                        commitTs,
                        "fastpath",
                        "DATA");
        wal.append(walRecord, true);
    }

    private void appendFastPathCommitMarker(WAL wal, long batchId, String tableId) {
        TxnWALRecord commitRecord = TxnWALRecord.fastpathCommit(batchId, tableId);
        byte[] encoded = TxnWALRecordCodec.encode(commitRecord);
        WALRecord walRecord =
                new DefaultWALRecord(
                        System.nanoTime(),
                        encoded,
                        System.currentTimeMillis(),
                        batchId,
                        "fastpath",
                        "DATA");
        wal.append(walRecord, true);
    }
}
