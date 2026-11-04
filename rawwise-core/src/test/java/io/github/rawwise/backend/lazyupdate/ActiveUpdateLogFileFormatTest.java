package io.github.rawwise.backend.lazyupdate;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ActiveUpdateLogFileFormatTest {

    @TempDir Path tempDir;

    private UpdateLogEntry sampleEntry(String tableId, long txnId, long seq) {
        Map<String, Comparable<?>> updates = new HashMap<>();
        updates.put("col1", "value" + seq);
        return new UpdateLogEntry(
                "entry-" + seq, tableId, null, updates, txnId, seq, System.currentTimeMillis());
    }

    @Test
    void writeAndRollover_thenReadArchive_viaFooter() throws IOException {
        Path activePath = tempDir.resolve("active.nwl");
        ActiveUpdateLogFile active = new ActiveUpdateLogFile(activePath, "t1");

        for (long seq = 1; seq <= 200; seq++) {
            active.appendEntry(sampleEntry("t1", 100L, seq));
        }
        active.appendCommitRecord(100L);
        active.fsync();

        Path archivePath = active.rollover();
        assertTrue(archivePath.toFile().exists());
        active.close();

        UpdateLogReader reader = new UpdateLogReader();
        UpdateLog fake =
                new UpdateLog(
                        archivePath.toString(),
                        io.github.rawwise.engine.storage.RawWiseFile.FileFormat.NWL,
                        0L,
                        0L,
                        "t1",
                        0L,
                        0L,
                        0L);
        List<UpdateLogEntry> entries = reader.read(fake);

        assertEquals(200, entries.size(), "rollover 后应读到全部 200 条 entry");
        assertEquals(1L, entries.get(0).sequenceNumber());
        assertEquals(200L, entries.get(199).sequenceNumber());
    }

    @Test
    void prefixRead_viaFooterIndex_returnsCorrectSubset() throws IOException {
        Path activePath = tempDir.resolve("active.nwl");
        ActiveUpdateLogFile active = new ActiveUpdateLogFile(activePath, "t1");

        for (long seq = 1; seq <= 200; seq++) {
            active.appendEntry(sampleEntry("t1", 100L, seq));
        }
        active.appendCommitRecord(100L);
        active.fsync();
        Path archivePath = active.rollover();
        active.close();

        UpdateLogReader reader = new UpdateLogReader();
        UpdateLog fake =
                new UpdateLog(
                        archivePath.toString(),
                        io.github.rawwise.engine.storage.RawWiseFile.FileFormat.NWL,
                        0L,
                        0L,
                        "t1",
                        0L,
                        0L,
                        0L);

        List<UpdateLogEntry> prefix = reader.read(fake, 150L);

        assertEquals(150, prefix.size(), "前缀读 maxSeq=150 应返回 150 条 entry");
        long maxSeq = prefix.stream().mapToLong(UpdateLogEntry::sequenceNumber).max().orElse(0);
        assertTrue(maxSeq <= 150L, "前缀读不应包含 seq > 150 的 entry, actual max=" + maxSeq);
    }

    @Test
    void scanTxnRecords_findsCommitAndAbort() throws IOException {
        Path activePath = tempDir.resolve("active.nwl");
        ActiveUpdateLogFile active = new ActiveUpdateLogFile(activePath, "t1");

        active.appendEntry(sampleEntry("t1", 200L, 1L));
        active.appendCommitRecord(200L);
        active.appendEntry(sampleEntry("t1", 300L, 2L));
        active.appendAbortRecord(300L);
        active.fsync();
        active.close();

        UpdateLogReader reader = new UpdateLogReader();
        UpdateLogReader.TxnRecoverySummary summary = reader.scanTxnRecords(activePath);

        assertTrue(summary.commitTsByTxn().containsKey(200L), "应找到 txn=200 的 commit record");
        assertTrue(summary.abortTsByTxn().containsKey(300L), "应找到 txn=300 的 abort record");
    }

    @Test
    void openExistingFile_rebuildsMemoryState() throws IOException {
        Path activePath = tempDir.resolve("active.nwl");
        ActiveUpdateLogFile active = new ActiveUpdateLogFile(activePath, "t1");

        for (long seq = 1; seq <= 130; seq++) {
            active.appendEntry(sampleEntry("t1", 100L, seq));
        }
        active.fsync();
        active.close();

        ActiveUpdateLogFile reopened = new ActiveUpdateLogFile(activePath, "t1");
        assertEquals(130, reopened.getEntryCount(), "重开后内存 entryCount 应重建");
        reopened.appendEntry(sampleEntry("t1", 100L, 131L));
        assertEquals(131, reopened.getEntryCount());

        Path archivePath = reopened.rollover();
        reopened.close();

        UpdateLogReader reader = new UpdateLogReader();
        UpdateLog fake =
                new UpdateLog(
                        archivePath.toString(),
                        io.github.rawwise.engine.storage.RawWiseFile.FileFormat.NWL,
                        0L,
                        0L,
                        "t1",
                        0L,
                        0L,
                        0L);
        List<UpdateLogEntry> entries = reader.read(fake);
        assertEquals(131, entries.size(), "重开后 rollover 应保留全部 131 条 entry");
    }

    @Test
    void readActiveWithoutFooter_sequentialScan() throws IOException {
        Path activePath = tempDir.resolve("active.nwl");
        ActiveUpdateLogFile active = new ActiveUpdateLogFile(activePath, "t1");

        for (long seq = 1; seq <= 50; seq++) {
            active.appendEntry(sampleEntry("t1", 100L, seq));
        }
        active.fsync();

        UpdateLogReader reader = new UpdateLogReader();
        UpdateLog fake =
                new UpdateLog(
                        activePath.toString(),
                        io.github.rawwise.engine.storage.RawWiseFile.FileFormat.NWL,
                        0L,
                        0L,
                        "t1",
                        0L,
                        0L,
                        0L);
        List<UpdateLogEntry> entries = reader.read(fake);

        assertEquals(50, entries.size(), "active 文件无 footer 应顺序扫描读取全部");
        active.close();
    }
}
