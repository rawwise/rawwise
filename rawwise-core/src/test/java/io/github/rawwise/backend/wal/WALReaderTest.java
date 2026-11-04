package io.github.rawwise.backend.wal;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.CRC32;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("WALReader - WALread test")
class WALReaderTest {

    @TempDir Path tempDir;

    private String walDir;
    private Configuration hadoopConfig;

    @BeforeEach
    void setUp() {
        walDir = tempDir.resolve("wal").toString();
        hadoopConfig = new Configuration();
    }

    private DefaultWALRecord createDataRecord(long sequence, byte[] data, String tableId) {
        return new DefaultWALRecord(sequence, data, System.nanoTime(), 1L, tableId, "DATA");
    }

    private DefaultWALRecord createSyncMarker(long sequence, String tableId) {
        return new DefaultWALRecord(
                sequence, new byte[0], System.nanoTime(), -1L, tableId, "SYNC_MARKER");
    }

    private void writeNWLFile(LocalWALWriter writer, List<WALRecord> records) throws IOException {
        for (WALRecord record : records) {
            writer.write(record);
        }
        writer.close();
    }

    @Nested
    @DisplayName("Write-then-Read test")
    class RoundTripTest {

        @Test
        @DisplayName("writeNWL afterWALReadercanread allrecord")
        void testReadNWLFileWrittenByLocalWALWriter() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(3);
            assertThat(records.get(0).sequence()).isEqualTo(1);
            assertThat(records.get(1).sequence()).isEqualTo(2);
            assertThat(records.get(2).sequence()).isEqualTo(3);
        }

        @Test
        @DisplayName("read ofrecorddata correct")
        void testReadBackDataContent() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            byte[] data1 = "hello_world".getBytes();
            byte[] data2 = "another_record".getBytes();
            writer.write(createDataRecord(1, data1, "table1"));
            writer.write(createDataRecord(2, data2, "table2"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(2);
            assertThat(records.get(0).value()).isEqualTo(data1);
            assertThat(records.get(0).uniqTableIdentifier()).isEqualTo("table1");
            assertThat(records.get(1).value()).isEqualTo(data2);
            assertThat(records.get(1).uniqTableIdentifier()).isEqualTo("table2");
        }

        @Test
        @DisplayName("read ofcommitIdcorrect")
        void testReadBackCommitId() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            DefaultWALRecord record =
                    new DefaultWALRecord(
                            1, "data".getBytes(), System.nanoTime(), 42L, "table1", "DATA");
            writer.write(record);
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(1);
            assertThat(records.get(0).commitId()).isEqualTo(42L);
        }
    }

    @Nested
    @DisplayName("multiNWL readtest")
    class MultipleNWLFilesTest {

        @Test
        @DisplayName("readmultipleNWL")
        void testReadMultipleNWLFiles() throws IOException {
            LocalWALWriter writer1 = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer1.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer1.close();

            LocalWALWriter writer2 = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer2.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer2.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(2);
        }
    }

    @Nested
    @DisplayName("filterUnflushedtest")
    class FilterUnflushedTest {

        @Test
        @DisplayName("noSYNC_MARKERreturns whenallDATArecord")
        void testFilterUnflushedWithoutSyncMarker() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.close();

            NwlWALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).hasSize(2);
        }

        @Test
        @DisplayName("hasSYNC_MARKERreturns when afterSYNC_MARKER afterofrecord")
        void testFilterUnflushedWithSyncMarker() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.write(createSyncMarker(2, "table1"));
            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            writer.write(createDataRecord(4, "data4".getBytes(), "table1"));
            writer.close();

            NwlWALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).hasSize(2);
            assertThat(unflushed.get(0).sequence()).isEqualTo(3);
            assertThat(unflushed.get(1).sequence()).isEqualTo(4);
        }

        @Test
        @DisplayName("SYNC_MARKERis after itemrecord returns empty list")
        void testFilterUnflushedWhenSyncMarkerIsLast() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker(1, "table1"));
            writer.close();

            NwlWALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).isEmpty();
        }

        @Test
        @DisplayName("multipleSYNC_MARKER after itemis")
        void testFilterUnflushedWithMultipleSyncMarkers() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker(1, "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.write(createSyncMarker(2, "table1"));
            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            writer.close();

            NwlWALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).hasSize(1);
            assertThat(unflushed.get(0).sequence()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("WAL test")
    class EmptyWALDirectoryTest {

        @Test
        @DisplayName("readnon-existent returns empty list")
        void testReadNonExistentDirectory() throws IOException {
            String nonExistent = tempDir.resolve("non_existent").toString();
            WALReader reader = new NwlWALReader(nonExistent, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }

        @Test
        @DisplayName("read returns empty list")
        void testReadEmptyDirectory() throws IOException {
            Path emptyDir = tempDir.resolve("empty_wal");
            Files.createDirectories(emptyDir);

            WALReader reader = new NwlWALReader(emptyDir.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }
    }

    @Nested
    @DisplayName("NWL test")
    class CorruptedNWLFileTest {

        @Test
        @DisplayName("MAGICmismatched NWL files are skipped")
        void testBadMagicSkipped() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            ByteBuffer buffer = ByteBuffer.allocate(64);
            buffer.put("BAD0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(1);
            buffer.putInt(0);
            buffer.putInt(0);
            Files.write(badFile, buffer.array());

            WALReader reader = new NwlWALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }

        @Test
        @DisplayName("VERSIONmismatched NWL files are skipped")
        void testBadVersionSkipped() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            ByteBuffer buffer = ByteBuffer.allocate(64);
            buffer.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(999);
            buffer.putInt(0);
            buffer.putInt(0);
            Files.write(badFile, buffer.array());

            WALReader reader = new NwlWALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }

        @Test
        @DisplayName("Header CRCmismatched NWL files are skipped")
        void testBadHeaderCrcSkipped() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            String headerJson =
                    "{\"storeId\":\"test\",\"executorId\":\"local\",\"createdAt\":1000,\"compression\":\"NONE\"}";
            byte[] headerJsonBytes = headerJson.getBytes(StandardCharsets.UTF_8);

            int totalSize = 4 + 4 + 4 + headerJsonBytes.length + 4;
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            buffer.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(1);
            buffer.putInt(headerJsonBytes.length);
            buffer.put(headerJsonBytes);
            buffer.putInt(0xDEADBEEF);

            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            Files.write(badFile, buffer.array());

            WALReader reader = new NwlWALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }

        @Test
        @DisplayName("Record CRCmismatched records are skipped")
        void testBadRecordCrcSkipped() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            String headerJson =
                    "{\"storeId\":\"test\",\"executorId\":\"local\",\"createdAt\":1000,\"compression\":\"NONE\"}";
            byte[] headerJsonBytes = headerJson.getBytes(StandardCharsets.UTF_8);

            ByteBuffer headerBuf = ByteBuffer.allocate(4 + 4 + 4 + headerJsonBytes.length + 4);
            headerBuf.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            headerBuf.putInt(1);
            headerBuf.putInt(headerJsonBytes.length);
            headerBuf.put(headerJsonBytes);

            CRC32 headerCrc = new CRC32();
            headerCrc.update(headerBuf.array(), 0, headerBuf.position());
            headerBuf.putInt((int) headerCrc.getValue());

            byte[] payload = new byte[1 + 8 + 8 + 4 + 0 + 8 + 4 + 0];
            ByteBuffer payloadBuf = ByteBuffer.wrap(payload);
            payloadBuf.put((byte) 1);
            payloadBuf.putLong(1L);
            payloadBuf.putLong(System.nanoTime());
            payloadBuf.putInt(0);
            payloadBuf.putLong(1L);
            payloadBuf.putInt(0);

            ByteBuffer recordBuf = ByteBuffer.allocate(4 + payload.length + 4);
            recordBuf.putInt(payload.length);
            recordBuf.put(payload);
            recordBuf.putInt(0xBADBAD);

            ByteBuffer fileBuf = ByteBuffer.allocate(headerBuf.position() + recordBuf.position());
            fileBuf.put(headerBuf.array(), 0, headerBuf.position());
            fileBuf.put(recordBuf.array(), 0, recordBuf.position());

            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            Files.write(badFile, fileBuf.array());

            WALReader reader = new NwlWALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }
    }

    @Nested
    @DisplayName("format test")
    class MixedFormatTest {

        @Test
        @DisplayName("innon-NWL/Parquet be")
        void testNonWalFilesIgnored() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            Path txtFile = walPath.resolve("readme.txt");
            Files.writeString(txtFile, "not a wal file");

            Path jsonFile = walPath.resolve("config.json");
            Files.writeString(jsonFile, "{}");

            WALReader reader = new NwlWALReader(walPath.toString(), hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isEmpty();
        }
    }

    @Nested
    @DisplayName("sorttest")
    class FileOrderingTest {

        @Test
        @DisplayName("NWL sequence numbersort")
        void testFilesSortedBySequenceNumber() throws IOException {
            long maxSize = 256L;
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, maxSize, 0L);

            byte[] data = new byte[100];
            java.util.Arrays.fill(data, (byte) 'A');

            writer.write(createDataRecord(1, data, "table1"));
            writer.write(createDataRecord(2, data, "table1"));
            writer.write(createDataRecord(3, data, "table1"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(3);
            assertThat(records.get(0).sequence()).isEqualTo(1);
            assertThat(records.get(1).sequence()).isEqualTo(2);
            assertThat(records.get(2).sequence()).isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("Header CRCverifytest")
    class HeaderCrcValidationTest {

        @Test
        @DisplayName("correctofHeader CRCviaverify")
        void testValidHeaderCrcPasses() throws IOException {
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).isNotEmpty();
        }
    }

    @Nested
    @DisplayName("closetest")
    class CloseTest {

        @Test
        @DisplayName("close()security")
        void testCloseIsSafe() throws IOException {
            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            reader.close();
            reader.close();
        }
    }

    // ==================== v16 FIX: validateSequenceOrder ====================

    @Nested
    @DisplayName("v16: validateSequenceOrder column verifytest (C2)")
    class ValidateSequenceOrderTest {

        @Test
        @DisplayName("singlecall of column shouldtheviaverify")
        void testValidateSequenceOrderAcceptsMonotonicIncreasing() throws Exception {
            WALReader reader = new NwlWALReader(walDir, hadoopConfig);

            List<WALRecord> records =
                    List.of(
                            createDataRecord(1, "data1".getBytes(), "table1"),
                            createDataRecord(2, "data2".getBytes(), "table1"),
                            createDataRecord(3, "data3".getBytes(), "table1"),
                            createDataRecord(100, "data4".getBytes(), "table1"));

            // via call has
            var method = NwlWALReader.class.getDeclaredMethod("validateSequenceOrder", List.class);
            method.setAccessible(true);

            assertThatCode(() -> method.invoke(reader, records)).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("non-singlecall of column shouldthe WALRecoveryException")
        void testValidateSequenceOrderThrowsOnNonMonotonicSequence() throws Exception {
            WALReader reader = new NwlWALReader(walDir, hadoopConfig);

            // column : 1, 3, 2 -> 2 < 3, non-singlecall
            List<WALRecord> records =
                    List.of(
                            createDataRecord(1, "data1".getBytes(), "table1"),
                            createDataRecord(3, "data2".getBytes(), "table1"),
                            createDataRecord(2, "data3".getBytes(), "table1"));

            var method = NwlWALReader.class.getDeclaredMethod("validateSequenceOrder", List.class);
            method.setAccessible(true);

            assertThatThrownBy(() -> method.invoke(reader, records))
                    .isInstanceOf(java.lang.reflect.InvocationTargetException.class)
                    .cause()
                    .isInstanceOf(WALRecoveryException.class)
                    .hasMessageContaining("WAL sequence number not monotonically increasing");
        }

        @Test
        @DisplayName("columntableshouldtheviaverify")
        void testValidateSequenceOrderAcceptsEmptyList() throws Exception {
            WALReader reader = new NwlWALReader(walDir, hadoopConfig);

            var method = NwlWALReader.class.getDeclaredMethod("validateSequenceOrder", List.class);
            method.setAccessible(true);

            assertThatCode(() -> method.invoke(reader, List.of())).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("same column of recordshouldtheviaverify (etc fallback)")
        void testValidateSequenceOrderAcceptsEqualSequences() throws Exception {
            WALReader reader = new NwlWALReader(walDir, hadoopConfig);

            List<WALRecord> records =
                    List.of(
                            createDataRecord(1, "data1".getBytes(), "table1"),
                            createDataRecord(1, "data2".getBytes(), "table1"),
                            createDataRecord(2, "data3".getBytes(), "table1"));

            var method = NwlWALReader.class.getDeclaredMethod("validateSequenceOrder", List.class);
            method.setAccessible(true);

            assertThatCode(() -> method.invoke(reader, records)).doesNotThrowAnyException();
        }
    }

    // ==================== v16 FIX: Header Length Validation ====================

    @Nested
    @DisplayName("v16: Header verifytest (C2)")
    class HeaderLengthValidationTest {

        @Test
        @DisplayName("no ofheader ()shouldthrows exception")
        void testNegativeHeaderLengthThrows() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            // createheaderLengthis of
            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            ByteBuffer buffer = ByteBuffer.allocate(64);
            buffer.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(1);
            buffer.putInt(-1); // ofheaderLength
            Files.write(badFile, buffer.array());

            WALReader reader = new NwlWALReader(walPath.toString(), hadoopConfig);

            assertThatThrownBy(() -> reader.readAll())
                    .isInstanceOf(WALRecoveryException.class)
                    .hasMessageContaining("Invalid NWL header length");
        }

        @Test
        @DisplayName("no ofheader (exceedsMAX_HEADER_LENGTH)shouldthrows exception")
        void testExcessiveHeaderLengthThrows() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            // createheaderLengthexceeds1MBof
            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            ByteBuffer buffer = ByteBuffer.allocate(64);
            buffer.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(1);
            buffer.putInt(2 * 1024 * 1024); // 2MB, exceeds 1MB limit
            Files.write(badFile, buffer.array());

            WALReader reader = new NwlWALReader(walPath.toString(), hadoopConfig);

            assertThatThrownBy(() -> reader.readAll())
                    .isInstanceOf(WALRecoveryException.class)
                    .hasMessageContaining("Invalid NWL header length");
        }

        @Test
        @DisplayName("local no header should throwWALRecoveryException")
        void testLocalFileNegativeHeaderLengthThrows() throws IOException {
            Path walPath = tempDir.resolve("wal");
            Files.createDirectories(walPath);

            // createheaderLengthis of local
            Path badFile = walPath.resolve("wal_testStore_1000_0.nwl");
            ByteBuffer buffer = ByteBuffer.allocate(64);
            buffer.put("NWL0".getBytes(StandardCharsets.US_ASCII));
            buffer.putInt(1);
            buffer.putInt(-1); // ofheaderLength
            Files.write(badFile, buffer.array());

            WALReader reader = new NwlWALReader(walPath.toString(), hadoopConfig);

            assertThatThrownBy(() -> reader.readAll())
                    .isInstanceOf(WALRecoveryException.class)
                    .hasMessageContaining("Invalid NWL header length");
        }
    }
}
