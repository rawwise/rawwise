package io.github.rawwise.backend.wal;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("WALIntegrationTest - WALintegration test")
class WALIntegrationTest {

    @TempDir Path tempDir;

    private Configuration hadoopConfig;

    private DefaultWALRecord createDataRecord(long sequence, byte[] data, String tableId) {
        return new DefaultWALRecord(sequence, data, System.nanoTime(), 1L, tableId, "DATA");
    }

    private DefaultWALRecord createSyncMarker(long sequence, String tableId) {
        return new DefaultWALRecord(
                sequence, new byte[0], System.nanoTime(), -1L, tableId, "SYNC_MARKER");
    }

    @Nested
    @DisplayName("write-recovery test")
    class FullWriteRecoverCycleTest {

        @Test
        @DisplayName("writerecordafterviaWALReaderread , data verify")
        void testWriteAndRecoverDataIntegrity() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            byte[] data1 = "key1:value1".getBytes();
            byte[] data2 = "key2:value2".getBytes();
            byte[] data3 = "key3:value3".getBytes();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, data1, "table1"));
            writer.write(createDataRecord(2, data2, "table1"));
            writer.write(createDataRecord(3, data3, "table1"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(3);
            assertThat(records.get(0).value()).isEqualTo(data1);
            assertThat(records.get(1).value()).isEqualTo(data2);
            assertThat(records.get(2).value()).isEqualTo(data3);

            assertThat(records.get(0).sequence()).isEqualTo(1);
            assertThat(records.get(1).sequence()).isEqualTo(2);
            assertThat(records.get(2).sequence()).isEqualTo(3);
        }

        @Test
        @DisplayName("write recordafterread data")
        void testWriteAndRecoverLargeNumberOfRecords() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            int numRecords = 100;
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            for (int i = 0; i < numRecords; i++) {
                writer.write(createDataRecord(i, ("record_" + i).getBytes(), "table1"));
            }
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(numRecords);
            for (int i = 0; i < numRecords; i++) {
                assertThat(records.get(i).sequence()).isEqualTo(i);
                assertThat(records.get(i).value()).isEqualTo(("record_" + i).getBytes());
            }
        }
    }

    @Nested
    @DisplayName("multi-tablewritetest")
    class MultipleTablesTest {

        @Test
        @DisplayName("writerecords with different tableId, WALReadercorrect tableIdgroup")
        void testWriteMultipleTableIdsAndGroupByTableId() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data_t1_1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "data_t2_1".getBytes(), "table2"));
            writer.write(createDataRecord(3, "data_t1_2".getBytes(), "table1"));
            writer.write(createDataRecord(4, "data_t2_2".getBytes(), "table2"));
            writer.write(createDataRecord(5, "data_t3_1".getBytes(), "table3"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(5);

            Map<String, List<WALRecord>> byTableId =
                    records.stream().collect(Collectors.groupingBy(WALRecord::uniqTableIdentifier));

            assertThat(byTableId).containsKeys("table1", "table2", "table3");
            assertThat(byTableId.get("table1")).hasSize(2);
            assertThat(byTableId.get("table2")).hasSize(2);
            assertThat(byTableId.get("table3")).hasSize(1);
        }

        @Test
        @DisplayName("records with different tableIdpreserves original order")
        void testMultipleTableIdsPreserveOrder() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(10, "data1".getBytes(), "alpha"));
            writer.write(createDataRecord(20, "data2".getBytes(), "beta"));
            writer.write(createDataRecord(30, "data3".getBytes(), "alpha"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records.get(0).sequence()).isEqualTo(10);
            assertThat(records.get(0).uniqTableIdentifier()).isEqualTo("alpha");
            assertThat(records.get(1).sequence()).isEqualTo(20);
            assertThat(records.get(1).uniqTableIdentifier()).isEqualTo("beta");
            assertThat(records.get(2).sequence()).isEqualTo(30);
            assertThat(records.get(2).uniqTableIdentifier()).isEqualTo("alpha");
        }
    }

    @Nested
    @DisplayName("SYNC_MARKERintegration test")
    class SyncMarkerIntegrationTest {

        @Test
        @DisplayName("writerecord+SYNC_MARKER+ multirecord, filterUnflushedcorrectly filters")
        void testSyncMarkerFilterUnflushedIntegration() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "before_sync_1".getBytes(), "table1"));
            writer.write(createDataRecord(2, "before_sync_2".getBytes(), "table1"));
            writer.write(createSyncMarker(2, "table1"));
            writer.write(createDataRecord(3, "after_sync_1".getBytes(), "table1"));
            writer.write(createDataRecord(4, "after_sync_2".getBytes(), "table1"));
            writer.close();

            NwlWALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> allRecords = reader.readAll();
            List<WALRecord> unflushedRecords = reader.readUnflushed();
            reader.close();

            assertThat(allRecords).hasSize(5);
            assertThat(unflushedRecords).hasSize(2);
            assertThat(unflushedRecords.get(0).sequence()).isEqualTo(3);
            assertThat(unflushedRecords.get(1).sequence()).isEqualTo(4);
        }

        @Test
        @DisplayName("multipleSYNC_MARKERscenario, filterUnflushed after itemis")
        void testMultipleSyncMarkersIntegration() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker(1, "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.write(createDataRecord(3, "data3".getBytes(), "table1"));
            writer.write(createSyncMarker(3, "table1"));
            writer.write(createDataRecord(4, "data4".getBytes(), "table1"));
            writer.close();

            NwlWALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> unflushed = reader.readUnflushed();
            reader.close();

            assertThat(unflushed).hasSize(1);
            assertThat(unflushed.get(0).sequence()).isEqualTo(4);
        }

        @Test
        @DisplayName("SYNC_MARKERinreadAllinpackage markerfieldisSYNC_MARKER")
        void testSyncMarkerInReadAll() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data1".getBytes(), "table1"));
            writer.write(createSyncMarker(1, "table1"));
            writer.write(createDataRecord(2, "data2".getBytes(), "table1"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> allRecords = reader.readAll();
            reader.close();

            assertThat(allRecords).hasSize(3);
            assertThat(allRecords.get(0).marker()).isEqualTo("DATA");
            assertThat(allRecords.get(1).marker()).isEqualTo("SYNC_MARKER");
            assertThat(allRecords.get(2).marker()).isEqualTo("DATA");
        }
    }

    @Nested
    @DisplayName("+recoveryintegration test")
    class FileRollingRecoveryTest {

        @Test
        @DisplayName("afterallrecordreadable")
        void testFileRollingAndRecovery() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            long maxSize = 512L;
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, maxSize, 0L);

            byte[] largeData = new byte[200];
            java.util.Arrays.fill(largeData, (byte) 'A');

            for (int i = 0; i < 5; i++) {
                writer.write(createDataRecord(i, largeData, "table1"));
            }
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(5);
            for (int i = 0; i < 5; i++) {
                assertThat(records.get(i).sequence()).isEqualTo(i);
                assertThat(records.get(i).value()).isEqualTo(largeData);
            }
        }
    }

    @Nested
    @DisplayName("valueandboundary conditionsintegration test")
    class EdgeCaseIntegrationTest {

        @Test
        @DisplayName("dataofDATArecordcancorrectly writesandread")
        void testEmptyDataRecordRoundTrip() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(
                    new DefaultWALRecord(1, new byte[0], System.nanoTime(), 1L, "table1", "DATA"));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(1);
            assertThat(records.get(0).value()).isEmpty();
            assertThat(records.get(0).marker()).isEqualTo("DATA");
        }

        @Test
        @DisplayName("tableIdofrecordcancorrectly writesandread")
        void testLongTableIdRoundTrip() throws IOException {
            String walDir = tempDir.resolve("wal").toString();
            hadoopConfig = new Configuration();

            String longTableId =
                    "catalog.database.very_long_table_name_with_many_segments_1234567890";
            LocalWALWriter writer = new LocalWALWriter("testStore", walDir, 1024 * 1024, 0L);
            writer.write(createDataRecord(1, "data".getBytes(), longTableId));
            writer.close();

            WALReader reader = new NwlWALReader(walDir, hadoopConfig);
            List<WALRecord> records = reader.readAll();
            reader.close();

            assertThat(records).hasSize(1);
            assertThat(records.get(0).uniqTableIdentifier()).isEqualTo(longTableId);
        }
    }
}
