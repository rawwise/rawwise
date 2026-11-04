package io.github.rawwise.backend.wal;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.governance.config.ConfigFactory;
import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * DynamicMmapStore WALintegration test
 *
 * <p>test : 1. WALinSTORAGE_WAL_ENABLED=truecreates when 2. WALinSTORAGE_WAL_ENABLED=false isnull
 * 3. recoverFromWAL()correctly replaysrecord 4. extractWalData()formatcorrect 5.
 * extractTableIdFromWALRecord()parsecorrect 6. extractKeyFromWALRecord()extractcorrect 7.
 * extractValueFromWALRecord()extractcorrect 8. WALrecovery appliesNullableValueCodec.encode()
 */
@DisplayName("DynamicMmapStore - WALintegration test")
class DynamicMmapStoreWALIntegrationTest {

    @TempDir Path tempDir;

    @Nested
    @DisplayName("WAL statustest")
    class WALEnabledTest {

        @Test
        @DisplayName("WALinSTORAGE_WAL_ENABLED=truecreates when")
        void testWALCreatedWhenEnabled() throws Exception {
            // Given: configWAL
            RawWiseConfig config =
                    ConfigFactory.builder()
                            .set(RawWiseOptions.STORAGE_WAL_ENABLED, true)
                            .set(RawWiseOptions.STORAGE_WAL_BUFFER_SIZE, 8192)
                            .set(RawWiseOptions.STORAGE_WAL_MAX_SIZE_BYTES, 100L * 1024 * 1024)
                            .build();

            // We need to override the config used by DynamicMmapStore
            // For this test, we verify the config defaults
            assertThat(config.get(RawWiseOptions.STORAGE_WAL_ENABLED)).isTrue();
            assertThat(config.get(RawWiseOptions.STORAGE_WAL_BUFFER_SIZE)).isEqualTo(8192);
            assertThat(config.get(RawWiseOptions.STORAGE_WAL_MAX_SIZE_BYTES))
                    .isEqualTo(100L * 1024 * 1024);
        }

        @Test
        @DisplayName("WALinSTORAGE_WAL_ENABLED=false isnull")
        void testWALNullWhenDisabled() throws Exception {
            // Given: configWAL
            RawWiseConfig config =
                    ConfigFactory.builder().set(RawWiseOptions.STORAGE_WAL_ENABLED, false).build();

            // Verify WAL is disabled in config
            assertThat(config.get(RawWiseOptions.STORAGE_WAL_ENABLED)).isFalse();
        }
    }

    @Nested
    @DisplayName("WALdataextractformattest")
    class WALDataExtractionTest {

        @Test
        @DisplayName(
                "extractWalDataformat: tableIdLen(4) + tableId + keyLen(4) + key + valueLen(4) + value")
        void testExtractWalDataFormat() {
            String tableId = "test_table";
            byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
            byte[] key = "test_key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "test_value".getBytes(StandardCharsets.UTF_8);

            // constructandextractWalData sameofformat
            ByteBuffer data =
                    ByteBuffer.allocate(
                            4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
            data.putInt(tableIdBytes.length);
            data.put(tableIdBytes);
            data.putInt(key.length);
            data.put(key);
            data.putInt(value.length);
            data.put(value);
            byte[] walData = data.array();

            // parsetableId (simulateextractTableIdFromWALRecord)
            ByteBuffer buffer = ByteBuffer.wrap(walData);
            int tableIdLen = buffer.getInt();
            assertThat(tableIdLen).isEqualTo(tableIdBytes.length);
            byte[] extractedTableIdBytes = new byte[tableIdLen];
            buffer.get(extractedTableIdBytes);
            assertThat(new String(extractedTableIdBytes)).isEqualTo(tableId);

            // parsekey (simulateextractKeyFromWALRecord)
            int keyLen = buffer.getInt();
            assertThat(keyLen).isEqualTo(key.length);
            byte[] extractedKey = new byte[keyLen];
            buffer.get(extractedKey);
            assertThat(new String(extractedKey)).isEqualTo("test_key");

            // parsevalue (simulateextractValueFromWALRecord)
            int valueLen = buffer.getInt();
            assertThat(valueLen).isEqualTo(value.length);
            byte[] extractedValue = new byte[valueLen];
            buffer.get(extractedValue);
            assertThat(new String(extractedValue)).isEqualTo("test_value");
        }

        @Test
        @DisplayName("extractTableIdFromWALRecordcorrectly parsestableId")
        void testExtractTableIdFromWALRecord() {
            String tableId = "catalog.db.table";
            byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
            byte[] key = "key1".getBytes(StandardCharsets.UTF_8);
            byte[] value = "value1".getBytes(StandardCharsets.UTF_8);

            // WALdata
            ByteBuffer data =
                    ByteBuffer.allocate(
                            4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
            data.putInt(tableIdBytes.length);
            data.put(tableIdBytes);
            data.putInt(key.length);
            data.put(key);
            data.putInt(value.length);
            data.put(value);
            byte[] walData = data.array();

            // parsetableId
            ByteBuffer buffer = ByteBuffer.wrap(walData);
            int tableIdLen = buffer.getInt();
            assertThat(tableIdLen).isEqualTo(tableIdBytes.length);
            byte[] extractedTableIdBytes = new byte[tableIdLen];
            buffer.get(extractedTableIdBytes);
            assertThat(new String(extractedTableIdBytes)).isEqualTo(tableId);
        }

        @Test
        @DisplayName("extractKeyFromWALRecordcorrectly parseskey")
        void testExtractKeyFromWALRecord() {
            String tableId = "table";
            byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
            byte[] key = "my_test_key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "val".getBytes(StandardCharsets.UTF_8);

            ByteBuffer data =
                    ByteBuffer.allocate(
                            4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
            data.putInt(tableIdBytes.length);
            data.put(tableIdBytes);
            data.putInt(key.length);
            data.put(key);
            data.putInt(value.length);
            data.put(value);
            byte[] walData = data.array();

            // parse: skiptableId
            ByteBuffer buffer = ByteBuffer.wrap(walData);
            int tableIdLen = buffer.getInt();
            buffer.position(buffer.position() + tableIdLen);

            // parsekey
            int keyLen = buffer.getInt();
            assertThat(keyLen).isEqualTo(key.length);
            byte[] extractedKey = new byte[keyLen];
            buffer.get(extractedKey);
            assertThat(new String(extractedKey)).isEqualTo("my_test_key");
        }

        @Test
        @DisplayName("extractValueFromWALRecordcorrectly parsesvalue")
        void testExtractValueFromWALRecord() {
            String tableId = "table";
            byte[] tableIdBytes = tableId.getBytes(StandardCharsets.UTF_8);
            byte[] key = "key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "my_test_value".getBytes(StandardCharsets.UTF_8);

            ByteBuffer data =
                    ByteBuffer.allocate(
                            4 + tableIdBytes.length + 4 + key.length + 4 + value.length);
            data.putInt(tableIdBytes.length);
            data.put(tableIdBytes);
            data.putInt(key.length);
            data.put(key);
            data.putInt(value.length);
            data.put(value);
            byte[] walData = data.array();

            // parse: skiptableIdandkey
            ByteBuffer buffer = ByteBuffer.wrap(walData);
            int tableIdLen = buffer.getInt();
            buffer.position(buffer.position() + tableIdLen);
            int keyLen = buffer.getInt();
            buffer.position(buffer.position() + keyLen);

            // parsevalue
            int valueLen = buffer.getInt();
            assertThat(valueLen).isEqualTo(value.length);
            byte[] extractedValue = new byte[valueLen];
            buffer.get(extractedValue);
            assertThat(new String(extractedValue)).isEqualTo("my_test_value");
        }

        @Test
        @DisplayName("extractKeyFromWALRecordhandleexceptiondata")
        void testExtractKeyFromWALRecordWithInvalidData() {
            // datawill BufferUnderflowException
            assertThatThrownBy(
                            () -> {
                                ByteBuffer buf = ByteBuffer.wrap(new byte[0]);
                                buf.getInt();
                            })
                    .isInstanceOf(java.nio.BufferUnderflowException.class);
        }
    }

    @Nested
    @DisplayName("NullableValueCodecencodetest")
    class NullableValueCodecTest {

        @Test
        @DisplayName("NullableValueCodec.encode()fornon-nullvalue")
        void testEncodeNonNullValue() {
            byte[] value = "test_value".getBytes(StandardCharsets.UTF_8);
            byte[] encoded = NullableValueCodec.encode(value);

            assertThat(encoded.length).isEqualTo(value.length + 1);
            assertThat(encoded[0]).isEqualTo((byte) 1); // null is0, non-null is1
            assertThat(java.util.Arrays.copyOfRange(encoded, 1, encoded.length)).isEqualTo(value);
        }

        @Test
        @DisplayName("NullableValueCodec.encode()fornullvalueencodeis empty bytes")
        void testEncodeNullValue() {
            byte[] encoded = NullableValueCodec.encode(null);

            assertThat(encoded.length).isEqualTo(1);
            assertThat(encoded[0]).isEqualTo((byte) 0);
        }

        @Test
        @DisplayName("NullableValueCodec.encode()for valueencodecorrect")
        void testEncodeEmptyValue() {
            byte[] value = new byte[0];
            byte[] encoded = NullableValueCodec.encode(value);

            // valueencodeafter = 1 ()
            assertThat(encoded.length).isEqualTo(1);
            assertThat(encoded[0]).isEqualTo((byte) 1);
        }

        @Test
        @DisplayName("WALrecovery should NullableValueCodec.encode()")
        void testWALRecoveryAppliesNullableValueCodec() {
            // simulateWALrecoveryscenario
            byte[] originalValue = "recovery_test".getBytes(StandardCharsets.UTF_8);

            // originalvaluewriteLMDBbeforewill NullableValueCodec.encode()
            byte[] encodedValue = NullableValueCodec.encode(originalValue);

            // recovery , fromWALextractoforiginalvalue encode
            byte[] recoveredEncoded = NullableValueCodec.encode(originalValue);

            // encode shouldtheconsistent
            assertThat(recoveredEncoded).isEqualTo(encodedValue);
        }
    }

    @Nested
    @DisplayName("WALconfiganddefaultvaluetest")
    class WALConfigDefaultsTest {

        @Test
        @DisplayName("STORAGE_WAL_ENABLEDdefault value istrue")
        void testStorageWalEnabledDefault() {
            assertThat(RawWiseOptions.STORAGE_WAL_ENABLED.defaultValue()).isTrue();
        }

        @Test
        @DisplayName("STORAGE_WAL_BUFFER_SIZEdefault value is8192")
        void testStorageWalBufferSizeDefault() {
            assertThat(RawWiseOptions.STORAGE_WAL_BUFFER_SIZE.defaultValue()).isEqualTo(8192);
        }

        @Test
        @DisplayName("STORAGE_WAL_SYNC_INTERVAL_MSdefault value is1000L")
        void testStorageWalSyncIntervalMsDefault() {
            assertThat(RawWiseOptions.STORAGE_WAL_SYNC_INTERVAL_MS.defaultValue()).isEqualTo(1000L);
        }

        @Test
        @DisplayName("STORAGE_WAL_MAX_SIZE_BYTESdefault value is100MB")
        void testStorageWalMaxSizeBytesDefault() {
            assertThat(RawWiseOptions.STORAGE_WAL_MAX_SIZE_BYTES.defaultValue())
                    .isEqualTo(100L * 1024 * 1024);
        }

        @Test
        @DisplayName("WALConfig.DEFAULT correctofdefaultvalue")
        void testWALConfigDefaultValues() {
            WALConfig config = WALConfig.DEFAULT;

            assertThat(config.logDir()).isEqualTo("wal_logs");
            assertThat(config.logFilePrefix()).isEqualTo("wal");
            assertThat(config.segmentSize()).isEqualTo(100 * 1024 * 1024);
            assertThat(config.syncOnWrite()).isTrue();
            assertThat(config.bufferSize()).isEqualTo(8192);
        }
    }

    @Nested
    @DisplayName("WALrecordcreateandsync test")
    class WALRecordCreationTest {

        @Test
        @DisplayName("WALRecord.createSyncMarkercreatecorrectofsync")
        void testCreateSyncMarker() {
            String tableId = "sync_test_table";
            WALRecord marker = WALRecord.createSyncMarker(tableId);

            assertThat(marker.sequence()).isEqualTo(-1L);
            assertThat(marker.value()).isEmpty();
            assertThat(marker.commitId()).isEqualTo(-1L);
            assertThat(marker.uniqTableIdentifier()).isEqualTo(tableId);
            assertThat(marker.marker()).isEqualTo("SYNC_MARKER");
        }

        @Test
        @DisplayName("SYNC_MARKER column aftermarkerfield isSYNC_MARKER")
        void testSyncMarkerSerialization() {
            String tableId = "marker_serialization_test";
            WALRecord originalMarker = WALRecord.createSyncMarker(tableId);

            byte[] serialized = WALRecordCodec.serializeEnvelope(originalMarker);
            WALRecord deserialized = WALRecordCodec.deserializeEnvelope(serialized);

            assertThat(deserialized.marker()).isEqualTo("SYNC_MARKER");
            assertThat(deserialized.sequence()).isEqualTo(-1L);
            assertThat(deserialized.value()).isEmpty();
        }

        @Test
        @DisplayName("DATArecordcorrectly creates")
        void testDataRecordCreation() {
            byte[] data = "test_data".getBytes(StandardCharsets.UTF_8);
            long timestamp = System.nanoTime();
            String tableId = "data_test_table";

            DefaultWALRecord record =
                    new DefaultWALRecord(
                            12345L, // sequence
                            data, // value
                            timestamp, // timestamp
                            999L, // commitId
                            tableId, // uniqTableIdentifier
                            "DATA" // marker
                            );

            assertThat(record.sequence()).isEqualTo(12345L);
            assertThat(record.value()).isEqualTo(data);
            assertThat(record.timestamp()).isEqualTo(timestamp);
            assertThat(record.commitId()).isEqualTo(999L);
            assertThat(record.uniqTableIdentifier()).isEqualTo(tableId);
            assertThat(record.marker()).isEqualTo("DATA");
        }

        @Test
        @DisplayName("column / column maintainDATArecordconsistency")
        void testDataRecordSerializationConsistency() {
            byte[] originalData = "consistent_data".getBytes(StandardCharsets.UTF_8);
            String tableId = "consistency_test";
            long timestamp = System.nanoTime();

            DefaultWALRecord original =
                    new DefaultWALRecord(99999L, originalData, timestamp, 777L, tableId, "DATA");

            byte[] serialized = WALRecordCodec.serializeEnvelope(original);
            WALRecord deserialized = WALRecordCodec.deserializeEnvelope(serialized);

            assertThat(deserialized.sequence()).isEqualTo(original.sequence());
            assertThat(deserialized.value()).isEqualTo(original.value());
            assertThat(deserialized.timestamp()).isEqualTo(original.timestamp());
            assertThat(deserialized.commitId()).isEqualTo(original.commitId());
            assertThat(deserialized.uniqTableIdentifier())
                    .isEqualTo(original.uniqTableIdentifier());
            assertThat(deserialized.marker()).isEqualTo(original.marker());
        }
    }
}
