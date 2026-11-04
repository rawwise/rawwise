package io.github.rawwise.model.record;

import io.github.rawwise.engine.storage.codec.ValueCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class RecordConverter {

    private RecordConverter() {}

    /**
     * Convert a list of row data maps to Records using the table's schema and codec.
     *
     * <p>Each map entry should have field names matching the schema. Primary keys are encoded using
     * the Value Codec; if no primary keys are defined, a synthetic UUID key is generated.
     */
    public static List<Record> convertToRecords(
            RawWiseTable table,
            ValueCodec codec,
            List<Map<String, Object>> data,
            long tableId,
            long sequenceNumber,
            long commitTimestamp,
            Record.Kind kind) {
        if (data == null || data.isEmpty()) {
            return List.of();
        }

        Schema schema = table.schema();
        List<Record> records = new ArrayList<>(data.size());
        for (Map<String, Object> rowData : data) {
            if (rowData == null || rowData.isEmpty()) {
                continue;
            }
            try {
                Record record =
                        Records.ofSchema(
                                schema,
                                rowData,
                                kind,
                                codec,
                                tableId,
                                sequenceNumber,
                                commitTimestamp);
                records.add(record);
            } catch (Exception e) {
                log.warn("Failed to convert row data to Record: {}", e.getMessage());
            }
        }
        return records;
    }

    /** Convert a single row data map to a Record. */
    public static Record convertSingleRecord(
            Schema schema,
            ValueCodec codec,
            Map<String, Object> rowData,
            long tableId,
            long sequenceNumber,
            long commitTimestamp,
            Record.Kind kind) {
        if (rowData == null || rowData.isEmpty()) {
            return null;
        }
        return Records.ofSchema(
                schema, rowData, kind, codec, tableId, sequenceNumber, commitTimestamp);
    }

    /** Convert a Record's body to a Map using Record.Body.to Map(). */
    public static Map<String, Object> recordToMap(Record record) {
        if (record == null) {
            return Map.of();
        }
        return record.body().toMap();
    }
}
