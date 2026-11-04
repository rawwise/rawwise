package io.github.rawwise.engine.storage;

import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class ParquetFormatConstants {

    public static final String PK_COLUMN_NAME = "__pk__";
    public static final String COMMIT_TIMESTAMP_COLUMN_NAME = "__commit_ts__";

    public static final String META_FORMAT_VERSION = "rawwise.formatVersion";
    public static final String META_SCHEMA_VERSION = "rawwise.schemaVersion";
    public static final String META_BODY_CODEC = "rawwise.bodyCodec";
    public static final String META_NUMERIC_TABLE_ID = "rawwise.numericTableId";
    public static final String META_PK_FIELD_NAMES = "rawwise.pkFieldNames";
    public static final String META_COLUMN_GROUPS = "rawwise.columnGroups";
    public static final String META_SPARSE_INDEX = "rawwise.sparseIndex";

    private ParquetFormatConstants() {}

    public static List<String> computeNonCgFieldNames(
            Schema schema, List<ColumnGroup> columnGroups) {
        Set<String> excluded = new HashSet<>();
        if (columnGroups != null) {
            for (ColumnGroup cg : columnGroups) {
                excluded.addAll(cg.columnNames());
            }
        }
        List<String> result = new ArrayList<>();
        for (StructField field : schema.structType().fields()) {
            if (!excluded.contains(field.name())) {
                result.add(field.name());
            }
        }
        return result;
    }
}
