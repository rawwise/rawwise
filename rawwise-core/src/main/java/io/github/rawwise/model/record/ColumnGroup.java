package io.github.rawwise.model.record;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record ColumnGroup(
        String name,
        List<String> columnNames,
        List<RawWiseType> columnTypes,
        Encoding encoding,
        int schemaVersion,
        List<Integer> columnFieldIds)
        implements Serializable {

    private static final long serialVersionUID = 2L;

    public enum Encoding {
        FIXED_OFFSET,
        TLV
    }

    public ColumnGroup {
        if (name == null || name.trim().isEmpty())
            throw new IllegalArgumentException("ColumnGroup name cannot be null or empty");
        if (columnNames == null || columnNames.isEmpty())
            throw new IllegalArgumentException("ColumnGroup must contain at least one column");
        if (encoding == null)
            throw new IllegalArgumentException("ColumnGroup encoding cannot be null");
        if (columnTypes != null
                && !columnTypes.isEmpty()
                && columnTypes.size() != columnNames.size())
            throw new IllegalArgumentException("columnTypes size must match columnNames size");
        if (columnFieldIds != null
                && !columnFieldIds.isEmpty()
                && columnFieldIds.size() != columnNames.size())
            throw new IllegalArgumentException("columnFieldIds size must match columnNames size");
        Set<String> seen = new HashSet<>();
        for (String col : columnNames) {
            if (!seen.add(col))
                throw new IllegalArgumentException("Duplicate column name in ColumnGroup: " + col);
        }
        columnNames = List.copyOf(columnNames);
        columnTypes = columnTypes != null ? List.copyOf(columnTypes) : List.of();
        columnFieldIds = columnFieldIds != null ? List.copyOf(columnFieldIds) : List.of();
    }

    public ColumnGroup(
            String name,
            List<String> columnNames,
            List<RawWiseType> columnTypes,
            Encoding encoding) {
        this(name, columnNames, columnTypes, encoding, 1, null);
    }

    public ColumnGroup(
            String name,
            List<String> columnNames,
            List<RawWiseType> columnTypes,
            Encoding encoding,
            int schemaVersion) {
        this(name, columnNames, columnTypes, encoding, schemaVersion, null);
    }

    public ColumnGroup(
            String name, List<String> columnNames, Encoding encoding, int schemaVersion) {
        this(name, columnNames, null, encoding, schemaVersion, null);
    }

    public ColumnGroup(String name, List<String> columnNames, Encoding encoding) {
        this(name, columnNames, null, encoding, 1, null);
    }

    public int columnFieldId(int index) {
        if (columnFieldIds.isEmpty()) return 0;
        return columnFieldIds.get(index);
    }

    public int columnFieldId(String columnName) {
        int idx = columnNames.indexOf(columnName);
        if (idx < 0 || columnFieldIds.isEmpty()) return 0;
        return columnFieldIds.get(idx);
    }

    public RawWiseType columnType(int index) {
        if (columnTypes.isEmpty()) return null;
        return columnTypes.get(index);
    }

    public RawWiseType columnType(String columnName) {
        int idx = columnNames.indexOf(columnName);
        if (idx < 0 || columnTypes.isEmpty()) return null;
        return columnTypes.get(idx);
    }

    public ColumnGroup withNewVersion() {
        return new ColumnGroup(
                name, columnNames, columnTypes, encoding, schemaVersion + 1, columnFieldIds);
    }

    public boolean containsColumn(String columnName) {
        return columnNames.contains(columnName);
    }
}
