package org.apache.parquet.column.impl;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * A ColumnReadStore that produces {@link RowSkipColumnReader} instances for efficient PK-based
 * point queries. All column readers (including PK column) support row-level skipping without full
 * value decoding.
 */
public class RowSkipReadStore extends ColumnReadStoreImpl {
    private final PageReadStore pageReadStore;
    private final GroupConverter recordConverter;
    private final MessageType schema;
    private final VersionParser.ParsedVersion writerVersion;

    public RowSkipReadStore(
            PageReadStore pageReadStore,
            GroupConverter recordConverter,
            MessageType schema,
            String createdBy) {
        super(pageReadStore, recordConverter, schema, createdBy);
        if (pageReadStore.getRowIndexes().isPresent()) {
            throw new UnsupportedOperationException(
                    "RowSkipReadStore does not support column index filtering");
        }
        this.pageReadStore = pageReadStore;
        this.recordConverter = recordConverter;
        this.schema = schema;
        this.writerVersion = parseVersion(createdBy);
    }

    private static VersionParser.ParsedVersion parseVersion(String createdBy) {
        try {
            return VersionParser.parse(createdBy);
        } catch (RuntimeException | VersionParser.VersionParseException e) {
            return null;
        }
    }

    @Override
    public ColumnReader getColumnReader(ColumnDescriptor path) {
        PrimitiveConverter converter = resolveConverter(path);
        PageReader pageReader = pageReadStore.getPageReader(path);
        return new RowSkipColumnReader(path, pageReader, converter, writerVersion);
    }

    private PrimitiveConverter resolveConverter(ColumnDescriptor path) {
        Type currentType = schema;
        Converter currentConverter = recordConverter;
        for (String fieldName : path.getPath()) {
            GroupType groupType = currentType.asGroupType();
            int fieldIndex = groupType.getFieldIndex(fieldName);
            currentType = groupType.getType(fieldName);
            currentConverter = currentConverter.asGroupConverter().getConverter(fieldIndex);
        }
        return currentConverter.asPrimitiveConverter();
    }
}
