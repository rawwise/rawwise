package io.github.rawwise.model.record;

import io.github.rawwise.engine.storage.table.read.TableReader;
import io.github.rawwise.model.op.Find;
import java.util.Map;

public interface KVTable extends BucketTable {

    PrimaryKey primaryKey();

    default boolean hasPrimaryKey() {
        return primaryKey() != null && !primaryKey().isEmpty();
    }

    default Find.Result find(Map<String, Comparable> pkValues) {
        if (pkValues == null || pkValues.isEmpty()) {
            return Find.Result.error("Primary key values are required", null);
        }
        try {
            TableReader reader = tableReader();
            if (reader == null) {
                return Find.Result.error("TableReader not available for KVTable.find()", null);
            }
            Find find =
                    Find.builder()
                            .withTable(this)
                            .withPrimaryKey(pkValues)
                            .withReader(reader)
                            .build();
            return find.exec();
        } catch (UnsupportedOperationException e) {
            return Find.Result.error("KVTable.tableReader() not implemented: " + e.getMessage(), e);
        }
    }

    default TableReader tableReader() {
        throw new UnsupportedOperationException("tableReader() not implemented");
    }
}
