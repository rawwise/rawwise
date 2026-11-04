package io.github.rawwise.backend.spark;

import io.github.rawwise.model.record.TableMeta;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public record RawWiseSparkTable(
        String basePath,
        String storeId,
        String tableIdentifier,
        StructType schema,
        TableMeta tableMeta)
        implements Table, SupportsRead, SupportsWrite {

    @Override
    public String name() {
        return tableIdentifier;
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new RawWiseDataSource.RawWiseScanBuilder(
                basePath, storeId, tableIdentifier, schema, tableMeta);
    }

    @Override
    public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
        return new RawWiseDataSource.RawWiseWriteBuilder(
                basePath,
                storeId,
                tableIdentifier,
                info.schema(),
                tableMeta,
                RawWiseDataSource.ServerTxnCoordinatorHolder.getConfig(),
                RawWiseDataSource.ServerTxnCoordinatorHolder.getCoordinator());
    }
}
