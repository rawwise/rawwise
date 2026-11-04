package io.github.rawwise.backend.spark;

import io.github.rawwise.common.catalog.Catalog;
import io.github.rawwise.common.catalog.CatalogProviderRegistry;
import io.github.rawwise.common.catalog.TableProperties;
import io.github.rawwise.common.exception.CatalogException;
import io.github.rawwise.common.exception.TableAlreadyExistsException;
import io.github.rawwise.common.exception.TableNotFoundException;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.governance.config.RawWiseOptions;
import io.github.rawwise.model.record.PartitionSpec;
import io.github.rawwise.model.record.PrimaryKey;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.SparkSchemaConverter;
import io.github.rawwise.model.record.TableId;
import io.github.rawwise.model.record.TableMeta;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

@Slf4j
public final class RawWiseSparkCatalog implements TableCatalog, SupportsNamespaces {

    private String name;
    private Catalog catalog;
    private String warehousePath;
    private final String defaultNamespace = "default";

    @Override
    public String name() {
        return name;
    }

    @Override
    public String[] defaultNamespace() {
        return new String[] {defaultNamespace};
    }

    @Override
    public void initialize(String catalogName, CaseInsensitiveStringMap options) {
        this.name = catalogName;
        this.warehousePath =
                options.getOrDefault(
                        "warehouse-path", RawWiseOptions.CATALOG_WAREHOUSE_PATH.defaultValue());
        String catalogImpl = options.getOrDefault("catalog-implementation", "metastore");

        StorageRegistry.registerDefaults();

        CatalogProviderRegistry registry = CatalogProviderRegistry.getInstance();
        if (!registry.isRegistered(catalogImpl)) {
            registry.registerDefaults();
        }
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("catalog-name", catalogName);
        catalogProps.put("base-path", warehousePath);
        String storeKind = options.getOrDefault("store-kind", "inmemory");
        catalogProps.put("store-kind", storeKind);
        if ("grpc".equals(storeKind)) {
            catalogProps.put(
                    "metastore-grpc-host",
                    options.getOrDefault("metastore-grpc-host", "127.0.0.1"));
            catalogProps.put(
                    "metastore-grpc-port", options.getOrDefault("metastore-grpc-port", "9001"));
        }
        this.catalog = registry.create(catalogImpl, catalogProps);

        // Ensure default namespace exists in shared metastore
        try {
            if (!catalog.namespaceExists(defaultNamespace)) {
                catalog.createNamespace(defaultNamespace);
            }
        } catch (Exception e) {
            log.error("Failed to ensure default namespace: {}", e.getMessage(), e);
        }

        log.info(
                "RawWiseSparkCatalog initialized: name={}, warehouse={}, impl={}, storeKind={}",
                catalogName,
                warehousePath,
                catalogImpl,
                storeKind);
    }

    // ============ SupportsNamespaces ============

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        List<String> namespaces = catalog.listNamespaces();
        if (namespaces.contains(defaultNamespace)) {
            return namespaces.stream().map(ns -> new String[] {ns}).toArray(String[][]::new);
        }
        namespaces = new ArrayList<>(namespaces);
        namespaces.add(0, defaultNamespace);
        return namespaces.stream().map(ns -> new String[] {ns}).toArray(String[][]::new);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        return listNamespaces();
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata)
            throws org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException {
        if (namespace == null || namespace.length == 0) {
            throw new IllegalArgumentException("Namespace cannot be empty");
        }
        catalog.createNamespace(namespace[0]);
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade)
            throws NoSuchNamespaceException,
                    org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException {
        if (namespace == null || namespace.length == 0) {
            throw new IllegalArgumentException("Namespace cannot be empty");
        }
        return catalog.dropNamespace(namespace[0]);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace)
            throws NoSuchNamespaceException {
        if (namespace == null || namespace.length == 0) {
            throw new IllegalArgumentException("Namespace cannot be empty");
        }
        if (!catalog.namespaceExists(namespace[0])) {
            throw new NoSuchNamespaceException(namespace);
        }
        Map<String, String> metadata = new HashMap<>();
        metadata.put("name", namespace[0]);
        return metadata;
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes)
            throws NoSuchNamespaceException {
        throw new UnsupportedOperationException("alterNamespace is not supported");
    }

    // ============ TableCatalog ============

    @Override
    public Table loadTable(Identifier ident)
            throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException {
        TableId tableId = toTableId(ident);
        log.info(
                "loadTable called: ident={}, tableId={}, warehousePath={}",
                ident,
                tableId,
                warehousePath);
        try {
            RawWiseTable rawwiseTable = catalog.loadTable(tableId);
            TableMeta meta = rawwiseTable.meta();
            if (meta.primaryKey().isEmpty() && meta.properties().containsKey("primary-key")) {
                String pkProp = meta.properties().get("primary-key");
                List<String> pkFields = java.util.Arrays.asList(pkProp.split(","));
                PrimaryKey pk =
                        new PrimaryKey(
                                pkFields.stream()
                                        .map(String::trim)
                                        .filter(s -> !s.isEmpty())
                                        .toList());
                meta = TableMeta.builder(meta).withPrimaryKey(pk).existingTable(false).build();
                log.info(
                        "loadTable: auto-corrected PK from properties for {}: {}",
                        ident,
                        pk.fields());
            }
            StructType sparkSchema = SparkSchemaConverter.toSparkSchema(meta.schema());
            log.info(
                    "loadTable success: ident={}, pk={}, properties={}",
                    ident,
                    meta.primaryKey().fields(),
                    meta.properties());
            return new RawWiseSparkTable(
                    warehousePath, "spark", ident.toString(), sparkSchema, meta);
        } catch (TableNotFoundException e) {
            log.warn("loadTable not found: ident={}", ident);
            throw new org.apache.spark.sql.catalyst.analysis.NoSuchTableException(ident);
        }
    }

    @Override
    public Table createTable(
            Identifier ident,
            StructType schema,
            Transform[] partitions,
            Map<String, String> properties)
            throws org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException,
                    NoSuchNamespaceException {
        TableId tableId = toTableId(ident);
        try {
            if (!catalog.namespaceExists(tableId.database())) {
                log.info("Creating namespace: {}", tableId.database());
                try {
                    catalog.createNamespace(tableId.database());
                } catch (Exception e) {
                    log.error(
                            "Failed to create namespace {}: {}",
                            tableId.database(),
                            e.getMessage(),
                            e);
                    throw new NoSuchNamespaceException(ident.namespace());
                }
            }
            Schema rawwiseSchema = SparkSchemaConverter.toRawWiseSchema(schema);
            PartitionSpec partitionSpec = convertTransforms(partitions);
            TableProperties tableProps =
                    new TableProperties(properties != null ? properties : Map.of());
            RawWiseTable rawwiseTable =
                    catalog.createTable(tableId, rawwiseSchema, partitionSpec, tableProps);
            TableMeta meta = rawwiseTable.meta();
            return new RawWiseSparkTable(warehousePath, "spark", ident.toString(), schema, meta);
        } catch (TableAlreadyExistsException e) {
            throw new org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException(ident);
        } catch (CatalogException e) {
            log.error("createTable failed for {}: {}", ident, e.getMessage(), e);
            throw new NoSuchNamespaceException(ident.namespace());
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes)
            throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException {
        TableId tableId = toTableId(ident);
        try {
            RawWiseTable rawwiseTable = catalog.loadTable(tableId);
            Schema currentSchema = rawwiseTable.meta().schema();

            List<TableChange> columnChanges = new ArrayList<>();
            Map<String, String> propertyChanges = new HashMap<>();

            for (TableChange change : changes) {
                if (isColumnChange(change)) {
                    columnChanges.add(change);
                } else if (change instanceof TableChange.SetProperty sp) {
                    propertyChanges.put(sp.property(), sp.value());
                } else if (change instanceof TableChange.RemoveProperty rp) {
                    propertyChanges.put(rp.property(), null);
                }
            }

            if (!columnChanges.isEmpty()) {
                StructType currentSparkSchema = SparkSchemaConverter.toSparkSchema(currentSchema);
                StructType newSparkSchema = applyColumnChanges(currentSparkSchema, columnChanges);
                Schema newSchema = SparkSchemaConverter.toRawWiseSchema(newSparkSchema);
                catalog.alterSchema(tableId, newSchema);
            }

            if (!propertyChanges.isEmpty()) {
                catalog.updateProperties(tableId, new TableProperties(propertyChanges));
            }

            rawwiseTable = catalog.loadTable(tableId);
            TableMeta meta = rawwiseTable.meta();
            StructType sparkSchema = SparkSchemaConverter.toSparkSchema(meta.schema());
            return new RawWiseSparkTable(
                    warehousePath, "spark", ident.toString(), sparkSchema, meta);
        } catch (TableNotFoundException e) {
            throw new org.apache.spark.sql.catalyst.analysis.NoSuchTableException(ident);
        }
    }

    @Override
    public boolean dropTable(Identifier ident) {
        TableId tableId = toTableId(ident);
        return catalog.dropTable(tableId, false);
    }

    @Override
    public boolean purgeTable(Identifier ident) {
        TableId tableId = toTableId(ident);
        return catalog.dropTable(tableId, true);
    }

    @Override
    public void renameTable(Identifier oldIdent, Identifier newIdent)
            throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException,
                    org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException {
        TableId from = toTableId(oldIdent);
        TableId to = toTableId(newIdent);
        catalog.renameTable(from, to);
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        if (namespace == null || namespace.length == 0) {
            return new Identifier[0];
        }
        List<TableId> tableIds = catalog.listTables(namespace[0]);
        return tableIds.stream()
                .map(tid -> Identifier.of(new String[] {tid.database()}, tid.tableName()))
                .toArray(Identifier[]::new);
    }

    // ============ Internal Helpers ============

    private static TableId toTableId(Identifier ident) {
        String[] ns = ident.namespace();
        String database = ns.length > 0 ? ns[0] : "default";
        return new TableId(database, ident.name());
    }

    private static boolean isColumnChange(TableChange change) {
        return change instanceof TableChange.AddColumn
                || change instanceof TableChange.DeleteColumn
                || change instanceof TableChange.RenameColumn
                || change instanceof TableChange.UpdateColumnType
                || change instanceof TableChange.UpdateColumnNullability
                || change instanceof TableChange.UpdateColumnComment
                || change instanceof TableChange.UpdateColumnPosition;
    }

    private static StructType applyColumnChanges(
            StructType currentSchema, List<TableChange> columnChanges) {
        List<StructField> fields = new ArrayList<>(Arrays.asList(currentSchema.fields()));

        for (TableChange change : columnChanges) {
            if (change instanceof TableChange.AddColumn ac) {
                String[] fieldNames = ac.fieldNames();
                if (fieldNames.length == 1) {
                    boolean nullable = ac.isNullable();
                    String comment = ac.comment() != null ? ac.comment() : "";
                    MetadataBuilder mb = new MetadataBuilder();
                    if (!comment.isEmpty()) {
                        mb.putString("comment", comment);
                    }
                    StructField newField =
                            new StructField(fieldNames[0], ac.dataType(), nullable, mb.build());
                    fields.add(newField);
                }
            } else if (change instanceof TableChange.DeleteColumn dc) {
                String[] fieldNames = dc.fieldNames();
                if (fieldNames.length == 1) {
                    fields.removeIf(f -> f.name().equals(fieldNames[0]));
                }
            } else if (change instanceof TableChange.RenameColumn rc) {
                String[] fieldNames = rc.fieldNames();
                if (fieldNames.length == 1) {
                    for (int i = 0; i < fields.size(); i++) {
                        if (fields.get(i).name().equals(fieldNames[0])) {
                            StructField old = fields.get(i);
                            fields.set(
                                    i,
                                    new StructField(
                                            rc.newName(),
                                            old.dataType(),
                                            old.nullable(),
                                            old.metadata()));
                            break;
                        }
                    }
                }
            } else if (change instanceof TableChange.UpdateColumnType uct) {
                String[] fieldNames = uct.fieldNames();
                if (fieldNames.length == 1) {
                    for (int i = 0; i < fields.size(); i++) {
                        if (fields.get(i).name().equals(fieldNames[0])) {
                            StructField old = fields.get(i);
                            fields.set(
                                    i,
                                    new StructField(
                                            fieldNames[0],
                                            uct.newDataType(),
                                            old.nullable(),
                                            old.metadata()));
                            break;
                        }
                    }
                }
            } else if (change instanceof TableChange.UpdateColumnNullability ucn) {
                String[] fieldNames = ucn.fieldNames();
                if (fieldNames.length == 1) {
                    for (int i = 0; i < fields.size(); i++) {
                        if (fields.get(i).name().equals(fieldNames[0])) {
                            StructField old = fields.get(i);
                            fields.set(
                                    i,
                                    new StructField(
                                            fieldNames[0],
                                            old.dataType(),
                                            ucn.nullable(),
                                            old.metadata()));
                            break;
                        }
                    }
                }
            } else if (change instanceof TableChange.UpdateColumnComment ucc) {
                String[] fieldNames = ucc.fieldNames();
                if (fieldNames.length == 1) {
                    for (int i = 0; i < fields.size(); i++) {
                        if (fields.get(i).name().equals(fieldNames[0])) {
                            StructField old = fields.get(i);
                            MetadataBuilder mb = new MetadataBuilder();
                            if (ucc.newComment() != null && !ucc.newComment().isEmpty()) {
                                mb.putString("comment", ucc.newComment());
                            }
                            fields.set(
                                    i,
                                    new StructField(
                                            fieldNames[0],
                                            old.dataType(),
                                            old.nullable(),
                                            mb.build()));
                            break;
                        }
                    }
                }
            } else if (change instanceof TableChange.UpdateColumnPosition ucp) {
                String[] fieldNames = ucp.fieldNames();
                if (fieldNames.length == 1) {
                    int idx = -1;
                    for (int i = 0; i < fields.size(); i++) {
                        if (fields.get(i).name().equals(fieldNames[0])) {
                            idx = i;
                            break;
                        }
                    }
                    if (idx >= 0) {
                        StructField field = fields.remove(idx);
                        if (ucp.position() == null) {
                            fields.add(0, field);
                        } else {
                            fields.add(field);
                        }
                    }
                }
            }
        }
        return new StructType(fields.toArray(new StructField[0]));
    }

    private static PartitionSpec convertTransforms(Transform[] partitions) {
        PartitionSpec.Builder builder = PartitionSpec.builder();
        for (Transform transform : partitions) {
            if ("identity".equals(transform.name())) {
                builder.withIdentityBucket(false);
            }
        }
        return builder.build();
    }
}
