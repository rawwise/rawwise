package io.github.rawwise.model.record;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TableMeta implements Serializable {
    private final long tableId;
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final TableType tableType;
    private final Schema schema;
    private final PrimaryKey primaryKey;
    private final PartitionSpec partitionSpec;
    private final HiddenColumnConfig hiddenColumnConfig;
    private final Map<String, String> properties;
    private final String location;
    private final Instant createdAt;
    private final Instant updatedAt;

    public enum TableType {
        PRIMARY_KEY,
        APPEND_ONLY
    }

    private TableMeta(Builder builder) {
        this.tableId = builder.tableId;
        this.catalogName = builder.catalogName != null ? builder.catalogName : "default";
        this.databaseName = builder.databaseName;
        this.tableName = builder.tableName;
        this.tableType = builder.tableType;
        this.schema = builder.schema;
        this.primaryKey = builder.primaryKey;
        this.partitionSpec =
                builder.partitionSpec != null
                        ? builder.partitionSpec
                        : PartitionSpec.unpartitioned();
        this.hiddenColumnConfig =
                builder.hiddenColumnConfig != null
                        ? builder.hiddenColumnConfig
                        : HiddenColumnConfig.defaultConfig(builder.tableType);
        this.properties = builder.properties != null ? Map.copyOf(builder.properties) : Map.of();
        this.location = builder.location;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
    }

    public long tableId() {
        return tableId;
    }

    public String catalogName() {
        return catalogName;
    }

    public String databaseName() {
        return databaseName;
    }

    public String tableName() {
        return tableName;
    }

    public String fullName() {
        return catalogName + "." + databaseName + "." + tableName;
    }

    public TableType tableType() {
        return tableType;
    }

    public Schema schema() {
        return schema;
    }

    public PrimaryKey primaryKey() {
        return primaryKey;
    }

    public boolean hasPrimaryKey() {
        return primaryKey != null && !primaryKey.fields().isEmpty();
    }

    public PartitionSpec partitionSpec() {
        return partitionSpec;
    }

    public boolean isPartitioned() {
        return partitionSpec.isPartitioned();
    }

    public int numBuckets() {
        return partitionSpec.numBuckets();
    }

    public HiddenColumnConfig hiddenColumnConfig() {
        return hiddenColumnConfig;
    }

    public Map<String, String> properties() {
        return properties;
    }

    public String location() {
        return location;
    }

    public Instant createdAt() {
        return createdAt;
    }

    public Instant updatedAt() {
        return updatedAt;
    }

    private static final String AI_NATIVE_PROPERTY_KEY = "rawwise.ai-native";
    private static final String DERIVATION_STRATEGY_KEY = "derivation.strategy";
    private static final String CHUNKING_STRATEGY_KEY = "chunking.strategy";
    private static final String SEMANTIC_PATTERN_KEY = "semantic.pattern";

    public boolean isAiNative() {
        return "true".equalsIgnoreCase(properties.get(AI_NATIVE_PROPERTY_KEY));
    }

    public String derivationStrategy() {
        return properties.getOrDefault(DERIVATION_STRATEGY_KEY, "auto").toLowerCase();
    }

    public String chunkingStrategy() {
        return properties.get(CHUNKING_STRATEGY_KEY);
    }

    public String semanticPattern() {
        return properties.get(SEMANTIC_PATTERN_KEY);
    }

    public ImmutabilitySource defaultImmutabilitySource() {
        return isAiNative() ? ImmutabilitySource.CREATED : ImmutabilitySource.MUTABLE;
    }

    public boolean isAppendOnly() {
        return tableType == TableType.APPEND_ONLY;
    }

    public boolean isPrimaryKeyTable() {
        return tableType == TableType.PRIMARY_KEY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableMeta that)) return false;
        return tableId == that.tableId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(tableId);
    }

    @Override
    public String toString() {
        return "TableMeta{"
                + "tableId="
                + tableId
                + ", name='"
                + fullName()
                + '\''
                + ", type="
                + tableType
                + ", location='"
                + location
                + '\''
                + '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(TableMeta meta) {
        return new Builder(meta);
    }

    public static class Builder {
        private long tableId;
        private String catalogName = "default";
        private String databaseName;
        private String tableName;
        private TableType tableType;
        private Schema schema;
        private PrimaryKey primaryKey;
        private PartitionSpec partitionSpec;
        private HiddenColumnConfig hiddenColumnConfig;
        private Map<String, String> properties;
        private String location;
        private Instant createdAt;
        private Instant updatedAt;
        private boolean existingTable;

        public Builder() {}

        public Builder(TableMeta meta) {
            this.tableId = meta.tableId;
            this.catalogName = meta.catalogName;
            this.databaseName = meta.databaseName;
            this.tableName = meta.tableName;
            this.tableType = meta.tableType;
            this.schema = meta.schema;
            this.primaryKey = meta.primaryKey;
            this.partitionSpec = meta.partitionSpec;
            this.hiddenColumnConfig = meta.hiddenColumnConfig;
            this.properties = meta.properties;
            this.location = meta.location;
            this.createdAt = meta.createdAt;
            this.updatedAt = meta.updatedAt;
            this.existingTable = meta.tableId() > 0;
        }

        public Builder withTableId(long tableId) {
            this.tableId = tableId;
            return this;
        }

        public Builder withCatalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public Builder withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withTableType(TableType tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder withSchema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder withPrimaryKey(PrimaryKey primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder withPartitionSpec(PartitionSpec partitionSpec) {
            this.partitionSpec = partitionSpec;
            return this;
        }

        public Builder withHiddenColumnConfig(HiddenColumnConfig hiddenColumnConfig) {
            this.hiddenColumnConfig = hiddenColumnConfig;
            return this;
        }

        public Builder withProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public Builder withLocation(String location) {
            this.location = location;
            return this;
        }

        public Builder withCreatedAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        public Builder withUpdatedAt(Instant updatedAt) {
            this.updatedAt = updatedAt;
            return this;
        }

        public Builder existingTable(boolean existingTable) {
            this.existingTable = existingTable;
            return this;
        }

        public TableMeta build() {
            Objects.requireNonNull(tableName, "tableName is required");
            Objects.requireNonNull(tableType, "tableType is required");
            Objects.requireNonNull(schema, "schema is required");
            if (updatedAt == null) {
                updatedAt = Instant.now();
            }
            if (createdAt == null) {
                createdAt = updatedAt;
            }

            PrimaryKey expectedPk = PrimaryKey.fromSchema(schema);
            if (primaryKey == null || primaryKey.isEmpty()) {
                if (!expectedPk.isEmpty()) {
                    if (existingTable) {
                        throw new IllegalStateException(
                                "TableMeta.primaryKey is empty but Schema has primaryKeyConstraint="
                                        + expectedPk.fields()
                                        + "; auto-correction disabled for existing tables");
                    }
                    primaryKey = expectedPk;
                    log.warn(
                            "TableMeta.primaryKey was empty but Schema has primaryKey={}; auto-corrected",
                            expectedPk.fields());
                }
            } else if (!expectedPk.isEmpty() && !primaryKey.fields().equals(expectedPk.fields())) {
                if (existingTable) {
                    throw new IllegalStateException(
                            "TableMeta.primaryKey "
                                    + primaryKey.fields()
                                    + " inconsistent with Schema.primaryKeyConstraint "
                                    + expectedPk.fields()
                                    + "; auto-correction disabled for existing tables");
                }
                log.warn(
                        "TableMeta.primaryKey {} inconsistent with Schema.primaryKeyConstraint {}; "
                                + "auto-correcting from Schema",
                        primaryKey.fields(),
                        expectedPk.fields());
                primaryKey = expectedPk;
            } else if (expectedPk.isEmpty() && !primaryKey.isEmpty()) {
                if (existingTable) {
                    throw new IllegalStateException(
                            "TableMeta.primaryKey "
                                    + primaryKey.fields()
                                    + " present but Schema has no primaryKeyConstraint; "
                                    + "auto-correction disabled for existing tables");
                }
                log.info(
                        "TableMeta.primaryKey {} retained (explicitly set, schema has no primaryKeyConstraint)",
                        primaryKey.fields());
            }

            return new TableMeta(this);
        }
    }
}
