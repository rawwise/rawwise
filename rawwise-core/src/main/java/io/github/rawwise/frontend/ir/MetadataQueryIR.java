package io.github.rawwise.frontend.ir;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

public sealed interface MetadataQueryIR extends QueryIR, Serializable
        permits MetadataQueryIR.CountAll,
                MetadataQueryIR.CountColumn,
                MetadataQueryIR.MinValue,
                MetadataQueryIR.MaxValue,
                MetadataQueryIR.TableInfo {

    TableMetaIR tableMeta();

    long snapshotId();

    record CountAll(TableMetaIR tableMeta, long snapshotId) implements MetadataQueryIR {
        public Set<String> tableIdentifiers() {
            return Set.of(tableMeta.tableIdentifier());
        }

        public IRType type() {
            return IRType.METADATA_COUNT_ALL;
        }
    }

    record CountColumn(TableMetaIR tableMeta, long snapshotId, String column, boolean distinct)
            implements MetadataQueryIR {
        public CountColumn {
            Objects.requireNonNull(column, "column");
        }

        public Set<String> tableIdentifiers() {
            return Set.of(tableMeta.tableIdentifier());
        }

        public IRType type() {
            return IRType.METADATA_COUNT_COLUMN;
        }
    }

    record MinValue(TableMetaIR tableMeta, long snapshotId, String column)
            implements MetadataQueryIR {
        public MinValue {
            Objects.requireNonNull(column, "column");
        }

        public Set<String> tableIdentifiers() {
            return Set.of(tableMeta.tableIdentifier());
        }

        public IRType type() {
            return IRType.METADATA_MIN;
        }
    }

    record MaxValue(TableMetaIR tableMeta, long snapshotId, String column)
            implements MetadataQueryIR {
        public MaxValue {
            Objects.requireNonNull(column, "column");
        }

        public Set<String> tableIdentifiers() {
            return Set.of(tableMeta.tableIdentifier());
        }

        public IRType type() {
            return IRType.METADATA_MAX;
        }
    }

    record TableInfo(TableMetaIR tableMeta, long snapshotId) implements MetadataQueryIR {
        public Set<String> tableIdentifiers() {
            return Set.of(tableMeta.tableIdentifier());
        }

        public IRType type() {
            return IRType.METADATA_TABLE_INFO;
        }
    }
}
