package io.github.rawwise.common.catalog;

import io.github.rawwise.metastore.Metastore;
import java.util.Objects;

public class MetastoreCatalogConfig implements CatalogConfig {

    private final Metastore store;
    private final String basePath;
    private final String catalogName;

    private MetastoreCatalogConfig(Builder builder) {
        this.store =
                Objects.requireNonNull(
                        builder.store, "Metastore is required for MetastoreCatalogConfig");
        this.basePath =
                Objects.requireNonNull(
                        builder.basePath, "basePath is required for MetastoreCatalogConfig");
        this.catalogName = builder.catalogName != null ? builder.catalogName : "default";
    }

    public Metastore store() {
        return store;
    }

    public String basePath() {
        return basePath;
    }

    public String catalogName() {
        return catalogName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Metastore store;
        private String basePath;
        private String catalogName;

        public Builder store(Metastore store) {
            this.store = store;
            return this;
        }

        public Builder basePath(String basePath) {
            this.basePath = basePath;
            return this;
        }

        public Builder catalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public MetastoreCatalogConfig build() {
            if (this.store == null) {
                throw new IllegalStateException(
                        "Metastore is required and cannot be null. "
                                + "Please call .store(metastore) before building.");
            }
            if (this.basePath == null || this.basePath.isBlank()) {
                throw new IllegalStateException(
                        "basePath is required and cannot be null or blank. "
                                + "Please call .basePath('/path/to/storage') before building.");
            }
            return new MetastoreCatalogConfig(this);
        }
    }
}
