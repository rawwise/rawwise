package io.github.rawwise.common.catalog;

import java.util.Map;
import java.util.Objects;

public class HadoopCatalogConfig implements FileCatalogConfig {

    private final String warehousePath;

    private final String catalogName;

    private final Map<String, String> hadoopConf;

    private HadoopCatalogConfig(Builder builder) {
        this.warehousePath =
                Objects.requireNonNull(
                        builder.warehousePath, "warehousePath is required for HadoopCatalogConfig");
        this.catalogName = builder.catalogName != null ? builder.catalogName : "default";
        this.hadoopConf = builder.hadoopConf != null ? Map.copyOf(builder.hadoopConf) : Map.of();
    }

    public String warehousePath() {
        return warehousePath;
    }

    public String catalogName() {
        return catalogName;
    }

    public Map<String, String> hadoopConf() {
        return hadoopConf;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String warehousePath;
        private String catalogName;
        private Map<String, String> hadoopConf;

        public Builder warehousePath(String warehousePath) {
            this.warehousePath = warehousePath;
            return this;
        }

        public Builder catalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public Builder hadoopConf(Map<String, String> hadoopConf) {
            this.hadoopConf = hadoopConf;
            return this;
        }

        public HadoopCatalogConfig build() {
            return new HadoopCatalogConfig(this);
        }
    }
}
