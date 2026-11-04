package io.github.rawwise.common.catalog;

import io.github.rawwise.engine.storage.spi.LocalStorage;
import java.util.Map;

public class HadoopFileCatalogFactory implements CatalogFactory {

    private static final String KEY_CATALOG_NAME = "catalog-name";
    private static final String KEY_SCHEME = "scheme";
    private static final String KEY_ROOT_PATH = "root-path";

    @Override
    public String kind() {
        return "hadoop-file";
    }

    @Override
    public Catalog create(Map<String, String> properties) {
        String catalogName = properties.get(KEY_CATALOG_NAME);
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException("catalog-name is required for hadoop-file catalog");
        }

        String scheme = properties.get(KEY_SCHEME);
        if (scheme == null || scheme.isBlank()) {
            throw new IllegalArgumentException("scheme is required for hadoop-file catalog");
        }

        String rootPath = properties.get(KEY_ROOT_PATH);
        if (rootPath == null || rootPath.isBlank()) {
            throw new IllegalArgumentException("root-path is required for hadoop-file catalog");
        }

        var storage = new LocalStorage("hadoop-file-" + catalogName);
        return new HadoopFileCatalog(catalogName, storage, scheme, rootPath);
    }
}
