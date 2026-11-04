package io.github.rawwise.common.catalog;

import java.util.Map;

public interface CatalogFactory {
    String kind();

    Catalog create(Map<String, String> properties);
}
