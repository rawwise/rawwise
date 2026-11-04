package io.github.rawwise.common.catalog;

import io.github.rawwise.governance.config.RawWiseConfig;

public interface CatalogProvider {
    String kind();

    Catalog create(RawWiseConfig config);
}
