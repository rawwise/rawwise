package io.github.rawwise.metastore;

import io.github.rawwise.governance.config.RawWiseConfig;

public interface MetastoreFactory {
    String kind();

    Metastore create(RawWiseConfig config);
}
