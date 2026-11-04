package io.github.rawwise.metastore;

import io.github.rawwise.governance.config.RawWiseConfig;

/** {@link Metastore Factory} implementation,create {@link In Memory Metastore} instance. */
public class InMemoryMetastoreFactory implements MetastoreFactory {

    @Override
    public String kind() {
        return "inmemory";
    }

    @Override
    public Metastore create(RawWiseConfig config) {
        return new InMemoryMetastore();
    }
}
