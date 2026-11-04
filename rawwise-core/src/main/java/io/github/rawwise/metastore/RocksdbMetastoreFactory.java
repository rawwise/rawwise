package io.github.rawwise.metastore;

import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;
import java.io.IOException;

/** {@link Metastore Factory} implementation,create {@link Rocksdb Metastore} instance. */
public class RocksdbMetastoreFactory implements MetastoreFactory {

    @Override
    public String kind() {
        return "rocksdb";
    }

    @Override
    public Metastore create(RawWiseConfig config) {
        String path = config.get(RawWiseOptions.METASTORE_BASE_PATH);
        try {
            return new RocksdbMetastore(path);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create RocksDB metastore at " + path, e);
        }
    }
}
