package io.github.rawwise.engine.storage.spi;

import io.github.rawwise.governance.config.RawWiseConfig;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public class HdfsStorageProvider implements StorageProvider {

    private final Configuration conf;

    public HdfsStorageProvider() {
        this(new Configuration());
    }

    public HdfsStorageProvider(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public String scheme() {
        return "hdfs";
    }

    @Override
    public Storage create(Storage.StoragePath path, RawWiseConfig config) throws IOException {
        return new HdfsStorage(path.location(), conf);
    }
}
