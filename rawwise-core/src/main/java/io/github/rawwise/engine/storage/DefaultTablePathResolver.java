package io.github.rawwise.engine.storage;

import io.github.rawwise.governance.config.RawWiseConfig;
import io.github.rawwise.governance.config.RawWiseOptions;

public class DefaultTablePathResolver implements TablePathResolver {

    private final String hdfsBasePath;

    public DefaultTablePathResolver(RawWiseConfig config) {
        this.hdfsBasePath = config.get(RawWiseOptions.INGESTION_SINK_PARQUET_HDFS_BASE_PATH);
    }

    public DefaultTablePathResolver(String hdfsBasePath) {
        this.hdfsBasePath = hdfsBasePath;
    }

    @Override
    public String resolveDataPath(String tableId) {
        String tablePath = resolveTablePath(tableId);
        return hdfsBasePath + "/" + tablePath + "/data";
    }

    @Override
    public String resolveTmpPath(String tableId) {
        String tablePath = resolveTablePath(tableId);
        return hdfsBasePath + "/" + tablePath + "/tmp";
    }

    @Override
    public String resolveManifestPath(String tableId) {
        String tablePath = resolveTablePath(tableId);
        return hdfsBasePath + "/" + tablePath + "/metadata";
    }
}
