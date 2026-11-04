package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.backend.wal.HdfsWALWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

public final class HdfsWALWriterFactory implements WriterFactory<HdfsWALWriter> {

    @Override
    public String writerType() {
        return "WAL_HDFS";
    }

    @Override
    public Class<HdfsWALWriter> writerClass() {
        return HdfsWALWriter.class;
    }

    @Override
    public HdfsWALWriter create(WriterConfig config, WriterContext context) throws IOException {
        String storeId = config.extraProp("storeId");
        if (storeId == null) storeId = "default";
        String executorId = config.extraProp("executorId");
        if (executorId == null) executorId = "executor-0";
        String hdfsBasePath = config.filePath();
        if (hdfsBasePath == null || hdfsBasePath.isEmpty()) {
            throw new IllegalArgumentException(
                    "filePath (hdfsBasePath) is required for HdfsWALWriter");
        }
        long maxSizeBytes = config.maxFileSize().orElse(100L * 1024 * 1024);
        long syncIntervalMs = config.syncIntervalMs().orElse(1000L);
        Configuration hadoopConfig = context.get("hadoopConfiguration", Configuration.class);
        if (hadoopConfig == null) {
            hadoopConfig = new Configuration();
        }
        return new HdfsWALWriter(
                storeId, executorId, hdfsBasePath, maxSizeBytes, syncIntervalMs, hadoopConfig);
    }
}
