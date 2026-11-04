package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.writer.RollingPolicy;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class HdfsWALWriter extends AbstractWALWriter {

    private final String executorId;
    private final String hdfsBasePath;
    private final Configuration hadoopConfig;
    private final AtomicLong fileSequence = new AtomicLong(0);

    private FSDataOutputStream currentStream;
    private String currentFilePathStr;

    private FSDataOutputStream nextStream;
    private String nextFilePathStr;
    private long nextHeaderSize;

    public HdfsWALWriter(
            String storeId,
            String executorId,
            String hdfsBasePath,
            RollingPolicy rollingPolicy,
            long syncIntervalMs,
            Configuration hadoopConfig) {
        super(storeId, rollingPolicy, syncIntervalMs);
        this.executorId = executorId;
        this.hdfsBasePath = hdfsBasePath;
        this.hadoopConfig = hadoopConfig;
    }

    public HdfsWALWriter(
            String storeId,
            String executorId,
            String hdfsBasePath,
            long maxSizeBytes,
            long syncIntervalMs,
            Configuration hadoopConfig) {
        this(
                storeId,
                executorId,
                hdfsBasePath,
                new io.github.rawwise.engine.storage.writer.SizeBasedRollingPolicy(maxSizeBytes),
                syncIntervalMs,
                hadoopConfig,
                false);
    }

    public HdfsWALWriter(
            String storeId,
            String executorId,
            String hdfsBasePath,
            long maxSizeBytes,
            long syncIntervalMs,
            Configuration hadoopConfig,
            boolean skipAutoSync) {
        this(
                storeId,
                executorId,
                hdfsBasePath,
                new io.github.rawwise.engine.storage.writer.SizeBasedRollingPolicy(maxSizeBytes),
                syncIntervalMs,
                hadoopConfig,
                skipAutoSync);
    }

    public HdfsWALWriter(
            String storeId,
            String executorId,
            String hdfsBasePath,
            RollingPolicy rollingPolicy,
            long syncIntervalMs,
            Configuration hadoopConfig,
            boolean skipAutoSync) {
        super(storeId, rollingPolicy, syncIntervalMs, skipAutoSync);
        this.executorId = executorId;
        this.hdfsBasePath = hdfsBasePath;
        this.hadoopConfig = hadoopConfig;
    }

    @Override
    protected boolean isStreamOpen() {
        return currentStream != null;
    }

    @Override
    protected void doWriteBytes(byte[] data) throws IOException {
        currentStream.write(data);
    }

    @Override
    protected void doPrepareNewFile() throws IOException {
        String filePath = generateFilePath();
        nextFilePathStr = filePath;
        Path hdfsPath = new Path(filePath);

        FileSystem fs = hdfsPath.getFileSystem(hadoopConfig);
        fs.mkdirs(hdfsPath.getParent());

        FSDataOutputStream fos = fs.create(hdfsPath, false);
        try {
            byte[] headerBytes = serializeHeader(executorId);
            fos.write(headerBytes);
            nextStream = fos;
            nextHeaderSize = headerBytes.length;
            log.info("Prepared new WAL file on HDFS: {}", filePath);
        } catch (IOException e) {
            try {
                fos.close();
            } catch (IOException ignored) {
            }
            try {
                fs.delete(hdfsPath, false);
            } catch (IOException ignored) {
            }
            throw e;
        }
    }

    @Override
    protected void doCloseCurrentFile() throws IOException {
        if (currentStream != null) {
            try {
                currentStream.close();
            } finally {
                currentStream = null;
            }
        }
    }

    @Override
    protected void doSwitchToNewFile() {
        currentStream = nextStream;
        currentFilePathStr = nextFilePathStr;
        setCurrentFileSize(nextHeaderSize);
        nextStream = null;
        nextFilePathStr = null;
        nextHeaderSize = 0;
    }

    @Override
    protected void doCleanupPreparedFile() throws IOException {
        if (nextStream != null) {
            try {
                nextStream.close();
            } catch (IOException ignored) {
            }
            nextStream = null;
        }
        if (nextFilePathStr != null) {
            try {
                Path hdfsPath = new Path(nextFilePathStr);
                FileSystem fs = hdfsPath.getFileSystem(hadoopConfig);
                fs.delete(hdfsPath, false);
            } catch (IOException ignored) {
            }
            nextFilePathStr = null;
        }
    }

    @Override
    protected void doSync() throws IOException {
        if (currentStream == null) {
            return;
        }
        try {
            currentStream.hsync();
            return;
        } catch (IOException hsyncError) {
            log.warn(
                    "hsync failed, attempting hflush as fallback - durability degraded",
                    hsyncError);
        }
        currentStream.hflush();
    }

    @Override
    protected void doFlushStream() throws IOException {
        if (currentStream != null) {
            currentStream.hflush();
        }
    }

    @Override
    public Optional<Storage.StoragePath> currentFilePath() {
        if (currentFilePathStr == null) return Optional.empty();
        return Optional.of(new Storage.StoragePath("hdfs", currentFilePathStr));
    }

    private String generateFilePath() {
        long timestamp = System.currentTimeMillis();
        long seq = fileSequence.getAndIncrement();
        return String.format(
                "%s/%s/wal_%s_%d_%d.nwl", hdfsBasePath, executorId, storeId, timestamp, seq);
    }
}
