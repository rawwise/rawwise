package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.spi.Storage;
import io.github.rawwise.engine.storage.writer.RollingPolicy;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalWALWriter extends AbstractWALWriter {

    private final String baseDir;
    private final AtomicLong fileSequence = new AtomicLong(0);

    private OutputStream currentStream;
    private FileChannel currentChannel;
    private String currentFilePathStr;

    private OutputStream nextStream;
    private FileChannel nextChannel;
    private String nextFilePathStr;
    private long nextHeaderSize;

    public LocalWALWriter(
            String storeId,
            String baseDir,
            RollingPolicy rollingPolicy,
            long maxSizeBytes,
            long syncIntervalMs) {
        super(storeId, rollingPolicy, syncIntervalMs);
        this.baseDir = baseDir;
    }

    public LocalWALWriter(String storeId, String baseDir, long maxSizeBytes, long syncIntervalMs) {
        this(
                storeId,
                baseDir,
                new io.github.rawwise.engine.storage.writer.SizeBasedRollingPolicy(maxSizeBytes),
                maxSizeBytes,
                syncIntervalMs,
                false);
    }

    public LocalWALWriter(
            String storeId,
            String baseDir,
            long maxSizeBytes,
            long syncIntervalMs,
            boolean skipAutoSync) {
        this(
                storeId,
                baseDir,
                new io.github.rawwise.engine.storage.writer.SizeBasedRollingPolicy(maxSizeBytes),
                maxSizeBytes,
                syncIntervalMs,
                skipAutoSync);
    }

    public LocalWALWriter(
            String storeId,
            String baseDir,
            RollingPolicy rollingPolicy,
            long maxSizeBytes,
            long syncIntervalMs,
            boolean skipAutoSync) {
        super(storeId, rollingPolicy, syncIntervalMs, skipAutoSync);
        this.baseDir = baseDir;
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

        Path path = Paths.get(filePath);
        Files.createDirectories(path.getParent());

        FileOutputStream fos = new FileOutputStream(path.toFile());
        try {
            nextChannel = fos.getChannel();
            byte[] headerBytes = serializeHeader("LocalWALWriter");
            fos.write(headerBytes);
            nextStream = fos;
            nextHeaderSize = headerBytes.length;
            log.info("Prepared new WAL file locally: {}", filePath);
        } catch (IOException e) {
            try {
                fos.close();
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
                currentChannel = null;
            }
        }
    }

    @Override
    protected void doSwitchToNewFile() {
        currentStream = nextStream;
        currentChannel = nextChannel;
        currentFilePathStr = nextFilePathStr;
        setCurrentFileSize(nextHeaderSize);
        nextStream = null;
        nextChannel = null;
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
            nextChannel = null;
        }
        if (nextFilePathStr != null) {
            try {
                Files.deleteIfExists(Paths.get(nextFilePathStr));
            } catch (IOException ignored) {
            }
            nextFilePathStr = null;
        }
    }

    @Override
    protected void doSync() throws IOException {
        if (currentChannel != null) {
            currentChannel.force(true);
        }
    }

    @Override
    protected void doFlushStream() throws IOException {
        if (currentStream != null) {
            currentStream.flush();
        }
    }

    @Override
    public Optional<Storage.StoragePath> currentFilePath() {
        if (currentFilePathStr == null) return Optional.empty();
        return Optional.of(new Storage.StoragePath("file", currentFilePathStr));
    }

    private String generateFilePath() {
        long timestamp = System.currentTimeMillis();
        long seq = fileSequence.getAndIncrement();
        return String.format("%s/wal_%s_%d_%d.nwl", baseDir, storeId, timestamp, seq);
    }
}
