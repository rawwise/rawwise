package io.github.rawwise.engine.storage.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

@Slf4j
public class HdfsStorage implements Storage {

    private static final StorageCapabilities CAPABILITIES =
            new StorageCapabilities(true, false, true, true, true, false);

    private final String name;
    private final FileSystem fs;

    public HdfsStorage(String name, Configuration conf) throws IOException {
        this.name = name;
        this.fs = FileSystem.newInstance(conf);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StorageCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public InputHandle openRead(StoragePath path) throws IOException {
        var hdfsPath = toHdfsPath(path);
        var in = fs.open(hdfsPath);
        var fileLength = fs.getFileStatus(hdfsPath).getLen();
        return new HdfsInputHandle(path, in, fileLength);
    }

    @Override
    public OutputHandle openWrite(StoragePath path, WriteMode mode) throws IOException {
        var hdfsPath = toHdfsPath(path);
        switch (mode) {
            case CREATE -> {
                var out = fs.create(hdfsPath, false);
                return new HdfsOutputHandle(path, out);
            }
            case OVERWRITE -> {
                var out = fs.create(hdfsPath, true);
                return new HdfsOutputHandle(path, out);
            }
            case APPEND -> {
                var out = fs.append(hdfsPath);
                return new HdfsOutputHandle(path, out);
            }
            default -> throw new IllegalArgumentException("Unsupported write mode: " + mode);
        }
    }

    @Override
    public StorageStatus status(StoragePath path) throws IOException {
        var hdfsPath = toHdfsPath(path);
        if (!fs.exists(hdfsPath)) {
            return new StorageStatus(path, false, false, 0, 0);
        }
        var fileStatus = fs.getFileStatus(hdfsPath);
        return new StorageStatus(
                path,
                true,
                fileStatus.isDirectory(),
                fileStatus.getLen(),
                fileStatus.getModificationTime());
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        return fs.exists(toHdfsPath(path));
    }

    @Override
    public void createDirectories(StoragePath path) throws IOException {
        var hdfsPath = toHdfsPath(path);
        if (!fs.mkdirs(hdfsPath)) {
            throw new IOException("Failed to create directories: " + hdfsPath);
        }
    }

    @Override
    public List<StoragePath> list(StoragePath prefix) throws IOException {
        var hdfsPath = toHdfsPath(prefix);
        if (!fs.exists(hdfsPath)) {
            return List.of();
        }
        var statuses = fs.listStatus(hdfsPath);
        var results = new ArrayList<StoragePath>(statuses.length);
        for (FileStatus status : statuses) {
            results.add(new StoragePath("hdfs", status.getPath().toUri().getPath()));
        }
        return results;
    }

    @Override
    public void delete(StoragePath path) throws IOException {
        if (!fs.delete(toHdfsPath(path), false)) {
            throw new IOException("Failed to delete: " + path);
        }
    }

    @Override
    public void deleteRecursively(StoragePath path) throws IOException {
        if (!fs.delete(toHdfsPath(path), true)) {
            throw new IOException("Failed to delete recursively: " + path);
        }
    }

    @Override
    public void rename(StoragePath source, StoragePath target) throws IOException {
        if (!fs.rename(toHdfsPath(source), toHdfsPath(target))) {
            throw new IOException("Failed to rename " + source + " to " + target);
        }
    }

    @Override
    public void close() {
        try {
            if (fs != null) {
                fs.close();
            }
        } catch (IOException e) {
            log.warn("Failed to close HDFS FileSystem: {}", e.getMessage());
        }
    }

    private Path toHdfsPath(StoragePath path) {
        if (!"hdfs".equals(path.scheme())) {
            throw new IllegalArgumentException(
                    "HdfsStorage only supports 'hdfs' scheme, got: " + path.scheme());
        }
        return new Path(path.location());
    }

    private static class HdfsInputHandle implements InputHandle {
        private final StoragePath path;
        private final org.apache.hadoop.fs.FSDataInputStream in;
        private final long fileLength;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        HdfsInputHandle(
                StoragePath path, org.apache.hadoop.fs.FSDataInputStream in, long fileLength) {
            this.path = path;
            this.in = in;
            this.fileLength = fileLength;
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public long position() {
            try {
                return in.getPos();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long length() {
            return fileLength;
        }

        @Override
        public void seek(long position) throws IOException {
            in.seek(position);
        }

        @Override
        public InputStream stream() throws IOException {
            return in;
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            in.close();
        }
    }

    private static class HdfsOutputHandle implements OutputHandle {
        private final StoragePath path;
        private final org.apache.hadoop.fs.FSDataOutputStream out;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        HdfsOutputHandle(StoragePath path, org.apache.hadoop.fs.FSDataOutputStream out) {
            this.path = path;
            this.out = out;
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public long position() {
            return out.getPos();
        }

        @Override
        public OutputStream stream() throws IOException {
            return out;
        }

        @Override
        public void flush() throws IOException {
            out.hflush();
        }

        @Override
        public void sync() throws IOException {
            out.hsync();
        }

        @Override
        public void hsync() throws IOException {
            out.hsync();
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            out.close();
        }
    }
}
