package io.github.rawwise.engine.storage.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalStorage implements MappableStorage {

    private static final StorageCapabilities CAPABILITIES =
            new StorageCapabilities(true, true, true, true, true, true);

    private final String name;

    public LocalStorage(String name) {
        this.name = name;
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
        var javaPath = toJavaPath(path);
        var raf = new RandomAccessFile(javaPath.toFile(), "r");
        var channel = raf.getChannel();
        return new LocalInputHandle(path, channel, raf);
    }

    @Override
    public OutputHandle openWrite(StoragePath path, WriteMode mode) throws IOException {
        var javaPath = toJavaPath(path);
        FileChannel channel;
        switch (mode) {
            case CREATE -> {
                channel =
                        FileChannel.open(
                                javaPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            }
            case OVERWRITE -> {
                channel =
                        FileChannel.open(
                                javaPath,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.TRUNCATE_EXISTING);
            }
            case APPEND -> {
                channel =
                        FileChannel.open(
                                javaPath,
                                StandardOpenOption.CREATE,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.APPEND);
            }
            default -> throw new IllegalArgumentException("Unsupported write mode: " + mode);
        }
        return new LocalOutputHandle(path, channel);
    }

    @Override
    public StorageStatus status(StoragePath path) throws IOException {
        var javaPath = toJavaPath(path);
        if (!Files.exists(javaPath)) {
            return new StorageStatus(path, false, false, 0, 0);
        }
        var attrs = Files.readAttributes(javaPath, BasicFileAttributes.class);
        return new StorageStatus(
                path, true, attrs.isDirectory(), attrs.size(), attrs.lastModifiedTime().toMillis());
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        return Files.exists(toJavaPath(path));
    }

    @Override
    public void createDirectories(StoragePath path) throws IOException {
        Files.createDirectories(toJavaPath(path));
    }

    @Override
    public List<StoragePath> list(StoragePath prefix) throws IOException {
        var javaPath = toJavaPath(prefix);
        if (!Files.exists(javaPath)) {
            return List.of();
        }
        var results = new ArrayList<StoragePath>();
        try (var stream = Files.list(javaPath)) {
            stream.forEach(p -> results.add(new StoragePath("file", p.toString())));
        }
        return results;
    }

    @Override
    public void delete(StoragePath path) throws IOException {
        Files.delete(toJavaPath(path));
    }

    @Override
    public void deleteRecursively(StoragePath path) throws IOException {
        var javaPath = toJavaPath(path);
        if (!Files.exists(javaPath)) {
            return;
        }
        Files.walkFileTree(
                javaPath,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                            throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    @Override
    public void rename(StoragePath source, StoragePath target) throws IOException {
        try {
            Files.move(toJavaPath(source), toJavaPath(target), StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(toJavaPath(source), toJavaPath(target), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public MappedRegion map(StoragePath path, long offset, long length, AccessMode mode)
            throws IOException {
        if (offset < 0 || length <= 0) {
            throw new IllegalArgumentException(
                    "Invalid map range: offset=" + offset + ", length=" + length);
        }
        var javaPath = toJavaPath(path);
        var rafMode = mode == AccessMode.READ_WRITE ? "rw" : "r";
        var raf = new RandomAccessFile(javaPath.toFile(), rafMode);
        var channel = raf.getChannel();
        var mapMode =
                mode == AccessMode.READ_WRITE
                        ? FileChannel.MapMode.READ_WRITE
                        : FileChannel.MapMode.READ_ONLY;
        var mappedBuffer = channel.map(mapMode, offset, length);
        return new LocalMappedRegion(path, offset, length, mode, mappedBuffer, raf, channel);
    }

    @Override
    public void close() {}

    private Path toJavaPath(StoragePath path) {
        if (!"file".equals(path.scheme())) {
            throw new IllegalArgumentException(
                    "LocalStorage only supports 'file' scheme, got: " + path.scheme());
        }
        return Path.of(path.location());
    }

    private static class LocalInputHandle implements InputHandle {
        private final StoragePath path;
        private final FileChannel channel;
        private final RandomAccessFile raf;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final InputStream cachedStream;

        LocalInputHandle(StoragePath path, FileChannel channel, RandomAccessFile raf) {
            this.path = path;
            this.channel = channel;
            this.raf = raf;
            this.cachedStream = Channels.newInputStream(channel);
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public long position() {
            try {
                return channel.position();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long length() {
            try {
                return channel.size();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void seek(long position) throws IOException {
            channel.position(position);
        }

        @Override
        public InputStream stream() throws IOException {
            return cachedStream;
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            IOException first = null;
            try {
                channel.close();
            } catch (IOException e) {
                first = e;
            }
            try {
                raf.close();
            } catch (IOException e) {
                if (first != null) e.addSuppressed(first);
                throw e;
            }
            if (first != null) throw first;
        }
    }

    private static class LocalOutputHandle implements OutputHandle {
        private final StoragePath path;
        private final FileChannel channel;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final OutputStream cachedStream;

        LocalOutputHandle(StoragePath path, FileChannel channel) {
            this.path = path;
            this.channel = channel;
            this.cachedStream = Channels.newOutputStream(channel);
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public long position() {
            try {
                return channel.position();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public OutputStream stream() throws IOException {
            return cachedStream;
        }

        @Override
        public void flush() throws IOException {}

        @Override
        public void sync() throws IOException {
            channel.force(true);
        }

        @Override
        public void hsync() throws IOException {
            channel.force(true);
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            channel.close();
        }
    }

    private static class LocalMappedRegion implements MappedRegion {
        private final StoragePath path;
        private final long offset;
        private final long length;
        private final AccessMode mode;
        private final java.nio.MappedByteBuffer buffer;
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        LocalMappedRegion(
                StoragePath path,
                long offset,
                long length,
                AccessMode mode,
                java.nio.MappedByteBuffer buffer,
                RandomAccessFile raf,
                FileChannel channel) {
            this.path = path;
            this.offset = offset;
            this.length = length;
            this.mode = mode;
            this.buffer = buffer;
            this.raf = raf;
            this.channel = channel;
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public AccessMode mode() {
            return mode;
        }

        @Override
        public ByteBuffer asByteBuffer() {
            if (mode == AccessMode.READ_ONLY) {
                return buffer.asReadOnlyBuffer();
            }
            return buffer.duplicate();
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            unmapBuffer(buffer);
            IOException first = null;
            try {
                channel.close();
            } catch (IOException e) {
                first = e;
            }
            try {
                raf.close();
            } catch (IOException e) {
                if (first != null) e.addSuppressed(first);
                throw e;
            }
            if (first != null) throw first;
        }

        private static void unmapBuffer(java.nio.MappedByteBuffer buffer) {
            if (buffer == null || !buffer.isDirect()) return;
            try {
                var unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                var unsafe = (sun.misc.Unsafe) unsafeField.get(null);
                unsafe.invokeCleaner(buffer);
            } catch (Exception e) {
                log.warn("Failed to unmap buffer: {}", e.getMessage());
            }
        }
    }
}
