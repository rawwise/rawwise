package io.github.rawwise.engine.storage.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;

public interface Storage extends AutoCloseable {

    String name();

    StorageCapabilities capabilities();

    InputHandle openRead(StoragePath path) throws IOException;

    OutputHandle openWrite(StoragePath path, WriteMode mode) throws IOException;

    StorageStatus status(StoragePath path) throws IOException;

    boolean exists(StoragePath path) throws IOException;

    void createDirectories(StoragePath path) throws IOException;

    List<StoragePath> list(StoragePath prefix) throws IOException;

    void delete(StoragePath path) throws IOException;

    void deleteRecursively(StoragePath path) throws IOException;

    void rename(StoragePath source, StoragePath target) throws IOException;

    record StoragePath(String scheme, String location) implements java.io.Serializable {}

    record StorageStatus(
            StoragePath path,
            boolean exists,
            boolean directory,
            long length,
            long lastModifiedMillis)
            implements java.io.Serializable {}

    record StorageCapabilities(
            boolean supportsSequentialRead,
            boolean supportsRandomRead,
            boolean supportsSequentialWrite,
            boolean supportsAppend,
            boolean supportsRename,
            boolean supportsMmap) {}

    enum WriteMode {
        CREATE,
        OVERWRITE,
        APPEND
    }

    interface InputHandle extends AutoCloseable {

        StoragePath path();

        long position();

        long length();

        void seek(long position) throws IOException;

        InputStream stream() throws IOException;
    }

    interface OutputHandle extends AutoCloseable {

        StoragePath path();

        long position();

        OutputStream stream() throws IOException;

        void flush() throws IOException;

        void sync() throws IOException;

        void hsync() throws IOException;
    }

    interface MappedRegion extends AutoCloseable {

        StoragePath path();

        long offset();

        long length();

        AccessMode mode();

        ByteBuffer asByteBuffer();
    }

    enum AccessMode {
        READ_ONLY,
        READ_WRITE
    }
}
