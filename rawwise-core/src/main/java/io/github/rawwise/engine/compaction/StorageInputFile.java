package io.github.rawwise.engine.compaction;

import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

class StorageInputFile implements InputFile {

    private final Storage storage;
    private final Storage.StoragePath path;
    private final long length;

    StorageInputFile(Storage storage, Storage.StoragePath path) throws IOException {
        this.storage = storage;
        this.path = path;
        this.length = storage.status(path).length();
    }

    @Override
    public long getLength() {
        return length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        Storage.InputHandle handle = storage.openRead(path);
        return new StorageSeekableStream(handle, length);
    }

    private static class StorageSeekableStream extends SeekableInputStream {

        private final Storage.InputHandle handle;
        private final long length;
        private boolean closed = false;

        StorageSeekableStream(Storage.InputHandle handle, long length) {
            this.handle = handle;
            this.length = length;
        }

        @Override
        public long getPos() throws IOException {
            return handle.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
            handle.seek(newPos);
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int n = read(b, 0, 1);
            return n == -1 ? -1 : b[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            byte[] buf = handle.stream().readNBytes(len);
            if (buf.length == 0) return -1;
            System.arraycopy(buf, 0, b, off, buf.length);
            return buf.length;
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException {
            if (buf.hasArray()) {
                int n = read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
                if (n < 0) throw new IOException("EOF");
                buf.position(buf.position() + n);
            } else {
                byte[] tmp = new byte[buf.remaining()];
                int n = read(tmp, 0, tmp.length);
                if (n < 0) throw new IOException("EOF");
                buf.put(tmp, 0, n);
            }
        }

        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException {
            if (buf.hasArray()) {
                int n = read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
                if (n < 0) return -1;
                buf.position(buf.position() + n);
                return n;
            } else {
                byte[] tmp = new byte[Math.min(buf.remaining(), 8192)];
                int n = read(tmp, 0, tmp.length);
                if (n < 0) return -1;
                buf.put(tmp, 0, n);
                return n;
            }
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            int totalRead = 0;
            while (totalRead < len) {
                int n = read(b, off + totalRead, len - totalRead);
                if (n == -1) throw new IOException("Unexpected end of stream");
                totalRead += n;
            }
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                try {
                    handle.close();
                } catch (Exception e) {
                    throw new IOException("Failed to close storage input handle", e);
                }
            }
        }

        @Override
        public int available() throws IOException {
            long remaining = length - handle.position();
            return remaining > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) remaining;
        }
    }
}
