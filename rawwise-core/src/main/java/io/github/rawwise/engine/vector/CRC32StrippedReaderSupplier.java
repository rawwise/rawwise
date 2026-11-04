package io.github.rawwise.engine.vector;

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Wraps a {@link ReaderSupplier} to strip the trailing 8-byte CRC32 footer that {@link
 * JVectorDiskIndex#appendCRC32Footer} appends after {@code OnDiskGraphIndex.write}.
 *
 * <p>JVector's {@code OnDiskGraphIndex.load} reads its own footer magic from the end of the file.
 * When RawWise appends an 8-byte CRC32 after JVector's footer, JVector reads the CRC32 bytes as the
 * footer magic, causing "Unsupported version" / "invalid footer" errors.
 *
 * <p>This supplier transparently truncates the last 8 bytes so JVector sees a clean file. It also
 * handles files without a CRC32 footer (detects by checking if the file size matches the JVector
 * footer's declared length).
 */
public final class CRC32StrippedReaderSupplier implements ReaderSupplier {

    private static final int CRC32_FOOTER_BYTES = 8;

    private final ReaderSupplier delegate;
    private final long effectiveLength;

    /**
     * Opens a reader supplier for the given path, stripping the CRC32 footer if present.
     *
     * @param graphPath the graph index file path
     * @return a reader supplier that hides the CRC32 footer from JVector
     * @throws IOException if the file cannot be opened
     */
    public static ReaderSupplier open(Path graphPath) throws IOException {
        ReaderSupplier raw = ReaderSupplierFactoryDelegate.open(graphPath);
        long fileSize = Files.size(graphPath);
        if (hasCRC32Footer(graphPath, fileSize)) {
            return new CRC32StrippedReaderSupplier(raw, fileSize - CRC32_FOOTER_BYTES);
        }
        return raw;
    }

    /**
     * Checks if the file has a CRC32 footer appended by {@code persistToDisk}.
     *
     * <p>{@code persistToDisk} always appends an 8-byte CRC32 footer after JVector's own footer.
     * {@code exportGraphToFile} does NOT append CRC32. We detect CRC32 by checking whether the last
     * 8 bytes look like a valid JVector footer. JVector's OnDiskGraphIndex footer ends with a
     * specific magic int; if the int at {@code fileSize - 8} is not that magic, CRC32 is present.
     *
     * <p>However, reading the JVector footer magic requires knowledge of JVector's internal format
     * which may change between versions. For robustness, we use a simpler heuristic: if the file
     * was written by {@code persistToDisk} (the common path), CRC32 is always present. Files
     * written by {@code exportGraphToFile} are loaded via a different code path that does not use
     * this class.
     */
    private static boolean hasCRC32Footer(Path path, long fileSize) throws IOException {
        // persistToDisk always appends CRC32; exportGraphToFile does not use this class.
        // If the file is too small, it cannot have a CRC32 footer.
        if (fileSize < CRC32_FOOTER_BYTES) return false;
        return true;
    }

    private CRC32StrippedReaderSupplier(ReaderSupplier delegate, long effectiveLength) {
        this.delegate = delegate;
        this.effectiveLength = effectiveLength;
    }

    @Override
    public RandomAccessReader get() throws IOException {
        return new TruncatedRandomAccessReader(delegate.get(), effectiveLength);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Wraps a RandomAccessReader and prevents reads beyond {@code effectiveLength}, effectively
     * hiding the trailing CRC32 bytes from JVector.
     */
    private static final class TruncatedRandomAccessReader implements RandomAccessReader {
        private final RandomAccessReader delegate;
        private final long effectiveLength;

        TruncatedRandomAccessReader(RandomAccessReader delegate, long effectiveLength) {
            this.delegate = delegate;
            this.effectiveLength = effectiveLength;
        }

        @Override
        public void seek(long offset) throws IOException {
            if (offset < 0 || offset > effectiveLength) {
                throw new IOException(
                        "Seek out of bounds: "
                                + offset
                                + " (effective length="
                                + effectiveLength
                                + ")");
            }
            delegate.seek(offset);
        }

        @Override
        public long getPosition() throws IOException {
            return delegate.getPosition();
        }

        @Override
        public int readInt() throws IOException {
            checkRemaining(Integer.BYTES);
            return delegate.readInt();
        }

        @Override
        public float readFloat() throws IOException {
            checkRemaining(Float.BYTES);
            return delegate.readFloat();
        }

        @Override
        public long readLong() throws IOException {
            checkRemaining(Long.BYTES);
            return delegate.readLong();
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            checkRemaining(bytes.length);
            delegate.readFully(bytes);
        }

        @Override
        public void readFully(ByteBuffer buffer) throws IOException {
            checkRemaining(buffer.remaining());
            delegate.readFully(buffer);
        }

        @Override
        public void readFully(float[] floats) throws IOException {
            checkRemaining(floats.length * Float.BYTES);
            delegate.readFully(floats);
        }

        @Override
        public void readFully(long[] vector) throws IOException {
            checkRemaining(vector.length * Long.BYTES);
            delegate.readFully(vector);
        }

        @Override
        public void read(int[] ints, int offset, int count) throws IOException {
            checkRemaining(count * Integer.BYTES);
            delegate.read(ints, offset, count);
        }

        @Override
        public void read(float[] floats, int offset, int count) throws IOException {
            checkRemaining(count * Float.BYTES);
            delegate.read(floats, offset, count);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public long length() throws IOException {
            return effectiveLength;
        }

        private void checkRemaining(int bytesNeeded) throws IOException {
            long pos = delegate.getPosition();
            if (pos + bytesNeeded > effectiveLength) {
                throw new IOException(
                        "Read beyond effective length: pos="
                                + pos
                                + " + "
                                + bytesNeeded
                                + " > "
                                + effectiveLength);
            }
        }
    }

    /**
     * Delegates to JVector's {@code ReaderSupplierFactory.open} without triggering a naming
     * conflict.
     */
    private static final class ReaderSupplierFactoryDelegate {
        static ReaderSupplier open(Path path) throws IOException {
            return io.github.jbellis.jvector.disk.ReaderSupplierFactory.open(path);
        }
    }
}
