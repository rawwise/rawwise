package io.github.rawwise.engine.vector;

import io.github.jbellis.jvector.disk.RandomAccessWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;

@Slf4j
class LmdbRandomAccessWriter implements RandomAccessWriter {
    private final Dbi<ByteBuffer> dbi;
    private final Env<ByteBuffer> env;
    private final int blockSize;
    private static final int MAX_DIRTY_BLOCKS = 256;
    private long position;
    private ByteBuffer writeBuffer;
    private long currentBlockNumber = -1;
    private boolean currentBlockDirty = false;
    private final LinkedHashMap<Long, ByteBuffer> dirtyBlocks = new LinkedHashMap<>();
    private final ByteBuffer keyBuffer;
    private final ByteBuffer totalLengthKeyBuffer;
    private volatile boolean closed = false;
    private Txn<ByteBuffer> longLivedReadTxn;
    private final AtomicBoolean flushing = new AtomicBoolean(false);

    LmdbRandomAccessWriter(Dbi<ByteBuffer> dbi, Env<ByteBuffer> env, int blockSize) {
        this.dbi = dbi;
        this.env = env;
        this.blockSize = blockSize;
        this.keyBuffer = ByteBuffer.allocateDirect(8);
        this.totalLengthKeyBuffer = ByteBuffer.allocateDirect(8);
        this.totalLengthKeyBuffer.putLong(-1L).flip();
        this.longLivedReadTxn = env.txnRead();
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public void seek(long pos) throws IOException {
        ensureOpen();
        if (currentBlockDirty && currentBlockNumber >= 0) {
            saveDirtyBlock(currentBlockNumber, writeBuffer);
        }
        this.position = pos;
        long blockNumber = pos / blockSize;
        loadOrInitBlock(blockNumber);
        int blockOffset = (int) (pos % blockSize);
        writeBuffer.position(blockOffset);
    }

    @Override
    public void writeInt(int v) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[4];
        ByteBuffer.wrap(bytes).putInt(v);
        write(bytes);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[4];
        ByteBuffer.wrap(bytes).putFloat(v);
        write(bytes);
    }

    @Override
    public void writeLong(long v) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putLong(v);
        write(bytes);
    }

    @Override
    public void writeShort(int v) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[2];
        ByteBuffer.wrap(bytes).putShort((short) v);
        write(bytes);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(v);
        write(bytes);
    }

    @Override
    public void writeFloats(float[] floats, int offset, int count) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[count * 4];
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < count; i++) {
            bb.putFloat(floats[offset + i]);
        }
        write(bytes);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        ensureOpen();
        int utfLen = modifiedUtf8Length(s);
        byte[] bytes = new byte[2 + utfLen];
        bytes[0] = (byte) (utfLen >>> 8);
        bytes[1] = (byte) utfLen;
        writeModifiedUtf8(s, bytes, 2);
        write(bytes);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        int remaining = len;
        int srcOffset = off;
        while (remaining > 0) {
            ensureWriteBuffer(1);
            int available = Math.min(remaining, writeBuffer.remaining());
            writeBuffer.put(b, srcOffset, available);
            position += available;
            srcOffset += available;
            remaining -= available;
            currentBlockDirty = true;
            if (remaining > 0) {
                saveDirtyBlock(currentBlockNumber, writeBuffer);
                loadOrInitBlock(position / blockSize);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        ensureOpen();
        flushAllDirtyBlocks();
    }

    @Override
    public long checksum(long startOffset, long endOffset) throws IOException {
        ensureOpen();
        flushAllDirtyBlocks();
        CRC32 crc = new CRC32();
        try (Txn<ByteBuffer> tempTxn = env.txnRead();
                LmdbRandomAccessReader reader =
                        new LmdbRandomAccessReader(dbi, tempTxn, blockSize, totalLength())) {
            reader.seek(startOffset);
            long remaining = endOffset - startOffset;
            while (remaining > 0) {
                int toRead = (int) Math.min(8192, remaining);
                byte[] buf = new byte[toRead];
                reader.readFully(buf);
                crc.update(buf);
                remaining -= toRead;
            }
        }
        return crc.getValue();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                flushAllDirtyBlocks();
            } catch (Exception e) {
                log.warn("Failed to flush dirty blocks during close", e);
            }
            try {
                persistTotalLengthOnClose();
            } catch (Exception e) {
                log.warn("Failed to persist total length during close", e);
            }
            closed = true;
            if (longLivedReadTxn != null) {
                longLivedReadTxn.close();
                longLivedReadTxn = null;
            }
            writeBuffer = null;
            dirtyBlocks.clear();
        }
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[] {(byte) b});
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        write(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) throws IOException {
        write(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        writeShort(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        write(s.getBytes(StandardCharsets.ISO_8859_1));
    }

    @Override
    public void writeChars(String s) throws IOException {
        for (char c : s.toCharArray()) {
            writeChar(c);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("Writer is closed");
    }

    private void flushAllDirtyBlocks() {
        if (!flushing.compareAndSet(false, true)) return;
        try {
            if (currentBlockDirty && currentBlockNumber >= 0) {
                currentBlockDirty = false;
                saveDirtyBlock(currentBlockNumber, writeBuffer);
            }
            if (dirtyBlocks.isEmpty()) return;
            if (longLivedReadTxn != null) {
                longLivedReadTxn.close();
                longLivedReadTxn = null;
            }
            try {
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    for (Map.Entry<Long, ByteBuffer> entry : dirtyBlocks.entrySet()) {
                        keyBuffer.clear();
                        keyBuffer.putLong(entry.getKey()).flip();
                        ByteBuffer value = entry.getValue().duplicate();
                        value.flip();
                        dbi.put(txn, keyBuffer, value);
                    }
                    ByteBuffer valBuf = ByteBuffer.allocateDirect(8);
                    valBuf.putLong(position).flip();
                    dbi.put(txn, totalLengthKeyBuffer, valBuf);
                    txn.commit();
                }
                dirtyBlocks.clear();
            } finally {
                if (longLivedReadTxn == null) {
                    try {
                        longLivedReadTxn = env.txnRead();
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to rebuild longLivedReadTxn after flush", e);
                    }
                }
            }
        } finally {
            flushing.set(false);
        }
    }

    private void saveDirtyBlock(long blockNumber, ByteBuffer buffer) {
        int savedPosition = buffer.position();
        ByteBuffer copy = ByteBuffer.allocateDirect(blockSize);
        buffer.rewind();
        copy.put(buffer);
        buffer.position(savedPosition);
        dirtyBlocks.put(blockNumber, copy);
        if (dirtyBlocks.size() > MAX_DIRTY_BLOCKS && !flushing.get()) {
            flushAllDirtyBlocks();
        }
    }

    private void persistTotalLengthOnClose() {
        if (longLivedReadTxn != null) {
            longLivedReadTxn.close();
            longLivedReadTxn = null;
        }
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            ByteBuffer valBuf = ByteBuffer.allocateDirect(8);
            valBuf.putLong(position).flip();
            dbi.put(txn, totalLengthKeyBuffer, valBuf);
            txn.commit();
        }
    }

    private void loadOrInitBlock(long blockNumber) throws IOException {
        if (longLivedReadTxn == null) {
            throw new IOException("longLivedReadTxn is null, cannot load block " + blockNumber);
        }
        ByteBuffer dirtyBlock = dirtyBlocks.get(blockNumber);
        if (dirtyBlock != null) {
            writeBuffer = ByteBuffer.allocateDirect(blockSize);
            ByteBuffer dup = dirtyBlock.duplicate();
            dup.flip();
            writeBuffer.put(dup);
            writeBuffer.position((int) (position % blockSize));
        } else {
            keyBuffer.clear();
            keyBuffer.putLong(blockNumber).flip();
            ByteBuffer existing = dbi.get(longLivedReadTxn, keyBuffer);
            if (existing != null) {
                writeBuffer = ByteBuffer.allocateDirect(blockSize);
                writeBuffer.put(existing.duplicate());
                writeBuffer.position((int) (position % blockSize));
            } else {
                writeBuffer = ByteBuffer.allocateDirect(blockSize);
                writeBuffer.position((int) (position % blockSize));
            }
        }
        currentBlockNumber = blockNumber;
        currentBlockDirty = false;
    }

    private void ensureWriteBuffer(int needed) throws IOException {
        if (writeBuffer == null || writeBuffer.remaining() < needed) {
            if (writeBuffer != null && currentBlockNumber >= 0) {
                saveDirtyBlock(currentBlockNumber, writeBuffer);
            }
            loadOrInitBlock(position / blockSize);
            int blockOffset = (int) (position % blockSize);
            writeBuffer.position(blockOffset);
        }
    }

    private long totalLength() {
        return position;
    }

    private static int modifiedUtf8Length(String s) {
        int len = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 0x0001 && c <= 0x007F) {
                len++;
            } else if (c <= 0x07FF) {
                len += 2;
            } else {
                len += 3;
            }
        }
        return len;
    }

    private static void writeModifiedUtf8(String s, byte[] bytes, int offset) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 0x0001 && c <= 0x007F) {
                bytes[offset++] = (byte) c;
            } else if (c <= 0x07FF) {
                bytes[offset++] = (byte) (0xC0 | (c >> 6) & 0x1F);
                bytes[offset++] = (byte) (0x80 | c & 0x3F);
            } else {
                bytes[offset++] = (byte) (0xE0 | (c >> 12) & 0x0F);
                bytes[offset++] = (byte) (0x80 | (c >> 6) & 0x3F);
                bytes[offset++] = (byte) (0x80 | c & 0x3F);
            }
        }
    }
}
