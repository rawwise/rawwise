package io.github.rawwise.engine.vector;

import io.github.jbellis.jvector.disk.RandomAccessReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

class LmdbRandomAccessReader implements RandomAccessReader {
    private final Dbi<ByteBuffer> dbi;
    private final Txn<ByteBuffer> readTxn;
    private final int blockSize;
    private long position;
    private final long totalLength;
    private ByteBuffer currentBlock;
    private long currentBlockNumber = -1;
    private final ByteBuffer keyBuffer;
    private volatile boolean closed = false;

    LmdbRandomAccessReader(
            Dbi<ByteBuffer> dbi, Txn<ByteBuffer> readTxn, int blockSize, long totalLength) {
        this.dbi = dbi;
        this.readTxn = readTxn;
        this.blockSize = blockSize;
        this.totalLength = totalLength;
        this.keyBuffer = ByteBuffer.allocateDirect(8);
    }

    @Override
    public void seek(long offset) throws IOException {
        ensureOpen();
        this.position = offset;
        long blockNumber = offset / blockSize;
        if (blockNumber != currentBlockNumber) {
            loadBlock(blockNumber);
        }
        int blockOffset = (int) (offset % blockSize);
        currentBlock.position(blockOffset);
    }

    @Override
    public long getPosition() throws IOException {
        return position;
    }

    @Override
    public int readInt() throws IOException {
        ensureOpen();
        if (currentBlock != null && currentBlock.remaining() >= 4) {
            position += 4;
            return currentBlock.getInt();
        }
        byte[] bytes = new byte[4];
        readFully(bytes);
        return ByteBuffer.wrap(bytes).getInt();
    }

    @Override
    public float readFloat() throws IOException {
        ensureOpen();
        if (currentBlock != null && currentBlock.remaining() >= 4) {
            position += 4;
            return currentBlock.getFloat();
        }
        byte[] bytes = new byte[4];
        readFully(bytes);
        return ByteBuffer.wrap(bytes).getFloat();
    }

    @Override
    public long readLong() throws IOException {
        ensureOpen();
        if (currentBlock != null && currentBlock.remaining() >= 8) {
            position += 8;
            return currentBlock.getLong();
        }
        byte[] bytes = new byte[8];
        readFully(bytes);
        return ByteBuffer.wrap(bytes).getLong();
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        ensureOpen();
        int remaining = bytes.length;
        int offset = 0;
        while (remaining > 0) {
            ensureBuffer(1);
            int available = Math.min(remaining, currentBlock.remaining());
            currentBlock.get(bytes, offset, available);
            offset += available;
            remaining -= available;
            position += available;
            if (remaining > 0) {
                advanceToNextBlock();
            }
        }
    }

    @Override
    public void readFully(ByteBuffer buffer) throws IOException {
        ensureOpen();
        int remaining = buffer.remaining();
        while (remaining > 0) {
            ensureBuffer(1);
            int available = Math.min(remaining, currentBlock.remaining());
            int limit = currentBlock.limit();
            currentBlock.limit(currentBlock.position() + available);
            buffer.put(currentBlock);
            currentBlock.limit(limit);
            position += available;
            remaining -= available;
            if (remaining > 0) {
                advanceToNextBlock();
            }
        }
    }

    @Override
    public void readFully(float[] floats) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[floats.length * 4];
        readFully(bytes);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < floats.length; i++) {
            floats[i] = bb.getFloat();
        }
    }

    @Override
    public void readFully(long[] vector) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[vector.length * 8];
        readFully(bytes);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < vector.length; i++) {
            vector[i] = bb.getLong();
        }
    }

    @Override
    public void read(int[] ints, int offset, int count) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[count * 4];
        readFully(bytes);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < count; i++) {
            ints[offset + i] = bb.getInt();
        }
    }

    @Override
    public void read(float[] floats, int offset, int count) throws IOException {
        ensureOpen();
        byte[] bytes = new byte[count * 4];
        readFully(bytes);
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        for (int i = 0; i < count; i++) {
            floats[offset + i] = bb.getFloat();
        }
    }

    @Override
    public long length() throws IOException {
        return totalLength;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            currentBlock = null;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("Reader is closed");
    }

    private void loadBlock(long blockNumber) throws IOException {
        keyBuffer.clear();
        keyBuffer.putLong(blockNumber).flip();
        ByteBuffer value = dbi.get(readTxn, keyBuffer);
        if (value != null) {
            currentBlock = value.duplicate();
        } else {
            throw new IOException("Block not found: " + blockNumber);
        }
        currentBlockNumber = blockNumber;
    }

    private void ensureBuffer(int needed) throws IOException {
        if (currentBlock == null || currentBlock.remaining() < needed) {
            advanceToNextBlock();
        }
    }

    private void advanceToNextBlock() throws IOException {
        long nextBlock = position / blockSize;
        loadBlock(nextBlock);
        int blockOffset = (int) (position % blockSize);
        currentBlock.position(blockOffset);
    }
}
