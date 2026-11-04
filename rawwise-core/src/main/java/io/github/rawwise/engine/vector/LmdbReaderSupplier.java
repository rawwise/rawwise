package io.github.rawwise.engine.vector;

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.lmdbjava.Dbi;
import org.lmdbjava.Txn;

class LmdbReaderSupplier implements ReaderSupplier {
    private final Dbi<ByteBuffer> dbi;
    private final int blockSize;
    private final long totalLength;
    private final Txn<ByteBuffer> sharedReadTxn;
    private volatile boolean closed = false;

    LmdbReaderSupplier(
            Dbi<ByteBuffer> dbi, Txn<ByteBuffer> sharedReadTxn, int blockSize, long totalLength) {
        this.dbi = dbi;
        this.sharedReadTxn = sharedReadTxn;
        this.blockSize = blockSize;
        this.totalLength = totalLength;
    }

    static LmdbReaderSupplier create(
            Dbi<ByteBuffer> dbi, org.lmdbjava.Env<ByteBuffer> env, int blockSize) {
        Txn<ByteBuffer> sharedReadTxn;
        try {
            sharedReadTxn = env.txnRead();
        } catch (Exception e) {
            throw new VectorSearchUnavailableException(
                    "Failed to acquire read transaction: " + e.getMessage(), e);
        }
        long totalLength = readTotalLength(dbi, sharedReadTxn);
        return new LmdbReaderSupplier(dbi, sharedReadTxn, blockSize, totalLength);
    }

    @Override
    public RandomAccessReader get() throws IOException {
        if (closed) throw new IOException("ReaderSupplier is closed");
        return new LmdbRandomAccessReader(dbi, sharedReadTxn, blockSize, totalLength);
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            sharedReadTxn.close();
        }
    }

    private static long readTotalLength(Dbi<ByteBuffer> dbi, Txn<ByteBuffer> txn) {
        ByteBuffer keyBuf = ByteBuffer.allocateDirect(8);
        keyBuf.putLong(-1L).flip();
        ByteBuffer valBuf = dbi.get(txn, keyBuf);
        if (valBuf != null) {
            return valBuf.getLong();
        }
        return 0;
    }
}
