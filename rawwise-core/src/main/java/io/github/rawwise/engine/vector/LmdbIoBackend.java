package io.github.rawwise.engine.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.extern.slf4j.Slf4j;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;

@Slf4j
public class LmdbIoBackend implements AutoCloseable {
    private static final int DEFAULT_BLOCK_SIZE = 65536;
    private static final long DEFAULT_MAP_SIZE = 256L * 1024L * 1024L;

    private final Env<ByteBuffer> env;
    private final Dbi<ByteBuffer> dbi;
    private final int blockSize;

    public LmdbIoBackend(Path lmdbPath) {
        this(lmdbPath, DEFAULT_BLOCK_SIZE);
    }

    LmdbIoBackend(Path lmdbPath, int blockSize) {
        this.blockSize = blockSize;
        try {
            Files.createDirectories(lmdbPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create LMDB path: " + lmdbPath, e);
        }
        this.env =
                Env.create()
                        .setMapSize(DEFAULT_MAP_SIZE)
                        .setMaxDbs(1)
                        .setMaxReaders(126)
                        .open(lmdbPath.toFile(), EnvFlags.MDB_NOLOCK);
        this.dbi = env.openDbi("vector_graph", DbiFlags.MDB_CREATE);
        log.info("LmdbIoBackend initialized at {} (blockSize={})", lmdbPath, blockSize);
    }

    LmdbReaderSupplier createReaderSupplier() {
        return LmdbReaderSupplier.create(dbi, env, blockSize);
    }

    LmdbRandomAccessWriter createWriter() {
        return new LmdbRandomAccessWriter(dbi, env, blockSize);
    }

    boolean hasData() {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer keyBuf = ByteBuffer.allocateDirect(8);
            keyBuf.putLong(-1L).flip();
            return dbi.get(txn, keyBuf) != null;
        }
    }

    Env<ByteBuffer> env() {
        return env;
    }

    Dbi<ByteBuffer> dbi() {
        return dbi;
    }

    int blockSize() {
        return blockSize;
    }

    @Override
    public void close() {
        try {
            env.close();
        } catch (Exception e) {
            log.warn("Failed to close LMDB env", e);
        }
    }
}
