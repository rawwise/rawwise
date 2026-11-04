package io.github.rawwise.metastore;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.rocksdb.*;

/**
 * Metastore implementation based on single Rocks DB instance.
 *
 * <p>uses single Column Family(default),no sharding.CAS via {@code Write Batch With Index +
 * Snapshot} implements MVCC-level atomicity check and write. all write operations routed to {@code
 * synchronized},guarantees no race condition window with CAS.
 */
public class RocksdbMetastore implements Metastore {

    private final RocksDB db;

    public RocksdbMetastore(String dbPath) throws IOException {
        try {
            RocksDB.loadLibrary();
            try (Options options =
                    new Options()
                            .setCreateIfMissing(true)
                            .setCompressionType(CompressionType.LZ4_COMPRESSION)
                            .setMaxBackgroundJobs(
                                    Math.max(2, Runtime.getRuntime().availableProcessors()))
                            .setWriteBufferSize(64 * 1024 * 1024)
                            .setMaxWriteBufferNumber(4)
                            .setMinWriteBufferNumberToMerge(2)) {
                Files.createDirectories(Path.of(dbPath));
                this.db = RocksDB.open(options, dbPath);
            }
        } catch (RocksDBException e) {
            throw new IOException("Failed to open RocksDB at " + dbPath, e);
        }
    }

    @Override
    public Optional<byte[]> get(byte[] key) throws IOException {
        try {
            byte[] value = db.get(key);
            return Optional.ofNullable(value);
        } catch (RocksDBException e) {
            throw new IOException("RocksDB get failed", e);
        }
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IOException("RocksDB put failed", e);
        }
    }

    @Override
    public synchronized void delete(byte[] key) throws IOException {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new IOException("RocksDB delete failed", e);
        }
    }

    @Override
    public boolean exists(byte[] key) throws IOException {
        try {
            return db.get(key) != null;
        } catch (RocksDBException e) {
            throw new IOException("RocksDB exists check failed", e);
        }
    }

    @Override
    public CloseableIterator<KeyValue> scan(byte[] prefix) throws IOException {
        return scanWithLimit(prefix, -1);
    }

    @Override
    public CloseableIterator<KeyValue> scan(byte[] prefix, int limit) throws IOException {
        return scanWithLimit(prefix, limit);
    }

    @Override
    public CloseableIterator<KeyValue> scanRange(byte[] startInclusive, byte[] endExclusive)
            throws IOException {
        RocksIterator iterator = db.newIterator();
        iterator.seek(startInclusive);
        return new RocksIteratorAdapter(iterator, endExclusive, -1);
    }

    @Override
    public CloseableIterator<KeyValue> scanRange(
            byte[] startInclusive, byte[] endExclusive, int limit) throws IOException {
        RocksIterator iterator = db.newIterator();
        iterator.seek(startInclusive);
        return new RocksIteratorAdapter(iterator, endExclusive, limit);
    }

    @Override
    public synchronized boolean compareAndSwap(
            List<Precondition> preconditions, List<Mutation> mutations) throws IOException {
        try (WriteBatchWithIndex batch = new WriteBatchWithIndex()) {
            Snapshot snapshot = db.getSnapshot();
            ReadOptions ro = new ReadOptions().setSnapshot(snapshot);
            try {
                for (Precondition pc : preconditions) {
                    byte[] key =
                            switch (pc) {
                                case Precondition.Exists e -> e.key();
                                case Precondition.NotExists ne -> ne.key();
                                case Precondition.ValueEquals ve -> ve.key();
                            };
                    byte[] current = db.get(ro, key);
                    switch (pc) {
                        case Precondition.Exists e -> {
                            if (current == null) {
                                return false;
                            }
                        }
                        case Precondition.NotExists ne -> {
                            if (current != null) {
                                return false;
                            }
                        }
                        case Precondition.ValueEquals ve -> {
                            if (!Arrays.equals(current, ve.expected())) {
                                return false;
                            }
                        }
                    }
                }
                for (Mutation m : mutations) {
                    switch (m) {
                        case Mutation.Put p -> batch.put(p.key(), p.value());
                        case Mutation.Delete d -> batch.delete(d.key());
                    }
                }
                try (WriteOptions wo = new WriteOptions()) {
                    db.write(wo, batch.getWriteBatch());
                }
            } finally {
                try {
                    db.releaseSnapshot(snapshot);
                } finally {
                    ro.close();
                }
            }
        } catch (RocksDBException e) {
            throw new IOException("RocksDB CAS failed", e);
        }
        return true;
    }

    @Override
    public synchronized void batchWrite(List<Mutation> mutations) throws IOException {
        try (WriteBatch batch = new WriteBatch()) {
            for (Mutation m : mutations) {
                switch (m) {
                    case Mutation.Put p -> batch.put(p.key(), p.value());
                    case Mutation.Delete d -> batch.delete(d.key());
                }
            }
            try (WriteOptions wo = new WriteOptions()) {
                db.write(wo, batch);
            }
        } catch (RocksDBException e) {
            throw new IOException("RocksDB batch write failed", e);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            try (FlushOptions fo = new FlushOptions()) {
                db.flush(fo);
            }
        } catch (RocksDBException e) {
            throw new IOException("RocksDB flush failed", e);
        }
    }

    @Override
    public boolean supportsCrossKeyCas() {
        return true;
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    public void createCheckpoint(String targetPath) {
        try (var checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(targetPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to create RocksDB checkpoint at " + targetPath, e);
        }
    }

    private CloseableIterator<KeyValue> scanWithLimit(byte[] prefix, int limit) throws IOException {
        RocksIterator iterator = db.newIterator();
        iterator.seek(prefix);
        return new RocksPrefixIteratorAdapter(iterator, prefix, limit);
    }

    private static final class RocksPrefixIteratorAdapter implements CloseableIterator<KeyValue> {
        private final RocksIterator iterator;
        private final byte[] prefix;
        private final int limit;
        private int emitted;

        RocksPrefixIteratorAdapter(RocksIterator iterator, byte[] prefix, int limit) {
            this.iterator = iterator;
            this.prefix = prefix;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            if (!iterator.isValid()) {
                return false;
            }
            if (limit > 0 && emitted >= limit) {
                return false;
            }
            if (!startsWithPrefix(iterator.key())) {
                return false;
            }
            return true;
        }

        @Override
        public KeyValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] k = iterator.key();
            byte[] v = iterator.value();
            KeyValue kv = new KeyValue(Arrays.copyOf(k, k.length), Arrays.copyOf(v, v.length));
            iterator.next();
            emitted++;
            return kv;
        }

        @Override
        public void close() {
            iterator.close();
        }

        private boolean startsWithPrefix(byte[] key) {
            if (key == null || key.length < prefix.length) {
                return false;
            }
            for (int i = 0; i < prefix.length; i++) {
                if (key[i] != prefix[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    private static final class RocksIteratorAdapter implements CloseableIterator<KeyValue> {
        private final RocksIterator iterator;
        private final byte[] end;
        private final int limit;
        private int emitted;

        RocksIteratorAdapter(RocksIterator iterator, byte[] end, int limit) {
            this.iterator = iterator;
            this.end = end;
            this.limit = limit;
        }

        @Override
        public boolean hasNext() {
            if (!iterator.isValid()) {
                return false;
            }
            if (limit > 0 && emitted >= limit) {
                return false;
            }
            if (end != null && Arrays.compareUnsigned(iterator.key(), end) >= 0) {
                return false;
            }
            return true;
        }

        @Override
        public KeyValue next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            byte[] k = iterator.key();
            byte[] v = iterator.value();
            KeyValue kv = new KeyValue(Arrays.copyOf(k, k.length), Arrays.copyOf(v, v.length));
            iterator.next();
            emitted++;
            return kv;
        }

        @Override
        public void close() {
            iterator.close();
        }
    }
}
