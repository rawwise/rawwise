package io.github.rawwise.engine.txn;

import static org.junit.jupiter.api.Assertions.*;

import io.github.rawwise.metastore.Metastore;
import io.github.rawwise.metastore.Mutation;
import io.github.rawwise.metastore.Precondition;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TxnCommitLogDaoTest {

    private TxnCommitLogDao dao;

    private static Metastore newStore() {
        ConcurrentHashMap<String, byte[]> map = new ConcurrentHashMap<>();
        return new Metastore() {
            @Override
            public Optional<byte[]> get(byte[] key) {
                byte[] v = map.get(new String(key, StandardCharsets.UTF_8));
                return Optional.ofNullable(v);
            }

            @Override
            public void put(byte[] key, byte[] value) {
                map.put(new String(key, StandardCharsets.UTF_8), value);
            }

            @Override
            public void delete(byte[] key) {
                map.remove(new String(key, StandardCharsets.UTF_8));
            }

            @Override
            public boolean exists(byte[] key) {
                return map.containsKey(new String(key, StandardCharsets.UTF_8));
            }

            @Override
            public io.github.rawwise.common.collection.iterator.CloseableIterator<
                            io.github.rawwise.metastore.KeyValue>
                    scan(byte[] prefix) {
                throw new UnsupportedOperationException();
            }

            @Override
            public io.github.rawwise.common.collection.iterator.CloseableIterator<
                            io.github.rawwise.metastore.KeyValue>
                    scan(byte[] prefix, int limit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public io.github.rawwise.common.collection.iterator.CloseableIterator<
                            io.github.rawwise.metastore.KeyValue>
                    scanRange(byte[] startInclusive, byte[] endExclusive) {
                throw new UnsupportedOperationException();
            }

            @Override
            public io.github.rawwise.common.collection.iterator.CloseableIterator<
                            io.github.rawwise.metastore.KeyValue>
                    scanRange(byte[] startInclusive, byte[] endExclusive, int limit) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean compareAndSwap(
                    List<Precondition> preconditions, List<Mutation> mutations) {
                for (Precondition p : preconditions) {
                    if (p instanceof Precondition.NotExists ne) {
                        if (map.containsKey(new String(ne.key(), StandardCharsets.UTF_8)))
                            return false;
                    } else if (p instanceof Precondition.Exists ex) {
                        if (!map.containsKey(new String(ex.key(), StandardCharsets.UTF_8)))
                            return false;
                    } else if (p instanceof Precondition.ValueEquals ve) {
                        byte[] existing = map.get(new String(ve.key(), StandardCharsets.UTF_8));
                        if (existing == null || !java.util.Arrays.equals(existing, ve.expected()))
                            return false;
                    }
                }
                for (Mutation m : mutations) {
                    if (m instanceof Mutation.Put put) {
                        put(put.key(), put.value());
                    } else if (m instanceof Mutation.Delete del) {
                        delete(del.key());
                    }
                }
                return true;
            }

            @Override
            public void batchWrite(java.util.List<io.github.rawwise.metastore.Mutation> mutations) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {}
        };
    }

    @BeforeEach
    void setUp() {
        dao = new TxnCommitLogDao(newStore());
    }

    @Test
    void findByTxnId_returnsEmptyWhenNotFound() throws IOException {
        assertTrue(dao.findByTxnId(999L).isEmpty());
    }

    @Test
    void insertAndFindByTxnId() throws IOException {
        TxnCommitLogDao.TxnCommitRecord record =
                new TxnCommitLogDao.TxnCommitRecord(1L, 100L, 200L, System.currentTimeMillis());
        dao.insert(record);

        TxnCommitLogDao.TxnCommitRecord found = dao.findByTxnId(1L).orElseThrow();
        assertEquals(1L, found.txnId());
        assertEquals(100L, found.commitId());
        assertEquals(200L, found.snapshotId());
    }

    @Test
    void insertDuplicateTxnId_throwsException() throws IOException {
        TxnCommitLogDao.TxnCommitRecord record =
                new TxnCommitLogDao.TxnCommitRecord(1L, 100L, 200L, System.currentTimeMillis());
        dao.insert(record);

        TxnCommitLogDao.TxnCommitRecord duplicate =
                new TxnCommitLogDao.TxnCommitRecord(1L, 101L, 201L, System.currentTimeMillis());
        assertThrows(IOException.class, () -> dao.insert(duplicate));
    }

    @Test
    void idempotentCommit_returnsExistingOnDuplicate() throws IOException {
        TxnCommitLogDao.TxnCommitRecord record =
                new TxnCommitLogDao.TxnCommitRecord(1L, 100L, 200L, System.currentTimeMillis());
        dao.insert(record);

        TxnCommitLogDao.TxnCommitRecord existing = dao.findByTxnId(1L).orElseThrow();
        assertEquals(100L, existing.commitId());
    }

    @Test
    void constructor_rejectsNullStore() {
        assertThrows(IllegalArgumentException.class, () -> new TxnCommitLogDao(null));
    }

    @Test
    void multipleRecords_differentTxnIds() throws IOException {
        for (long i = 1; i <= 10; i++) {
            dao.insert(
                    new TxnCommitLogDao.TxnCommitRecord(
                            i, i * 100, i * 200, System.currentTimeMillis()));
        }

        for (long i = 1; i <= 10; i++) {
            TxnCommitLogDao.TxnCommitRecord found = dao.findByTxnId(i).orElseThrow();
            assertEquals(i * 100, found.commitId());
            assertEquals(i * 200, found.snapshotId());
        }
    }
}
