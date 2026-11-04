package io.github.rawwise.model.record.impl;

import io.github.rawwise.model.op.Op;
import io.github.rawwise.model.op.OpResult;
import io.github.rawwise.model.record.DynamicTableInfo;
import io.github.rawwise.model.record.RawWiseTable;
import io.github.rawwise.model.record.Snapshot;
import io.github.rawwise.model.record.TableMeta;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RawWiseTableImpl implements RawWiseTable, Serializable {

    private static final long serialVersionUID = 1L;

    private final String tableIdentifier;

    private final TableMeta meta;

    private volatile DynamicTableInfo dynamicInfo;

    private transient ReentrantReadWriteLock lock;

    private transient AtomicBoolean closed;

    public RawWiseTableImpl(String tableIdentifier, TableMeta meta, DynamicTableInfo dynamicInfo) {
        this.tableIdentifier =
                Objects.requireNonNull(tableIdentifier, "tableIdentifier cannot be null");
        this.meta = Objects.requireNonNull(meta, "meta cannot be null");
        this.dynamicInfo = dynamicInfo != null ? dynamicInfo : DynamicTableInfo.builder().build();

        // initialize transient fields
        this.lock = new ReentrantReadWriteLock();
        this.closed = new AtomicBoolean(false);

        log.debug(
                "RawWiseTableImpl initialized (Serializable): {} (tableId={}, type={})",
                tableIdentifier,
                meta.tableId(),
                meta.tableType());
    }

    @Override
    public TableMeta meta() {
        return meta;
    }

    @Override
    public Snapshot currentSnapshot() {
        lock.readLock().lock();
        try {
            return dynamicInfo != null ? dynamicInfo.currentSnapshot() : null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public DynamicTableInfo dynamicInfo() {
        lock.readLock().lock();
        try {
            return dynamicInfo;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Snapshot snapshot(long snapshotId) {
        lock.readLock().lock();
        try {
            if (dynamicInfo == null) {
                return null;
            }
            return dynamicInfo.snapshots().stream()
                    .filter(s -> s.snapshotId() == snapshotId)
                    .findFirst()
                    .orElse(null);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<Snapshot> snapshots() {
        lock.readLock().lock();
        try {
            return dynamicInfo != null ? dynamicInfo.snapshots() : List.of();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public <R extends OpResult> R op(Op<R> op) {
        ensureOpen();
        Objects.requireNonNull(op, "op cannot be null");
        return op.exec();
    }

    @Override
    public void refresh() {
        ensureOpen();
        log.debug(
                "refresh() invocation acknowledged (no-op in serializable Table mode). "
                        + "Use table.op(Refresh.builder().withTable(this).build()) to refresh.");
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            log.info(
                    "Closing RawWiseTableImpl (lightweight - no storage resources): {}",
                    tableIdentifier);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    public void updateDynamicInfo(DynamicTableInfo newInfo) {
        Objects.requireNonNull(newInfo, "newInfo cannot be null");
        lock.writeLock().lock();
        try {
            this.dynamicInfo = newInfo;
            log.debug(
                    "Dynamic info updated for table: {}, snapshotCount={}",
                    tableIdentifier,
                    newInfo.snapshots().size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Table is closed: " + tableIdentifier);
        }
    }

    private void readObject(java.io.ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        // 1. perform default deserialization(restore non-transient fields)
        in.defaultReadObject();

        // 2. rebuild transient fields
        this.lock = new ReentrantReadWriteLock();
        this.closed = new AtomicBoolean(false);

        log.debug("Reconstructed transient fields after deserialization: {}", tableIdentifier);
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    @Override
    public String toString() {
        return "RawWiseTableImpl{"
                + "tableIdentifier='"
                + tableIdentifier
                + '\''
                + ", tableId="
                + meta.tableId()
                + ", tableName='"
                + meta.tableName()
                + '\''
                + ", closed="
                + closed.get()
                + ", serializable=true"
                + // marked as serializable
                '}';
    }
}
