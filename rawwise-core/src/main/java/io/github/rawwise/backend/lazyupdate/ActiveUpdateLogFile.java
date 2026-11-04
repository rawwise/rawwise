package io.github.rawwise.backend.lazyupdate;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ActiveUpdateLogFile implements AutoCloseable {

    static final byte[] MAGIC = "NWL".getBytes(StandardCharsets.US_ASCII);
    static final int MAGIC_SIZE = 3;
    static final byte RECORD_TYPE_UPDATE_LOG_ENTRY = 3;
    static final byte RECORD_TYPE_COMMIT = 0x30;
    static final byte RECORD_TYPE_ABORT = 0x31;
    static final byte FOOTER_MAGIC = 0x4E; // 'N'
    static final int FOOTER_INDEX_INTERVAL = 64;
    static final int HEADER_SIZE = MAGIC_SIZE + 4 + 4;
    static final int MAX_PAYLOAD_SIZE = 256 * 1024 * 1024;
    static final int FOOTER_OFFSET_BYTES = 8;
    static final int FOOTER_MAGIC_BYTES = 1;
    static final int FOOTER_CRC_BYTES = 4;
    static final int FOOTER_TAIL_SIZE = FOOTER_OFFSET_BYTES + FOOTER_MAGIC_BYTES + FOOTER_CRC_BYTES;

    private final Path activePath;
    private final String tableId;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final TreeMap<Long, Long> memoryFooterIndex = new TreeMap<>();
    private RandomAccessFile raf;
    private long entryCount = 0;
    private long minSeq = Long.MAX_VALUE;
    private long maxSeq = Long.MIN_VALUE;
    private volatile boolean closed = false;

    public ActiveUpdateLogFile(Path activePath, String tableId) throws IOException {
        this.activePath = activePath;
        this.tableId = tableId;
        Files.createDirectories(activePath.getParent());
        boolean exists = Files.exists(activePath) && Files.size(activePath) > 0;
        this.raf = new RandomAccessFile(activePath.toFile(), "rw");
        if (!exists) {
            writeHeader();
        } else {
            raf.seek(raf.length());
            rebuildMemoryState();
            log.info(
                    "ActiveUpdateLogFile opened existing: path={} entries={} seqRange=[{},{}]",
                    activePath,
                    entryCount,
                    minSeq,
                    maxSeq);
        }
    }

    private void rebuildMemoryState() throws IOException {
        long savedPos = raf.getFilePointer();
        raf.seek(MAGIC_SIZE);
        int jsonLen = raf.readInt();
        raf.skipBytes(4);
        raf.skipBytes(jsonLen);
        long fileEnd = raf.length();
        entryCount = 0;
        minSeq = Long.MAX_VALUE;
        maxSeq = Long.MIN_VALUE;
        memoryFooterIndex.clear();

        while (raf.getFilePointer() + 4 < fileEnd) {
            long recordOffset = raf.getFilePointer();
            int payloadSize = raf.readInt();
            if (payloadSize <= 0 || payloadSize > MAX_PAYLOAD_SIZE) break;
            if (raf.getFilePointer() + payloadSize + 4L > fileEnd) break;
            byte[] payload = new byte[payloadSize];
            raf.readFully(payload);
            int storedCrc = raf.readInt();
            CRC32 crc32 = new CRC32();
            crc32.update(payload);
            if (storedCrc != (int) crc32.getValue()) {
                log.warn(
                        "CRC32 mismatch in record at offset {} during rebuildMemoryState, skipping remaining entries",
                        recordOffset);
                break;
            }
            ByteBuffer buf = ByteBuffer.wrap(payload);
            byte type = buf.get();
            if (type == RECORD_TYPE_UPDATE_LOG_ENTRY) {
                entryCount++;
                int tableIdLen = buf.getInt();
                buf.position(buf.position() + tableIdLen);
                buf.getLong();
                long seq = buf.getLong();
                if (seq < minSeq) minSeq = seq;
                if (seq > maxSeq) maxSeq = seq;
                if (entryCount % FOOTER_INDEX_INTERVAL == 0) {
                    memoryFooterIndex.put(seq, recordOffset);
                }
            }
        }
        raf.seek(savedPos);
    }

    private void writeHeader() throws IOException {
        raf.seek(0);
        byte[] headerJson =
                ("{\"format\":\"update_log\",\"tableId\":\"" + tableId + "\"}")
                        .getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE);
        buf.put(MAGIC);
        buf.putInt(headerJson.length);
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc.getValue());
        raf.write(buf.array());
        raf.write(headerJson);
    }

    public AppendResult appendEntry(UpdateLogEntry entry) throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            long offset = raf.getFilePointer();
            byte[] record = serializeEntry(entry);
            raf.write(record);

            entryCount++;
            long seq = entry.sequenceNumber();
            if (seq < minSeq) minSeq = seq;
            if (seq > maxSeq) maxSeq = seq;

            if (entryCount % FOOTER_INDEX_INTERVAL == 0) {
                memoryFooterIndex.put(seq, offset);
            }
            return new AppendResult(offset, offset + record.length, seq);
        } finally {
            writeLock.unlock();
        }
    }

    public void appendCommitRecord(long txnId) throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            byte[] record = serializeTxnRecord(RECORD_TYPE_COMMIT, txnId);
            raf.write(record);
        } finally {
            writeLock.unlock();
        }
    }

    public void appendAbortRecord(long txnId) throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            byte[] record = serializeTxnRecord(RECORD_TYPE_ABORT, txnId);
            raf.write(record);
        } finally {
            writeLock.unlock();
        }
    }

    public void fsync() throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            raf.getFD().sync();
        } finally {
            writeLock.unlock();
        }
    }

    public Path rollover() throws IOException {
        writeLock.lock();
        try {
            ensureOpen();
            writeFooter();
            raf.close();
            String archiveName = "archive-" + System.currentTimeMillis() + ".nwl";
            Path archivePath = activePath.resolveSibling(archiveName);
            Files.move(activePath, archivePath, StandardCopyOption.ATOMIC_MOVE);
            log.info(
                    "Rolled over: {} → {} (entries={}, seqRange=[{},{}])",
                    activePath,
                    archivePath,
                    entryCount,
                    minSeq,
                    maxSeq);
            raf = new RandomAccessFile(activePath.toFile(), "rw");
            writeHeader();
            memoryFooterIndex.clear();
            entryCount = 0;
            minSeq = Long.MAX_VALUE;
            maxSeq = Long.MIN_VALUE;
            return archivePath;
        } finally {
            writeLock.unlock();
        }
    }

    private void writeFooter() throws IOException {
        long footerOffset = raf.getFilePointer();
        List<Map.Entry<Long, Long>> indexEntries = new ArrayList<>(memoryFooterIndex.entrySet());
        int indexBytes = indexEntries.size() * 16;
        int bodySize = 8 + 8 + 8 + 4 + indexBytes;
        int tailSize = FOOTER_OFFSET_BYTES + FOOTER_MAGIC_BYTES + FOOTER_CRC_BYTES;
        int footerSize = bodySize + tailSize;

        ByteBuffer buf = ByteBuffer.allocate(footerSize);
        buf.putLong(entryCount);
        buf.putLong(minSeq == Long.MAX_VALUE ? 0 : minSeq);
        buf.putLong(maxSeq == Long.MIN_VALUE ? 0 : maxSeq);
        buf.putInt(indexEntries.size());
        for (Map.Entry<Long, Long> e : indexEntries) {
            buf.putLong(e.getKey());
            buf.putLong(e.getValue());
        }
        buf.putLong(footerOffset);
        buf.put((byte) FOOTER_MAGIC);
        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, buf.position());
        buf.putInt((int) crc.getValue());
        raf.write(buf.array());
    }

    private byte[] serializeEntry(UpdateLogEntry entry) {
        byte[] tableIdBytes = entry.tableId().getBytes(StandardCharsets.UTF_8);
        byte[] entryIdBytes = entry.entryId().getBytes(StandardCharsets.UTF_8);
        byte[] predicateBytes = new byte[0];
        if (entry.predicate() != null) {
            predicateBytes = ProtobufExpressionSerializerHolder.serialize(entry.predicate());
        }
        byte[] updatesBytes = UpdateLogEntry.serializeUpdatesDirect(entry.updates());

        int payloadSize =
                1
                        + 4
                        + tableIdBytes.length
                        + 8
                        + 8
                        + 8
                        + 4
                        + entryIdBytes.length
                        + 4
                        + predicateBytes.length
                        + 4
                        + updatesBytes.length;
        int totalSize = 4 + payloadSize + 4;

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(payloadSize);
        buffer.put(RECORD_TYPE_UPDATE_LOG_ENTRY);
        buffer.putInt(tableIdBytes.length);
        buffer.put(tableIdBytes);
        buffer.putLong(entry.transactionId());
        buffer.putLong(entry.sequenceNumber());
        buffer.putLong(entry.createdAtMillis());
        buffer.putInt(entryIdBytes.length);
        buffer.put(entryIdBytes);
        buffer.putInt(predicateBytes.length);
        buffer.put(predicateBytes);
        buffer.putInt(updatesBytes.length);
        buffer.put(updatesBytes);

        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 4, payloadSize);
        buffer.putInt((int) crc32.getValue());
        return buffer.array();
    }

    private byte[] serializeTxnRecord(byte type, long txnId) {
        int payloadSize = 1 + 8 + 8;
        int totalSize = 4 + payloadSize + 4;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(payloadSize);
        buffer.put(type);
        buffer.putLong(txnId);
        buffer.putLong(System.currentTimeMillis());
        CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), 4, payloadSize);
        buffer.putInt((int) crc32.getValue());
        return buffer.array();
    }

    public long getFileSize() throws IOException {
        writeLock.lock();
        try {
            return raf.length();
        } finally {
            writeLock.unlock();
        }
    }

    public long getEntryCount() {
        return entryCount;
    }

    public boolean isClosed() {
        return closed;
    }

    public Path getActivePath() {
        return activePath;
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("ActiveUpdateLogFile is closed: " + activePath);
    }

    @Override
    public void close() throws IOException {
        writeLock.lock();
        try {
            if (!closed && raf != null) {
                raf.close();
                closed = true;
            }
        } finally {
            writeLock.unlock();
        }
    }

    public record AppendResult(long startOffset, long endOffset, long sequenceNumber) {}

    private static final class ProtobufExpressionSerializerHolder {
        static byte[] serialize(io.github.rawwise.model.expression.Expression expr) {
            return io.github.rawwise.model.expression.serializer.ProtobufExpressionSerializer
                    .getInstance()
                    .serialize(expr);
        }
    }
}
