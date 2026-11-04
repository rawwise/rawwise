package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.serializer.ProtobufExpressionSerializer;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpdateLogReader extends AbstractReader {

    static final byte[] EXPECTED_MAGIC = "NWL".getBytes(StandardCharsets.US_ASCII);
    static final int MAGIC_SIZE = 3;
    static final byte RECORD_TYPE_UPDATE_LOG_ENTRY = 3;
    static final byte RECORD_TYPE_COMMIT = 0x30;
    static final byte RECORD_TYPE_ABORT = 0x31;
    static final byte FOOTER_MAGIC = 0x4E;
    static final int FOOTER_OFFSET_BYTES = 8;
    static final int FOOTER_MAGIC_BYTES = 1;
    static final int FOOTER_CRC_BYTES = 4;
    static final int FOOTER_TAIL_SIZE = FOOTER_OFFSET_BYTES + FOOTER_MAGIC_BYTES + FOOTER_CRC_BYTES;
    static final int MAX_PAYLOAD_SIZE = 256 * 1024 * 1024;
    static final int MAX_HEADER_LENGTH = 1024 * 1024;
    static final int MAX_STRING_FIELD_LENGTH = 64 * 1024;
    static final int MAX_BINARY_FIELD_LENGTH = 4 * 1024 * 1024;
    static final int MAX_RECORD_COUNT = 1_000_000;

    @Getter private final ProtobufExpressionSerializer serializer;

    public UpdateLogReader() {
        this.serializer = ProtobufExpressionSerializer.getInstance();
    }

    public List<UpdateLogEntry> read(UpdateLog updateLog) throws IOException {
        return read(updateLog, Long.MAX_VALUE);
    }

    public List<UpdateLogEntry> read(UpdateLog updateLog, long maxSeq) throws IOException {
        ensureOpen();
        Path path = Path.of(updateLog.filePath());
        if (!Files.exists(path)) {
            throw new IOException("UpdateLog file does not exist: " + updateLog.filePath());
        }

        byte[] magic = readMagic(path);
        if (!java.util.Arrays.equals(magic, EXPECTED_MAGIC)) {
            throw new IOException(
                    "Invalid magic in UpdateLog file: "
                            + updateLog.filePath()
                            + ", expected NWL, got: "
                            + new String(magic, StandardCharsets.US_ASCII));
        }
        return readNwl(path, maxSeq);
    }

    public TxnRecoverySummary scanTxnRecords(Path path) throws IOException {
        ensureOpen();
        if (!Files.exists(path)) {
            return new TxnRecoverySummary(Map.of(), Map.of());
        }
        byte[] magic = readMagic(path);
        if (!java.util.Arrays.equals(magic, EXPECTED_MAGIC)) {
            throw new IOException("Invalid magic scanning txn records: " + path);
        }
        return scanNwlTxnRecords(path);
    }

    private byte[] readMagic(Path path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            byte[] magic = new byte[MAGIC_SIZE];
            int n = raf.read(magic);
            if (n < MAGIC_SIZE) {
                throw new IOException("Cannot read magic from UpdateLog file: " + path);
            }
            return magic;
        }
    }

    private List<UpdateLogEntry> readNwl(Path path, long maxSeq) throws IOException {
        Footer footer = tryParseFooter(path);
        if (footer != null && footer.maxSeq <= maxSeq) {
            log.debug("NWL full read (footer maxSeq={} <= cut={}) {}", footer.maxSeq, maxSeq, path);
            return readNwlSequential(path, footer.footerOffset, -1L, Long.MAX_VALUE);
        }
        if (footer != null) {
            long fastScanEnd = findPrefixOffset(footer, maxSeq);
            log.debug("NWL prefix read (cut={}, fastScanEnd={}) {}", maxSeq, fastScanEnd, path);
            return readNwlSequential(path, footer.footerOffset, fastScanEnd, maxSeq);
        }
        log.debug("NWL sequential scan (no footer) {}", path);
        return readNwlSequential(path, Long.MAX_VALUE, -1L, maxSeq);
    }

    private List<UpdateLogEntry> readNwlSequential(
            Path path, long recordsEndOffset, long fastScanEndOffset, long maxSeq)
            throws IOException {
        List<UpdateLogEntry> entries = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(MAGIC_SIZE);
            int headerLength = raf.readInt();
            if (headerLength < 0 || headerLength > MAX_HEADER_LENGTH) {
                throw new IOException("Invalid NWL header length: " + headerLength);
            }
            raf.seek(raf.getFilePointer() + 4L + headerLength);

            while (raf.getFilePointer() < recordsEndOffset) {
                long curOffset = raf.getFilePointer();
                UpdateLogEntry entry = readNextEntry(raf);
                if (entry == null) break;
                boolean inFastScanRegion = fastScanEndOffset >= 0 && curOffset < fastScanEndOffset;
                if (inFastScanRegion || entry.sequenceNumber() <= maxSeq) {
                    entries.add(entry);
                    if (entries.size() > MAX_RECORD_COUNT) {
                        throw new IOException(
                                "UpdateLog record count exceeds limit: " + MAX_RECORD_COUNT);
                    }
                }
            }
        }
        log.info(
                "Read {} UpdateLog entries from {} (NWL, maxSeq={})", entries.size(), path, maxSeq);
        return entries;
    }

    private UpdateLogEntry readNextEntry(RandomAccessFile raf) throws IOException {
        if (raf.getFilePointer() + 4 > raf.length()) return null;
        int payloadSize = raf.readInt();
        if (payloadSize <= 0 || payloadSize > MAX_PAYLOAD_SIZE) return null;
        if (raf.getFilePointer() + payloadSize + 4L > raf.length()) {
            raf.seek(raf.length());
            return null;
        }
        byte[] payload = new byte[payloadSize];
        raf.readFully(payload);
        int storedCrc = raf.readInt();
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        if (storedCrc != (int) crc32.getValue()) {
            log.warn("CRC32 mismatch in UpdateLog record, skipping");
            return null;
        }
        return deserializeEntry(payload);
    }

    private Footer tryParseFooter(Path path) throws IOException {
        long fileLen = Files.size(path);
        if (fileLen < FOOTER_TAIL_SIZE) return null;
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(fileLen - FOOTER_TAIL_SIZE);
            long footerOffset = raf.readLong();
            byte magic = raf.readByte();
            int footerCrc = raf.readInt();
            if (magic != FOOTER_MAGIC) {
                log.debug("No footer magic in {}", path);
                return null;
            }
            if (footerOffset < 0 || footerOffset > fileLen - FOOTER_TAIL_SIZE) {
                log.warn("Invalid footer offset {} in {}", footerOffset, path);
                return null;
            }
            int bodySize = (int) (fileLen - FOOTER_TAIL_SIZE - footerOffset);
            if (bodySize < 8 + 8 + 8 + 4) {
                log.warn("Footer body too small {} in {}", bodySize, path);
                return null;
            }
            byte[] footerRegion = new byte[bodySize + FOOTER_OFFSET_BYTES + FOOTER_MAGIC_BYTES];
            raf.seek(footerOffset);
            raf.readFully(footerRegion);
            CRC32 crc32 = new CRC32();
            crc32.update(footerRegion);
            if (footerCrc != (int) crc32.getValue()) {
                log.warn("Footer CRC mismatch in {}", path);
                return null;
            }
            ByteBuffer buf = ByteBuffer.wrap(footerRegion);
            long entryCount = buf.getLong();
            long minSeq = buf.getLong();
            long maxSeq = buf.getLong();
            int indexSize = buf.getInt();
            if (indexSize < 0 || indexSize > 1_000_000) {
                log.warn("Invalid footer index size {} in {}", indexSize, path);
                return null;
            }
            TreeMap<Long, Long> index = new TreeMap<>();
            for (int i = 0; i < indexSize; i++) {
                long seq = buf.getLong();
                long off = buf.getLong();
                index.put(seq, off);
            }
            return new Footer(entryCount, minSeq, maxSeq, index, footerOffset);
        } catch (IOException e) {
            log.debug("Footer parse failed for {}: {}", path, e.getMessage());
            return null;
        }
    }

    private long findPrefixOffset(Footer footer, long maxSeq) {
        Map.Entry<Long, Long> floor = footer.index.floorEntry(maxSeq);
        if (floor == null) return Long.MAX_VALUE;
        return floor.getValue();
    }

    private TxnRecoverySummary scanNwlTxnRecords(Path path) throws IOException {
        Map<Long, Long> commitTsByTxn = new java.util.HashMap<>();
        Map<Long, Long> abortTsByTxn = new java.util.HashMap<>();
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "r")) {
            raf.seek(MAGIC_SIZE);
            int headerLength = raf.readInt();
            raf.seek(raf.getFilePointer() + 4L + headerLength);

            while (true) {
                if (raf.getFilePointer() + 4 > raf.length()) break;
                int payloadSize = raf.readInt();
                if (payloadSize <= 0 || payloadSize > MAX_PAYLOAD_SIZE) break;
                if (raf.getFilePointer() + payloadSize + 4L > raf.length()) break;
                byte[] payload = new byte[payloadSize];
                raf.readFully(payload);
                raf.readInt();
                ByteBuffer buf = ByteBuffer.wrap(payload);
                byte type = buf.get();
                if (type == RECORD_TYPE_COMMIT) {
                    long txnId = buf.getLong();
                    long ts = buf.getLong();
                    commitTsByTxn.put(txnId, ts);
                } else if (type == RECORD_TYPE_ABORT) {
                    long txnId = buf.getLong();
                    long ts = buf.getLong();
                    abortTsByTxn.put(txnId, ts);
                }
            }
        }
        return new TxnRecoverySummary(commitTsByTxn, abortTsByTxn);
    }

    UpdateLogEntry deserializeEntry(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte recordType = buffer.get();
        if (recordType != RECORD_TYPE_UPDATE_LOG_ENTRY) {
            log.debug("Skipping non-entry record type {} in UpdateLog", recordType);
            return null;
        }

        int tableIdLen = buffer.getInt();
        if (tableIdLen < 0 || tableIdLen > MAX_STRING_FIELD_LENGTH) {
            throw new IOException("Invalid tableId length: " + tableIdLen);
        }
        byte[] tableIdBytes = new byte[tableIdLen];
        buffer.get(tableIdBytes);
        String tableId = new String(tableIdBytes, StandardCharsets.UTF_8);

        long transactionId = buffer.getLong();
        long sequenceNumber = buffer.getLong();
        long createdAtMillis = buffer.getLong();

        int entryIdLen = buffer.getInt();
        if (entryIdLen < 0 || entryIdLen > MAX_STRING_FIELD_LENGTH) {
            throw new IOException("Invalid entryId length: " + entryIdLen);
        }
        byte[] entryIdBytes = new byte[entryIdLen];
        buffer.get(entryIdBytes);
        String entryId = new String(entryIdBytes, StandardCharsets.UTF_8);

        int predicateLen = buffer.getInt();
        if (predicateLen < 0 || predicateLen > MAX_BINARY_FIELD_LENGTH) {
            throw new IOException("Invalid predicate length: " + predicateLen);
        }
        byte[] predicateBytes = new byte[predicateLen];
        buffer.get(predicateBytes);

        int updatesLen = buffer.getInt();
        if (updatesLen < 0 || updatesLen > MAX_BINARY_FIELD_LENGTH) {
            throw new IOException("Invalid updates length: " + updatesLen);
        }
        byte[] updatesBytes = new byte[updatesLen];
        buffer.get(updatesBytes);

        Expression predicate = null;
        if (predicateLen > 0) {
            predicate = serializer.deserialize(predicateBytes);
        }

        Map<String, Comparable<?>> updates = UpdateLogEntry.deserializeUpdatesMap(updatesBytes);

        return new UpdateLogEntry(
                entryId,
                tableId,
                predicate,
                updates,
                transactionId,
                sequenceNumber,
                createdAtMillis);
    }

    private record Footer(
            long entryCount,
            long minSeq,
            long maxSeq,
            TreeMap<Long, Long> index,
            long footerOffset) {}

    public record TxnRecoverySummary(Map<Long, Long> commitTsByTxn, Map<Long, Long> abortTsByTxn) {}
}
