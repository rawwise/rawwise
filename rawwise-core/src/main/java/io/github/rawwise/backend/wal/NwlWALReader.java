package io.github.rawwise.backend.wal;

import io.github.rawwise.engine.storage.metrics.ReaderErrorType;
import io.github.rawwise.engine.storage.metrics.ReaderMetrics;
import io.github.rawwise.engine.storage.reader.AbstractReader;
import io.github.rawwise.engine.storage.spi.Storage;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

@Slf4j
public class NwlWALReader extends AbstractReader implements WALReader {

    private static final byte[] EXPECTED_MAGIC = "NWL0".getBytes(StandardCharsets.US_ASCII);
    private static final Pattern NWL_FILE_PATTERN =
            Pattern.compile("wal_([^_]+)_(\\d+)_(\\d+)\\.nwl");
    private static final Pattern PARQUET_FILE_PATTERN =
            Pattern.compile("wal_(\\d+)_(\\d+)\\.parquet");
    private static final int MAX_PAYLOAD_SIZE = 256 * 1024 * 1024;
    private static final int MAX_HEADER_LENGTH = 1024 * 1024;

    private final String basePath;
    private final Configuration hadoopConfig;

    public NwlWALReader(String basePath, Configuration hadoopConfig) {
        this.basePath = basePath;
        this.hadoopConfig = hadoopConfig;
    }

    public NwlWALReader(String basePath, Configuration hadoopConfig, ReaderMetrics metrics) {
        super(metrics);
        this.basePath = basePath;
        this.hadoopConfig = hadoopConfig;
    }

    @Override
    public Storage.StoragePath walBasePath() {
        String scheme = isHdfsPath(basePath) ? "hdfs" : "file";
        return new Storage.StoragePath(scheme, basePath);
    }

    @Override
    public void forEach(RecordPredicate<WALRecord> predicate) throws IOException {
        ensureOpen();
        long startTime = System.nanoTime();
        long recordCount = 0;
        try {
            boolean isHdfs = isHdfsPath(basePath);
            if (isHdfs) {
                recordCount = forEachFromHdfs(predicate);
            } else {
                recordCount = forEachFromLocal(predicate);
            }
        } catch (IOException e) {
            metrics().recordError(ReaderErrorType.CORRUPTION, e, "forEach");
            throw e;
        } finally {
            long duration = System.nanoTime() - startTime;
            metrics().recordRead("forEach", 0, recordCount, duration);
        }
    }

    @Override
    public long estimatedSize() {
        return -1;
    }

    public List<WALRecord> readAll() throws IOException {
        ensureOpen();
        List<WALRecord> records = new ArrayList<>();
        forEach(records::add);
        return records;
    }

    public List<WALRecord> readUnflushed() throws IOException {
        ensureOpen();
        List<WALRecord> allRecords = readAll();
        return filterUnflushed(allRecords);
    }

    private long forEachFromHdfs(RecordPredicate<WALRecord> predicate) throws IOException {
        Path path = new Path(basePath);
        FileSystem fs = path.getFileSystem(hadoopConfig);

        if (!fs.exists(path)) {
            log.info("WAL HDFS path does not exist: {}", basePath);
            return 0;
        }

        List<Path> walFiles = new ArrayList<>();
        FileStatus[] files = fs.listStatus(path);
        for (FileStatus file : files) {
            String name = file.getPath().getName();
            if (name.endsWith(".nwl") || name.endsWith(".parquet")) {
                walFiles.add(file.getPath());
            }
        }

        walFiles.sort(this::compareWALFiles);

        long count = 0;
        for (Path walFile : walFiles) {
            if (!forEachSingleFile(walFile, fs, predicate)) {
                return count;
            }
        }
        return count;
    }

    private long forEachFromLocal(RecordPredicate<WALRecord> predicate) throws IOException {
        java.nio.file.Path dir = Paths.get(basePath);

        if (!Files.exists(dir)) {
            log.info("WAL local path does not exist: {}", basePath);
            return 0;
        }

        List<java.nio.file.Path> walFiles = new ArrayList<>();
        try (var stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile)
                    .filter(
                            p -> {
                                String name = p.getFileName().toString();
                                return name.endsWith(".nwl") || name.endsWith(".parquet");
                            })
                    .forEach(walFiles::add);
        }

        walFiles.sort(this::compareLocalWALFiles);

        long count = 0;
        for (java.nio.file.Path walFile : walFiles) {
            String name = walFile.getFileName().toString();
            if (name.endsWith(".nwl")) {
                if (!forEachNWLFileLocal(walFile, predicate)) {
                    return count;
                }
            } else if (name.endsWith(".parquet")) {
                if (!forEachParquetFileLocal(walFile, predicate)) {
                    return count;
                }
            }
        }
        return count;
    }

    private boolean forEachSingleFile(
            Path walFile, FileSystem fs, RecordPredicate<WALRecord> predicate) throws IOException {
        String name = walFile.getName();
        if (name.endsWith(".nwl")) {
            return forEachNWLFileHdfs(walFile, fs, predicate);
        } else if (name.endsWith(".parquet")) {
            return forEachParquetFileHdfs(walFile, fs, predicate);
        }
        return true;
    }

    private boolean forEachNWLFileHdfs(
            Path hdfsPath, FileSystem fs, RecordPredicate<WALRecord> predicate) throws IOException {
        try (var in = fs.open(hdfsPath)) {
            byte[] magic = new byte[4];
            if (in.read(magic) < 4) {
                log.warn("Cannot read magic from NWL file: {}", hdfsPath);
                return true;
            }
            if (!java.util.Arrays.equals(magic, EXPECTED_MAGIC)) {
                log.warn("Invalid magic in NWL file: {}", hdfsPath);
                return true;
            }

            int version = in.readInt();
            int headerLength = in.readInt();
            if (headerLength < 0 || headerLength > MAX_HEADER_LENGTH) {
                throw new WALRecoveryException(
                        String.format(
                                "Invalid NWL header length: %d in file %s",
                                headerLength, hdfsPath));
            }
            if (headerLength > 0) {
                byte[] headerJson = new byte[headerLength];
                in.readFully(headerJson);
            }
            in.readInt();

            while (true) {
                try {
                    int payloadSize = in.readInt();
                    if (payloadSize <= 0 || payloadSize > MAX_PAYLOAD_SIZE) {
                        break;
                    }

                    byte[] payload = new byte[payloadSize];
                    int totalRead = 0;
                    while (totalRead < payloadSize) {
                        int read = in.read(payload, totalRead, payloadSize - totalRead);
                        if (read < 0) {
                            throw new IOException(
                                    "Unexpected EOF while reading payload. Expected "
                                            + payloadSize
                                            + " bytes, got "
                                            + totalRead);
                        }
                        totalRead += read;
                    }

                    int storedCrc = in.readInt();

                    CRC32 crc32 = new CRC32();
                    crc32.update(payload);
                    int computedCrc = (int) crc32.getValue();

                    if (storedCrc != computedCrc) {
                        log.warn(
                                "CRC32 mismatch in NWL file: {}, stored={}, computed={}, skipping record",
                                hdfsPath,
                                storedCrc,
                                computedCrc);
                        continue;
                    }

                    WALRecord record = deserializeRecord(payload);
                    if (record != null) {
                        if (!predicate.test(record)) {
                            return false;
                        }
                    }
                } catch (java.io.EOFException e) {
                    break;
                } catch (Exception e) {
                    log.warn("Error reading record from NWL file: {}, skipping", hdfsPath, e);
                    break;
                }
            }
        }
        return true;
    }

    private boolean forEachNWLFileLocal(
            java.nio.file.Path localPath, RecordPredicate<WALRecord> predicate) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(localPath.toFile(), "r");
                var channel = raf.getChannel()) {

            ByteBuffer magicBuf = ByteBuffer.allocate(4);
            if (channel.read(magicBuf) < 4) {
                log.warn("Cannot read magic from NWL file: {}", localPath);
                return true;
            }
            magicBuf.flip();
            byte[] magic = new byte[4];
            magicBuf.get(magic);
            if (!java.util.Arrays.equals(magic, EXPECTED_MAGIC)) {
                log.warn("Invalid magic in NWL file: {}", localPath);
                return true;
            }

            ByteBuffer fixedBuf = ByteBuffer.allocate(8);
            int fixedRead = channel.read(fixedBuf);
            if (fixedRead < 8) {
                log.warn("Truncated header in NWL file: {}", localPath);
                return true;
            }
            fixedBuf.flip();
            int version = fixedBuf.getInt();
            int headerLength = fixedBuf.getInt();

            if (headerLength < 0 || headerLength > MAX_HEADER_LENGTH) {
                throw new WALRecoveryException(
                        String.format(
                                "Invalid NWL header length: %d in file %s",
                                headerLength, localPath));
            }
            if (headerLength > 0) {
                ByteBuffer headerBuf = ByteBuffer.allocate(headerLength);
                channel.read(headerBuf);
            }
            raf.readInt();

            while (true) {
                try {
                    ByteBuffer sizeBuf = ByteBuffer.allocate(4);
                    int read = channel.read(sizeBuf);
                    if (read < 4) {
                        break;
                    }
                    sizeBuf.flip();
                    int payloadSize = sizeBuf.getInt();
                    if (payloadSize <= 0 || payloadSize > MAX_PAYLOAD_SIZE) {
                        break;
                    }

                    ByteBuffer payloadBuf = ByteBuffer.allocate(payloadSize + 4);
                    read = channel.read(payloadBuf);
                    if (read < payloadSize + 4) {
                        break;
                    }
                    payloadBuf.flip();

                    byte[] payload = new byte[payloadSize];
                    payloadBuf.get(payload);
                    int storedCrc = payloadBuf.getInt();

                    CRC32 crc32 = new CRC32();
                    crc32.update(payload);
                    int computedCrc = (int) crc32.getValue();

                    if (storedCrc != computedCrc) {
                        log.warn("CRC32 mismatch in NWL file: {}, skipping record", localPath);
                        continue;
                    }

                    WALRecord record = deserializeRecord(payload);
                    if (record != null) {
                        if (!predicate.test(record)) {
                            return false;
                        }
                    }
                } catch (Exception e) {
                    log.warn("Error reading record from NWL file: {}, stopping", localPath, e);
                    break;
                }
            }
        }
        return true;
    }

    private boolean forEachParquetFileHdfs(
            Path parquetPath, FileSystem fs, RecordPredicate<WALRecord> predicate)
            throws IOException {
        try (var reader =
                AvroParquetReader.<GenericRecord>builder(parquetPath)
                        .withConf(hadoopConfig)
                        .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                WALRecord walRecord = parquetRecordToWALRecord(record);
                if (walRecord != null && !predicate.test(walRecord)) {
                    return false;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read Parquet WAL file: {}", parquetPath, e);
        }
        return true;
    }

    private boolean forEachParquetFileLocal(
            java.nio.file.Path localPath, RecordPredicate<WALRecord> predicate) throws IOException {
        try (var reader =
                AvroParquetReader.<GenericRecord>builder(new Path(localPath.toUri()))
                        .withConf(hadoopConfig)
                        .build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                WALRecord walRecord = parquetRecordToWALRecord(record);
                if (walRecord != null && !predicate.test(walRecord)) {
                    return false;
                }
            }
        } catch (Exception e) {
            log.warn("Failed to read Parquet WAL file: {}", localPath, e);
        }
        return true;
    }

    private WALRecord parquetRecordToWALRecord(GenericRecord record) {
        long sequence = (Long) record.get("sequence");
        long timestamp = (Long) record.get("timestamp");
        byte[] data = toBytes(record.get("data"));
        int dataLength = (Integer) record.get("dataLength");

        long commitId = -1L;
        String uniqTableIdentifier = "";
        String marker = (dataLength == 0) ? "SYNC_MARKER" : "DATA";

        return new DefaultWALRecord(
                sequence,
                dataLength > 0 ? data : new byte[0],
                timestamp,
                commitId,
                uniqTableIdentifier,
                marker);
    }

    private byte[] toBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof ByteBuffer byteBuffer) {
            ByteBuffer duplicate = byteBuffer.duplicate();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }
        if (value instanceof CharSequence sequence) {
            return sequence.toString().getBytes();
        }
        return value.toString().getBytes();
    }

    private List<WALRecord> filterUnflushed(List<WALRecord> allRecords) {
        long lastSyncSequence = -1;
        for (WALRecord record : allRecords) {
            if ("SYNC_MARKER".equals(record.marker())) {
                lastSyncSequence = record.sequence();
            }
        }

        if (lastSyncSequence == -1) {
            return allRecords.stream().filter(r -> !"SYNC_MARKER".equals(r.marker())).toList();
        }

        final long syncSeq = lastSyncSequence;
        return allRecords.stream()
                .filter(r -> r.sequence() > syncSeq)
                .filter(r -> !"SYNC_MARKER".equals(r.marker()))
                .toList();
    }

    private WALRecord deserializeRecord(byte[] payload) {
        return WALRecordCodec.deserializeEnvelope(payload);
    }

    private boolean isHdfsPath(String path) {
        return path != null && (path.startsWith("hdfs://") || path.startsWith("viewfs://"));
    }

    private int compareWALFiles(Path p1, Path p2) {
        long seq1 = extractSequenceNumber(p1.getName());
        long seq2 = extractSequenceNumber(p2.getName());
        return Long.compare(seq1, seq2);
    }

    private int compareLocalWALFiles(java.nio.file.Path p1, java.nio.file.Path p2) {
        long seq1 = extractSequenceNumber(p1.getFileName().toString());
        long seq2 = extractSequenceNumber(p2.getFileName().toString());
        return Long.compare(seq1, seq2);
    }

    private long extractSequenceNumber(String fileName) {
        Matcher nwlMatcher = NWL_FILE_PATTERN.matcher(fileName);
        if (nwlMatcher.matches()) {
            try {
                return Long.parseLong(nwlMatcher.group(2));
            } catch (NumberFormatException e) {
                log.warn("Cannot parse sequence number from WAL file: {}", fileName);
                return 0;
            }
        }
        Matcher parquetMatcher = PARQUET_FILE_PATTERN.matcher(fileName);
        if (parquetMatcher.matches()) {
            try {
                return Long.parseLong(parquetMatcher.group(1));
            } catch (NumberFormatException e) {
                log.warn("Cannot parse sequence number from Parquet WAL file: {}", fileName);
                return 0;
            }
        }
        log.warn("WAL file does not match expected naming pattern: {}", fileName);
        return 0;
    }

    void validateSequenceOrder(List<WALRecord> records) {
        long lastSequence = -1;
        for (WALRecord record : records) {
            long seq = record.sequence();
            if (seq >= 0 && seq < lastSequence) {
                throw new WALRecoveryException(
                        String.format(
                                "WAL sequence number not monotonically increasing: "
                                        + "expected >%d, got %d. This indicates corrupted WAL segments or out-of-order flush.",
                                lastSequence, seq));
            }
            if (seq >= 0) {
                lastSequence = seq;
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        log.debug("WALReader closed");
    }
}
