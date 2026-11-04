package io.github.rawwise.engine.txn.wal;

import io.github.rawwise.backend.wal.NwlWALReader;
import io.github.rawwise.backend.wal.TxnWALRecord;
import io.github.rawwise.backend.wal.TxnWALRecordCodec;
import io.github.rawwise.backend.wal.WALRecord;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class NwlBackedFastPathWalReader implements FastPathWalReader {

    private final String walPath;
    private final Configuration hadoopConfig;

    public NwlBackedFastPathWalReader(String walPath) {
        this(walPath, new Configuration());
    }

    public NwlBackedFastPathWalReader(String walPath, Configuration hadoopConfig) {
        this.walPath = walPath;
        this.hadoopConfig = hadoopConfig;
    }

    @Override
    public List<FastPathReplayEntry> scanReplayEntries() {
        List<FastPathReplayEntry> entries = new ArrayList<>();

        NwlWALReader reader = new NwlWALReader(walPath, hadoopConfig);
        List<WALRecord> allRecords;
        try {
            allRecords = reader.readAll();
        } catch (Exception e) {
            log.warn("Failed to read FastPath WAL from {}: {}", walPath, e.getMessage());
            return entries;
        } finally {
            try {
                reader.close();
            } catch (Exception ignored) {
            }
        }

        for (WALRecord walRecord : allRecords) {
            try {
                byte[] data = walRecord.value();
                if (data == null || data.length == 0) continue;
                TxnWALRecord txnRecord = TxnWALRecordCodec.decode(data);
                if (txnRecord.type() != TxnWALRecord.Type.FASTPATH_WRITE) continue;

                entries.add(
                        new FastPathReplayEntry(
                                txnRecord.commitTs(),
                                txnRecord.tableId(),
                                txnRecord.key(),
                                txnRecord.value(),
                                txnRecord.writeType()));
            } catch (Exception e) {
                log.debug("Skipping non-FastPath or undecodable WAL record: {}", e.getMessage());
            }
        }

        log.info("FastPath WAL scan complete: {} replay entries from {}", entries.size(), walPath);
        return entries;
    }
}
