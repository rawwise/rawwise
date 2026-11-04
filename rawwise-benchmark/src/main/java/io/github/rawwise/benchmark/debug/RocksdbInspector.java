package io.github.rawwise.benchmark.debug;

import java.nio.charset.StandardCharsets;
import org.rocksdb.*;

public class RocksdbInspector {
    public static void main(String[] args) throws Exception {
        String dbPath = args.length > 0 ? args[0] : "/tmp/rawwise/metastore";
        RocksDB.loadLibrary();
        try (var db = RocksDB.openReadOnly(dbPath)) {
            System.out.println("=== Keys in " + dbPath + " ===");
            try (var it = db.newIterator()) {
                for (it.seekToFirst(); it.isValid(); it.next()) {
                    byte[] key = it.key();
                    String keyStr = new String(key, StandardCharsets.UTF_8);
                    System.out.println("  " + keyStr);
                }
            }
            System.out.println("=== Done ===");
        } catch (RocksDBException e) {
            if (e.getMessage().contains("LOCK")) {
                System.out.println("LOCK conflict - database is in use by another process");
            } else {
                System.out.println("Error: " + e.getMessage());
            }
        }
    }
}
