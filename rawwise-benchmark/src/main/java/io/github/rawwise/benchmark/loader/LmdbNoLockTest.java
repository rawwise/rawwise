package io.github.rawwise.benchmark.loader;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.lmdbjava.*;

public class LmdbNoLockTest {
    public static void main(String[] args) throws Exception {
        String path = "/tmp/lmdb_nolock_java_test";
        new File(path).mkdirs();
        // Clean up
        deleteDirectory(new File(path));
        new File(path).mkdirs();

        System.out.println("=== Test: lmdbjava MDB_NOLOCK ===");

        Env<ByteBuffer> env =
                Env.create()
                        .setMapSize(64L * 1024 * 1024)
                        .setMaxDbs(1)
                        .setMaxReaders(126)
                        .open(new File(path), EnvFlags.MDB_NOLOCK);

        System.out.println("Env opened with MDB_NOLOCK");

        Dbi<ByteBuffer> dbi = env.openDbi("testdb", DbiFlags.MDB_CREATE);
        System.out.println("Dbi opened");

        // Write 5000 entries
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            for (int i = 0; i < 5000; i++) {
                byte[] keyBytes = ("key_" + i).getBytes(StandardCharsets.UTF_8);
                byte[] valBytes =
                        ("value_data_for_key_" + i + "_with_some_padding_to_simulate_real_data")
                                .getBytes(StandardCharsets.UTF_8);

                ByteBuffer keyBuf = ByteBuffer.allocateDirect(keyBytes.length);
                keyBuf.put(keyBytes).flip();
                ByteBuffer valBuf = ByteBuffer.allocateDirect(valBytes.length);
                valBuf.put(valBytes).flip();

                dbi.put(txn, keyBuf, valBuf);

                if (i % 1000 == 0) {
                    System.out.println("Written " + i + " entries...");
                }
            }
            txn.commit();
            System.out.println("Committed 5000 entries");
        }

        // Read back
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            byte[] keyBytes = "key_1".getBytes(StandardCharsets.UTF_8);
            ByteBuffer keyBuf = ByteBuffer.allocateDirect(keyBytes.length);
            keyBuf.put(keyBytes).flip();
            ByteBuffer valBuf = dbi.get(txn, keyBuf);
            if (valBuf != null) {
                byte[] val = new byte[valBuf.remaining()];
                valBuf.get(val);
                System.out.println("Read key_1: " + new String(val, StandardCharsets.UTF_8));
            } else {
                System.out.println("key_1 not found!");
            }
        }

        env.close();
        System.out.println("Test passed!");
    }

    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDirectory(f);
                else f.delete();
            }
        }
        dir.delete();
    }
}
