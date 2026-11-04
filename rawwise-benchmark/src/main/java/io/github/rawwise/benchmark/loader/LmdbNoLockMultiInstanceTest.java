package io.github.rawwise.benchmark.loader;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.lmdbjava.*;

public class LmdbNoLockMultiInstanceTest {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Test: lmdbjava MDB_NOLOCK multi-instance ===");

        int numInstances = 4;
        int rowsPerInstance = 50000;
        CountDownLatch latch = new CountDownLatch(numInstances);
        ExecutorService executor = Executors.newFixedThreadPool(numInstances);

        for (int inst = 0; inst < numInstances; inst++) {
            final int instId = inst;
            executor.submit(
                    () -> {
                        try {
                            String path = "/tmp/lmdb_nolock_multi_" + instId;
                            deleteDirectory(new File(path));
                            new File(path).mkdirs();

                            Env<ByteBuffer> env =
                                    Env.create()
                                            .setMapSize(256L * 1024 * 1024)
                                            .setMaxDbs(1)
                                            .setMaxReaders(126)
                                            .open(new File(path), EnvFlags.MDB_NOLOCK);

                            Dbi<ByteBuffer> dbi = env.openDbi("testdb", DbiFlags.MDB_CREATE);

                            // Write in batches
                            int batchSize = 1000;
                            for (int batch = 0; batch < rowsPerInstance / batchSize; batch++) {
                                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                                    for (int i = 0; i < batchSize; i++) {
                                        int rowId = batch * batchSize + i;
                                        byte[] keyBytes =
                                                ("part_" + rowId).getBytes(StandardCharsets.UTF_8);
                                        byte[] valBytes =
                                                ("value_data_for_part_key_"
                                                                + rowId
                                                                + "_with_padding_to_simulate_tpch_part_row_data_that_is_quite_long")
                                                        .getBytes(StandardCharsets.UTF_8);

                                        ByteBuffer keyBuf =
                                                ByteBuffer.allocateDirect(keyBytes.length);
                                        keyBuf.put(keyBytes).flip();
                                        ByteBuffer valBuf =
                                                ByteBuffer.allocateDirect(valBytes.length);
                                        valBuf.put(valBytes).flip();

                                        dbi.put(txn, keyBuf, valBuf);
                                    }
                                    txn.commit();
                                }
                            }

                            System.out.println(
                                    "Instance "
                                            + instId
                                            + ": committed "
                                            + rowsPerInstance
                                            + " entries");
                            env.close();
                        } catch (Exception e) {
                            System.err.println("Instance " + instId + " FAILED: " + e.getMessage());
                            e.printStackTrace();
                        } finally {
                            latch.countDown();
                        }
                    });
        }

        latch.await(60, TimeUnit.SECONDS);
        executor.shutdown();
        System.out.println("All instances done!");
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
