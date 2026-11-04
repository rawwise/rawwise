package io.github.rawwise.benchmark.loader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;

public class LmdbMmapTest {
    public static void main(String[] args) throws Exception {
        Path baseDir = Path.of(System.getProperty("lmdb.test.dir", "/tmp/lmdb-mmap-test"));
        Files.createDirectories(baseDir);

        long mapSize = args.length > 0 ? Long.parseLong(args[0]) * 1024 * 1024 : 4;
        int maxInstances = args.length > 1 ? Integer.parseInt(args[1]) : 50;

        System.out.println(
                "Testing LMDB mmap creation: mapSize="
                        + mapSize
                        + " bytes, maxInstances="
                        + maxInstances);
        System.out.println(
                "JVM max memory: " + Runtime.getRuntime().maxMemory() / (1024 * 1024) + "MB");

        // First test: Java NIO mmap
        System.out.println("\n--- Test 1: Java NIO MappedByteBuffer ---");
        testNioMmap(baseDir, mapSize);

        // Second test: LMDB
        System.out.println("\n--- Test 2: LMDB Env.open() ---");
        testLmdb(baseDir, mapSize, maxInstances);
    }

    private static void testNioMmap(Path baseDir, long mapSize) {
        Path testFile = baseDir.resolve("nio-test.bin");
        try {
            RandomAccessFile raf = new RandomAccessFile(testFile.toFile(), "rw");
            raf.setLength(mapSize);
            FileChannel channel = raf.getChannel();
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, mapSize);
            mbb.put(0, (byte) 1);
            System.out.println("NIO mmap SUCCESS: size=" + mapSize + " bytes");
            channel.close();
            raf.close();
            Files.deleteIfExists(testFile);
        } catch (IOException e) {
            System.out.println("NIO mmap FAILED: " + e.getMessage());
        }
    }

    private static void testLmdb(Path baseDir, long mapSize, int maxInstances) {
        int created = 0;
        Env<?>[] envs = new Env[maxInstances];

        for (int i = 0; i < maxInstances; i++) {
            Path instanceDir = baseDir.resolve("inst-" + i);
            try {
                Files.createDirectories(instanceDir);
            } catch (IOException e) {
                System.out.println("FAILED to create dir at instance " + i + ": " + e.getMessage());
                break;
            }
            try {
                Env<ByteBuffer> env =
                        Env.create()
                                .setMapSize(mapSize)
                                .setMaxDbs(1)
                                .setMaxReaders(4)
                                .open(
                                        instanceDir.toFile(),
                                        EnvFlags.MDB_NORDAHEAD,
                                        EnvFlags.MDB_NOMEMINIT);
                Dbi<ByteBuffer> dbi = env.openDbi("default", DbiFlags.MDB_CREATE);
                envs[i] = env;
                created++;
                if (i % 5 == 0) {
                    System.out.println("Created " + (i + 1) + " instances so far...");
                }
            } catch (Exception e) {
                System.out.println("FAILED at instance " + i + ": " + e.getMessage());
                e.printStackTrace(System.out);
                break;
            }
        }

        System.out.println(
                "Successfully created " + created + " LMDB instances with mapSize=" + mapSize);

        for (int i = 0; i < created; i++) {
            if (envs[i] != null) {
                envs[i].close();
            }
        }
        System.out.println("All closed.");
    }
}
