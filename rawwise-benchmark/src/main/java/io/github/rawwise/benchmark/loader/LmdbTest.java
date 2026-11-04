package io.github.rawwise.benchmark.loader;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;

public class LmdbTest {
    public static void main(String[] args) throws Exception {
        String warehousePath = args.length > 0 ? args[0] : "/tmp/rawwise/warehouse";

        Path warehouse = Path.of(warehousePath);
        if (!Files.isDirectory(warehouse)) {
            System.err.println("Warehouse directory not found: " + warehousePath);
            return;
        }

        try (Stream<Path> dirs = Files.list(warehouse)) {
            dirs.filter(Files::isDirectory)
                    .forEach(
                            tableDir -> {
                                String tableName = tableDir.getFileName().toString();
                                try (Stream<Path> instanceDirs = Files.list(tableDir)) {
                                    instanceDirs
                                            .filter(Files::isDirectory)
                                            .filter(
                                                    d ->
                                                            d.getFileName()
                                                                    .toString()
                                                                    .startsWith("inst-"))
                                            .forEach(
                                                    instanceDir -> {
                                                        try {
                                                            Env<ByteBuffer> env =
                                                                    Env.create()
                                                                            .setMapSize(
                                                                                    1L * 1024 * 1024
                                                                                            * 1024)
                                                                            .setMaxDbs(1)
                                                                            .open(
                                                                                    instanceDir
                                                                                            .toFile(),
                                                                                    EnvFlags
                                                                                            .MDB_RDONLY_ENV);
                                                            Dbi<ByteBuffer> dbi =
                                                                    env.openDbi("default");
                                                            try (Txn<ByteBuffer> txn =
                                                                    env.txnRead()) {
                                                                Stat stat = dbi.stat(txn);
                                                                System.out.printf(
                                                                        "  %-40s entries=%,d  size=%dMB%n",
                                                                        instanceDir.getFileName(),
                                                                        stat.entries,
                                                                        Files.walk(instanceDir)
                                                                                        .filter(
                                                                                                Files
                                                                                                        ::isRegularFile)
                                                                                        .mapToLong(
                                                                                                p ->
                                                                                                        p.toFile()
                                                                                                                .length())
                                                                                        .sum()
                                                                                / (1024 * 1024));
                                                            }
                                                            env.close();
                                                        } catch (Exception e) {
                                                            System.err.println(
                                                                    "  Error reading "
                                                                            + instanceDir
                                                                            + ": "
                                                                            + e.getMessage());
                                                        }
                                                    });
                                } catch (Exception e) {
                                    System.err.println(
                                            "Error listing " + tableDir + ": " + e.getMessage());
                                }
                            });
        }
    }
}
