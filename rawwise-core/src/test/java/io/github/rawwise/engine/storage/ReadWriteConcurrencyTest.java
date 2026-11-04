package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.engine.storage.codec.NullableValueCodec;
import io.github.rawwise.engine.storage.spi.LocalStorageProvider;
import io.github.rawwise.engine.storage.spi.StorageRegistry;
import io.github.rawwise.engine.storage.table.TableStore;
import io.github.rawwise.engine.storage.table.key.KeyCodec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("readwriteconcurrenttest")
class ReadWriteConcurrencyTest {

    @BeforeAll
    static void registerStorageProvider() {
        try {
            StorageRegistry.instance().register(new LocalStorageProvider());
        } catch (Exception e) {
            // Already registered
        }
    }

    @Test
    @DisplayName("after writingconcurrentread, data noexception")
    void testSequentialWriteConcurrentRead(@TempDir Path tempDir) throws Exception {
        String tableIdentifier =
                "catalog.db.conc_test_" + UUID.randomUUID().toString().substring(0, 8);
        DynamicMmapStore store =
                DynamicMmapStore.getInstance(tempDir.toString(), "conc-store-" + UUID.randomUUID());
        try {
            int recordCount = 20;
            for (int i = 0; i < recordCount; i++) {
                Map<String, Comparable> pk = Map.of("id", "seq-key-" + i);
                StorageTestHelper.writeKv(store, tableIdentifier, pk, "value-" + i);
            }

            int readerCount = 5;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(readerCount);
            AtomicInteger readSuccess = new AtomicInteger(0);
            AtomicInteger readErrors = new AtomicInteger(0);

            ExecutorService executor = Executors.newFixedThreadPool(readerCount);
            for (int i = 0; i < readerCount; i++) {
                executor.submit(
                        () -> {
                            try {
                                startLatch.await();
                                for (int j = 0; j < recordCount; j++) {
                                    try {
                                        TableStore ts = store.tableStore(tableIdentifier);
                                        KeyCodec keyCodec = ts.getKeyCodec();
                                        byte[] encodedKey =
                                                keyCodec.encodePrimaryKey(
                                                        Map.of("id", "seq-key-" + j));
                                        byte[] rawValue = ts.getRawValue(encodedKey);
                                        if (rawValue != null) {
                                            String decoded =
                                                    new String(
                                                            NullableValueCodec.decode(rawValue),
                                                            StandardCharsets.UTF_8);
                                            assertThat(decoded).isEqualTo("value-" + j);
                                            readSuccess.incrementAndGet();
                                        }
                                    } catch (AssertionError e) {
                                        throw e;
                                    } catch (Exception e) {
                                        readErrors.incrementAndGet();
                                    }
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            } finally {
                                doneLatch.countDown();
                            }
                        });
            }

            startLatch.countDown();
            boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
            executor.shutdown();

            assertThat(completed).isTrue();
            assertThat(readErrors.get()).isEqualTo(0);
            assertThat(readSuccess.get()).isGreaterThan(0);
        } finally {
            store.close();
        }
    }
}
