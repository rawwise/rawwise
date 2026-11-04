package io.github.rawwise.metastore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.rawwise.common.collection.iterator.CloseableIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public abstract class MetastoreContractTest {

    protected abstract Metastore createStore() throws IOException;

    @Test
    void shouldPutAndGet() throws IOException {
        try (Metastore store = createStore()) {
            byte[] key = "test-key".getBytes();
            byte[] value = "test-value".getBytes();
            store.put(key, value);
            Optional<byte[]> result = store.get(key);
            assertThat(result).hasValueSatisfying(v -> assertThat(v).isEqualTo(value));
        }
    }

    @Test
    void shouldReturnEmptyForMissingKey() throws IOException {
        try (Metastore store = createStore()) {
            Optional<byte[]> result = store.get("missing".getBytes());
            assertThat(result).isEmpty();
        }
    }

    @Test
    void shouldDelete() throws IOException {
        try (Metastore store = createStore()) {
            byte[] key = "del-key".getBytes();
            store.put(key, "val".getBytes());
            store.delete(key);
            assertThat(store.exists(key)).isFalse();
        }
    }

    @Test
    void shouldExist() throws IOException {
        try (Metastore store = createStore()) {
            byte[] key = "exist-key".getBytes();
            assertThat(store.exists(key)).isFalse();
            store.put(key, "val".getBytes());
            assertThat(store.exists(key)).isTrue();
        }
    }

    @Test
    void shouldScanByPrefix() throws IOException {
        try (Metastore store = createStore()) {
            store.put("prefix:a".getBytes(), "va".getBytes());
            store.put("prefix:b".getBytes(), "vb".getBytes());
            store.put("other:c".getBytes(), "vc".getBytes());
            List<KeyValue> results = store.scanAll("prefix:".getBytes());
            assertThat(results).hasSize(2);
            assertThat(results)
                    .extracting(kv -> new String(kv.key()))
                    .containsExactlyInAnyOrder("prefix:a", "prefix:b");
            assertThat(results)
                    .extracting(kv -> new String(kv.value()))
                    .containsExactlyInAnyOrder("va", "vb");
        }
    }

    @Test
    void shouldScanWithLimit() throws IOException {
        try (Metastore store = createStore()) {
            store.put("limit:a".getBytes(), "va".getBytes());
            store.put("limit:b".getBytes(), "vb".getBytes());
            store.put("limit:c".getBytes(), "vc".getBytes());
            try (CloseableIterator<KeyValue> it = store.scan("limit:".getBytes(), 2)) {
                List<KeyValue> results = new ArrayList<>();
                while (it.hasNext()) results.add(it.next());
                assertThat(results).hasSize(2);
            }
        }
    }

    @Test
    void shouldScanRange() throws IOException {
        try (Metastore store = createStore()) {
            store.put("range:a".getBytes(), "va".getBytes());
            store.put("range:b".getBytes(), "vb".getBytes());
            store.put("range:c".getBytes(), "vc".getBytes());
            store.put("range:d".getBytes(), "vd".getBytes());
            List<KeyValue> results = store.scanRangeAll("range:b".getBytes(), "range:d".getBytes());
            assertThat(results).hasSize(2);
            assertThat(results)
                    .extracting(kv -> new String(kv.key()))
                    .containsExactly("range:b", "range:c");
        }
    }

    @Test
    void shouldScanRangeWithLimit() throws IOException {
        try (Metastore store = createStore()) {
            store.put("rl:a".getBytes(), "va".getBytes());
            store.put("rl:b".getBytes(), "vb".getBytes());
            store.put("rl:c".getBytes(), "vc".getBytes());
            try (CloseableIterator<KeyValue> it =
                    store.scanRange("rl:a".getBytes(), "rl:d".getBytes(), 2)) {
                List<KeyValue> results = new ArrayList<>();
                while (it.hasNext()) results.add(it.next());
                assertThat(results).hasSize(2);
            }
        }
    }

    @Test
    void shouldReturnFalseWhenPreconditionNotMet() throws IOException {
        try (Metastore store = createStore()) {
            byte[] key = "cas-key".getBytes();
            boolean result =
                    store.compareAndSwap(
                            List.of(new Precondition.ValueEquals(key, "wrong".getBytes())),
                            List.of(new Mutation.Put(key, "new-val".getBytes())));
            assertThat(result).isFalse();
            assertThat(store.exists(key)).isFalse();
        }
    }

    @Test
    void shouldCommitCasWhenPreconditionMet() throws IOException {
        try (Metastore store = createStore()) {
            byte[] key = "cas-key2".getBytes();
            store.put(key, "expected".getBytes());
            boolean result =
                    store.compareAndSwap(
                            List.of(new Precondition.ValueEquals(key, "expected".getBytes())),
                            List.of(new Mutation.Put(key, "updated".getBytes())));
            assertThat(result).isTrue();
            assertThat(store.get(key))
                    .hasValueSatisfying(v -> assertThat(v).isEqualTo("updated".getBytes()));
        }
    }

    @Test
    void shouldHandleNotExistsPrecondition() throws IOException {
        try (Metastore store = createStore()) {
            byte[] key = "not-exists-key".getBytes();
            boolean result =
                    store.compareAndSwap(
                            List.of(new Precondition.NotExists(key)),
                            List.of(new Mutation.Put(key, "created".getBytes())));
            assertThat(result).isTrue();
            assertThat(store.get(key))
                    .hasValueSatisfying(v -> assertThat(v).isEqualTo("created".getBytes()));
        }
    }

    @Test
    void shouldFailNotExistsWhenKeyExists() throws IOException {
        try (Metastore store = createStore()) {
            byte[] key = "already-exists".getBytes();
            store.put(key, "existing".getBytes());
            boolean result =
                    store.compareAndSwap(
                            List.of(new Precondition.NotExists(key)),
                            List.of(new Mutation.Put(key, "never-written".getBytes())));
            assertThat(result).isFalse();
        }
    }

    @Test
    void shouldHandleExistsPrecondition() throws IOException {
        try (Metastore store = createStore()) {
            byte[] key = "exists-precond-key".getBytes();
            boolean result =
                    store.compareAndSwap(
                            List.of(new Precondition.Exists(key)),
                            List.of(new Mutation.Put(key, "written".getBytes())));
            assertThat(result).isFalse();
            store.put(key, "existing".getBytes());
            result =
                    store.compareAndSwap(
                            List.of(new Precondition.Exists(key)),
                            List.of(new Mutation.Put(key, "updated".getBytes())));
            assertThat(result).isTrue();
            assertThat(store.get(key))
                    .hasValueSatisfying(v -> assertThat(v).isEqualTo("updated".getBytes()));
        }
    }

    @Test
    void shouldSupportCrossKeyCas() throws IOException {
        try (Metastore store = createStore()) {
            byte[] keyA = "xcas:a".getBytes();
            byte[] keyB = "xcas:b".getBytes();
            store.put(keyA, "va".getBytes());
            boolean result =
                    store.compareAndSwap(
                            List.of(
                                    new Precondition.ValueEquals(keyA, "va".getBytes()),
                                    new Precondition.NotExists(keyB)),
                            List.of(
                                    new Mutation.Put(keyA, "va-updated".getBytes()),
                                    new Mutation.Put(keyB, "vb".getBytes())));
            assertThat(result).isTrue();
            assertThat(store.get(keyA))
                    .hasValueSatisfying(v -> assertThat(v).isEqualTo("va-updated".getBytes()));
            assertThat(store.get(keyB))
                    .hasValueSatisfying(v -> assertThat(v).isEqualTo("vb".getBytes()));
        }
    }

    @Test
    void shouldBatchWrite() throws IOException {
        try (Metastore store = createStore()) {
            store.batchWrite(
                    List.of(
                            new Mutation.Put("b1".getBytes(), "v1".getBytes()),
                            new Mutation.Put("b2".getBytes(), "v2".getBytes()),
                            new Mutation.Delete("b3".getBytes())));
            assertThat(store.get("b1".getBytes()))
                    .hasValueSatisfying(v -> assertThat(v).isEqualTo("v1".getBytes()));
            assertThat(store.get("b2".getBytes()))
                    .hasValueSatisfying(v -> assertThat(v).isEqualTo("v2".getBytes()));
            assertThat(store.exists("b3".getBytes())).isFalse();
        }
    }

    @Test
    void shouldDeleteViaBatchWrite() throws IOException {
        try (Metastore store = createStore()) {
            store.put("bd-key".getBytes(), "bd-val".getBytes());
            store.batchWrite(List.of(new Mutation.Delete("bd-key".getBytes())));
            assertThat(store.exists("bd-key".getBytes())).isFalse();
        }
    }

    @Test
    void shouldThrowOnIncrementAndGetByDefault() throws IOException {
        try (Metastore store = createStore()) {
            assertThatCode(() -> store.incrementAndGet("counter".getBytes(), 1L))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void shouldDeclareMaxCasBatchSize() throws IOException {
        try (Metastore store = createStore()) {
            int maxBatch = store.maxCasBatchSize();
            assertThat(maxBatch == -1 || maxBatch > 0).isTrue();
        }
    }

    @Test
    void shouldDeclareSupportsCrossKeyCas() throws IOException {
        try (Metastore store = createStore()) {
            assertThat(store.supportsCrossKeyCas()).isNotNull();
        }
    }
}
