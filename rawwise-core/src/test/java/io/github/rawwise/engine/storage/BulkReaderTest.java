package io.github.rawwise.engine.storage;

import static org.assertj.core.api.Assertions.*;

import io.github.rawwise.engine.storage.reader.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BulkReaderTest {

    static class SimpleBulkReader extends AbstractReader implements BulkReader<String> {
        private final List<String> data;
        private int index = 0;

        SimpleBulkReader(List<String> data) {
            this.data = data;
        }

        @Override
        public void forEach(RecordPredicate<String> predicate) throws IOException {
            ensureOpen();
            for (String record : data) {
                if (!predicate.test(record)) {
                    break;
                }
            }
        }

        @Override
        public long estimatedSize() {
            return data.size();
        }
    }

    @Test
    void bulkReaderExtendsReader() {
        assertThat(Reader.class).isAssignableFrom(BulkReader.class);
    }

    @Test
    void forEachIteratesAllWhenPredicateAlwaysTrue() throws IOException {
        List<String> visited = new ArrayList<>();
        SimpleBulkReader reader = new SimpleBulkReader(List.of("a", "b", "c"));
        reader.forEach(
                s -> {
                    visited.add(s);
                    return true;
                });
        assertThat(visited).containsExactly("a", "b", "c");
    }

    @Test
    void forEachStopsWhenPredicateReturnsFalse() throws IOException {
        List<String> visited = new ArrayList<>();
        SimpleBulkReader reader = new SimpleBulkReader(List.of("a", "b", "c"));
        reader.forEach(
                s -> {
                    visited.add(s);
                    return !s.equals("b");
                });
        assertThat(visited).containsExactly("a", "b");
    }

    @Test
    void readBatchReturnsUpToMaxRecords() throws IOException {
        SimpleBulkReader reader = new SimpleBulkReader(List.of("a", "b", "c", "d", "e"));
        List<String> batch = reader.readBatch(3);
        assertThat(batch).containsExactly("a", "b", "c");
    }

    @Test
    void readBatchReturnsAllWhenFewerThanMax() throws IOException {
        SimpleBulkReader reader = new SimpleBulkReader(List.of("a", "b"));
        List<String> batch = reader.readBatch(10);
        assertThat(batch).containsExactly("a", "b");
    }

    @Test
    void readBatchThrowsOnZeroOrNegative() throws IOException {
        SimpleBulkReader reader = new SimpleBulkReader(List.of("a"));
        assertThatThrownBy(() -> reader.readBatch(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> reader.readBatch(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void readAllReturnsAllRecords() throws IOException {
        SimpleBulkReader reader = new SimpleBulkReader(List.of("a", "b", "c"));
        List<String> all = reader.readAll();
        assertThat(all).containsExactly("a", "b", "c");
    }

    @Test
    void estimatedSizeReturnsRecordCount() throws IOException {
        SimpleBulkReader reader = new SimpleBulkReader(List.of("a", "b", "c"));
        assertThat(reader.estimatedSize()).isEqualTo(3);
    }

    @Test
    void recordPredicateCanThrowIOException() {
        BulkReader.RecordPredicate<String> throwingPredicate =
                s -> {
                    throw new IOException("test IO error");
                };
        assertThatThrownBy(() -> throwingPredicate.test("x")).isInstanceOf(IOException.class);
    }

    @Test
    void forEachOnClosedReaderThrows() throws IOException {
        SimpleBulkReader reader = new SimpleBulkReader(List.of("a"));
        reader.close();
        assertThatThrownBy(() -> reader.forEach(s -> true))
                .isInstanceOf(IllegalStateException.class);
    }
}
