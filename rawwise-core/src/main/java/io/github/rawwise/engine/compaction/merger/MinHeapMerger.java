package io.github.rawwise.engine.compaction.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MinHeapMerger implements MultiWayMerger {

    @Override
    public MergeResult merge(
            List<SortedFileReader> sources, MergeFunction mergeFn, OutputWriter output)
            throws IOException {

        PriorityQueue<SortedFileReader> heap = new PriorityQueue<>();
        try {
            for (SortedFileReader reader : sources) {
                if (reader.hasNext()) {
                    reader.advance();
                    heap.offer(reader);
                }
            }

            while (!heap.isEmpty()) {
                SortedFileReader min = heap.poll();
                byte[] currentKey = min.cachedKey();

                List<VersionedValue> values = new ArrayList<>();
                values.add(min.currentVersionedValue());

                while (!heap.isEmpty() && Arrays.equals(heap.peek().cachedKey(), currentKey)) {
                    SortedFileReader same = heap.poll();
                    values.add(same.currentVersionedValue());
                    advanceAndOffer(same, heap);
                }

                byte[] merged = mergeFn.merge(currentKey, values);
                if (merged != null) {
                    long maxSeq =
                            values.stream()
                                    .mapToLong(VersionedValue::sequenceNumber)
                                    .max()
                                    .orElse(0);
                    long maxCommitTs =
                            values.stream()
                                    .filter(v -> v.sequenceNumber() == maxSeq)
                                    .mapToLong(VersionedValue::commitTimestamp)
                                    .findFirst()
                                    .orElse(0);
                    output.write(currentKey, merged, maxSeq, maxCommitTs, false);
                }

                advanceAndOffer(min, heap);
            }

            return new MergeResult(output.writtenRows());
        } finally {
            for (SortedFileReader reader : sources) {
                try {
                    reader.close();
                } catch (Exception e) {
                    log.warn("Failed to close reader", e);
                }
            }
        }
    }

    private void advanceAndOffer(SortedFileReader reader, PriorityQueue<SortedFileReader> heap)
            throws IOException {
        if (reader.hasNext()) {
            reader.advance();
            heap.offer(reader);
        }
    }
}
