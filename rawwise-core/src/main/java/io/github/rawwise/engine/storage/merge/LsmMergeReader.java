package io.github.rawwise.engine.storage.merge;

import io.github.rawwise.engine.storage.block.DataBlock;
import io.github.rawwise.engine.storage.block.read.DataBlockReader;
import io.github.rawwise.model.op.Find;
import io.github.rawwise.model.op.Scan;
import io.github.rawwise.model.record.Record;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LsmMergeReader implements DataBlockReader {

    private final List<DataBlockReader> readers;

    public LsmMergeReader(List<DataBlockReader> readers) {
        this.readers = List.copyOf(readers);
    }

    @Override
    public boolean supports(DataBlock block) {
        return readers.stream().anyMatch(r -> r.supports(block));
    }

    @Override
    public void close() throws IOException {
        IOException first = null;
        for (DataBlockReader reader : readers) {
            try {
                reader.close();
            } catch (IOException e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        if (first != null) throw first;
    }

    @Override
    public boolean isClosed() {
        return readers.stream().allMatch(DataBlockReader::isClosed);
    }

    @Override
    public Find.Result find(Find find, List<DataBlock> blocks) throws IOException {
        long bestSeq = Long.MIN_VALUE;
        Find.Result bestResult = null;
        boolean bestIsDelete = false;
        Find.Result firstError = null;
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed = filterSupported(reader, blocks);
            if (routed.isEmpty()) continue;
            Find.Result result = reader.find(find, routed);
            if (!result.success() && !result.isFound()) {
                if (firstError == null) firstError = result;
                continue;
            }
            if (result.isFound() && result.foundRecord().isPresent()) {
                Record record = result.foundRecord().get();
                long seq = record.header().sequenceNumber();
                if (seq > bestSeq) {
                    bestSeq = seq;
                    bestResult = result;
                    bestIsDelete = record.header().kind() == Record.Kind.DELETE;
                }
            }
        }
        if (bestIsDelete) {
            return Find.Result.notFound();
        }
        if (bestResult != null) return bestResult;
        if (firstError != null) return firstError;
        return Find.Result.notFound();
    }

    @Override
    public Scan.Result scan(Scan scan, List<DataBlock> blocks) throws IOException {
        List<Record> rows = new ArrayList<>();
        for (DataBlockReader reader : readers) {
            List<DataBlock> routed = filterSupported(reader, blocks);
            if (routed.isEmpty()) continue;
            Scan.Result result = reader.scan(scan, routed);
            if (!result.success()) return result;
            for (Record r : result.data()) {
                if (r.header().kind() != Record.Kind.DELETE) {
                    rows.add(r);
                }
            }
        }
        return Scan.Result.ok(rows, false);
    }

    private List<DataBlock> filterSupported(DataBlockReader reader, List<DataBlock> blocks) {
        List<DataBlock> result = new ArrayList<>();
        for (DataBlock block : blocks) {
            if (reader.supports(block)) {
                result.add(block);
            }
        }
        return result;
    }
}
