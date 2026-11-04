package io.github.rawwise.backend.lazyupdate;

import io.github.rawwise.backend.read.ReadPruning.KeyPruningSpec;
import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.KeyRange;
import io.github.rawwise.model.expression.ColumnRef;
import io.github.rawwise.model.expression.Eq;
import io.github.rawwise.model.expression.Expression;
import io.github.rawwise.model.expression.Literal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FilePruner {

    private FilePruner() {}

    public static boolean predicateOverlapsFile(
            UpdateLogEntry entry, DataFile dataFile, String keyColumn) {
        Expression predicate = entry.predicate();
        if (predicate == null) {
            return true;
        }

        KeyRange fileRange = dataFile.keyRange();
        if (fileRange == null) {
            return true;
        }

        byte[] extractedKey = extractKeyFromPredicate(predicate, keyColumn);
        if (extractedKey == null || extractedKey.length == 0) {
            return true;
        }

        boolean contained = fileRange.contains(extractedKey);
        if (!contained) {
            log.debug(
                    "FilePruner: skipping file={}, extractedKey not in keyRange",
                    dataFile.filePath());
        }
        return contained;
    }

    public static boolean predicateOverlapsFile(
            UpdateLogEntry entry, DataFile dataFile, KeyPruningSpec spec) {
        if (entry.predicate() == null) {
            return true;
        }
        if (spec == null || spec.fullScan()) {
            return true;
        }
        if (spec.noMatch()) {
            return false;
        }
        return fileOverlapsSpec(dataFile, spec);
    }

    public static List<DataFile> pruneFiles(
            List<UpdateLogEntry> logs, List<DataFile> files, String keyColumn) {
        if (logs == null || logs.isEmpty() || files == null || files.isEmpty()) {
            return files != null ? files : List.of();
        }
        if (keyColumn == null) {
            return pruneFiles(logs, files, (KeyPruningSpec) null);
        }
        List<byte[]> pointKeys = new ArrayList<>();
        for (UpdateLogEntry entry : logs) {
            if (entry.predicate() == null) {
                return pruneFiles(logs, files, (KeyPruningSpec) null);
            }
            byte[] key = extractKeyFromPredicate(entry.predicate(), keyColumn);
            if (key == null) {
                return pruneFiles(logs, files, (KeyPruningSpec) null);
            }
            pointKeys.add(key);
        }
        KeyPruningSpec spec = pointKeys.isEmpty() ? null : KeyPruningSpec.of(null, pointKeys);
        return pruneFiles(logs, files, spec);
    }

    public static List<DataFile> pruneFiles(
            List<UpdateLogEntry> logs, List<DataFile> files, KeyPruningSpec spec) {
        if (logs == null || logs.isEmpty() || files == null || files.isEmpty()) {
            return files != null ? files : List.of();
        }

        for (UpdateLogEntry entry : logs) {
            if (entry.predicate() == null) {
                log.debug(
                        "FilePruner: null-predicate entry found, keeping all {} files",
                        files.size());
                return new ArrayList<>(files);
            }
        }

        if (spec == null || spec.fullScan()) {
            return new ArrayList<>(files);
        }

        if (spec.noMatch()) {
            return new ArrayList<>();
        }

        List<DataFile> kept = new ArrayList<>();
        for (DataFile file : files) {
            if (fileOverlapsSpec(file, spec)) {
                kept.add(file);
            }
        }

        int prunedCount = files.size() - kept.size();
        if (prunedCount > 0) {
            log.info(
                    "FilePruner: pruned {}/{} files via KeyPruningSpec", prunedCount, files.size());
        }
        return kept;
    }

    private static boolean fileOverlapsSpec(DataFile file, KeyPruningSpec spec) {
        KeyRange fileRange = file.keyRange();
        if (fileRange == null) {
            return true;
        }
        List<byte[]> points = spec.pointKeys();
        if (points != null && !points.isEmpty()) {
            for (byte[] pk : points) {
                if (fileRange.contains(pk)) {
                    return true;
                }
            }
            return false;
        }
        KeyRange queryRange = spec.keyRange();
        if (queryRange == null) {
            return true;
        }
        return fileRange.overlaps(queryRange);
    }

    static byte[] extractKeyFromPredicate(Expression predicate, String keyColumn) {
        if (predicate instanceof Eq eq) {
            if (eq.getLeft() instanceof ColumnRef col && eq.getRight() instanceof Literal lit) {
                if (keyColumn == null) {
                    return null;
                }
                if (!keyColumn.equals(col.getColumnName())) {
                    return null;
                }
                Comparable<?> value = lit.getValue();
                if (value != null) {
                    return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
                }
            }
        }
        return null;
    }
}
