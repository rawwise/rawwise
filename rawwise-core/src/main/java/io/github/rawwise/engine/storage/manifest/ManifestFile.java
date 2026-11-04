package io.github.rawwise.engine.storage.manifest;

import io.github.rawwise.engine.storage.DataFile;
import io.github.rawwise.engine.storage.LogFile;
import io.github.rawwise.engine.storage.RawWiseFile;
import java.util.Arrays;
import java.util.List;

public record ManifestFile(
        String manifestId,
        RawWiseFile.FileFormat format,
        long fileSizeBytes,
        long createdAtMillis,
        long snapshotId,
        List<DataFile> dataFiles,
        List<LogFile> logFiles,
        long minSequenceNumber,
        long maxSequenceNumber,
        byte[] bloomFilter)
        implements RawWiseFile {

    public ManifestFile {
        dataFiles = dataFiles == null ? List.of() : List.copyOf(dataFiles);
        logFiles = logFiles == null ? List.of() : List.copyOf(logFiles);
        bloomFilter = bloomFilter == null ? null : bloomFilter.clone();
    }

    @Override
    public String filePath() {
        return manifestId;
    }

    public byte[] bloomFilter() {
        return bloomFilter == null ? null : bloomFilter.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ManifestFile that)) return false;
        return manifestId.equals(that.manifestId)
                && format == that.format
                && fileSizeBytes == that.fileSizeBytes
                && createdAtMillis == that.createdAtMillis
                && snapshotId == that.snapshotId
                && dataFiles.equals(that.dataFiles)
                && logFiles.equals(that.logFiles)
                && minSequenceNumber == that.minSequenceNumber
                && maxSequenceNumber == that.maxSequenceNumber
                && Arrays.equals(bloomFilter, that.bloomFilter);
    }

    @Override
    public int hashCode() {
        int result = manifestId.hashCode();
        result = 31 * result + format.hashCode();
        result = 31 * result + Long.hashCode(fileSizeBytes);
        result = 31 * result + Long.hashCode(createdAtMillis);
        result = 31 * result + Long.hashCode(snapshotId);
        result = 31 * result + dataFiles.hashCode();
        result = 31 * result + logFiles.hashCode();
        result = 31 * result + Long.hashCode(minSequenceNumber);
        result = 31 * result + Long.hashCode(maxSequenceNumber);
        result = 31 * result + Arrays.hashCode(bloomFilter);
        return result;
    }
}
