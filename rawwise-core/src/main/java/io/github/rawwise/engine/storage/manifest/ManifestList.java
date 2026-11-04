package io.github.rawwise.engine.storage.manifest;

import java.io.Serializable;
import java.util.List;

public record ManifestList(
        String filePath, long snapshotId, List<ManifestFile> manifests, long createdAtMillis)
        implements Serializable {

    public ManifestList {
        manifests = manifests == null ? List.of() : List.copyOf(manifests);
    }
}
