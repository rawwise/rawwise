package io.github.rawwise.engine.compaction.cleanup;

import io.github.rawwise.engine.storage.spi.Storage;
import java.net.URI;

public final class StoragePathResolver {

    private StoragePathResolver() {}

    public static Storage.StoragePath resolvePath(String path) {
        try {
            URI uri = URI.create(path);
            String scheme = uri.getScheme();
            if (scheme != null) {
                String location =
                        scheme.equals("file")
                                ? uri.getPath()
                                : (uri.getRawAuthority() != null
                                        ? "//" + uri.getRawAuthority() + uri.getRawPath()
                                        : uri.getRawPath());
                return new Storage.StoragePath(scheme, location);
            }
        } catch (IllegalArgumentException ignored) {
        }
        return new Storage.StoragePath("file", path);
    }
}
