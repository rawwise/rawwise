package io.github.rawwise.engine.storage.write;

import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LmdbWritePathCoordinator implements WritePathCoordinator {
    private final BiFunction<String, byte[], Boolean> writableChecker;

    public LmdbWritePathCoordinator(BiFunction<String, byte[], Boolean> writableChecker) {
        this.writableChecker = writableChecker;
    }

    @Override
    public boolean ensureWritable(String tableIdentifier, byte[] key) {
        return writableChecker.apply(tableIdentifier, key);
    }

    @Override
    public boolean isLmdbBacked() {
        return true;
    }
}
