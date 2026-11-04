package io.github.rawwise.benchmark;

import java.nio.file.Path;

public interface DataGenerator {
    void generate(int scaleFactor, Path outputDir, Object spark);
}
