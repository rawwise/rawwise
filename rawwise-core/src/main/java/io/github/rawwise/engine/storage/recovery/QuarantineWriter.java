package io.github.rawwise.engine.storage.recovery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QuarantineWriter {

    private final Path baseDir;

    public QuarantineWriter(String basePath) {
        this.baseDir = Paths.get(basePath, "quarantine");
    }

    public void write(String tableId, List<ReplayError> errors) {
        if (errors.isEmpty()) {
            return;
        }
        String timestamp = String.valueOf(System.currentTimeMillis());
        Path quarantinePath = baseDir.resolve(tableId);
        try {
            Files.createDirectories(quarantinePath);
        } catch (IOException e) {
            log.error("Failed to create quarantine directory: {}", quarantinePath, e);
            return;
        }
        Path filePath = quarantinePath.resolve(timestamp + ".log");
        List<String> lines = new ArrayList<>(errors.size() + 1);
        lines.add("=== Quarantine for table " + tableId + " ===");
        for (ReplayError error : errors) {
            lines.add(error.toLogLine());
        }
        try {
            Files.write(
                    filePath,
                    lines,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            log.warn("Quarantine written: {} errors saved to {}", errors.size(), filePath);
        } catch (IOException e) {
            log.error("Failed to write quarantine file: {}", filePath, e);
        }
    }

    public Path baseDir() {
        return baseDir;
    }
}
