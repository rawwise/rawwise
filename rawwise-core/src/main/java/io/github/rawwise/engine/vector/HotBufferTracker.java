package io.github.rawwise.engine.vector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HotBufferTracker implements AutoCloseable {

    private final Path trackingFile;
    private BufferedWriter writer;

    public HotBufferTracker(Path basePath, String namespace) {
        Path dir = basePath.resolve("hot-buffer");
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create hot-buffer directory: " + dir, e);
        }
        this.trackingFile = dir.resolve(namespace + ".keys");
        openWriter();
    }

    private void openWriter() {
        try {
            this.writer =
                    Files.newBufferedWriter(
                            trackingFile,
                            StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open tracking file: " + trackingFile, e);
        }
    }

    public synchronized void append(String entryKey) {
        try {
            writer.write(entryKey);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            log.warn("Failed to append entryKey to tracking file: {}", trackingFile, e);
        }
    }

    public List<String> getKeys() {
        return loadKeys();
    }

    public synchronized void clear() {
        try {
            writer.close();
        } catch (IOException e) {
            log.warn("Failed to close writer before clear: {}", trackingFile, e);
        }
        try {
            Files.deleteIfExists(trackingFile);
        } catch (IOException e) {
            log.warn("Failed to delete tracking file: {}", trackingFile, e);
        }
        openWriter();
    }

    public List<String> loadKeys() {
        List<String> keys = new ArrayList<>();
        if (!Files.exists(trackingFile)) {
            return keys;
        }
        try (BufferedReader reader =
                Files.newBufferedReader(trackingFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank()) {
                    keys.add(line);
                }
            }
        } catch (IOException e) {
            log.warn("Failed to read tracking file: {}", trackingFile, e);
        }
        return keys;
    }

    @Override
    public void close() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            log.debug("Failed to close tracking file writer: {}", trackingFile, e);
        }
    }
}
