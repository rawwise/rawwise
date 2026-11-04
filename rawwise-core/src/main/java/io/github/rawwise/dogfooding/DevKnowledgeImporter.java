package io.github.rawwise.dogfooding;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DevKnowledgeImporter {

    private final HttpClient httpClient;
    private final String frontendUrl;
    private final String tableId;
    private final ObjectMapper mapper;

    public DevKnowledgeImporter(String frontendUrl, String tableId) {
        this.httpClient = HttpClient.newHttpClient();
        this.frontendUrl = frontendUrl;
        this.tableId = tableId;
        this.mapper = new ObjectMapper();
    }

    public record ImportStats(int total, int success, int failed, long elapsedMs) {}

    public ImportStats importAll(String projectRoot) throws Exception {
        List<KnowledgeEntry> entries = new ArrayList<>();

        // 1. All markdown files under skills/ (recursively)
        Path skillsDir = Path.of(projectRoot, ".trae", "skills");
        if (Files.exists(skillsDir)) {
            try (var stream = Files.walk(skillsDir)) {
                stream.filter(p -> p.toString().endsWith(".md"))
                        .filter(p -> !p.toString().contains("node_modules"))
                        .forEach(
                                p -> {
                                    try {
                                        String relPath = skillsDir.relativize(p).toString();
                                        // Skip generic non-RawWise skills
                                        if (relPath.startsWith("axolotl/")
                                                || relPath.startsWith("deepspeed/")
                                                || relPath.startsWith("langchain/")
                                                || relPath.startsWith("crewai/")
                                                || relPath.startsWith("dspy/")
                                                || relPath.startsWith("unsloth/")
                                                || relPath.startsWith("llama-factory/")
                                                || relPath.startsWith("vllm/")
                                                || relPath.startsWith("patent-")
                                                || relPath.startsWith("lark-")
                                                || relPath.startsWith("TRAE-")
                                                || relPath.startsWith("skill-creator")
                                                || relPath.startsWith("brainstorming")
                                                || relPath.startsWith("writing-")
                                                || relPath.startsWith("using-")
                                                || relPath.startsWith("requesting-")
                                                || relPath.startsWith("receiving-")
                                                || relPath.startsWith("executing-")
                                                || relPath.startsWith("finishing-")
                                                || relPath.startsWith("dispatching-")
                                                || relPath.startsWith("subagent-")
                                                || relPath.startsWith("test-driven-")
                                                || relPath.startsWith("systematic-")
                                                || relPath.startsWith("verification-")
                                                || relPath.startsWith("clean-commit-")
                                                || relPath.startsWith("using-git-")
                                                || relPath.startsWith("web-dev")) {
                                            return;
                                        }
                                        entries.addAll(parseSkillFile(p, relPath));
                                    } catch (Exception e) {
                                        log.warn(
                                                "Failed to parse skill file {}: {}",
                                                p,
                                                e.getMessage());
                                    }
                                });
            }
        }

        // 2. Project rules
        Path rulesDir = Path.of(projectRoot, ".trae", "rules");
        if (Files.exists(rulesDir)) {
            try (var stream = Files.list(rulesDir)) {
                stream.filter(p -> p.toString().endsWith(".md"))
                        .forEach(
                                p -> {
                                    try {
                                        entries.addAll(parseDocumentFile(p, "rule"));
                                    } catch (Exception e) {
                                        log.warn("Failed to parse rules {}: {}", p, e.getMessage());
                                    }
                                });
            }
        }

        // 3. Standalone documents
        Path docsDir = Path.of(projectRoot, ".trae", "documents");
        if (Files.exists(docsDir)) {
            try (var stream = Files.list(docsDir)) {
                stream.filter(p -> p.toString().endsWith(".md"))
                        .forEach(
                                p -> {
                                    try {
                                        entries.addAll(parseDocumentFile(p, "document"));
                                    } catch (Exception e) {
                                        log.warn(
                                                "Failed to parse document {}: {}",
                                                p,
                                                e.getMessage());
                                    }
                                });
            }
        }

        // 4. Agent collaboration log
        Path agentLog = Path.of(projectRoot, ".trae", "agent-collaboration-log.md");
        if (Files.exists(agentLog)) {
            entries.addAll(parseDocumentFile(agentLog, "log"));
        }

        log.info("Found {} knowledge entries to import", entries.size());
        long start = System.currentTimeMillis();
        int success = 0;
        int failed = 0;

        for (int i = 0; i < entries.size(); i++) {
            KnowledgeEntry entry = entries.get(i);
            try {
                ingestEntry(entry);
                success++;
                if ((i + 1) % 10 == 0) {
                    log.info("Progress: {}/{} imported", i + 1, entries.size());
                }
            } catch (Exception e) {
                failed++;
                log.warn("Failed to import entry {}: {}", entry.key(), e.getMessage());
            }
        }

        long elapsed = System.currentTimeMillis() - start;
        return new ImportStats(entries.size(), success, failed, elapsed);
    }

    private void ingestEntry(KnowledgeEntry entry) throws Exception {
        Map<String, Object> payload =
                Map.of(
                        "tableId", tableId,
                        "key", entry.key(),
                        "body", entry.content(),
                        "immutable", true,
                        "properties", Map.of("type", entry.type(), "source", entry.source()));

        String json = mapper.writeValueAsString(payload);
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(frontendUrl + "/api/axiom/ingest"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200 && response.statusCode() != 201) {
            throw new RuntimeException("HTTP " + response.statusCode() + ": " + response.body());
        }
    }

    /** Parse a skill SKILL.md — split domain/invariant files by ##, keep skills as single entry */
    private List<KnowledgeEntry> parseSkillFile(Path file, String relPath) throws Exception {
        String content = Files.readString(file);
        String filename = file.getFileName().toString().replace(".md", "");

        // Domain files: split by ## headers
        if (relPath.contains("domains/")
                || relPath.contains("invariants/")
                || relPath.contains("checklists/")) {
            return parseSectionedFile(content, filename, extractCategory(relPath));
        }

        // SKILL.md files: import as single entry per file
        String type = extractCategory(relPath);
        String key = type + "-" + filename;
        String body = content.length() > 4000 ? content.substring(0, 4000) : content;
        return List.of(new KnowledgeEntry(key, body, type, filename));
    }

    /** Parse a generic markdown document (rules, standalone docs) — split by ## */
    private List<KnowledgeEntry> parseDocumentFile(Path file, String type) throws Exception {
        String content = Files.readString(file);
        String filename = file.getFileName().toString().replace(".md", "");
        return parseSectionedFile(content, type + "-" + filename, type);
    }

    /** Split markdown by ## headers into separate entries */
    private List<KnowledgeEntry> parseSectionedFile(String content, String keyPrefix, String type) {
        List<KnowledgeEntry> entries = new ArrayList<>();
        String[] sections = content.split("(?<=\\n)## ");
        if (sections.length <= 1) {
            String body = content.length() > 4000 ? content.substring(0, 4000) : content;
            entries.add(new KnowledgeEntry(keyPrefix, body, type, keyPrefix));
            return entries;
        }
        for (int i = 0; i < sections.length; i++) {
            String section = sections[i].trim();
            if (section.isEmpty()) continue;
            String title =
                    section.contains("\n")
                            ? section.substring(0, section.indexOf("\n")).trim()
                            : section;
            String body =
                    title.startsWith("#")
                            ? title + "\n" + section.substring(section.indexOf("\n") + 1).trim()
                            : section;
            if (body.length() > 4000) body = body.substring(0, 4000);
            entries.add(new KnowledgeEntry(keyPrefix + "-s" + i, body, type, keyPrefix));
        }
        return entries;
    }

    /** Extract category from relative path for tagging */
    private static String extractCategory(String relPath) {
        if (relPath.contains("domains/")) return "domain";
        if (relPath.contains("invariants/")) return "invariant";
        if (relPath.contains("checklists/")) return "checklist";
        if (relPath.contains("rawwise-context")) return "context";
        if (relPath.contains("rawwise-expert")
                || relPath.contains("bigdata-engine")
                || relPath.contains("big-data-architect")
                || relPath.contains("code-review")) return "expert";
        if (relPath.contains("rawwise-test") || relPath.contains("big-data-testing"))
            return "testing";
        if (relPath.contains("rawwise-book") || relPath.contains("rawwise-evolver"))
            return "tooling";
        if (relPath.contains("ai-native")) return "methodology";
        if (relPath.contains("rawwise/")) return "rawwise_skill";
        if (relPath.contains("bigdata-driven")) return "workflow";
        if (relPath.contains("metastore-pitfalls")) return "checklist";
        return "skill";
    }

    public record KnowledgeEntry(String key, String content, String type, String source) {}

    public static void main(String[] args) throws Exception {
        String frontendUrl = System.getProperty("rawwise.frontend.url", "http://localhost:8080");
        String tableId = System.getProperty("rawwise.dogfooding.table", "default.dev_knowledge");
        String projectRoot =
                System.getProperty("rawwise.project.root", System.getProperty("user.dir"));

        log.info("Importing knowledge to {} via {}", tableId, frontendUrl);
        DevKnowledgeImporter importer = new DevKnowledgeImporter(frontendUrl, tableId);
        ImportStats stats = importer.importAll(projectRoot);

        log.info("=== Import Complete ===");
        log.info("Total: {}", stats.total());
        log.info("Success: {}", stats.success());
        log.info("Failed: {}", stats.failed());
        log.info("Elapsed: {}ms ({}s)", stats.elapsedMs(), stats.elapsedMs() / 1000.0);
    }
}
