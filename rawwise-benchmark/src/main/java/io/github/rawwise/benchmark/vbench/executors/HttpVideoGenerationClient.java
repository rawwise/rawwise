package io.github.rawwise.benchmark.vbench.executors;

import io.github.rawwise.benchmark.vbench.config.VBenchConfig;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Real video generation API client(HTTP).
 *
 * <p>Supports OpenAI-style request/response protocol, compatible with: - Zhipu CogVideoX OpenAPI
 * (https://open.bigmodel.cn) - Kling API - Alibaba Wan API - any POST {endpoint}/videos/generations
 * service returning video URL
 *
 * <p>Protocol convention: Request: POST {endpoint} Authorization: Bearer {api_key} Content-Type:
 * application/json Body: {"model": "...", "prompt": "...", "reference_image": "..."}
 *
 * <p>Response: {"video_url": "https://...", "task_id": "..."} or async: {"task_id": "..."} →
 * polling GET {endpoint}/tasks/{task_id}
 *
 * <p>Video file storage path: {videos_output_dir}/{tag}/{prompt_id}_run_{run_id}.mp4 also
 * writes.meta.json containing prompt / api_response / latency metadata
 *
 * <p>Config keys: provider.api.endpoint: API URL (required) provider.api.key: API key (required)
 * provider.api.model: model name (required)
 *
 * <p>If endpoint+key not configured, automatically falls back to local stub(CogVideoXClient).
 */
@Slf4j
public class HttpVideoGenerationClient {

    private final VBenchConfig config;
    private final HttpClient httpClient;
    private final CogVideoXClient fallbackStub;

    public HttpVideoGenerationClient() {
        this(new VBenchConfig());
    }

    public HttpVideoGenerationClient(VBenchConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
        this.fallbackStub = new CogVideoXClient();
    }

    /**
     * Generate video and write to disk.
     *
     * @param prompt text prompt
     * @param tag directory tag (e.g. "baseline" / "rawwise" / "corrections")
     * @param promptId prompt unique identifier (for filename)
     * @param runId repetition run index
     * @param extraConditions extra conditions (reference_embedding / drift_description etc.)
     * @return video generation result (with local path + metadata)
     */
    public VideoGenerationResult generate(
            String prompt,
            String tag,
            String promptId,
            int runId,
            Map<String, Object> extraConditions) {
        String endpoint = config.getApiEndpoint();
        String apiKey = config.getApiKey();

        boolean useStub =
                endpoint == null || endpoint.isBlank() || apiKey == null || apiKey.isBlank();
        Path outputDir = ensureDir(Path.of(config.getVideosOutputDir(), tag));
        String baseFileName = sanitize(promptId) + "_run_" + runId;
        Path videoPath = outputDir.resolve(baseFileName + ".mp4");
        Path metaPath = outputDir.resolve(baseFileName + ".meta.json");

        long start = System.currentTimeMillis();
        VideoGenerationResult result;
        try {
            if (useStub) {
                log.warn(
                        "API endpoint/key not configured, using stub generation for prompt={}",
                        truncate(prompt, 40));
                result = generateStub(prompt, videoPath, extraConditions);
            } else {
                result = generateViaApi(prompt, endpoint, apiKey, videoPath, extraConditions);
            }
        } catch (Exception e) {
            log.error("Video generation failed for prompt={}", truncate(prompt, 40), e);
            throw new RuntimeException("Video generation failed: " + e.getMessage(), e);
        }
        long elapsed = System.currentTimeMillis() - start;
        result.elapsedMs = elapsed;
        writeMetadata(metaPath, prompt, tag, promptId, runId, extraConditions, result);
        log.info("Video saved: {} ({} bytes, {}ms)", videoPath, result.fileSize, elapsed);
        return result;
    }

    private VideoGenerationResult generateStub(
            String prompt, Path videoPath, Map<String, Object> extraConditions) throws IOException {
        CogVideoXClient.GenerationRequest req = CogVideoXClient.GenerationRequest.of(prompt);
        CogVideoXClient.GenerationResponse resp = fallbackStub.generate(req);
        Files.write(
                videoPath,
                resp.videoBytes(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        VideoGenerationResult result = new VideoGenerationResult();
        result.videoPath = videoPath;
        result.fileSize = resp.videoBytes().length;
        result.modelVersion = resp.modelVersion();
        result.apiResponse = "stub";
        return result;
    }

    private VideoGenerationResult generateViaApi(
            String prompt,
            String endpoint,
            String apiKey,
            Path videoPath,
            Map<String, Object> extraConditions)
            throws IOException, InterruptedException {
        String model = config.getApiModel() != null ? config.getApiModel() : "cogvideox-2";
        int timeoutSec = config.getTimeoutSeconds();

        String requestJson = buildRequestJson(prompt, model, extraConditions);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(endpoint))
                        .timeout(Duration.ofSeconds(timeoutSec))
                        .header("Authorization", "Bearer " + apiKey)
                        .header("Content-Type", "application/json")
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        requestJson, StandardCharsets.UTF_8))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        String body = response.body();
        log.debug("API response status={}, body length={}", status, body.length());

        if (status < 200 || status >= 300) {
            throw new IOException("API returned HTTP " + status + ": " + truncate(body, 500));
        }

        // Zhipu CogVideoX and similar async APIs: return task id first, polling required
        String videoUrl = extractVideoUrl(body);
        if (videoUrl == null) {
            String taskId = extractTaskId(body);
            String taskStatus = extractField(body, "task_status");
            if (taskId != null) {
                log.info("Async task created: id={}, status={}, polling...", taskId, taskStatus);
                videoUrl = pollAsyncTask(taskId, apiKey, endpoint, timeoutSec);
            }
        }

        if (videoUrl == null) {
            throw new IOException(
                    "Could not extract video_url from response: " + truncate(body, 500));
        }

        byte[] videoBytes = downloadVideo(videoUrl, timeoutSec);
        Files.write(
                videoPath,
                videoBytes,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        VideoGenerationResult result = new VideoGenerationResult();
        result.videoPath = videoPath;
        result.fileSize = videoBytes.length;
        result.modelVersion = model;
        result.apiResponse = truncate(body, 500);
        result.videoUrl = videoUrl;
        return result;
    }

    /**
     * Poll Zhipu CogVideoX async task results. GET
     * https://open.bigmodel.cn/api/paas/v4/async-result/{taskId}
     */
    private String pollAsyncTask(
            String taskId, String apiKey, String submitEndpoint, int timeoutSec)
            throws IOException, InterruptedException {
        String pollBase = derivePollEndpoint(submitEndpoint);
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            Thread.sleep(Math.min(10000, 2000 + attempt * 1000L)); // 2s, 3s, 4s... 10s

            HttpRequest req =
                    HttpRequest.newBuilder()
                            .uri(URI.create(pollBase + "/" + taskId))
                            .timeout(Duration.ofSeconds(60))
                            .header("Authorization", "Bearer " + apiKey)
                            .GET()
                            .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                log.warn("Poll attempt {} failed: HTTP {}", attempt, resp.statusCode());
                continue;
            }
            String body = resp.body();
            String status = extractField(body, "task_status");
            log.info("Poll attempt {}: task_status={}", attempt, status);

            if ("SUCCESS".equalsIgnoreCase(status)) {
                String url = extractVideoUrl(body);
                if (url != null) {
                    return url;
                }
                throw new IOException("Task SUCCESS but no video URL in: " + truncate(body, 500));
            }
            if ("FAIL".equalsIgnoreCase(status) || "FAILED".equalsIgnoreCase(status)) {
                throw new IOException("Task failed: " + truncate(body, 500));
            }
            // PROCESSING / RUNNING / WAITING → continuepolling
        }
        throw new IOException("Polling timeout after " + timeoutSec + "s for task " + taskId);
    }

    private String derivePollEndpoint(String submitEndpoint) {
        // Zhipu: https://open.bigmodel.cn/api/paas/v4/videos/generations
        // → https://open.bigmodel.cn/api/paas/v4/async-result
        int idx = submitEndpoint.indexOf("/videos/generations");
        if (idx > 0) {
            return submitEndpoint.substring(0, idx) + "/async-result";
        }
        // generic fallback
        return submitEndpoint + "/tasks";
    }

    private String extractTaskId(String body) {
        return extractField(body, "id", "task_id", "request_id");
    }

    private String extractField(String body, String... keys) {
        for (String key : keys) {
            String quoted = "\"" + key + "\"";
            int idx = body.indexOf(quoted);
            if (idx < 0) continue;
            int colon = body.indexOf(':', idx);
            int start = body.indexOf('"', colon + 1);
            if (start < 0) continue;
            int end = body.indexOf('"', start + 1);
            if (end < 0) continue;
            return body.substring(start + 1, end);
        }
        return null;
    }

    private byte[] downloadVideo(String url, int timeoutSec)
            throws IOException, InterruptedException {
        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(timeoutSec))
                        .GET()
                        .build();
        HttpResponse<byte[]> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IOException("Video download failed HTTP " + resp.statusCode());
        }
        return resp.body();
    }

    private String buildRequestJson(
            String prompt, String model, Map<String, Object> extraConditions) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"model\":\"").append(escape(model)).append('"');
        sb.append(",\"prompt\":\"").append(escape(prompt)).append('"');
        if (extraConditions != null && !extraConditions.isEmpty()) {
            String drift = stringValue(extraConditions, "drift_description");
            if (drift != null) {
                sb.append(",\"drift_description\":\"").append(escape(drift)).append('"');
            }
            String refImage = stringValue(extraConditions, "reference_image_url");
            if (refImage != null) {
                sb.append(",\"reference_image\":\"").append(escape(refImage)).append('"');
            }
        }
        sb.append('}');
        return sb.toString();
    }

    private String extractVideoUrl(String body) {
        // Simple JSON extraction (avoiding JSON library dependency)
        // Zhipu CogVideoX: "video_result":[{"url":"https://..."}]
        // other: "video_url" / "url" / "videoUrl"
        for (String key : new String[] {"video_url", "url", "videoUrl"}) {
            String quoted = "\"" + key + "\"";
            int idx = body.indexOf(quoted);
            while (idx >= 0) {
                int colon = body.indexOf(':', idx);
                int start = body.indexOf('"', colon + 1);
                if (start < 0) break;
                int end = body.indexOf('"', start + 1);
                if (end < 0) break;
                String val = body.substring(start + 1, end);
                if (val.startsWith("http")) {
                    return val;
                }
                // Does not look like URL, continue searching
                idx = body.indexOf(quoted, end);
            }
        }
        return null;
    }

    private void writeMetadata(
            Path metaPath,
            String prompt,
            String tag,
            String promptId,
            int runId,
            Map<String, Object> extraConditions,
            VideoGenerationResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"prompt_id\": \"").append(escape(promptId)).append("\",\n");
        sb.append("  \"prompt\": \"").append(escape(prompt)).append("\",\n");
        sb.append("  \"tag\": \"").append(escape(tag)).append("\",\n");
        sb.append("  \"run_id\": ").append(runId).append(",\n");
        sb.append("  \"video_path\": \"")
                .append(escape(result.videoPath.toString()))
                .append("\",\n");
        sb.append("  \"file_size_bytes\": ").append(result.fileSize).append(",\n");
        sb.append("  \"model_version\": \"").append(escape(result.modelVersion)).append("\",\n");
        sb.append("  \"elapsed_ms\": ").append(result.elapsedMs).append(",\n");
        sb.append("  \"video_url\": \"").append(escape(result.videoUrl)).append("\",\n");
        sb.append("  \"has_reference_embedding\": ")
                .append(
                        extraConditions != null
                                && extraConditions.containsKey("reference_embedding"))
                .append("\n");
        sb.append("}\n");
        try {
            Files.writeString(
                    metaPath,
                    sb.toString(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            log.warn("Failed to write metadata to {}", metaPath, e);
        }
    }

    private Path ensureDir(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directory: " + dir, e);
        }
        return dir;
    }

    private String sanitize(String s) {
        if (s == null) return UUID.randomUUID().toString().substring(0, 8);
        return s.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private String stringValue(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : v.toString();
    }

    private String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    public static class VideoGenerationResult {
        public Path videoPath;
        public long fileSize;
        public String modelVersion;
        public String apiResponse;
        public String videoUrl = "";
        public long elapsedMs;
        public Map<String, Object> extras = new HashMap<>();
    }
}
