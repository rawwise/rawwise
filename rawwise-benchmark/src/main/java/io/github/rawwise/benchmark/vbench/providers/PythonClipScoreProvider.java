package io.github.rawwise.benchmark.vbench.providers;

import io.github.rawwise.axiom.pipeline.evaluation.ExternalEvaluationProvider;
import io.github.rawwise.model.record.View;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Real CLIP score evaluation: calls Python clip_score_eval.py via subprocess.
 *
 * <p>Python side uses openai/clip-vit-base-patch32 + MPS acceleration, single evaluation ~150ms.
 *
 * <p>Input: - image: image path or 'synth:<keyword>' - prompt: text prompt - reference: reference
 * image (for subject consistency)
 *
 * <p>Output (JSON): { "clip_text_image_similarity": 0.2162, -> INSTRUCTION_FOLLOWING
 * "clip_aesthetic_score": 0.4694, -> PERCEPTUAL_QUALITY "subject_consistency_score": 1.0, ->
 * DOMAIN_COHERENCE "eval_latency_ms": 243 }
 *
 * <p>params convention: - clip_image_path: image path - clip_prompt: text prompt -
 * clip_reference_path: reference image (optional) - python_venv_path: venv path (default
 * benchmark/vbench/.venv) - script_path: script path (default benchmark/vbench/clip_score_eval.py)
 */
@Slf4j
public class PythonClipScoreProvider implements ExternalEvaluationProvider {

    private static final String DEFAULT_VENV =
            "rawwise-benchmark/benchmark/vbench/.venv/bin/python";
    private static final String DEFAULT_SCRIPT =
            "rawwise-benchmark/benchmark/vbench/clip_score_eval.py";
    private static final int PROCESS_TIMEOUT_SEC = 60;

    private final String pythonPath;
    private final String scriptPath;
    private final String workdir;

    public PythonClipScoreProvider() {
        this(DEFAULT_VENV, DEFAULT_SCRIPT, System.getProperty("user.dir"));
    }

    public PythonClipScoreProvider(String pythonPath, String scriptPath, String workdir) {
        this.pythonPath = pythonPath;
        this.scriptPath = scriptPath;
        this.workdir = workdir;
    }

    @Override
    public Optional<Double> evaluate(View view, String dimension, Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return Optional.empty();
        }
        String imagePath = stringOrNull(params, "clip_image_path");
        String prompt = stringOrNull(params, "clip_prompt");
        if (imagePath == null || prompt == null) {
            return Optional.empty();
        }

        Map<String, Object> scores =
                invokePythonEval(imagePath, prompt, stringOrNull(params, "clip_reference_path"));
        if (scores.isEmpty()) {
            return Optional.empty();
        }

        return switch (dimension) {
            case "INSTRUCTION_FOLLOWING" -> doubleOrEmpty(scores.get("clip_text_image_similarity"));
            case "PERCEPTUAL_QUALITY" -> doubleOrEmpty(scores.get("clip_aesthetic_score"));
            case "DOMAIN_COHERENCE" -> doubleOrEmpty(scores.get("subject_consistency_score"));
            default -> Optional.empty();
        };
    }

    @Override
    public String providerId() {
        return "python-clip-vit-base-patch32";
    }

    Map<String, Object> invokePythonEval(String imagePath, String prompt, String referencePath) {
        List<String> cmd = new java.util.ArrayList<>();
        cmd.add(pythonPath);
        cmd.add(scriptPath);
        cmd.add("--image");
        cmd.add(imagePath);
        cmd.add("--prompt");
        cmd.add(prompt);
        if (referencePath != null) {
            cmd.add("--reference");
            cmd.add(referencePath);
        }

        try {
            ProcessBuilder pb =
                    new ProcessBuilder(cmd)
                            .directory(new java.io.File(workdir))
                            .redirectErrorStream(false);
            pb.environment().put("HF_ENDPOINT", "https://hf-mirror.com");

            Process process = pb.start();
            StringBuilder stdout = new StringBuilder();
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stdout.append(line).append('\n');
                }
            }

            boolean finished = process.waitFor(PROCESS_TIMEOUT_SEC, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                log.warn("Python CLIP eval timeout after {}s", PROCESS_TIMEOUT_SEC);
                return Map.of();
            }
            if (process.exitValue() != 0) {
                log.warn("Python CLIP eval exited with code {}", process.exitValue());
                return Map.of();
            }

            String lastLine = lastNonEmptyLine(stdout.toString());
            return parseJson(lastLine);
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Python CLIP eval invocation failed", e);
            return Map.of();
        }
    }

    static Map<String, Object> parseJson(String json) {
        if (json == null || !json.startsWith("{")) {
            return Map.of();
        }
        Map<String, Object> result = new HashMap<>();
        String inner = json.substring(1, json.length() - 1);
        String[] pairs = splitTopLevel(inner);
        for (String pair : pairs) {
            int colon = pair.indexOf(':');
            if (colon < 0) continue;
            String key = unquote(pair.substring(0, colon).trim());
            String val = pair.substring(colon + 1).trim();
            if (val.startsWith("\"")) {
                result.put(key, unquote(val));
            } else if ("null".equals(val)) {
                result.put(key, null);
            } else {
                try {
                    result.put(key, Double.parseDouble(val));
                } catch (NumberFormatException ignored) {
                    result.put(key, val);
                }
            }
        }
        return result;
    }

    private static String[] splitTopLevel(String s) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        int start = 0;
        boolean inStr = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') inStr = !inStr;
            if (c == ',' && !inStr) {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(s.substring(start));
        return parts.toArray(new String[0]);
    }

    private static String unquote(String s) {
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static String lastNonEmptyLine(String text) {
        String[] lines = text.split("\n");
        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            if (!line.isEmpty() && line.startsWith("{")) {
                return line;
            }
        }
        return "";
    }

    private String stringOrNull(Map<String, Object> params, String key) {
        Object val = params.get(key);
        return val == null ? null : val.toString();
    }

    private Optional<Double> doubleOrEmpty(Object val) {
        if (val == null) return Optional.empty();
        if (val instanceof Number n) return Optional.of(n.doubleValue());
        try {
            return Optional.of(Double.parseDouble(val.toString()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
