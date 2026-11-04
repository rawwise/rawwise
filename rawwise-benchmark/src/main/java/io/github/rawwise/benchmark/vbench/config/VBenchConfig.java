package io.github.rawwise.benchmark.vbench.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

/**
 * VBench config loader.
 *
 * <p>priority(high→low): 1. Java System Property: -Drawwise.vbench.<key>=<value> 2. Environment
 * variable: RAWWISE_VBENCH_<KEY>=<value>(dots replaced with underscores, all uppercase) 3. config
 * file: $HOME/.rawwise/vbench/config.properties 4. default value
 *
 * <p>security:API key etc.sensitiveconfig**notneed**writeversioncontrolledofconfig file,
 * recommenduseEnvironment variableor ~/.rawwise/vbench/config.properties(gitignored).
 *
 * <p>keyConfig keys: provider → cogvideox-api | kling | vidu | local-stub provider.api.endpoint →
 * API endpoint URL provider.api.key → API key (sensitive) provider.api.model → model name (e.g.
 * "cogvideox-2") provider.timeout.seconds → single generation timeout (default 300)
 * videos.output.dir → video output directory (default benchmark/vbench/videos) eval.results.dir →
 * evaluation results directory (default benchmark/vbench/eval_results) eval.python.path → Python
 * venv path eval.script.path → CLIP/VBench script path
 */
@Slf4j
public final class VBenchConfig {

    public static final String KEY_PROVIDER = "provider";
    public static final String KEY_API_ENDPOINT = "provider.api.endpoint";
    public static final String KEY_API_KEY = "provider.api.key";
    public static final String KEY_API_MODEL = "provider.api.model";
    public static final String KEY_TIMEOUT_SEC = "provider.timeout.seconds";
    public static final String KEY_VIDEOS_OUT_DIR = "videos.output.dir";
    public static final String KEY_EVAL_RESULTS_DIR = "eval.results.dir";
    public static final String KEY_PYTHON_PATH = "eval.python.path";
    public static final String KEY_SCRIPT_PATH = "eval.script.path";
    public static final String KEY_HF_ENDPOINT = "eval.hf.endpoint";

    private static final String SYS_PROP_PREFIX = "rawwise.vbench.";
    private static final String ENV_PREFIX = "RAWWISE_VBENCH_";
    private static final String DEFAULT_CONFIG_PATH =
            System.getProperty("user.home") + "/.rawwise/vbench/config.properties";

    private final Properties fileProperties;

    public VBenchConfig() {
        this(DEFAULT_CONFIG_PATH);
    }

    public VBenchConfig(String configFilePath) {
        this.fileProperties = loadFromFile(configFilePath);
    }

    public String get(String key) {
        return get(key, null);
    }

    public String get(String key, String defaultValue) {
        // 1. System Property
        String value = System.getProperty(SYS_PROP_PREFIX + key);
        if (value != null) return value;

        // 2. Environment Variable
        String envKey = ENV_PREFIX + key.toUpperCase().replace('.', '_');
        value = System.getenv(envKey);
        if (value != null) return value;

        // 3. Config file
        value = fileProperties.getProperty(key);
        if (value != null) return value;

        return defaultValue;
    }

    public int getInt(String key, int defaultValue) {
        String value = get(key);
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Invalid int for key={}: {}, using default={}", key, value, defaultValue);
            return defaultValue;
        }
    }

    public String getProvider() {
        return get(KEY_PROVIDER, "local-stub");
    }

    public String getApiEndpoint() {
        return get(KEY_API_ENDPOINT);
    }

    public String getApiKey() {
        return get(KEY_API_KEY);
    }

    public String getApiModel() {
        return get(KEY_API_MODEL);
    }

    public int getTimeoutSeconds() {
        return getInt(KEY_TIMEOUT_SEC, 600);
    }

    public String getVideosOutputDir() {
        return get(KEY_VIDEOS_OUT_DIR, "benchmark/vbench/videos");
    }

    public String getEvalResultsDir() {
        return get(KEY_EVAL_RESULTS_DIR, "benchmark/vbench/eval_results");
    }

    public String getPythonPath() {
        return get(KEY_PYTHON_PATH, "rawwise-benchmark/benchmark/vbench/.venv/bin/python");
    }

    public String getScriptPath() {
        return get(KEY_SCRIPT_PATH, "rawwise-benchmark/benchmark/vbench/clip_score_eval.py");
    }

    public String getHfEndpoint() {
        return get(KEY_HF_ENDPOINT, "https://hf-mirror.com");
    }

    /** Print currently effective config (sensitive fields masked). */
    public void printEffective() {
        System.out.println("=== VBench Configuration ===");
        printKey(KEY_PROVIDER, getProvider());
        printKey(KEY_API_ENDPOINT, getApiEndpoint());
        printSensitive(KEY_API_KEY, getApiKey());
        printKey(KEY_API_MODEL, getApiModel());
        printKey(KEY_TIMEOUT_SEC, String.valueOf(getTimeoutSeconds()));
        printKey(KEY_VIDEOS_OUT_DIR, getVideosOutputDir());
        printKey(KEY_EVAL_RESULTS_DIR, getEvalResultsDir());
        printKey(KEY_PYTHON_PATH, getPythonPath());
        printKey(KEY_SCRIPT_PATH, getScriptPath());
        printKey(KEY_HF_ENDPOINT, getHfEndpoint());
        System.out.println("============================");
    }

    private void printKey(String key, String effectiveValue) {
        String source = resolveSource(key);
        System.out.printf(
                "  %-32s = %s  (%s)%n",
                key, effectiveValue == null ? "<unset>" : effectiveValue, source);
    }

    private void printSensitive(String key, String effectiveValue) {
        String source = resolveSource(key);
        String masked =
                effectiveValue == null
                        ? "<unset>"
                        : (effectiveValue.length() <= 8
                                ? "***"
                                : effectiveValue.substring(0, 4)
                                        + "***"
                                        + effectiveValue.substring(effectiveValue.length() - 4));
        System.out.printf("  %-32s = %s  (%s)%n", key, masked, source);
    }

    private String resolveSource(String key) {
        if (System.getProperty(SYS_PROP_PREFIX + key) != null) return "sys-prop";
        if (System.getenv(ENV_PREFIX + key.toUpperCase().replace('.', '_')) != null) return "env";
        if (fileProperties.getProperty(key) != null) return "file";
        return "default";
    }

    private Properties loadFromFile(String path) {
        Properties props = new Properties();
        Path p = Path.of(path);
        if (!Files.exists(p)) {
            log.debug(
                    "VBench config file not found at {}, using defaults + env/sysprop only", path);
            return props;
        }
        try (InputStream in = Files.newInputStream(p)) {
            props.load(in);
            log.info("Loaded VBench config from {}", path);
        } catch (IOException e) {
            log.warn("Failed to load VBench config from {}: {}", path, e.getMessage());
        }
        return props;
    }
}
