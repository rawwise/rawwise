package io.github.rawwise.governance.config;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);

    private static final String RAWWISE_CONFIG_FILE_ENV = "RAWWISE_CONFIG_FILE";
    private static final String RAWWISE_HOME_ENV = "RAWWISE_HOME";
    private static final String CONFIG_FILE_NAME = "rawwise.conf";
    private static final String RAWWISE_PREFIX = "rawwise.";
    private static final String SPARK_RAWWISE_PREFIX = "spark.rawwise.";

    private static final Set<String> CONTROL_ENV_VARS =
            Set.of("RAWWISE_CONFIG_FILE", "RAWWISE_HOME");

    public static RawWiseConfig load() {
        return builder().load();
    }

    public static RawWiseConfig load(SparkConf sparkConf) {
        return builder().sparkConf(sparkConf).load();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Path configFile;
        private SparkConf sparkConf;
        private boolean strictMode = false;
        private Map<String, String> cliOverrides;

        public Builder configFile(Path path) {
            this.configFile = path;
            return this;
        }

        public Builder sparkConf(SparkConf conf) {
            this.sparkConf = conf;
            return this;
        }

        public Builder strictMode(boolean strict) {
            this.strictMode = strict;
            return this;
        }

        public Builder cliOverrides(Map<String, String> overrides) {
            this.cliOverrides = overrides;
            return this;
        }

        public RawWiseConfig load() {
            ConfigLoader loader = new ConfigLoader(configFile, sparkConf, strictMode, cliOverrides);
            return loader.doLoad();
        }
    }

    private final Path explicitConfigFile;
    private final SparkConf sparkConf;
    private boolean strictMode;
    private final Map<String, String> cliOverrides;

    private ConfigLoader(
            Path configFile,
            SparkConf sparkConf,
            boolean strictMode,
            Map<String, String> cliOverrides) {
        this.explicitConfigFile = configFile;
        this.sparkConf = sparkConf;
        this.strictMode = strictMode;
        this.cliOverrides = cliOverrides != null ? cliOverrides : Map.of();
    }

    private RawWiseConfig doLoad() {
        this.strictMode = this.strictMode || resolveStrictMode();

        Path resolvedFile = resolveConfigFile();
        boolean useClasspath = false;

        Map<String, Object> fileConfig = Map.of();
        if (resolvedFile != null) {
            checkFilePermissions(resolvedFile);
            fileConfig = HoconConfigReader.read(resolvedFile);
            log.info("Loaded config file: {}", resolvedFile);
        } else {
            fileConfig = HoconConfigReader.readResource(CONFIG_FILE_NAME);
            if (!fileConfig.isEmpty()) {
                useClasspath = true;
                URL sourceUrl =
                        HoconConfigReader.class
                                .getClassLoader()
                                .getResource(CONFIG_FILE_NAME + ".conf");
                String sourceInfo = sourceUrl != null ? sourceUrl.toString() : CONFIG_FILE_NAME;
                if (strictMode) {
                    throw new ConfigException.ValidationError(
                            "Strict mode: classpath config is not allowed. Source: " + sourceInfo);
                }
                log.warn(
                        "Loaded config from classpath: {} — this is not recommended in production",
                        sourceInfo);
            }
        }

        Map<String, String> envConfig = resolveEnvVars();

        Map<String, String> systemPropsConfig = resolveSystemProps();

        Map<String, String> sparkConfig = resolveSparkConf();

        Map<String, Object> merged =
                mergeConfigs(cliOverrides, envConfig, fileConfig, sparkConfig, systemPropsConfig);

        detectUnknownKeys(merged);

        RawWiseConfig config = ConfigFactory.buildValidated(merged, false, true);

        return config;
    }

    private boolean resolveStrictMode() {
        String envValue = System.getenv("RAWWISE_CONFIG_STRICT_MODE");
        if (envValue != null) {
            return Boolean.parseBoolean(envValue);
        }
        String sysProp = System.getProperty("rawwise.config.strict-mode");
        if (sysProp != null) {
            return Boolean.parseBoolean(sysProp);
        }
        return false;
    }

    public static Path discoverConfigFilePath() {
        String envPath = System.getenv(RAWWISE_CONFIG_FILE_ENV);
        if (envPath != null && !envPath.isEmpty()) {
            Path p = Path.of(envPath);
            if (Files.exists(p)) {
                return p;
            }
            log.warn("Config file from {} env var not found: {}", RAWWISE_CONFIG_FILE_ENV, envPath);
        }

        String rawwiseHome = System.getenv(RAWWISE_HOME_ENV);
        if (rawwiseHome != null && !rawwiseHome.isEmpty()) {
            Path p = Path.of(rawwiseHome, "conf", CONFIG_FILE_NAME);
            if (Files.exists(p)) {
                return p;
            }
        }

        Path cwdPath = Path.of(CONFIG_FILE_NAME);
        if (Files.exists(cwdPath)) {
            return cwdPath;
        }

        return null;
    }

    private Path resolveConfigFile() {
        if (explicitConfigFile != null) {
            if (Files.exists(explicitConfigFile)) {
                return explicitConfigFile;
            }
            throw new ConfigException("Explicit config file not found: " + explicitConfigFile);
        }

        Path discovered = discoverConfigFilePath();
        if (discovered != null) {
            return discovered;
        }

        return null;
    }

    private void checkFilePermissions(Path path) {
        try {
            if (!java.io.File.separator.equals("/")) {
                return;
            }
            if (!Files.exists(path)) {
                return;
            }
            Set<java.nio.file.attribute.PosixFilePermission> perms =
                    Files.getPosixFilePermissions(path);
            if (perms.contains(java.nio.file.attribute.PosixFilePermission.OTHERS_READ)) {
                String msg =
                        "Config file "
                                + path
                                + " is world-readable. "
                                + "Consider restricting permissions (e.g., chmod 600).";
                if (strictMode) {
                    throw new ConfigException.ValidationError(msg);
                }
                log.warn(msg);
            }
        } catch (ConfigException.ValidationError e) {
            throw e;
        } catch (Exception e) {
            log.debug("Could not check file permissions for {}", path, e);
        }
    }

    private Map<String, String> resolveEnvVars() {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("RAWWISE_") && !CONTROL_ENV_VARS.contains(key)) {
                String configKey = envVarToConfigKey(key);
                result.put(configKey, entry.getValue());
            }
        }
        return result;
    }

    private String envVarToConfigKey(String envVar) {
        String suffix = envVar.substring("RAWWISE_".length());
        String withHyphens = suffix.replace("__", "-");
        String withDots = withHyphens.replace("_", ".");
        return "rawwise." + withDots.toLowerCase();
    }

    private Map<String, String> resolveSystemProps() {
        Map<String, String> result = new HashMap<>();
        System.getProperties().stringPropertyNames().stream()
                .filter(k -> k.startsWith(RAWWISE_PREFIX))
                .forEach(k -> result.put(k, System.getProperty(k)));
        return result;
    }

    private Map<String, String> resolveSparkConf() {
        Map<String, String> result = new HashMap<>();
        if (sparkConf == null) {
            return result;
        }
        scala.Tuple2<String, String>[] sparkPrefixEntries =
                sparkConf.getAllWithPrefix("spark.rawwise.");
        for (scala.Tuple2<String, String> entry : sparkPrefixEntries) {
            result.put("rawwise." + entry._1(), entry._2());
        }
        scala.Tuple2<String, String>[] directPrefixEntries = sparkConf.getAllWithPrefix("rawwise.");
        for (scala.Tuple2<String, String> entry : directPrefixEntries) {
            result.put("rawwise." + entry._1(), entry._2());
        }
        return result;
    }

    private <V> Map<String, V> canonicalize(Map<String, V> source) {
        Map<String, V> result = new HashMap<>();
        Map<String, String> aliases = ConfigFactory.getKeyAliases();
        for (Map.Entry<String, V> entry : source.entrySet()) {
            String canonicalKey = aliases.getOrDefault(entry.getKey(), entry.getKey());
            result.put(canonicalKey, entry.getValue());
        }
        return result;
    }

    private Map<String, Object> mergeConfigs(
            Map<String, String> cliOverrides,
            Map<String, String> envConfig,
            Map<String, Object> fileConfig,
            Map<String, String> sparkConfig,
            Map<String, String> systemPropsConfig) {

        Map<String, Object> result = new HashMap<>();

        // Priority order (lowest to highest): systemProps -> spark -> file -> env -> cli
        for (Map.Entry<String, String> entry : canonicalize(systemPropsConfig).entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, String> entry : canonicalize(sparkConfig).entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Object> entry : canonicalize(fileConfig).entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, String> entry : canonicalize(envConfig).entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, String> entry : canonicalize(cliOverrides).entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    private void detectUnknownKeys(Map<String, Object> merged) {
        Set<String> knownKeys = RawWiseOptions.allKeys();
        Set<String> aliasKeys = ConfigFactory.getKeyAliases().keySet();

        Set<String> unknownKeys =
                merged.keySet().stream()
                        .filter(
                                k ->
                                        k.startsWith(RAWWISE_PREFIX)
                                                || k.startsWith(SPARK_RAWWISE_PREFIX))
                        .filter(k -> !knownKeys.contains(k))
                        .filter(k -> !aliasKeys.contains(k))
                        .collect(Collectors.toSet());

        if (!unknownKeys.isEmpty()) {
            if (strictMode) {
                throw new ConfigException.UnknownKeyError(
                        "Unknown config keys: "
                                + unknownKeys
                                + ". Check spelling or remove deprecated keys.");
            } else {
                log.warn(
                        "Unknown config keys ignored (enable strict-mode to error): {}",
                        unknownKeys);
            }
        }
    }
}
