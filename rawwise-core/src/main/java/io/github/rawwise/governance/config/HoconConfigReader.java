package io.github.rawwise.governance.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HoconConfigReader {

    static Map<String, Object> read(Path path) {
        String content = readFile(path);
        if (content.isBlank()) {
            throw new ConfigException.ParseError("Config file is empty: " + path);
        }
        Config typesafeConfig = parseHocon(content, path.toString());
        return extractValues(typesafeConfig);
    }

    static Map<String, Object> readResource(String resourcePath) {
        Config typesafeConfig;
        try {
            typesafeConfig =
                    com.typesafe.config.ConfigFactory.parseResourcesAnySyntax(
                                    HoconConfigReader.class.getClassLoader(), resourcePath)
                            .resolve();
        } catch (com.typesafe.config.ConfigException.UnresolvedSubstitution e) {
            throw new ConfigException.ParseError(
                    "Unresolved variable substitution in HOCON resource '"
                            + resourcePath
                            + "': "
                            + e.getMessage(),
                    e);
        } catch (com.typesafe.config.ConfigException e) {
            throw new ConfigException.ParseError(
                    "Failed to parse HOCON resource: " + resourcePath, e);
        }
        return extractValues(typesafeConfig);
    }

    static Map<String, Object> readString(String content) {
        if (content.isBlank()) {
            throw new ConfigException.ParseError("Config content is empty");
        }
        Config typesafeConfig = parseHocon(content, "<string>");
        return extractValues(typesafeConfig);
    }

    private static String readFile(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new ConfigException.ParseError("Failed to read config file: " + path, e);
        }
    }

    private static Config parseHocon(String content, String source) {
        try {
            return com.typesafe.config.ConfigFactory.parseString(content).resolve();
        } catch (com.typesafe.config.ConfigException.UnresolvedSubstitution e) {
            throw new ConfigException.ParseError(
                    "Unresolved variable substitution in " + source + ": " + e.getMessage(), e);
        } catch (com.typesafe.config.ConfigException.Parse e) {
            throw new ConfigException.ParseError("HOCON parse error in " + source, e);
        } catch (StackOverflowError e) {
            throw new ConfigException.ParseError(
                    "StackOverflow during HOCON parsing (likely deeply nested config) in " + source,
                    e);
        }
    }

    private static Map<String, Object> extractValues(Config config) {
        Map<String, Object> result = new HashMap<>();
        flattenConfig(config.root(), "", result);
        return result;
    }

    private static void flattenConfig(ConfigObject obj, String prefix, Map<String, Object> result) {
        for (Map.Entry<String, ConfigValue> entry : obj.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            ConfigValue value = entry.getValue();
            if (value.valueType() == ConfigValueType.OBJECT) {
                flattenConfig((ConfigObject) value, key, result);
            } else if (value.valueType() == ConfigValueType.NULL) {
                // skip null values so defaults can be applied
            } else {
                result.put(key, convertValue(value, key));
            }
        }
    }

    private static Object convertValue(ConfigValue value, String key) {
        return switch (value.valueType()) {
            case STRING -> {
                Object unwrapped = value.unwrapped();
                String str = (String) unwrapped;
                Duration dur = HoconParsers.parseDuration(str);
                if (dur != null) {
                    yield dur;
                }
                Long mem = HoconParsers.parseMemorySize(str);
                if (mem != null) {
                    yield mem;
                }
                yield unwrapped;
            }
            case NUMBER -> {
                Number num = (Number) value.unwrapped();
                if (num instanceof Integer) {
                    yield num.intValue();
                } else if (num instanceof Long) {
                    yield num.longValue();
                } else {
                    yield num.doubleValue();
                }
            }
            case BOOLEAN -> value.unwrapped();
            case LIST -> {
                ConfigList list = (ConfigList) value;
                List<Object> items = new ArrayList<>(list.size());
                for (ConfigValue item : list) {
                    items.add(item.unwrapped());
                }
                yield List.copyOf(items);
            }
            default -> value.unwrapped();
        };
    }
}
