package io.github.rawwise.governance.config;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class HoconParsers {

    static final Pattern DURATION_PATTERN =
            Pattern.compile("(?i)^\\s*(\\d+)\\s*(ns|us|ms|s|m|h|d)\\s*$");
    static final Pattern MEMORY_PATTERN =
            Pattern.compile("(?i)^\\s*(\\d+)\\s*(b|kb|k|mb|m|gb|g|tb|t)\\s*$");

    static final long KB = 1024L;
    static final long MB = KB * 1024L;
    static final long GB = MB * 1024L;
    static final long TB = GB * 1024L;

    static Duration parseDuration(String value) {
        Matcher m = DURATION_PATTERN.matcher(value);
        if (!m.matches()) {
            return null;
        }
        long amount;
        try {
            amount = Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            throw new ConfigException.TypeCoercionError(
                    "Duration value '" + value + "' overflows Long range", e);
        }
        String unit = m.group(2).toLowerCase();
        try {
            return switch (unit) {
                case "ns" -> Duration.ofNanos(amount);
                case "us" -> Duration.ofNanos(Math.multiplyExact(amount, 1000L));
                case "ms" -> Duration.ofMillis(amount);
                case "s" -> Duration.ofSeconds(amount);
                case "m" -> Duration.ofMinutes(amount);
                case "h" -> Duration.ofHours(amount);
                case "d" -> Duration.ofDays(amount);
                default -> null;
            };
        } catch (ArithmeticException e) {
            throw new ConfigException.TypeCoercionError(
                    "Duration value '" + value + "' overflows Long range", e);
        }
    }

    static Long parseMemorySize(String value) {
        Matcher m = MEMORY_PATTERN.matcher(value);
        if (!m.matches()) {
            return null;
        }
        long amount;
        try {
            amount = Long.parseLong(m.group(1));
        } catch (NumberFormatException e) {
            throw new ConfigException.TypeCoercionError(
                    "Memory size value '" + value + "' overflows Long range", e);
        }
        String unit = m.group(2).toLowerCase();
        long multiplier =
                switch (unit) {
                    case "b" -> 1L;
                    case "k", "kb" -> KB;
                    case "m", "mb" -> MB;
                    case "g", "gb" -> GB;
                    case "t", "tb" -> TB;
                    default -> 1L;
                };
        if (amount > Long.MAX_VALUE / multiplier) {
            throw new ConfigException.TypeCoercionError(
                    "Memory size value '" + value + "' overflows Long range");
        }
        return amount * multiplier;
    }

    private HoconParsers() {}
}
