package io.github.rawwise.axiom.pipeline.state;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GuardExpressionEvaluator {

    private static final Pattern COMPARISON_PATTERN =
            Pattern.compile("^\\s*(\\w+)\\s*(>=|<=|==|!=|>|<)\\s*(\\S+)\\s*$");

    public boolean evaluate(String expression, Map<String, String> metadata) {
        if (expression == null || expression.isBlank()) {
            return true;
        }

        Matcher matcher = COMPARISON_PATTERN.matcher(expression);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Malformed guard expression: " + expression);
        }

        String key = matcher.group(1);
        String operator = matcher.group(2);
        String expectedValue = matcher.group(3);

        String actualValue = metadata.get(key);
        if (actualValue == null) {
            return false;
        }

        return switch (operator) {
            case "==" -> actualValue.equals(expectedValue);
            case "!=" -> !actualValue.equals(expectedValue);
            case ">", ">=", "<", "<=" -> compareNumeric(actualValue, operator, expectedValue);
            default -> false;
        };
    }

    private boolean compareNumeric(String actual, String operator, String expected) {
        try {
            double actualNum = Double.parseDouble(actual);
            double expectedNum = Double.parseDouble(expected);
            if (!Double.isFinite(actualNum) || !Double.isFinite(expectedNum)) {
                return false;
            }
            return switch (operator) {
                case ">" -> actualNum > expectedNum;
                case ">=" -> actualNum >= expectedNum;
                case "<" -> actualNum < expectedNum;
                case "<=" -> actualNum <= expectedNum;
                default -> false;
            };
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
