package io.github.rawwise.axiom.pipeline.derivation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultSemanticPatternRenderer implements SemanticPatternRenderer {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)\\}");

    @Override
    public String render(String pattern, Map<String, Object> fields) {
        if (pattern == null || pattern.isEmpty()) {
            return "";
        }
        if (fields == null || fields.isEmpty()) {
            return pattern;
        }
        Matcher m = PLACEHOLDER.matcher(pattern);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String expr = m.group(1).trim();
            String replacement = evaluate(expr, fields);
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    @Override
    public String inferDefaultPattern(String tableId, List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append("，");
            }
            String col = columns.get(i);
            sb.append(col).append("为${").append(col).append("}");
        }
        sb.append("的记录");
        return sb.toString();
    }

    private String evaluate(String expr, Map<String, Object> fields) {
        if (expr.startsWith("join(") && expr.endsWith(")")) {
            return evalJoin(expr.substring(5, expr.length() - 1), fields);
        }
        if (expr.startsWith("if(") && expr.endsWith(")")) {
            return evalIf(expr.substring(3, expr.length() - 1), fields);
        }
        int colon = expr.indexOf(':');
        String field = colon > 0 ? expr.substring(0, colon).trim() : expr.trim();
        return stringify(fields.get(field));
    }

    private String evalJoin(String args, Map<String, Object> fields) {
        int comma = args.indexOf(',');
        if (comma < 0) {
            return stringify(fields.get(args.trim()));
        }
        String field = args.substring(0, comma).trim();
        String sep = stripQuotes(args.substring(comma + 1).trim());
        Object val = fields.get(field);
        if (val instanceof Iterable<?> it) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Object o : it) {
                if (!first) {
                    sb.append(sep);
                }
                sb.append(String.valueOf(o));
                first = false;
            }
            return sb.toString();
        }
        if (val instanceof Object[] arr) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Object o : arr) {
                if (!first) {
                    sb.append(sep);
                }
                sb.append(String.valueOf(o));
                first = false;
            }
            return sb.toString();
        }
        return stringify(val);
    }

    private String evalIf(String args, Map<String, Object> fields) {
        List<String> parts = splitArgs(args);
        if (parts.size() < 3) {
            return "";
        }
        String field = parts.get(0).trim();
        String trueVal = stripQuotes(parts.get(1).trim());
        String falseVal = stripQuotes(parts.get(2).trim());
        Object val = fields.get(field);
        return isPresent(val) ? trueVal : falseVal;
    }

    private static boolean isPresent(Object val) {
        if (val == null) {
            return false;
        }
        if (val instanceof String s) {
            return !s.isEmpty();
        }
        if (val instanceof Collection<?> c) {
            return !c.isEmpty();
        }
        if (val instanceof Object[] arr) {
            return arr.length > 0;
        }
        return true;
    }

    private static String stringify(Object val) {
        return val == null ? "" : String.valueOf(val);
    }

    private static String stripQuotes(String s) {
        if (s.length() >= 2) {
            char first = s.charAt(0);
            char last = s.charAt(s.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }

    private static List<String> splitArgs(String s) {
        List<String> parts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuote = false;
        char quoteChar = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (inQuote) {
                cur.append(c);
                if (c == quoteChar) {
                    inQuote = false;
                }
            } else if (c == '"' || c == '\'') {
                inQuote = true;
                quoteChar = c;
                cur.append(c);
            } else if (c == ',') {
                parts.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        parts.add(cur.toString());
        return parts;
    }
}
