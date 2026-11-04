package io.github.rawwise.client.repl;

import static io.github.rawwise.client.repl.AnsiColors.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.rawwise.frontend.api.dto.ColumnInfoDto;
import io.github.rawwise.frontend.api.dto.QueryResultDto;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ResultFormatter {

    private static final int MAX_COLUMN_WIDTH = 50;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String format(QueryResultDto result, ReplSession.Format format) {
        if (result == null) {
            return "";
        }
        return switch (format) {
            case TABLE -> formatTable(result);
            case CSV -> formatCsv(result);
            case JSON -> formatJson(result);
        };
    }

    private static String formatTable(QueryResultDto result) {
        List<String> headers = result.getColumns().stream().map(ColumnInfoDto::getName).toList();

        int colCount = headers.size();
        int[] widths = new int[colCount];

        for (int i = 0; i < colCount; i++) {
            widths[i] = displayWidth(headers.get(i));
        }

        if (result.getRows() != null) {
            for (List<Object> row : result.getRows()) {
                for (int i = 0; i < colCount && i < row.size(); i++) {
                    String val = formatValue(row.get(i));
                    int dw = displayWidth(val);
                    if (dw > widths[i]) {
                        widths[i] = Math.min(dw, MAX_COLUMN_WIDTH);
                    }
                }
            }
        }

        StringBuilder sb = new StringBuilder();

        sb.append(buildBorder(widths, '┌', '┬', '┐'));
        sb.append(buildHeaderRow(headers, widths));
        sb.append(buildBorder(widths, '├', '┼', '┤'));

        if (result.getRows() != null) {
            for (List<Object> row : result.getRows()) {
                List<String> values =
                        row.stream().map(v -> truncate(formatValue(v), MAX_COLUMN_WIDTH)).toList();
                sb.append(buildRow(values, widths));
            }
        }

        sb.append(buildBorder(widths, '└', '┴', '┘'));

        return sb.toString();
    }

    private static String buildBorder(int[] widths, char left, char mid, char right) {
        StringBuilder sb = new StringBuilder();
        sb.append(DIM);
        sb.append(left);
        for (int i = 0; i < widths.length; i++) {
            sb.append("─".repeat(widths[i] + 2));
            if (i < widths.length - 1) sb.append(mid);
        }
        sb.append(right);
        sb.append(RESET);
        sb.append('\n');
        return sb.toString();
    }

    private static String buildHeaderRow(List<String> values, int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append(DIM).append('│').append(RESET);
        for (int i = 0; i < widths.length; i++) {
            String val = i < values.size() ? values.get(i) : "";
            sb.append(' ')
                    .append(BRIGHT_MAGENTA)
                    .append(BOLD)
                    .append(padRight(val, widths[i]))
                    .append(RESET)
                    .append(' ');
            sb.append(DIM).append('│').append(RESET);
        }
        sb.append('\n');
        return sb.toString();
    }

    private static String buildRow(List<String> values, int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append(DIM).append('│').append(RESET);
        for (int i = 0; i < widths.length; i++) {
            String val = i < values.size() ? values.get(i) : "";
            sb.append(' ').append(padRight(val, widths[i])).append(' ');
            sb.append(DIM).append('│').append(RESET);
        }
        sb.append('\n');
        return sb.toString();
    }

    static String formatValue(Object v) {
        if (v == null) return "NULL";
        return String.valueOf(v);
    }

    static int displayWidth(String s) {
        int width = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isSurrogate(c) && i + 1 < s.length()) {
                width += 2;
                i++;
            } else if (isFullWidth(c)) {
                width += 2;
            } else {
                width += 1;
            }
        }
        return width;
    }

    private static boolean isFullWidth(char c) {
        return (c >= '\u1100' && c <= '\u115F')
                || (c >= '\u2E80' && c <= '\u303E')
                || (c >= '\u3040' && c <= '\uA4CF')
                || (c >= '\uAC00' && c <= '\uD7A3')
                || (c >= '\uF900' && c <= '\uFAFF')
                || (c >= '\uFE10' && c <= '\uFE19')
                || (c >= '\uFE30' && c <= '\uFE6F')
                || (c >= '\uFF01' && c <= '\uFF60')
                || (c >= '\uFFE0' && c <= '\uFFE6');
    }

    private static String padRight(String s, int width) {
        int displayW = displayWidth(s);
        return s + " ".repeat(Math.max(0, width - displayW));
    }

    private static String truncate(String s, int maxLen) {
        if (displayWidth(s) <= maxLen) return s;
        StringBuilder sb = new StringBuilder();
        int w = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            int cw;
            if (Character.isSurrogate(c) && i + 1 < s.length()) {
                cw = 2;
                if (w + cw > maxLen - 3) break;
                sb.append(c);
                sb.append(s.charAt(++i));
            } else {
                cw = isFullWidth(c) ? 2 : 1;
                if (w + cw > maxLen - 3) break;
                sb.append(c);
            }
            w += cw;
        }
        sb.append("...");
        return sb.toString();
    }

    private static String formatCsv(QueryResultDto result) {
        StringBuilder sb = new StringBuilder();

        sb.append(
                result.getColumns().stream()
                        .map(ColumnInfoDto::getName)
                        .collect(Collectors.joining(",")));

        if (result.getRows() != null) {
            for (List<Object> row : result.getRows()) {
                sb.append('\n');
                sb.append(
                        row.stream()
                                .map(v -> escapeCsv(formatValue(v)))
                                .collect(Collectors.joining(",")));
            }
        }

        return sb.toString();
    }

    private static String escapeCsv(String value) {
        if (value.contains(",")
                || value.contains("\"")
                || value.contains("\n")
                || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private static String formatJson(QueryResultDto result) {
        try {
            List<Map<String, Object>> jsonRows = new ArrayList<>();
            if (result.getRows() != null) {
                for (List<Object> row : result.getRows()) {
                    Map<String, Object> jsonRow = new LinkedHashMap<>();
                    for (int j = 0; j < result.getColumns().size() && j < row.size(); j++) {
                        jsonRow.put(result.getColumns().get(j).getName(), row.get(j));
                    }
                    jsonRows.add(jsonRow);
                }
            }
            return MAPPER.writeValueAsString(jsonRows);
        } catch (Exception e) {
            return "[{\"error\":\"Failed to format JSON\"}]";
        }
    }
}
