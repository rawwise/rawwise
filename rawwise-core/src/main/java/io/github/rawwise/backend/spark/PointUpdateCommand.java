package io.github.rawwise.backend.spark;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LeafCommand;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import scala.Enumeration;
import scala.collection.immutable.IndexedSeq;
import scala.collection.immutable.Seq;

public final class PointUpdateCommand extends LogicalPlan implements LeafCommand {

    private final String tableIdentifier;
    private final String primaryKeyValuesJson;
    private final String updatesJson;
    private final StructType tableSchema;
    private Seq<Enumeration.Value> nodePatterns;

    public PointUpdateCommand(
            String tableIdentifier,
            Map<String, Comparable<?>> primaryKeyValues,
            Map<String, Comparable<?>> updates,
            StructType tableSchema) {
        this.tableIdentifier = tableIdentifier;
        this.primaryKeyValuesJson = serializeMap(primaryKeyValues);
        this.updatesJson = serializeMap(updates);
        this.tableSchema = tableSchema;
        this.nodePatterns = asScalaSeq(java.util.List.<Enumeration.Value>of());
    }

    @Override
    public Seq<Enumeration.Value> nodePatterns() {
        return nodePatterns;
    }

    @Override
    public void org$apache$spark$sql$catalyst$plans$logical$Command$_setter_$nodePatterns_$eq(
            Seq<Enumeration.Value> x$1) {
        this.nodePatterns = x$1;
    }

    public String tableIdentifier() {
        return tableIdentifier;
    }

    public String primaryKeyValuesJson() {
        return primaryKeyValuesJson;
    }

    public String updatesJson() {
        return updatesJson;
    }

    public StructType tableSchema() {
        return tableSchema;
    }

    static String serializeMap(Map<String, Comparable<?>> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Comparable<?>> entry : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"');
            sb.append(escapeJson(entry.getKey()));
            sb.append('"');
            sb.append(':');
            Object value = entry.getValue();
            if (value == null) {
                sb.append("null");
            } else if (value instanceof Number) {
                sb.append(value);
            } else if (value instanceof Boolean) {
                sb.append(value);
            } else {
                sb.append('"');
                sb.append(escapeJson(value.toString()));
                sb.append('"');
            }
        }
        sb.append('}');
        return sb.toString();
    }

    static Map<String, Comparable<?>> deserializeMap(String json) {
        Map<String, Comparable<?>> result = new LinkedHashMap<>();
        if (json == null || json.isEmpty() || json.equals("{}")) {
            return result;
        }
        String content = json.trim();
        if (!content.startsWith("{") || !content.endsWith("}")) {
            return result;
        }
        content = content.substring(1, content.length() - 1).trim();
        if (content.isEmpty()) {
            return result;
        }
        int i = 0;
        while (i < content.length()) {
            i = skipWhitespace(content, i);
            if (i >= content.length()) break;
            if (content.charAt(i) != '"') break;
            i++;
            StringBuilder key = new StringBuilder();
            while (i < content.length() && content.charAt(i) != '"') {
                if (content.charAt(i) == '\\') {
                    i++;
                    if (i < content.length()) {
                        key.append(unescapeChar(content.charAt(i)));
                    }
                } else {
                    key.append(content.charAt(i));
                }
                i++;
            }
            i++;
            i = skipWhitespace(content, i);
            if (i >= content.length() || content.charAt(i) != ':') break;
            i++;
            i = skipWhitespace(content, i);
            if (i >= content.length()) break;
            Comparable<?> value;
            if (content.charAt(i) == '"') {
                i++;
                StringBuilder strVal = new StringBuilder();
                while (i < content.length() && content.charAt(i) != '"') {
                    if (content.charAt(i) == '\\') {
                        i++;
                        if (i < content.length()) {
                            strVal.append(unescapeChar(content.charAt(i)));
                        }
                    } else {
                        strVal.append(content.charAt(i));
                    }
                    i++;
                }
                i++;
                value = strVal.toString();
            } else if (content.charAt(i) == 't' || content.charAt(i) == 'f') {
                boolean boolVal = content.startsWith("true", i);
                value = boolVal;
                i += boolVal ? 4 : 5;
            } else if (content.charAt(i) == 'n') {
                value = null;
                i += 4;
            } else {
                StringBuilder numStr = new StringBuilder();
                boolean isDecimal = false;
                while (i < content.length()
                        && (Character.isDigit(content.charAt(i))
                                || content.charAt(i) == '-'
                                || content.charAt(i) == '+'
                                || content.charAt(i) == '.'
                                || content.charAt(i) == 'e'
                                || content.charAt(i) == 'E')) {
                    if (content.charAt(i) == '.'
                            || content.charAt(i) == 'e'
                            || content.charAt(i) == 'E') {
                        isDecimal = true;
                    }
                    numStr.append(content.charAt(i));
                    i++;
                }
                if (isDecimal) {
                    value = Double.parseDouble(numStr.toString());
                } else {
                    long longVal = Long.parseLong(numStr.toString());
                    if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
                        value = (int) longVal;
                    } else {
                        value = longVal;
                    }
                }
            }
            result.put(key.toString(), value);
            i = skipWhitespace(content, i);
            if (i < content.length() && content.charAt(i) == ',') {
                i++;
            }
        }
        return result;
    }

    private static int skipWhitespace(String s, int i) {
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }

    private static char unescapeChar(char c) {
        return switch (c) {
            case 'n' -> '\n';
            case 'r' -> '\r';
            case 't' -> '\t';
            case 'b' -> '\b';
            case 'f' -> '\f';
            case '\\' -> '\\';
            case '"' -> '"';
            case '/' -> '/';
            default -> c;
        };
    }

    private static String escapeJson(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static <T> Seq<T> asScalaSeq(java.util.List<T> javaList) {
        return scala.collection.immutable.List.from(
                scala.jdk.javaapi.CollectionConverters.asScala(javaList).toSeq());
    }

    @Override
    public Seq<Attribute> output() {
        return asScalaSeq(
                java.util.List.of(
                        (Attribute)
                                new AttributeReference(
                                        "num_affected_rows",
                                        LongType$.MODULE$,
                                        false,
                                        Metadata.empty(),
                                        NamedExpression.newExprId(),
                                        asScalaSeq(java.util.List.<String>of()))));
    }

    @Override
    public Seq<LogicalPlan> children() {
        return asScalaSeq(java.util.List.<LogicalPlan>of());
    }

    @Override
    public LogicalPlan withNewChildrenInternal(IndexedSeq<LogicalPlan> newChildren) {
        return this;
    }

    @Override
    public Object productElement(int n) {
        return switch (n) {
            case 0 -> tableIdentifier;
            case 1 -> primaryKeyValuesJson;
            case 2 -> updatesJson;
            case 3 -> tableSchema;
            default -> throw new IndexOutOfBoundsException(Integer.toString(n));
        };
    }

    @Override
    public int productArity() {
        return 4;
    }

    @Override
    public boolean canEqual(Object that) {
        return that instanceof PointUpdateCommand;
    }

    @Override
    public String productPrefix() {
        return "PointUpdateCommand";
    }
}
