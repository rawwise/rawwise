package io.github.rawwise.model.expression.serializer;

import io.github.rawwise.meta.ExprType;
import io.github.rawwise.meta.PbExpression;
import io.github.rawwise.meta.PbLiteralValue;
import io.github.rawwise.model.expression.*;
import io.github.rawwise.model.expression.assignment.*;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ProtobufExpressionSerializer implements ExpressionSerializer {

    private static final ProtobufExpressionSerializer INSTANCE = new ProtobufExpressionSerializer();

    private ProtobufExpressionSerializer() {}

    public static ProtobufExpressionSerializer getInstance() {
        return INSTANCE;
    }

    @Override
    public byte[] serialize(io.github.rawwise.model.expression.Expression expression) {
        return toBytes(expression);
    }

    @Override
    public io.github.rawwise.model.expression.Expression deserialize(byte[] bytes) {
        return fromBytes(bytes);
    }

    public static PbExpression toProto(io.github.rawwise.model.expression.Expression expr) {
        if (expr == null) return null;

        var builder = PbExpression.newBuilder().setExprType(toProtoExprType(expr.getType()));

        switch (expr) {
            case Literal literal -> {
                builder.setLiteral(toProtoLiteral(literal));
            }
            case ColumnRef columnRef -> {
                builder.setColumnRef(columnRef.getColumnName());
            }
            case BinaryExpression binary -> {
                builder.addChildren(toProto(binary.getLeft()));
                builder.addChildren(toProto(binary.getRight()));
            }
            case LogicalExpression logical -> {
                for (io.github.rawwise.model.expression.Expression child : logical.children()) {
                    builder.addChildren(toProto(child));
                }
            }
            case Between between -> {
                builder.addChildren(toProto(between.getLeft()));
                builder.addChildren(toProto(between.getLower()));
                builder.addChildren(toProto(between.getUpper()));
            }
            case In inExpr -> {
                builder.addChildren(toProto(inExpr.getLeft()));
                for (Comparable<?> value : inExpr.getValues()) {
                    builder.addChildren(toProto(new Literal(value)));
                }
            }
            case NotIn notIn -> {
                builder.addChildren(toProto(notIn.getLeft()));
                for (Comparable<?> value : notIn.getValues()) {
                    builder.addChildren(toProto(new Literal(value)));
                }
            }
            case Like like -> {
                builder.addChildren(toProto(like.getLeft()));
                builder.addChildren(toProto(new Literal(like.getPattern())));
            }
            case StartWith startWith -> {
                builder.addChildren(toProto(startWith.getLeft()));
                builder.addChildren(toProto(new Literal(startWith.getPrefix())));
            }
            case EndWith endWith -> {
                builder.addChildren(toProto(endWith.getLeft()));
                builder.addChildren(toProto(new Literal(endWith.getSuffix())));
            }
            case Contains contains -> {
                builder.addChildren(toProto(contains.getLeft()));
                builder.addChildren(toProto(new Literal(contains.getSubstring())));
            }
            case IsNull isNull -> {
                builder.addChildren(toProto(isNull.getColumn()));
            }
            case IsNotNull isNotNull -> {
                builder.addChildren(toProto(isNotNull.getColumn()));
            }
            case Direct direct -> {
                builder.putMetadata("direct_value", String.valueOf(direct.alwaysTrue()));
            }
            case ArithmeticBase arithmetic -> {
                builder.putMetadata("target_column", arithmetic.getTargetColumn());
                builder.addChildren(toProto(arithmetic.getLeftOperand()));
                builder.addChildren(toProto(arithmetic.getRightOperand()));
            }
            default -> log.warn("Unhandled expression type: {}", expr.getType());
        }

        return builder.build();
    }

    public static io.github.rawwise.model.expression.Expression fromProto(PbExpression proto) {
        if (proto == null) return null;

        ExprType type = proto.getExprType();

        return switch (type) {
            case LITERAL -> fromProtoLiteral(proto.getLiteral());
            case COLUMN_REF -> new ColumnRef(proto.getColumnRef());

            case EQUAL,
                            NOT_EQUAL,
                            GREATER_THAN,
                            GREATER_THAN_OR_EQUAL,
                            LESS_THAN,
                            LESS_THAN_OR_EQUAL ->
                    fromBinaryComparison(type, proto.getChildrenList());

            case IN -> fromIn(proto.getChildrenList(), false);
            case NOT_IN -> fromIn(proto.getChildrenList(), true);
            case AND -> new And(fromProtoList(proto.getChildrenList()));
            case OR -> new Or(fromProtoList(proto.getChildrenList()));
            case NOT -> new Not(fromProtoSingle(proto.getChildrenList()));
            case BETWEEN -> fromBetween(proto.getChildrenList());
            case LIKE -> fromLike(proto.getChildrenList());
            case STARTS_WITH -> fromStartsWith(proto.getChildrenList());
            case ENDS_WITH -> fromEndsWith(proto.getChildrenList());
            case CONTAINS -> fromContains(proto.getChildrenList());
            case IS_NULL -> new IsNull(fromProtoSingle(proto.getChildrenList()));
            case IS_NOT_NULL -> new IsNotNull(fromProtoSingle(proto.getChildrenList()));
            case DIRECT ->
                    new Direct(
                            Boolean.parseBoolean(
                                    proto.getMetadataOrDefault("direct_value", "true")));

            case ARITHMETIC_ADD, ARITHMETIC_SUBTRACT, ARITHMETIC_MULTIPLY, ARITHMETIC_DIVIDE ->
                    fromArithmetic(type, proto);

            default -> throw new IllegalArgumentException("Unknown expression type: " + type);
        };
    }

    public static byte[] toBytes(io.github.rawwise.model.expression.Expression expr) {
        return toProto(expr).toByteArray();
    }

    public static io.github.rawwise.model.expression.Expression fromBytes(byte[] data) {
        try {
            return fromProto(PbExpression.parseFrom(data));
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize Expression", e);
        }
    }

    private static ExprType toProtoExprType(
            io.github.rawwise.model.expression.Expression.ExpressionType type) {
        return switch (type) {
            case LITERAL -> ExprType.LITERAL;
            case COLUMN_REF -> ExprType.COLUMN_REF;
            case EQUAL -> ExprType.EQUAL;
            case NOT_EQUAL -> ExprType.NOT_EQUAL;
            case GREATER_THAN -> ExprType.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL -> ExprType.GREATER_THAN_OR_EQUAL;
            case LESS_THAN -> ExprType.LESS_THAN;
            case LESS_THAN_OR_EQUAL -> ExprType.LESS_THAN_OR_EQUAL;
            case IN -> ExprType.IN;
            case NOT_IN -> ExprType.NOT_IN;
            case AND -> ExprType.AND;
            case OR -> ExprType.OR;
            case NOT -> ExprType.NOT;
            case BETWEEN -> ExprType.BETWEEN;
            case LIKE -> ExprType.LIKE;
            case STARTS_WITH -> ExprType.STARTS_WITH;
            case ENDS_WITH -> ExprType.ENDS_WITH;
            case CONTAINS -> ExprType.CONTAINS;
            case IS_NULL -> ExprType.IS_NULL;
            case IS_NOT_NULL -> ExprType.IS_NOT_NULL;
            case DIRECT -> ExprType.DIRECT;
            case ARITHMETIC_ADD -> ExprType.ARITHMETIC_ADD;
            case ARITHMETIC_SUBTRACT -> ExprType.ARITHMETIC_SUBTRACT;
            case ARITHMETIC_MULTIPLY -> ExprType.ARITHMETIC_MULTIPLY;
            case ARITHMETIC_DIVIDE -> ExprType.ARITHMETIC_DIVIDE;
            default -> ExprType.EXPR_TYPE_UNSPECIFIED;
        };
    }

    private static PbLiteralValue toProtoLiteral(Literal literal) {
        Comparable value = literal.getValue();
        var builder = PbLiteralValue.newBuilder();
        if (value == null) {
            return builder.build();
        }

        if (value instanceof Boolean b) {
            return builder.setBoolVal(b).build();
        } else if (value instanceof Integer i) {
            return builder.setInt32Val(i).build();
        } else if (value instanceof Long l) {
            return builder.setInt64Val(l).build();
        } else if (value instanceof Float f) {
            return builder.setFloatVal(f).build();
        } else if (value instanceof Double d) {
            return builder.setDoubleVal(d).build();
        } else if (value instanceof String s) {
            return builder.setStringVal(s).build();
        } else {
            return builder.setStringVal(String.valueOf(value)).build();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T fromProtoLiteralValue(PbLiteralValue literal) {
        return switch (literal.getValueCase()) {
            case BOOL_VAL -> (T) Boolean.valueOf(literal.getBoolVal());
            case INT32_VAL -> (T) Integer.valueOf(literal.getInt32Val());
            case INT64_VAL -> (T) Long.valueOf(literal.getInt64Val());
            case FLOAT_VAL -> (T) Float.valueOf(literal.getFloatVal());
            case DOUBLE_VAL -> (T) Double.valueOf(literal.getDoubleVal());
            case STRING_VAL -> (T) literal.getStringVal();
            case BYTES_VAL -> (T) literal.getBytesVal().toByteArray();
            default -> null;
        };
    }

    private static Literal fromProtoLiteral(PbLiteralValue proto) {
        return new Literal(fromProtoLiteralValue(proto));
    }

    private static List<io.github.rawwise.model.expression.Expression> fromProtoList(
            List<PbExpression> list) {
        List<io.github.rawwise.model.expression.Expression> result = new ArrayList<>();
        for (PbExpression expr : list) {
            result.add(fromProto(expr));
        }
        return result;
    }

    private static io.github.rawwise.model.expression.Expression fromProtoSingle(
            List<PbExpression> list) {
        if (list.isEmpty()) return null;
        return fromProto(list.getFirst());
    }

    private static BinaryComparison fromBinaryComparison(
            ExprType type, List<PbExpression> children) {
        if (children.size() < 2) {
            throw new IllegalArgumentException("Binary comparison needs 2 children");
        }
        var left = fromProto(children.get(0));
        var right = fromProto(children.get(1));

        return switch (type) {
            case EQUAL -> new Eq(left, right);
            case NOT_EQUAL -> new NotEq(left, right);
            case GREATER_THAN -> new Gt(left, right);
            case GREATER_THAN_OR_EQUAL -> new GtEq(left, right);
            case LESS_THAN -> new Lt(left, right);
            case LESS_THAN_OR_EQUAL -> new LtEq(left, right);
            default -> throw new IllegalArgumentException("Not a binary comparison: " + type);
        };
    }

    private static io.github.rawwise.model.expression.Expression fromIn(
            List<PbExpression> children, boolean negated) {
        if (children.isEmpty()) return null;
        var column = fromProto(children.get(0));
        Set<Comparable<?>> values = new LinkedHashSet<>();
        for (int i = 1; i < children.size(); i++) {
            var lit = fromProto(children.get(i));
            if (lit instanceof Literal l && l.getValue() != null) {
                values.add(l.getValue());
            }
        }
        return negated ? new NotIn(column, values) : new In(column, values);
    }

    private static Between fromBetween(List<PbExpression> children) {
        if (children.size() < 3) {
            throw new IllegalArgumentException("Between needs 3 children");
        }
        return new Between(
                fromProto(children.get(0)), fromProto(children.get(1)), fromProto(children.get(2)));
    }

    private static Like fromLike(List<PbExpression> children) {
        if (children.size() < 2) {
            throw new IllegalArgumentException("Like needs 2 children");
        }
        var column = fromProto(children.get(0));
        String pattern = extractString(children.get(1));
        return new Like(column, pattern);
    }

    private static StartWith fromStartsWith(List<PbExpression> children) {
        if (children.size() < 2) {
            throw new IllegalArgumentException("StartsWith needs 2 children");
        }
        var column = fromProto(children.get(0));
        String prefix = extractString(children.get(1));
        return new StartWith(column, prefix);
    }

    private static EndWith fromEndsWith(List<PbExpression> children) {
        if (children.size() < 2) {
            throw new IllegalArgumentException("EndsWith needs 2 children");
        }
        var column = fromProto(children.get(0));
        String suffix = extractString(children.get(1));
        return new EndWith(column, suffix);
    }

    private static Contains fromContains(List<PbExpression> children) {
        if (children.size() < 2) {
            throw new IllegalArgumentException("Contains needs 2 children");
        }
        var column = fromProto(children.get(0));
        String substring = extractString(children.get(1));
        return new Contains(column, substring);
    }

    private static String extractString(PbExpression proto) {
        var expr = fromProto(proto);
        if (expr instanceof Literal l && l.getValue() instanceof String s) {
            return s;
        }
        return String.valueOf(expr);
    }

    private static Assignment fromArithmetic(ExprType type, PbExpression proto) {
        String targetColumn = proto.getMetadataOrDefault("target_column", "");
        List<PbExpression> children = proto.getChildrenList();
        if (children.size() < 2) {
            throw new IllegalArgumentException("Arithmetic needs 2 operand children");
        }
        var left = fromProto(children.get(0));
        var right = fromProto(children.get(1));

        return switch (type) {
            case ARITHMETIC_ADD -> new Add(targetColumn, left, right);
            case ARITHMETIC_SUBTRACT -> new Subtract(targetColumn, left, right);
            case ARITHMETIC_MULTIPLY -> new Multiply(targetColumn, left, right);
            case ARITHMETIC_DIVIDE -> new Divide(targetColumn, left, right);
            default -> throw new IllegalArgumentException("Unknown arithmetic type: " + type);
        };
    }
}
