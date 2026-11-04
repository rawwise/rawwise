package io.github.rawwise.model.expression;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.rawwise.model.record.MapRecord;
import io.github.rawwise.model.record.Record;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExpressionEvaluationTest {

    private Record makeRecord(Map<String, Object> data) {
        return MapRecord.fromMap(data);
    }

    @Nested
    @DisplayName("comparison Expression evaluation")
    class ComparisonEvaluation {

        @Test
        @DisplayName("Eq: returns when column value equals literaltrue")
        void testEqColumnEqualsLiteral() {
            Record row = makeRecord(Map.of("age", 25, "name", "Alice"));
            Expression expr = new Eq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Eq: columnvalue etc returns false")
        void testEqColumnNotEqualsLiteral() {
            Record row = makeRecord(Map.of("age", 30, "name", "Bob"));
            Expression expr = new Eq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("Eq: null value columnand comparisonreturns false")
        void testEqNullColumnReturnsFalse() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("age", null);
            data.put("name", "Alice");
            Record row = makeRecord(data);
            Expression expr = new Eq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("NotEq: columnvalue etc returns true")
        void testNotEqDifferentValues() {
            Record row = makeRecord(Map.of("age", 30));
            Expression expr = new NotEq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("NotEq: returns when column value equals literalfalse")
        void testNotEqSameValues() {
            Record row = makeRecord(Map.of("age", 25));
            Expression expr = new NotEq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("NotEq: null value columnand comparisonreturns true")
        void testNotEqNullColumnReturnsTrue() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("age", null);
            Record row = makeRecord(data);
            Expression expr = new NotEq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Gt: columnvalue returns true")
        void testGtColumnGreaterThanLiteral() {
            Record row = makeRecord(Map.of("age", 30));
            Expression expr = new Gt(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Gt: returns when column value equals literalfalse")
        void testGtColumnEqualReturnsFalse() {
            Record row = makeRecord(Map.of("age", 25));
            Expression expr = new Gt(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("GtEq: returns when column value equals literaltrue")
        void testGtEqColumnEqualReturnsTrue() {
            Record row = makeRecord(Map.of("age", 25));
            Expression expr = new GtEq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("GtEq: columnvalue returns true")
        void testGtEqColumnGreaterReturnsTrue() {
            Record row = makeRecord(Map.of("age", 30));
            Expression expr = new GtEq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Lt: columnvalue returns true")
        void testLtColumnLessThanLiteral() {
            Record row = makeRecord(Map.of("age", 20));
            Expression expr = new Lt(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("LtEq: returns when column value equals literaltrue")
        void testLtEqColumnEqualReturnsTrue() {
            Record row = makeRecord(Map.of("age", 25));
            Expression expr = new LtEq(new ColumnRef("age"), new Literal(25));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("comparison : null value columnand comparison returns false (Gt/GtEq/Lt/LtEq)")
        void testComparisonWithNullReturnsFalse() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("age", null);
            Record row = makeRecord(data);

            assertThat(new Gt(new ColumnRef("age"), new Literal(25)).evaluate(row)).isFalse();
            assertThat(new GtEq(new ColumnRef("age"), new Literal(25)).evaluate(row)).isFalse();
            assertThat(new Lt(new ColumnRef("age"), new Literal(25)).evaluate(row)).isFalse();
            assertThat(new LtEq(new ColumnRef("age"), new Literal(25)).evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("comparison : typecorrectly compares")
        void testStringComparison() {
            Record row = makeRecord(Map.of("name", "Charlie"));

            assertThat(new Eq(new ColumnRef("name"), new Literal("Charlie")).evaluate(row))
                    .isTrue();
            assertThat(new Gt(new ColumnRef("name"), new Literal("Alice")).evaluate(row)).isTrue();
            assertThat(new Lt(new ColumnRef("name"), new Literal("David")).evaluate(row)).isTrue();
        }
    }

    @Nested
    @DisplayName("logic Expression evaluation")
    class LogicalEvaluation {

        @Test
        @DisplayName("And: allsub-expressionwhen truereturns true")
        void testAndAllTrue() {
            Record row = makeRecord(Map.of("age", 30, "name", "Alice"));
            Expression expr =
                    new And(
                            new Eq(new ColumnRef("age"), new Literal(30)),
                            new Eq(new ColumnRef("name"), new Literal("Alice")));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("And: anysub-expressionwhen falsereturns false")
        void testAndOneFalse() {
            Record row = makeRecord(Map.of("age", 30, "name", "Bob"));
            Expression expr =
                    new And(
                            new Eq(new ColumnRef("age"), new Literal(30)),
                            new Eq(new ColumnRef("name"), new Literal("Alice")));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("And: short-circuit evaluation — firstwhen falsedoes not computesecond")
        void testAndShortCircuit() {
            Record row = makeRecord(Map.of("age", 20));
            Expression alwaysFalse = new Literal(false);
            Expression expr = new And(alwaysFalse, new Eq(new ColumnRef("age"), new Literal(20)));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("Or: anysub-expressionwhen truereturns true")
        void testOrOneTrue() {
            Record row = makeRecord(Map.of("age", 30, "name", "Bob"));
            Expression expr =
                    new Or(
                            new Eq(new ColumnRef("age"), new Literal(25)),
                            new Eq(new ColumnRef("name"), new Literal("Bob")));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Or: allsub-expressionwhen falsereturns false")
        void testOrAllFalse() {
            Record row = makeRecord(Map.of("age", 30, "name", "Bob"));
            Expression expr =
                    new Or(
                            new Eq(new ColumnRef("age"), new Literal(25)),
                            new Eq(new ColumnRef("name"), new Literal("Alice")));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("Or: short-circuit evaluation — firstwhen truedoes not computesecond")
        void testOrShortCircuit() {
            Record row = makeRecord(Map.of("age", 30));
            Expression alwaysTrue = new Literal(true);
            Expression expr = new Or(alwaysTrue, new Eq(new ColumnRef("age"), new Literal(999)));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Not: correct")
        void testNotNegation() {
            Record row = makeRecord(Map.of("age", 25));
            Expression eq = new Eq(new ColumnRef("age"), new Literal(25));

            assertThat(new Not(eq).evaluate(row)).isFalse();
            assertThat(new Not(new Not(eq)).evaluate(row)).isTrue();
        }
    }

    @Nested
    @DisplayName("Expression evaluation")
    class CollectionEvaluation {

        @Test
        @DisplayName("In: columnvaluein in returns true")
        void testInValueInSet() {
            Record row = makeRecord(Map.of("status", "active"));
            Expression expr =
                    new In(new ColumnRef("status"), Set.of("active", "pending", "completed"));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("In: columnvalue in in returns false")
        void testInValueNotInSet() {
            Record row = makeRecord(Map.of("status", "cancelled"));
            Expression expr =
                    new In(new ColumnRef("status"), Set.of("active", "pending", "completed"));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("In: null value columnreturns false")
        void testInNullColumnReturnsFalse() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("status", null);
            Record row = makeRecord(data);
            Expression expr = new In(new ColumnRef("status"), Set.of("active", "pending"));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("NotIn: columnvalue in in returns true")
        void testNotInValueNotInSet() {
            Record row = makeRecord(Map.of("status", "cancelled"));
            Expression expr = new NotIn(new ColumnRef("status"), Set.of("active", "pending"));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("NotIn: columnvaluein in returns false")
        void testNotInValueInSet() {
            Record row = makeRecord(Map.of("status", "active"));
            Expression expr = new NotIn(new ColumnRef("status"), Set.of("active", "pending"));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("NotIn: null value columnreturns true (null in has in)")
        void testNotInNullColumnReturnsTrue() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("status", null);
            Record row = makeRecord(data);
            Expression expr = new NotIn(new ColumnRef("status"), Set.of("active", "pending"));

            assertThat(expr.evaluate(row)).isTrue();
        }
    }

    @Nested
    @DisplayName("Expression evaluation")
    class RangeEvaluation {

        @Test
        @DisplayName("Between: column value within rangereturns true (inclusive boundary)")
        void testBetweenValueInRange() {
            Record row = makeRecord(Map.of("age", 25));
            Expression expr = new Between(new ColumnRef("age"), new Literal(20), new Literal(30));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Between: column value equals lower boundreturns true")
        void testBetweenValueAtLowerBound() {
            Record row = makeRecord(Map.of("age", 20));
            Expression expr = new Between(new ColumnRef("age"), new Literal(20), new Literal(30));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Between: column value equals upper boundreturns true")
        void testBetweenValueAtUpperBound() {
            Record row = makeRecord(Map.of("age", 30));
            Expression expr = new Between(new ColumnRef("age"), new Literal(20), new Literal(30));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Between: column value below lower boundreturns false")
        void testBetweenValueBelowRange() {
            Record row = makeRecord(Map.of("age", 15));
            Expression expr = new Between(new ColumnRef("age"), new Literal(20), new Literal(30));

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("Between: null value columnreturns false")
        void testBetweenNullColumnReturnsFalse() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("age", null);
            Record row = makeRecord(data);
            Expression expr = new Between(new ColumnRef("age"), new Literal(20), new Literal(30));

            assertThat(expr.evaluate(row)).isFalse();
        }
    }

    @Nested
    @DisplayName("Expression evaluation")
    class StringEvaluation {

        @Test
        @DisplayName("Like: matchsuccessreturns true")
        void testLikePatternMatch() {
            Record row = makeRecord(Map.of("name", "Alice"));
            Expression expr = new Like(new ColumnRef("name"), "%li%");

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Like: do not matchreturns false")
        void testLikePatternNoMatch() {
            Record row = makeRecord(Map.of("name", "Bob"));
            Expression expr = new Like(new ColumnRef("name"), "%li%");

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("StartWith: before matchsuccessreturns true")
        void testStartWithPrefixMatch() {
            Record row = makeRecord(Map.of("email", "alice@example.com"));
            Expression expr = new StartWith(new ColumnRef("email"), "alice");

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("StartWith: before do not matchreturns false")
        void testStartWithPrefixNoMatch() {
            Record row = makeRecord(Map.of("email", "bob@example.com"));
            Expression expr = new StartWith(new ColumnRef("email"), "alice");

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("EndWith: after matchsuccessreturns true")
        void testEndWithSuffixMatch() {
            Record row = makeRecord(Map.of("email", "alice@example.com"));
            Expression expr = new EndWith(new ColumnRef("email"), ".com");

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("EndWith: after do not matchreturns false")
        void testEndWithSuffixNoMatch() {
            Record row = makeRecord(Map.of("email", "alice@example.org"));
            Expression expr = new EndWith(new ColumnRef("email"), ".com");

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("Contains: matchsuccessreturns true")
        void testContainsSubstringMatch() {
            Record row = makeRecord(Map.of("bio", "Senior Software Engineer"));
            Expression expr = new Contains(new ColumnRef("bio"), "Software");

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("Contains: do not matchreturns false")
        void testContainsSubstringNoMatch() {
            Record row = makeRecord(Map.of("bio", "Senior Software Engineer"));
            Expression expr = new Contains(new ColumnRef("bio"), "Data");

            assertThat(expr.evaluate(row)).isFalse();
        }

        @Test
        @DisplayName(": null value columnreturns false")
        void testStringOpsWithNullReturnsFalse() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("name", null);
            Record row = makeRecord(data);

            assertThat(new Like(new ColumnRef("name"), "%test%").evaluate(row)).isFalse();
            assertThat(new StartWith(new ColumnRef("name"), "test").evaluate(row)).isFalse();
            assertThat(new EndWith(new ColumnRef("name"), "test").evaluate(row)).isFalse();
            assertThat(new Contains(new ColumnRef("name"), "test").evaluate(row)).isFalse();
        }

        @Test
        @DisplayName(": non- columnreturns false")
        void testStringOpsWithNonStringReturnsFalse() {
            Record row = makeRecord(Map.of("age", 25));

            assertThat(new Like(new ColumnRef("age"), "%5%").evaluate(row)).isFalse();
            assertThat(new StartWith(new ColumnRef("age"), "2").evaluate(row)).isFalse();
            assertThat(new EndWith(new ColumnRef("age"), "5").evaluate(row)).isFalse();
            assertThat(new Contains(new ColumnRef("age"), "5").evaluate(row)).isFalse();
        }
    }

    @Nested
    @DisplayName("Null Expression evaluation")
    class NullEvaluation {

        @Test
        @DisplayName("IsNull: null value columnreturns true")
        void testIsNullWithNullValue() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("age", null);
            data.put("name", "Alice");
            Record row = makeRecord(data);

            assertThat(new IsNull(new ColumnRef("age")).evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("IsNull: non-null value columnreturns false")
        void testIsNullWithNonNullValue() {
            Record row = makeRecord(Map.of("age", 25));

            assertThat(new IsNull(new ColumnRef("age")).evaluate(row)).isFalse();
        }

        @Test
        @DisplayName("IsNotNull: non-null value columnreturns true")
        void testIsNotNullWithNonNullValue() {
            Record row = makeRecord(Map.of("age", 25));

            assertThat(new IsNotNull(new ColumnRef("age")).evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("IsNotNull: null value columnreturns false")
        void testIsNotNullWithNullValue() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("age", null);
            data.put("name", "Alice");
            Record row = makeRecord(data);

            assertThat(new IsNotNull(new ColumnRef("age")).evaluate(row)).isFalse();
        }
    }

    @Nested
    @DisplayName("repeat expressionnested evaluation")
    class CompoundEvaluation {

        @Test
        @DisplayName("AND(OR, NOT) nested evaluationcorrect")
        void testAndOrNotNested() {
            Record row = makeRecord(Map.of("age", 30, "name", "Alice", "status", "active"));

            Expression expr =
                    new And(
                            new Or(
                                    new Eq(new ColumnRef("age"), new Literal(30)),
                                    new Eq(new ColumnRef("age"), new Literal(25))),
                            new Not(new Eq(new ColumnRef("status"), new Literal("inactive"))));

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("repeat scenario: agein20-40 statusisactiveorpending")
        void testComplexBusinessScenario() {
            Record activeRow = makeRecord(Map.of("age", 30, "status", "active"));
            Record pendingRow = makeRecord(Map.of("age", 25, "status", "pending"));
            Record inactiveRow = makeRecord(Map.of("age", 35, "status", "inactive"));
            Record tooYoungRow = makeRecord(Map.of("age", 18, "status", "active"));

            Expression ageRange =
                    new Between(new ColumnRef("age"), new Literal(20), new Literal(40));
            Expression statusFilter = new In(new ColumnRef("status"), Set.of("active", "pending"));
            Expression combined = new And(ageRange, statusFilter);

            assertThat(combined.evaluate(activeRow)).isTrue();
            assertThat(combined.evaluate(pendingRow)).isTrue();
            assertThat(combined.evaluate(inactiveRow)).isFalse();
            assertThat(combined.evaluate(tooYoungRow)).isFalse();
        }

        @Test
        @DisplayName("NOT IN + IS NOT NULL filter")
        void testNotInAndIsNotNull() {
            Map<String, Object> nullData = new LinkedHashMap<>();
            nullData.put("status", null);
            nullData.put("name", "Alice");
            Record nullRow = makeRecord(nullData);
            Record activeRow = makeRecord(Map.of("status", "active", "name", "Bob"));
            Record deletedRow = makeRecord(Map.of("status", "deleted", "name", "Charlie"));

            Expression notDeleted =
                    new NotIn(new ColumnRef("status"), Set.of("deleted", "archived"));
            Expression hasStatus = new IsNotNull(new ColumnRef("status"));
            Expression combined = new And(hasStatus, notDeleted);

            assertThat(combined.evaluate(nullRow)).isFalse();
            assertThat(combined.evaluate(activeRow)).isTrue();
            assertThat(combined.evaluate(deletedRow)).isFalse();
        }
    }

    @Nested
    @DisplayName("Literal and ColumnRef evaluation")
    class LiteralAndColumnRefEvaluation {

        @Test
        @DisplayName("Literal(true) returns true")
        void testLiteralTrueAlwaysTrue() {
            Record row = makeRecord(Map.of("x", 1));
            Expression expr = new Literal(true);

            assertThat(expr.evaluate(row)).isTrue();
            assertThat(expr.alwaysTrue()).isTrue();
        }

        @Test
        @DisplayName("Literal(false) returns false")
        void testLiteralFalseAlwaysFalse() {
            Record row = makeRecord(Map.of("x", 1));
            Expression expr = new Literal(false);

            assertThat(expr.evaluate(row)).isFalse();
            assertThat(expr.alwaysFalse()).isTrue();
        }

        @Test
        @DisplayName("ColumnRef: column in hasvalue returns true")
        void testColumnRefExistingColumn() {
            Record row = makeRecord(Map.of("name", "Alice"));

            assertThat(new ColumnRef("name").evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("ColumnRef: columnvalueisnull returns false (has )")
        void testColumnRefNullColumn() {
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("name", null);
            Record row = makeRecord(data);

            assertThat(new ColumnRef("name").evaluate(row)).isFalse();
        }
    }

    @Nested
    @DisplayName("Expression typeand")
    class ExpressionMetadata {

        @Test
        @DisplayName("Expressionreturns correctlyofExpressionType")
        void testExpressionTypes() {
            assertThat(new Eq(new ColumnRef("x"), new Literal(1)).getType())
                    .isEqualTo(Expression.ExpressionType.EQUAL);
            assertThat(new NotEq(new ColumnRef("x"), new Literal(1)).getType())
                    .isEqualTo(Expression.ExpressionType.NOT_EQUAL);
            assertThat(new Gt(new ColumnRef("x"), new Literal(1)).getType())
                    .isEqualTo(Expression.ExpressionType.GREATER_THAN);
            assertThat(new GtEq(new ColumnRef("x"), new Literal(1)).getType())
                    .isEqualTo(Expression.ExpressionType.GREATER_THAN_OR_EQUAL);
            assertThat(new Lt(new ColumnRef("x"), new Literal(1)).getType())
                    .isEqualTo(Expression.ExpressionType.LESS_THAN);
            assertThat(new LtEq(new ColumnRef("x"), new Literal(1)).getType())
                    .isEqualTo(Expression.ExpressionType.LESS_THAN_OR_EQUAL);
            assertThat(new And(new Literal(true), new Literal(true)).getType())
                    .isEqualTo(Expression.ExpressionType.AND);
            assertThat(new Or(new Literal(true), new Literal(true)).getType())
                    .isEqualTo(Expression.ExpressionType.OR);
            assertThat(new Not(new Literal(true)).getType())
                    .isEqualTo(Expression.ExpressionType.NOT);
            assertThat(new In(new ColumnRef("x"), Set.of(1)).getType())
                    .isEqualTo(Expression.ExpressionType.IN);
            assertThat(new NotIn(new ColumnRef("x"), Set.of(1)).getType())
                    .isEqualTo(Expression.ExpressionType.NOT_IN);
            assertThat(new Between(new ColumnRef("x"), new Literal(1), new Literal(10)).getType())
                    .isEqualTo(Expression.ExpressionType.BETWEEN);
            assertThat(new Like(new ColumnRef("x"), "%test%").getType())
                    .isEqualTo(Expression.ExpressionType.LIKE);
            assertThat(new StartWith(new ColumnRef("x"), "test").getType())
                    .isEqualTo(Expression.ExpressionType.STARTS_WITH);
            assertThat(new EndWith(new ColumnRef("x"), "test").getType())
                    .isEqualTo(Expression.ExpressionType.ENDS_WITH);
            assertThat(new Contains(new ColumnRef("x"), "test").getType())
                    .isEqualTo(Expression.ExpressionType.CONTAINS);
            assertThat(new IsNull(new ColumnRef("x")).getType())
                    .isEqualTo(Expression.ExpressionType.IS_NULL);
            assertThat(new IsNotNull(new ColumnRef("x")).getType())
                    .isEqualTo(Expression.ExpressionType.IS_NOT_NULL);
        }

        @Test
        @DisplayName("getReferencedColumns returns correctlyofcolumn")
        void testReferencedColumns() {
            assertThat(new Eq(new ColumnRef("age"), new Literal(25)).getReferencedColumns())
                    .containsExactly("age");
            assertThat(
                            new And(
                                            new Eq(new ColumnRef("age"), new Literal(25)),
                                            new Eq(new ColumnRef("name"), new Literal("Alice")))
                                    .getReferencedColumns())
                    .containsExactlyInAnyOrder("age", "name");
            assertThat(new Literal(true).getReferencedColumns()).isEmpty();
        }

        @Test
        @DisplayName("Expression.copy etc ofreplica")
        void testExpressionCopy() {
            Expression original =
                    new And(
                            new Eq(new ColumnRef("age"), new Literal(25)),
                            new Gt(new ColumnRef("score"), new Literal(80)));
            Expression copied = original.copy();

            Record row = makeRecord(Map.of("age", 25, "score", 90));
            assertThat(original.evaluate(row)).isEqualTo(copied.evaluate(row));
        }
    }

    @Nested
    @DisplayName("Expression simplify evaluation")
    class SimplifyEvaluation {

        @Test
        @DisplayName("twoLiteralcomparisondirectlysimplifies to")
        void testSimplifyLiteralComparison() {
            Expression eq = new Eq(new Literal(1), new Literal(1));
            Expression simplified = eq.simplify();
            assertThat(simplified).isInstanceOf(Literal.class);
            assertThat(simplified.alwaysTrue()).isTrue();

            Expression gt = new Gt(new Literal(1), new Literal(2));
            Expression simplifiedGt = gt.simplify();
            assertThat(simplifiedGt).isInstanceOf(Literal.class);
            assertThat(simplifiedGt.alwaysFalse()).isTrue();
        }

        @Test
        @DisplayName("NotIn simplifies toLiteral(true)")
        void testSimplifyNotInEmptySet() {
            Expression notIn = new NotIn(new ColumnRef("x"), Set.of());
            Expression simplified = notIn.simplify();

            assertThat(simplified).isInstanceOf(Literal.class);
            assertThat(simplified.alwaysTrue()).isTrue();
        }

        @Test
        @DisplayName("Between simplify is GtEq AND LtEq")
        void testSimplifyBetween() {
            Expression between =
                    new Between(new ColumnRef("age"), new Literal(20), new Literal(30));
            Expression simplified = between.simplify();

            Record row = makeRecord(Map.of("age", 25));
            assertThat(simplified.evaluate(row)).isTrue();

            Record outOfRange = makeRecord(Map.of("age", 15));
            assertThat(simplified.evaluate(outOfRange)).isFalse();
        }
    }

    @Nested
    @DisplayName("ExpressionBuilder constructevaluation")
    class ExpressionBuilderEvaluation {

        @Test
        @DisplayName("ExpressionBuilder.eq construct evaluation")
        void testBuilderEq() {
            Record row = makeRecord(Map.of("age", 25));
            Expression expr = ExpressionBuilder.eq("age", 25);

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("ExpressionBuilder.range construct evaluation")
        void testBuilderRange() {
            Record row = makeRecord(Map.of("age", 25));
            Expression expr = ExpressionBuilder.range("age", 20, true, 30, true);

            assertThat(expr.evaluate(row)).isTrue();
        }

        @Test
        @DisplayName("ExpressionBuilder.range evaluation")
        void testBuilderRangeExclusive() {
            Record row = makeRecord(Map.of("age", 20));
            Expression inclusive = ExpressionBuilder.range("age", 20, true, 30, true);
            Expression exclusive = ExpressionBuilder.range("age", 20, false, 30, true);

            assertThat(inclusive.evaluate(row)).isTrue();
            assertThat(exclusive.evaluate(row)).isFalse();
        }
    }
}
