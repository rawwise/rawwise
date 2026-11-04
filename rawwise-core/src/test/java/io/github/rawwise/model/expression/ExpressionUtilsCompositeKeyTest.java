package io.github.rawwise.model.expression;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import org.junit.jupiter.api.Test;

class ExpressionUtilsCompositeKeyTest {

    @Test
    void compositePK_allColumnsPresent_returnsTrue() {
        Expression condition =
                new And(
                        new Eq(new ColumnRef("user_id"), new Literal(2001)),
                        new Eq(new ColumnRef("region"), new Literal("us")));
        assertTrue(ExpressionUtils.isPrimaryKeyEquals(condition, Set.of("user_id", "region")));
    }

    @Test
    void compositePK_partialColumnsPresent_returnsFalse() {
        Expression condition = new Eq(new ColumnRef("user_id"), new Literal(2001));
        assertFalse(ExpressionUtils.isPrimaryKeyEquals(condition, Set.of("user_id", "region")));
    }

    @Test
    void compositePK_withNonPKEqCondition_returnsFalse() {
        Expression condition =
                new And(
                        new Eq(new ColumnRef("user_id"), new Literal(2001)),
                        new Eq(new ColumnRef("age"), new Literal(18)));
        assertFalse(ExpressionUtils.isPrimaryKeyEquals(condition, Set.of("user_id")));
    }

    @Test
    void compositePK_withRangeCondition_returnsFalse() {
        Expression condition =
                new And(
                        new Eq(new ColumnRef("user_id"), new Literal(2001)),
                        new Gt(new ColumnRef("age"), new Literal(18)));
        assertFalse(ExpressionUtils.isPrimaryKeyEquals(condition, Set.of("user_id")));
    }

    @Test
    void compositePK_threeWayAnd_returnsTrue() {
        Expression condition =
                new And(
                        new And(
                                new Eq(new ColumnRef("a"), new Literal(1)),
                                new Eq(new ColumnRef("b"), new Literal(2))),
                        new Eq(new ColumnRef("c"), new Literal(3)));
        assertTrue(ExpressionUtils.isPrimaryKeyEquals(condition, Set.of("a", "b", "c")));
    }

    @Test
    void compositePK_reversedEqOrder_returnsTrue() {
        Expression condition =
                new And(
                        new Eq(new ColumnRef("region"), new Literal("us")),
                        new Eq(new ColumnRef("user_id"), new Literal(2001)));
        assertTrue(ExpressionUtils.isPrimaryKeyEquals(condition, Set.of("user_id", "region")));
    }

    @Test
    void literalOnLeft_returnsTrue() {
        Expression condition =
                new And(
                        new Eq(new Literal(2001), new ColumnRef("user_id")),
                        new Eq(new ColumnRef("region"), new Literal("us")));
        assertTrue(ExpressionUtils.isPrimaryKeyEquals(condition, Set.of("user_id", "region")));
    }

    @Test
    void extractPrimaryKeyValues_nestedAnd_extractsAllValues() {
        Expression condition =
                new And(
                        new And(
                                new Eq(new ColumnRef("a"), new Literal(1)),
                                new Eq(new ColumnRef("b"), new Literal(2))),
                        new Eq(new ColumnRef("c"), new Literal(3)));
        var values = ExpressionUtils.extractPrimaryKeyValues(condition, Set.of("a", "b", "c"));
        assertEquals(1, values.get("a"));
        assertEquals(2, values.get("b"));
        assertEquals(3, values.get("c"));
    }

    @Test
    void extractPrimaryKeyValues_literalOnLeft_extractsAllValues() {
        Expression condition = new Eq(new Literal(2001), new ColumnRef("user_id"));
        var values = ExpressionUtils.extractPrimaryKeyValues(condition, Set.of("user_id"));
        assertEquals(2001, values.get("user_id"));
    }

    @Test
    void isAllEqPredicate_eqAndAndEq_returnsTrue() {
        Expression condition =
                new And(
                        new Eq(new ColumnRef("a"), new Literal(1)),
                        new Eq(new ColumnRef("b"), new Literal(2)));
        assertTrue(ExpressionUtils.isAllEqPredicate(condition));
    }

    @Test
    void isAllEqPredicate_withGt_returnsFalse() {
        Expression condition =
                new And(
                        new Eq(new ColumnRef("a"), new Literal(1)),
                        new Gt(new ColumnRef("b"), new Literal(2)));
        assertFalse(ExpressionUtils.isAllEqPredicate(condition));
    }
}
