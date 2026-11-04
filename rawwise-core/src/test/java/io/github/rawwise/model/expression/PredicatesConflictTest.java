package io.github.rawwise.model.expression;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PredicatesConflictTest {

    @Test
    void overlappingRangesConflict() {
        assertTrue(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.gt("age", 10), ExpressionBuilder.gt("age", 12)));
    }

    @Test
    void disjointRangesNoConflict() {
        assertFalse(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.gt("age", 10), ExpressionBuilder.lt("age", 9)));
    }

    @Test
    void disjointGtLtBoundaryNoConflict() {
        assertFalse(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.gt("age", 11), ExpressionBuilder.lt("age", 9)));
    }

    @Test
    void overlappingRangeConflict() {
        assertTrue(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.gt("age", 10), ExpressionBuilder.lt("age", 15)));
    }

    @Test
    void eqInsideRangeConflict() {
        assertTrue(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.eq("age", 5), ExpressionBuilder.gt("age", 3)));
    }

    @Test
    void eqOutsideRangeNoConflict() {
        assertFalse(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.eq("age", 5), ExpressionBuilder.lt("age", 3)));
    }

    @Test
    void differentColumnsConflict() {
        assertTrue(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.gt("age", 10), ExpressionBuilder.eq("region", "US")));
    }

    @Test
    void nullPredicateAlwaysConflict() {
        assertTrue(ExpressionUtils.predicatesConflict(null, ExpressionBuilder.gt("age", 10)));
        assertTrue(ExpressionUtils.predicatesConflict(ExpressionBuilder.gt("age", 10), null));
        assertTrue(ExpressionUtils.predicatesConflict(null, null));
    }

    @Test
    void compositePredicateConflict() {
        Expression composite =
                ExpressionBuilder.and(
                        ExpressionBuilder.gt("age", 10), ExpressionBuilder.eq("region", "US"));
        assertTrue(ExpressionUtils.predicatesConflict(composite, ExpressionBuilder.gt("age", 12)));
    }

    @Test
    void compositeDisjointNoConflict() {
        Expression composite =
                ExpressionBuilder.and(
                        ExpressionBuilder.gt("age", 10), ExpressionBuilder.eq("region", "US"));
        assertFalse(ExpressionUtils.predicatesConflict(composite, ExpressionBuilder.lt("age", 9)));
    }

    @Test
    void samePredicateConflict() {
        assertTrue(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.gt("age", 10), ExpressionBuilder.gt("age", 10)));
    }

    @Test
    void gtEqLtEqBoundaryConflict() {
        assertTrue(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.ge("age", 10), ExpressionBuilder.le("age", 10)));
    }

    @Test
    void gtEqLtEqDisjointNoConflict() {
        assertFalse(
                ExpressionUtils.predicatesConflict(
                        ExpressionBuilder.ge("age", 10), ExpressionBuilder.le("age", 9)));
    }
}
