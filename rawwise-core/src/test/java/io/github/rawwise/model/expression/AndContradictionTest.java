package io.github.rawwise.model.expression;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class AndContradictionTest {

    @Test
    void gtLtFullContradiction() {
        Expression ageGt10 = ExpressionBuilder.gt("age", 10);
        Expression ageLt9 = ExpressionBuilder.lt("age", 9);
        Expression and = ExpressionBuilder.and(ageGt10, ageLt9);
        assertTrue(and.simplify().alwaysFalse(), "age>10 AND age<9 should be alwaysFalse");
    }

    @Test
    void gtLtNoContradiction() {
        Expression ageGt10 = ExpressionBuilder.gt("age", 10);
        Expression ageLt15 = ExpressionBuilder.lt("age", 15);
        Expression and = ExpressionBuilder.and(ageGt10, ageLt15);
        assertFalse(and.simplify().alwaysFalse(), "age>10 AND age<15 should not be alwaysFalse");
    }

    @Test
    void gtLtBoundaryContradiction() {
        Expression ageGt10 = ExpressionBuilder.gt("age", 10);
        Expression ageLt10 = ExpressionBuilder.lt("age", 10);
        Expression and = ExpressionBuilder.and(ageGt10, ageLt10);
        assertTrue(and.simplify().alwaysFalse(), "age>10 AND age<10 should be alwaysFalse");
    }

    @Test
    void gtEqLtEqContradiction() {
        Expression ageGtEq10 = ExpressionBuilder.ge("age", 10);
        Expression ageLtEq9 = ExpressionBuilder.le("age", 9);
        Expression and = ExpressionBuilder.and(ageGtEq10, ageLtEq9);
        assertTrue(and.simplify().alwaysFalse(), "age>=10 AND age<=9 should be alwaysFalse");
    }

    @Test
    void gtEqLtEqBoundaryOk() {
        Expression ageGtEq10 = ExpressionBuilder.ge("age", 10);
        Expression ageLtEq10 = ExpressionBuilder.le("age", 10);
        Expression and = ExpressionBuilder.and(ageGtEq10, ageLtEq10);
        assertFalse(and.simplify().alwaysFalse(), "age>=10 AND age<=10 should NOT be alwaysFalse");
    }

    @Test
    void gtEqLtEqNoContradiction() {
        Expression ageGtEq10 = ExpressionBuilder.ge("age", 10);
        Expression ageLtEq15 = ExpressionBuilder.le("age", 15);
        Expression and = ExpressionBuilder.and(ageGtEq10, ageLtEq15);
        assertFalse(and.simplify().alwaysFalse(), "age>=10 AND age<=15 should NOT be alwaysFalse");
    }

    @Test
    void preExistingGtLtEqStillWorks() {
        Expression ageGt10 = ExpressionBuilder.gt("age", 10);
        Expression ageLtEq10 = ExpressionBuilder.le("age", 10);
        Expression and = ExpressionBuilder.and(ageGt10, ageLtEq10);
        assertTrue(and.simplify().alwaysFalse());
    }

    @Test
    void preExistingGtEqLtStillWorks() {
        Expression ageGtEq11 = ExpressionBuilder.ge("age", 11);
        Expression ageLt10 = ExpressionBuilder.lt("age", 10);
        Expression and = ExpressionBuilder.and(ageGtEq11, ageLt10);
        assertTrue(and.simplify().alwaysFalse());
    }

    @Test
    void differentColumnsNeverContradict() {
        Expression ageGt10 = ExpressionBuilder.gt("age", 10);
        Expression scoreLt5 = ExpressionBuilder.lt("score", 5);
        Expression and = ExpressionBuilder.and(ageGt10, scoreLt5);
        assertFalse(and.simplify().alwaysFalse());
    }

    @Test
    void stringGtLtContradiction() {
        Expression gt = ExpressionBuilder.gt("val", "m");
        Expression lt = ExpressionBuilder.lt("val", "a");
        Expression and = ExpressionBuilder.and(gt, lt);
        assertTrue(and.simplify().alwaysFalse());
    }

    @Test
    void stringGtLtNoContradiction() {
        Expression gt = ExpressionBuilder.gt("val", "a");
        Expression lt = ExpressionBuilder.lt("val", "m");
        Expression and = ExpressionBuilder.and(gt, lt);
        assertFalse(and.simplify().alwaysFalse());
    }
}
