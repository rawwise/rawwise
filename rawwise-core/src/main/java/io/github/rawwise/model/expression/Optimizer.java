package io.github.rawwise.model.expression;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class Optimizer {

    public static Expression optimize(Expression expression) {
        if (expression == null) {
            return null;
        }
        return applyOptimizationRules(expression);
    }

    private static Expression applyOptimizationRules(Expression expr) {
        // 1. constant folding
        expr = constantFolding(expr);

        // 2. simplify expression
        expr = expr.simplify();

        // 3. remove redundant conditions
        expr = removeRedundantConditions(expr);

        return expr;
    }

    private static Expression constantFolding(Expression expr) {
        if (expr instanceof BinaryComparison) {
            BinaryComparison binExpr = (BinaryComparison) expr;
            Expression left = binExpr.getLeft();
            Expression right = binExpr.getRight();

            // if both sides are literals,compute directly
            if (left instanceof Literal && right instanceof Literal) {
                Comparable leftVal = ((Literal) left).getValue();
                Comparable rightVal = ((Literal) right).getValue();

                // compute result by expression type
                boolean result;
                if (expr instanceof Eq) {
                    result = leftVal != null && leftVal.equals(rightVal);
                } else if (expr instanceof NotEq) {
                    result = leftVal != null && !leftVal.equals(rightVal);
                } else if (expr instanceof Gt) {
                    result = leftVal != null && rightVal != null && leftVal.compareTo(rightVal) > 0;
                } else if (expr instanceof GtEq) {
                    result =
                            leftVal != null && rightVal != null && leftVal.compareTo(rightVal) >= 0;
                } else if (expr instanceof Lt) {
                    result = leftVal != null && rightVal != null && leftVal.compareTo(rightVal) < 0;
                } else if (expr instanceof LtEq) {
                    result =
                            leftVal != null && rightVal != null && leftVal.compareTo(rightVal) <= 0;
                } else {
                    return expr;
                }

                return new Literal(result);
            }
        }

        // recursively process sub-expressions
        if (expr instanceof And andExpr) {
            List<Expression> optimizedChildren = new ArrayList<>();
            for (Expression child : andExpr.children()) {
                Expression optimizedChild = constantFolding(child);
                optimizedChildren.add(optimizedChild);
            }
            return new And(optimizedChildren);
        }

        if (expr instanceof Or orExpr) {
            List<Expression> optimizedChildren = new ArrayList<>();
            for (Expression child : orExpr.children()) {
                Expression optimizedChild = constantFolding(child);
                optimizedChildren.add(optimizedChild);
            }
            return new Or(optimizedChildren);
        }

        if (expr instanceof Not notExpr) {
            Expression optimizedChild = constantFolding(notExpr.children().get(0));
            return new Not(optimizedChild);
        }

        return expr;
    }

    private static Expression removeRedundantConditions(Expression expr) {
        if (expr instanceof And andExpr) {
            List<Expression> children = andExpr.children();

            // collect non-duplicate conditions
            Set<Expression> uniqueChildren = new LinkedHashSet<>();
            for (Expression child : children) {
                Expression optimizedChild = removeRedundantConditions(child);

                // skipcondition
                if (!optimizedChild.alwaysTrue()) {
                    uniqueChildren.add(optimizedChild);
                }
            }

            // build new AND expression
            if (uniqueChildren.isEmpty()) {
                return new Literal(true);
            } else if (uniqueChildren.size() == 1) {
                return uniqueChildren.iterator().next();
            } else {
                return new And(new ArrayList<>(uniqueChildren));
            }
        }

        if (expr instanceof Or orExpr) {
            List<Expression> children = orExpr.children();

            // collect non-duplicate conditions
            Set<Expression> uniqueChildren = new LinkedHashSet<>();
            for (Expression child : children) {
                Expression optimizedChild = removeRedundantConditions(child);

                // skipcondition
                if (!optimizedChild.alwaysFalse()) {
                    uniqueChildren.add(optimizedChild);
                }
            }

            // build new OR expression
            if (uniqueChildren.isEmpty()) {
                return new Literal(false);
            } else if (uniqueChildren.size() == 1) {
                return uniqueChildren.iterator().next();
            } else {
                return new Or(new ArrayList<>(uniqueChildren));
            }
        }

        return expr;
    }
}
