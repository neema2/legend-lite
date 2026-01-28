package org.finos.legend.pure.dsl;

/**
 * AST node for sortBy() on a ClassExpression.
 * 
 * Pure syntax: Person.all()->sortBy({p | $p.lastName})
 * 
 * @param source    The ClassExpression to sort
 * @param lambda    Lambda extracting the sort key property
 * @param ascending True for ascending, false for descending
 */
public record ClassSortByExpression(
        ClassExpression source,
        LambdaExpression lambda,
        boolean ascending) implements ClassExpression {
}
