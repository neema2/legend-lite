package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * Represents a filter() on a Class expression.
 * 
 * Example: Person.all()->filter({p | $p.lastName == 'Smith'})
 * 
 * Input: ClassExpression (e.g., ClassAllExpression)
 * Output: ClassExpression (stays Class type)
 * 
 * @param source The Class expression being filtered
 * @param lambda The filter predicate lambda
 */
public record ClassFilterExpression(
        ClassExpression source,
        LambdaExpression lambda) implements ClassExpression {
    public ClassFilterExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(lambda, "Lambda cannot be null");
    }
}
