package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * AST for update expressions.
 * 
 * Pure syntax: Person.all()->filter({p | $p.firstName == 'John'})->update({p |
 * $p.age == 31})
 * 
 * @param source       The source expression (Class.all() with optional filter)
 * @param updateLambda The lambda defining property assignments
 */
public record UpdateExpression(
        PureExpression source,
        LambdaExpression updateLambda) implements PureExpression {

    public UpdateExpression {
        Objects.requireNonNull(source, "Source cannot be null");
        Objects.requireNonNull(updateLambda, "Update lambda cannot be null");
    }
}
