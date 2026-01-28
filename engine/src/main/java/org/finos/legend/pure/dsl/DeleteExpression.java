package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * AST for delete expressions.
 * 
 * Pure syntax: Person.all()->filter({p | $p.firstName == 'John'})->delete()
 * 
 * @param source The source expression (Class.all() with optional filter)
 */
public record DeleteExpression(
        PureExpression source) implements PureExpression {

    public DeleteExpression {
        Objects.requireNonNull(source, "Source cannot be null");
    }
}
