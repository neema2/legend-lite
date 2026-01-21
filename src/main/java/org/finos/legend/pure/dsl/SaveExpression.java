package org.finos.legend.pure.dsl;

import java.util.Objects;

/**
 * AST for save() function call.
 * 
 * Pure syntax: ^Person(...)->save()
 * 
 * @param instance The expression to persist (usually InstanceExpression)
 */
public record SaveExpression(
        PureExpression instance) implements PureExpression {

    public SaveExpression {
        Objects.requireNonNull(instance, "Instance cannot be null");
    }
}
