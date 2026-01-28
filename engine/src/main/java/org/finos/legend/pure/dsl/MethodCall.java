package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Method call expression: source->methodName(args) or source.methodName(args)
 */
public record MethodCall(
        PureExpression source,
        String methodName,
        List<PureExpression> arguments) implements PureExpression {
}
