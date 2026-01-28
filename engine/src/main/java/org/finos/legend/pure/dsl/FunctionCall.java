package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Function call expression: funcName(arg1, arg2, ...)
 */
public record FunctionCall(
        String functionName,
        List<PureExpression> arguments) implements PureExpression {
}
