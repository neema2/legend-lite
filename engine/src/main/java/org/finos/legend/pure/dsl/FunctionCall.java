package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Unified function/method call expression.
 * 
 * In Pure, $x->foo(y) is equivalent to foo($x, y). This single AST node
 * represents both forms:
 * - Arrow/dot call: source->functionName(args) — source is non-null
 * - Standalone call: functionName(args) — source is null
 */
public record FunctionCall(
        String functionName,
        PureExpression source,
        List<PureExpression> arguments) implements PureExpression {

    /**
     * Convenience constructor for standalone function calls (no source).
     */
    public FunctionCall(String functionName, List<PureExpression> arguments) {
        this(functionName, null, arguments);
    }
}
