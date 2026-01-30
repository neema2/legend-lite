package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * A block of expressions, containing let statements and a final result
 * expression.
 * 
 * Example:
 * {
 * let x = 1;
 * let y = 2;
 * $x + $y
 * }
 */
public record BlockExpression(
        List<LetExpression> letStatements,
        PureExpression result) implements PureExpression {

    public BlockExpression {
        letStatements = List.copyOf(letStatements);
    }
}
