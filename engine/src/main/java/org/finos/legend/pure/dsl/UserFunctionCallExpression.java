package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Represents a call to a user-defined function that needs to be inlined.
 * 
 * Created by the parser when it encounters a method call to a registered Pure
 * function.
 * Stores the original source text for the receiver expression to enable textual
 * substitution.
 * 
 * The compiler handles this by:
 * 1. Looking up the function body source from the registry
 * 2. Substituting parameter names with argument source text
 * 3. Re-parsing the expanded expression
 * 4. Compiling the result
 */
public record UserFunctionCallExpression(
        PureExpression source, // The receiver expression (for AST traversal)
        String sourceText, // The receiver expression as Pure source text
        String functionName, // The qualified or simple function name
        List<PureExpression> arguments, // Additional arguments after source
        List<String> argumentTexts // Argument expressions as Pure source text
) implements PureExpression {
}
