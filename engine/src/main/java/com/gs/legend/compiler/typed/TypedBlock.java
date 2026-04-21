package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;
import java.util.List;

/**
 * A sequence of statements forming a body (function body, lambda body, match
 * branch body). Produced by the shared body-compile primitive whenever the body
 * contains intermediate {@link TypedLet} bindings before the terminal expression.
 *
 * <p>Single-statement bodies do <strong>not</strong> wrap in a {@code TypedBlock};
 * they are the terminal {@link TypedSpec} directly. A {@code TypedBlock} therefore
 * always has at least 2 statements, and the last one's {@link ExpressionType}
 * determines the block's own {@code info}.
 *
 * @param stmts Statement sequence. Intermediate statements are typically
 *              {@link TypedLet}s; the final statement is the body's result.
 *              Never empty; size &ge; 2 in practice.
 * @param info  Type + multiplicity of the terminal statement.
 */
public record TypedBlock(
        List<TypedSpec> stmts,
        ExpressionType info
) implements TypedSpec {
    public TypedBlock {
        stmts = List.copyOf(stmts);
        if (stmts.isEmpty()) {
            throw new IllegalArgumentException("TypedBlock must have at least one statement");
        }
    }
}
