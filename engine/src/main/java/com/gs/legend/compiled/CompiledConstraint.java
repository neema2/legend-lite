package com.gs.legend.compiled;

/**
 * Compiled-state class constraint: a name plus the compiled predicate body.
 * Checked at instance-construction time when enforced.
 */
public record CompiledConstraint(
        String name,
        CompiledExpression body) {
}
