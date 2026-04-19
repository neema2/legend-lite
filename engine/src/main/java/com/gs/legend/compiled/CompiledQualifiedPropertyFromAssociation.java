package com.gs.legend.compiled;

import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Type;

/**
 * Back-reference: a qualified property injected into {@code targetClassFqn}
 * by association {@code associationFqn}. Qualified properties carry a
 * compiled body in addition to their declared type and multiplicity.
 */
public record CompiledQualifiedPropertyFromAssociation(
        String targetClassFqn,
        String name,
        String associationFqn,
        CompiledExpression body,
        Type returnType,
        Multiplicity returnMultiplicity) implements CompiledBackReference {
}
