package com.gs.legend.compiled;

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
        TypeRef returnTypeRef,
        Multiplicity returnMultiplicity) implements CompiledBackReference {
}
