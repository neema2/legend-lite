package com.gs.legend.compiled;

/**
 * Back-reference: a simple property injected into {@code targetClassFqn}
 * by association {@code associationFqn}. The opposite end of the
 * association is {@code peerClassFqn}.
 *
 * <p>Materializes the association end's projection onto one of its target
 * classes, without mutating that class's {@link CompiledClass}.
 */
public record CompiledPropertyFromAssociation(
        String targetClassFqn,
        String name,
        String associationFqn,
        String peerClassFqn,
        Multiplicity multiplicity) implements CompiledBackReference {
}
