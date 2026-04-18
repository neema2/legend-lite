package com.gs.legend.compiled;

/**
 * Sealed hierarchy of individual back-reference contributions to a target
 * class. Mirrors Pure's {@code BackReference} sealed interface.
 *
 * <p>In v1, associations contribute either a
 * {@link CompiledPropertyFromAssociation} (simple injected property) or a
 * {@link CompiledQualifiedPropertyFromAssociation} (qualified property
 * with a body). Future work adds {@code CompiledSpecialization} for
 * subclass enumeration.
 *
 * <p>Back-references are <strong>never</strong> embedded inside
 * {@link CompiledClass}. They live in {@link CompiledBackRefFragment}
 * files and merge at lookup time — see Decision 10 in
 * {@code docs/PHASE_B_COMPILED_ELEMENTS.md}.
 */
public sealed interface CompiledBackReference
        permits CompiledPropertyFromAssociation,
                CompiledQualifiedPropertyFromAssociation {

    /** FQN of the class this back-reference contributes to. */
    String targetClassFqn();

    /** Name of the injected property or qualified property. */
    String name();
}
