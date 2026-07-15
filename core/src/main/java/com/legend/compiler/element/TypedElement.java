package com.legend.compiler.element;

/**
 * Sealed marker for a compiled (Phase F) packageable element &mdash; the typed
 * counterpart of {@link com.legend.model.PackageableElement}.
 *
 * <p>One {@code Typed*} record per element kind, mirroring the engine's
 * {@code m3} model. All variants are <strong>pure data</strong>: immutable,
 * acyclic, holding FQN <em>strings</em> for every cross-element reference
 * (never live {@code Typed*} pointers), so they serialize to {@code .legend}
 * unchanged (core/README invariant 11; AGENTS.md invariant 5).
 *
 * <p>Rollout is incremental (doc §6): this permit list grows one entry per
 * tested slice ({@code TypedAssociation}, {@code TypedDatabase}, … to come).
 */
public sealed interface TypedElement
        permits TypedNominal, TypedFunction {

    /** Fully qualified name; the element's identity and lookup key. */
    String qualifiedName();
}
