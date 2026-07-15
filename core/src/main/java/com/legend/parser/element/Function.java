package com.legend.parser.element;

import com.legend.parser.TypeExpression;

import com.legend.parser.Multiplicity;

import java.util.List;

/**
 * Sealed marker for parsed Pure function declarations.
 *
 * <p>Two variants:
 * <ul>
 *   <li>{@link FunctionDefinition} &mdash; concrete user functions with a parsed body.</li>
 *   <li>{@link NativeFunctionDefinition} &mdash; signature-only declarations
 *       (Pure {@code native function ...;}) for built-in primitives and
 *       platform / relation functions.</li>
 * </ul>
 *
 * <p>Both variants share signature shape (type/multiplicity parameters,
 * parameters, return type and multiplicity, stereotypes, tagged values).
 * This marker exposes that shared shape so downstream consumers
 * (overload resolution, signature compilation, documentation, etc.) can
 * treat the two uniformly &mdash; the only structural difference is the
 * presence of a body, which is exclusive to {@link FunctionDefinition}.
 *
 * <p>Mirrors Pure's M3 grammar where {@code nativeFunction} and
 * {@code functionDefinition} reuse a single {@code functionTypeSignature}
 * rule, differing only by the {@code FUNCTION}/{@code NATIVE FUNCTION}
 * keyword and the trailing body vs. {@code ;}.
 */
public sealed interface Function
        permits FunctionDefinition, NativeFunctionDefinition {

    /** Fully qualified name. Mirrors {@code PackageableElement.qualifiedName()}. */
    String qualifiedName();

    /** Declared generic type parameter names, in source order (may be empty). */
    List<String> typeParameters();

    /** Declared multiplicity parameter names, in source order (may be empty). */
    List<String> multiplicityParameters();

    /**
     * THE stable overload identity: qualified name + canonical parameter
     * spellings. Unique across the native catalog (pinned by
     * NativeFunctionTest's collapse test) and stable across parses — the
     * dispatch key for identity-keyed consumers (lowering rule tables),
     * which must not hold parser NODES (AUDIT_2026_07 §1c).
     */
    default String signatureKey() {
        // MEMOIZED: dispatch lookups call this once per lowered node and the
        // build walks full TypeExpression trees (re-audit M5). Catalog
        // definitions are singletons, so an identity cache is exact.
        // Built per call — audit 15 removed a static identity-keyed memo
        // here: user-parsed definitions flowed through it too, so every
        // compilation grew the map (and retained its ASTs) forever. The
        // build is a few appends; memoize per-INSTANCE if it ever shows up
        // in a profile, never in static state.
        StringBuilder key = new StringBuilder(qualifiedName()).append('(');
        for (var p : parameters()) {
            key.append(p.type()).append(':').append(p.multiplicity()).append(',');
        }
        return key.append(')').toString();
    }

    /** Declared parameters, in source order. */
    List<FunctionDefinition.ParameterDefinition> parameters();

    /** Return multiplicity ({@link Multiplicity.Concrete} or {@link Multiplicity.Parameter}). */
    Multiplicity returnMultiplicity();

    /** Declared return type as a structured AST. Pre-NameResolver may contain
     *  simple (unresolved) {@link TypeExpression.NameRef} leaves; post-resolution
     *  every leaf is FQN. */
    TypeExpression returnType();

    /** Applied stereotypes (may be empty). */
    List<StereotypeApplication> stereotypes();

    /** Applied tagged values (may be empty). */
    List<TaggedValue> taggedValues();
}
