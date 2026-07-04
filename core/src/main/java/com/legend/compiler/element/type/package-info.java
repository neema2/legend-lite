/**
 * The typed type system (Phase F) &mdash; the kinded, post-classification
 * counterpart of the parser's {@link com.legend.parser.TypeExpression}.
 * Mirrors {@code engine.m3.Type} / {@code m3.Multiplicity}. Pure-data,
 * acyclic, FQN-strings-only, so it serializes to {@code .legend} unchanged
 * (core/README invariant 11).
 *
 * <ul>
 *   <li>{@link com.legend.compiler.element.type.Type} &mdash; sealed kinded
 *       type: scalar leaves ({@code Primitive}, {@code PrecisionDecimal}),
 *       nominal ({@code ClassType}, {@code EnumType}), variables/application
 *       ({@code TypeVar}, {@code GenericType}), structural ({@code FunctionType},
 *       {@code RelationType}), and {@code SchemaAlgebra}.</li>
 *   <li>{@link com.legend.compiler.element.type.Multiplicity} &mdash; own
 *       typed multiplicity ({@code Bounded} | {@code Var}).</li>
 * </ul>
 *
 * <p>{@code Primitive} FQNs are sourced from {@code builtin/Pure} (single source
 * of truth); the primitive lattice is the {@code Pure.java} {@code extends} chain
 * walked via {@code ModelContext.isSubtype}, not re-encoded here.
 */
package com.legend.compiler.element.type;
