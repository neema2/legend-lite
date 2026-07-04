/**
 * The Element Compiler (Phase F) typed model &mdash; the compiled, type-checked
 * counterpart of the parser's {@link com.legend.parser.element} AST. Mirrors the
 * engine's {@code m3} metamodel.
 *
 * <h2>Contents</h2>
 * <ul>
 *   <li>{@link com.legend.compiler.element.TypedElement} &mdash; sealed marker;
 *       one {@code Typed*} record per element kind
 *       ({@link com.legend.compiler.element.TypedClass},
 *        {@link com.legend.compiler.element.TypedEnum},
 *        {@link com.legend.compiler.element.TypedFunction}; more per doc §6).</li>
 *   <li>{@link com.legend.compiler.element.Property} &mdash; polymorphic class
 *       member (stored / derived) returned by member lookup.</li>
 *   <li>{@link com.legend.compiler.element.TypedConstraint},
 *       {@link com.legend.compiler.element.TypedParameter} &mdash; member-level
 *       carriers.</li>
 *   <li>{@link com.legend.compiler.element.ModelContext} &mdash; the single
 *       lookup choke point (interface; engine parity).</li>
 *   <li>{@link com.legend.compiler.element.type} &mdash; the kinded type system.</li>
 * </ul>
 *
 * <h2>Invariants (doc §5; AGENTS.md invariants 5 &amp; 11)</h2>
 * <ul>
 *   <li>Pure data: immutable, acyclic records.</li>
 *   <li>Cross-element references are <strong>FQN strings</strong>, never live
 *       {@code Typed*} pointers &mdash; structural access goes through
 *       {@code ModelContext}, enabling lazy / cross-project resolution and
 *       {@code .legend} serialization with no reshape.</li>
 *   <li>Only {@link com.legend.compiler.element.TypedFunction} holds a parsed
 *       (un-type-checked) body; F never triggers G.</li>
 * </ul>
 *
 * <p>The concrete implementation is
 * {@link com.legend.compiler.element.PureModelContext} &mdash; it wraps the
 * parsed index ({@code compiler/ModelBuilder}) + bootstrap {@code builtin/Pure},
 * classifies via the kind manifest ({@link com.legend.compiler.element.ModelContext#findType}),
 * and materializes {@code Typed*} records lazily on demand (the Element
 * Compiler's Work, per {@code docs/TENETS.md}).
 *
 * <h2>Design decision: class vs enum (sealed decls + manifest classification)</h2>
 *
 * Class and enum are <em>not</em> merged into one record. The distinction is
 * irreducible because consumers branch on it (exhaustiveness, navigation,
 * storage mapping). It lives in exactly two places:
 *
 * <ol>
 *   <li><b>Declarations</b> &mdash; a sealed nominal pair
 *       ({@link com.legend.compiler.element.TypedClass} /
 *        {@link com.legend.compiler.element.TypedEnum}) under a common sealed
 *       parent ({@code TypedNominal}). Shared handling (lookup, hashing,
 *       caching) is unified, while a {@code switch} over the two variants is
 *       exhaustiveness-checked by javac &mdash; no stringly-typed {@code kind}
 *       flag.</li>
 *   <li><b>Type references</b> (a property/parameter/body declares {@code :
 *       Color}) &mdash; <b>uniformly kinded</b>
 *       ({@link com.legend.compiler.element.type.Type.ClassType} /
 *        {@link com.legend.compiler.element.type.Type.EnumType}, FQN-only).
 *       There is <em>no</em> {@code NameRef} / unclassified variant: every
 *       stored reference is already classified.</li>
 * </ol>
 *
 * <h3>Classification: the kind manifest (Knowledge, eager)</h3>
 *
 * Per the north-star tenet (<i>Total Knowledge, Demand-Driven Work</i> &mdash;
 * {@code docs/TENETS.md}), a type's <em>kind</em> is Knowledge: cheap, total,
 * and eagerly available. {@code ElementCompiler} classifies each
 * {@code TypeExpression} name into {@code ClassType}/{@code EnumType}/
 * {@code Primitive} by consulting a <b>kind manifest</b> (a flat
 * {@code FQN -> kind} map over the declared dependency closure) via
 * {@link com.legend.compiler.element.ModelContext}. An FQN absent from the
 * manifest is a <b>compile error</b>, never a runtime fallback (AGENTS.md
 * invariant 4).
 *
 * <p>This is option "B with a cheaper eager cost": closed-world classification
 * complete at compile time, uniform kinded references &mdash; but only the
 * <em>kind</em> is loaded eagerly (cheap), while full element <em>structure</em>
 * (properties, supertypes, bodies) is Tier-2 and materialized on-demand through
 * {@link com.legend.compiler.element.ModelContext#findClass} (AGENTS.md
 * invariant 5). Per-reference lookup stays flat (single FQN map hit) and
 * element content hashes stay flat (references are FQN strings, never inlined
 * referent content); the only transitivity is the cheap, one-time, kinds-only
 * manifest assembly.
 */
package com.legend.compiler.element;
