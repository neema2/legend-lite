/**
 * Phase E &mdash; post-parse normalization. Transforms parsed elements
 * into a uniform form that downstream phases (F: {@code ElementCompiler},
 * G: {@code SpecCompiler}, then SQL gen) can consume without special-casing.
 *
 * <h2>Entry point &mdash; {@code ModelNormalizer} (Phase E)</h2>
 *
 * <p>Phase E has a single entry point,
 * {@code ModelNormalizer.normalize(ParsedModel) -> NormalizedModel}, which externalizes
 * <em>every</em> Pure body site into synthesized
 * {@link com.legend.model.FunctionDefinition}s (owners keep signatures +
 * FQN refs). It composes the sub-slices below as chained
 * {@code ParsedModel -> ParsedModel} transforms: the three simpler body sites inline,
 * delegating the complex legacy-mapping desugaring to
 * {@link com.legend.normalizer.MappingNormalizer}. A sub-slice graduates to its own
 * class (as E.1 has) only when its logic warrants it.
 *
 * <h2>Sub-slice E.1 &mdash; {@link com.legend.normalizer.MappingNormalizer}</h2>
 *
 * <p>For each {@link com.legend.model.ClassMapping} inside every
 * {@link com.legend.model.LegacyMappingDefinition} in the parsed model,
 * synthesize one Layer-2 {@link com.legend.model.FunctionDefinition}
 * returning {@code TargetClass[*]}. The synth function's body is a Pure
 * pipeline that ends in {@code map(<bind> | ^Target(prop=expr, ...))} &mdash;
 * the {@code ^Target(...)} terminus carries the property bindings, the
 * upstream pipeline carries the relation algebra (Relational source) or
 * just {@code getAll(Src)} (M2M source).
 *
 * <h2>Sub-slice E.2 &mdash; derived properties</h2>
 *
 * <p>Externalize each
 * {@link com.legend.model.ClassDefinition.DerivedPropertyDefinition} body
 * into a synthesized {@code <owner>$prop$<name>(this:Owner[1], <params>):T[m]}
 * function (see {@code docs/CORE_PHASE_F_TYPED_ELEMENTS_V2.md} §1.5); the class keeps
 * the derived-property signature + the function FQN.
 *
 * <h2>Sub-slice E.3 &mdash; constraints</h2>
 *
 * <p>Externalize each constraint predicate (and optional message) into
 * {@code <owner>$constraint$<name>} / {@code <owner>$constraintMsg$<name>} functions;
 * the class keeps constraint metadata + FQN refs (§1.4).
 *
 * <h2>Sub-slice E.4 &mdash; service queries</h2>
 *
 * <p>Externalize {@code ServiceDefinition.functionBody} into a {@code <svc>$query}
 * function; the service keeps config + the function FQN.
 *
 * <h2>Pipeline placement</h2>
 *
 * <p>Phase E runs <strong>after</strong> Phase D ({@code NameResolver})
 * and <strong>before</strong> Phase F ({@code ElementCompiler}) and
 * Phase G ({@code SpecCompiler}). By the time the normalizer sees the
 * model, every simple-name reference in the AST has already been
 * rewritten to FQN by {@code NameResolver}. The normalizer treats name
 * strings as opaque FQN tokens and propagates them verbatim into
 * synthesized {@code PackageableElementPtr}s and {@code TypeExpression.NameRef}s;
 * it does not resolve, look up scopes, or invoke other phases. AGENTS.md
 * invariant 1 (F-must-not-trigger-G).
 *
 * <h2>Idempotence</h2>
 *
 * <p>{@link com.legend.normalizer.MappingNormalizer#normalize} is idempotent:
 * a second pass over an already-normalized model is a no-op (existing
 * {@code mappingFunctions} are replaced by structurally-equal lists).
 */
package com.legend.normalizer;
