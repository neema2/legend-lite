/**
 * Phase E &mdash; post-parse normalization. Transforms parsed elements
 * into a uniform form that downstream phases (F: name resolution,
 * G: type checking, H: SQL gen) can consume without special-casing.
 *
 * <h2>Sub-slice E.1 &mdash; {@link com.legend.normalizer.MappingNormalizer}</h2>
 *
 * <p>For each {@link com.legend.parser.element.ClassMapping} inside every
 * {@link com.legend.parser.element.MappingDefinition} in the parsed model,
 * synthesize one Layer-2 {@link com.legend.parser.element.FunctionDefinition}
 * returning {@code TargetClass[*]}. The synth function's body is a Pure
 * pipeline that ends in {@code map(<bind> | ^Target(prop=expr, ...))} &mdash;
 * the {@code ^Target(...)} terminus carries the property bindings, the
 * upstream pipeline carries the relation algebra (Relational source) or
 * just {@code getAll(Src)} (M2M source).
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
