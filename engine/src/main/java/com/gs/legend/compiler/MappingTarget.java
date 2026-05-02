package com.gs.legend.compiler;

/**
 * Marker carried on {@link TypeChecker.CompilationContext} when compiling a
 * synthetic mapping function body, naming the class the function semantically
 * <em>materializes</em> (the target class — e.g. {@code Person}, not the
 * source-side {@code RawPerson} the body actually computes from).
 *
 * <p>Used by binding-aware checkers (today: {@code ExtendChecker}; future:
 * {@code AssocChecker}) to look up declared property types and multiplicities
 * on the materialized class for per-ColSpec validation.
 *
 * <p><strong>Hint, not contract.</strong> This is deliberately a distinct type
 * (not {@code Optional<ExpressionType>}) so it cannot be confused with a
 * declared return-type or used as a strict body-validation target. Synthetic
 * mapping function bodies legitimately compute {@code SrcClass[*]} (M2M) or
 * {@code Relation<schema>} (relational) — neither is the materialized class.
 * Strict body-vs-declared validation happens via
 * {@code compileBodyInContext}'s explicit {@code expectedReturn} parameter,
 * a separate channel.
 *
 * <p>Populated in {@link TypeChecker#compileFunction} via
 * {@link com.gs.legend.model.ModelContext#findClassForMappingFunction} —
 * the inverse of the canonical class↔function binding maintained by
 * {@link NormalizedMapping#mappingFunctionFqns}. Not name-based: no FQN
 * suffix parsing.
 *
 * @param classFqn Materialized class FQN
 */
public record MappingTarget(String classFqn) {}
