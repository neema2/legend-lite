/**
 * Legend Lite Compiler — clean reimplementation, Strangler Fig sibling
 * to {@code engine/}. See {@code core/README.md} for the full
 * specification.
 *
 * <h2>The wall</h2>
 * Nothing under {@code com.legend.*} may import anything under
 * {@code com.gs.legend.*} (other than JDK + JUnit + JDBC drivers +
 * ArchUnit in tests). Enforced by
 * {@code com.legend.ArchitectureTest}.
 *
 * <h2>Pipeline</h2>
 * {@link com.legend.Compiler#compile} drives 11 steps:
 * <ol>
 *   <li>{@code lexer/}      — text → tokens</li>
 *   <li>{@code parser/}     — tokens → {@code parser.element.PackageableElement} (ElementParser)</li>
 *   <li>{@code parser/}     — tokens → {@code parser.spec.ValueSpecification} (SpecParser)</li>
 *   <li>{@code compiler/}   — simple name → FQN (NameResolver)</li>
 *   <li>{@code normalizer/} — {@code LegacyMappingDefinition} → binding-table {@code MappingDefinition} + lifted {@code FunctionDefinition}</li>
 *   <li>{@code compiler/}   — {@code PackageableElement} → {@code compiler.element.TypedElement} (ElementCompiler)</li>
 *   <li>{@code compiler/}   — {@code ValueSpecification} → {@code compiler.spec.TypedSpec} (SpecCompiler)</li>
 *   <li>{@code resolver/}   — logical {@code TypedSpec} → physical {@code TypedSpec} (StoreResolver, phase H)</li>
 *   <li>{@code lowering/}   — {@code TypedSpec} → {@code sql.SqlQuery} (Lowerer, phase I)</li>
 *   <li>{@code sql/dialect/} — {@code SqlQuery} → SQL string (phase J)</li>
 *   <li>{@code exec/}       — SQL string + JDBC → typed {@code ExecutionResult} (Executor, phase K)</li>
 * </ol>
 *
 * <h2>Invariants</h2>
 * See {@code core/README.md §Strong invariants} for the authoritative
 * list (12 numbered rules). Highlights:
 * <ul>
 *   <li>Pure-data records for all IR nodes; sealed roots for all variant families.</li>
 *   <li>No {@code FunctionCall(String name, args)}; every native is its own typed record.</li>
 *   <li>Sealed exhaustiveness over {@code default ->}: switches over sealed
 *       roots list every variant (javac then flags new ones). The sanctioned
 *       exceptions: guarded-pattern switches need a coverage default (make it
 *       THROW), and best-effort rewrite walkers may pass unknown nodes
 *       through ONLY where a downstream loud wall is guaranteed.</li>
 *   <li>No {@code util/} package anywhere.</li>
 *   <li>F (compile elements) MUST NOT trigger G (compile specs) — function bodies stay as
 *       {@code ValueSpecification} inside {@code TypedFunction}, type-checked on demand.</li>
 *   <li>{@code TypedGetAll} and {@code TypedUserCall} MUST NOT survive {@code resolver/}.</li>
 *   <li>{@code compiler/element/Typed*} hold FQN strings, not live refs, for cross-element references.</li>
 *   <li>{@code sql/} is closed pure data — no {@code toSql()}, no dialect coupling.</li>
 * </ul>
 */
package com.legend;
