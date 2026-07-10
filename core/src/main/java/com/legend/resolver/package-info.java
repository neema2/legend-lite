/**
 * Phase H &mdash; the StoreResolver: a pure {@code TypedSpec -> TypedSpec}
 * rewriter between G (type check) and I (SQL lowering) that replaces
 * object-space class queries with relation pipelines resolved against the
 * active mapping. Full design: {@code docs/PHASE_H2_H3_RESOLVER_PLAN.md}.
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li><strong>No sidecar.</strong> Resolution lives in the RETURNED tree;
 *       row schema stays derivable from the AST, never stored beside it.
 *       (V1's identity-keyed sidecar bred the restamp bug family; V2's one
 *       great idea was killing it.)</li>
 *   <li><strong>The map-terminal invariant.</strong> A mapping body's
 *       {@code map(row|^Class(...))} terminal survives resolution ONLY
 *       beneath a serialization boundary. Every relation-returning consumer
 *       (project/filter/sort/groupBy/scalar map) consumes the terminal as a
 *       property&rarr;typed-expression BINDING TABLE &mdash; the resolver
 *       never materializes an object (no JSON, no struct packing) that a
 *       downstream op would immediately flatten.</li>
 *   <li><strong>No store-only node escapes.</strong> A {@code TypedGetAll},
 *       {@code TypedJoinSlot}, or {@code TypedNavigate} reaching the lowerer
 *       after resolution is a resolver bug and fails on the lowerer's named
 *       walls.</li>
 *   <li><strong>Loud misses.</strong> An unmapped class/property, a missing
 *       or ambiguous execution context, an unsupported object-space
 *       construct: {@link com.legend.error.MappingResolutionException}
 *       naming the property, class, and mapping. NEVER the reference
 *       engines' silent property-name-as-column fallback.
 *       {@code IllegalStateException} stays "resolver bug".</li>
 * </ul>
 *
 * <h2>The join boundary (sharpened &sect;119)</h2>
 * OBJECT-SPACE (mapping-implied) joins &mdash; association navigation,
 * {@code @Join} chain slots, otherwise fallbacks &mdash; are access paths,
 * not row-set definitions: they are demand-gated aggressively (a join is
 * emitted iff a consumed expression reads through it), deduped by
 * navigation-path identity across the WHOLE op chain. Exempt (row-set
 * defining): joins feeding a JoinMediated mapping ~filter or mapping
 * ~groupBy &mdash; demanded by the pipeline itself. RELATION-SPACE
 * (user-written) joins are NEVER elided. The I-layer treats every join it
 * receives as required.
 *
 * <h2>Positional rule table (H3)</h2>
 * <ul>
 *   <li>projection / sortBy-key / groupBy-key navigation &rArr; LEFT JOIN
 *       through the one whole-chain NavPath registry (deliberately beats
 *       plangen's unshared sort-key scalar subqueries).</li>
 *   <li>filter-position to-many navigation and class-typed
 *       {@code isEmpty}/{@code isNotEmpty} &rArr; correlated
 *       {@code [NOT] EXISTS} via a nested ObjectRelation (its joins are
 *       local to the subquery). Single form &mdash; the engine's
 *       LEFT-JOIN-to-DISTINCT+null-check variant is a dialect strategy
 *       seam, not built.</li>
 *   <li>scalar-property {@code isEmpty} &rArr; {@code IS NULL} (existing
 *       scalar lowering).</li>
 *   <li>every navigation join is LEFT; INNER only where an INNER mapping
 *       ~filter forces the class source into a filtered subselect.</li>
 * </ul>
 */
package com.legend.resolver;
