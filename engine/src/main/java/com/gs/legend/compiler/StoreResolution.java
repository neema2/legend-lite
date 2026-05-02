package com.gs.legend.compiler;

import com.gs.legend.compiler.typed.TypedSpec;

import java.util.Map;

/**
 * Per-node store resolution produced by {@link MappingResolver}.
 *
 * <p>Keyed sidecar: an {@code IdentityHashMap<TypedSpec, StoreResolution>}
 * maps typed HIR nodes to their resolved physical-store info. PlanGenerator
 * reads this alongside the typed HIR to emit SQL.
 *
 * <p>MappingResolver produces high-level descriptors (WHAT);
 * PlanGenerator renders SQL (HOW).
 *
 * @param tableName        Physical table name for the root source (e.g., "T_RAW_PERSON").
 * @param className        FQN of the mapped class this resolution belongs to (null for
 *                         identity mappings and extend-override markers).
 * @param propertyToColumn Pure property name → physical column name (simple mappings).
 * @param joins            Association property → join resolution.
 * @param extendNodeCols    For {@link com.gs.legend.compiler.typed.TypedExtend}
 *                          nodes, records which declared extend-column
 *                          aliases are still active after the query-usage
 *                          analysis. {@code null} means “not an extend
 *                          node, or no pruning was performed” — lower as-is.
 *                          See {@link ExtendNodeCols}.
 */
public record StoreResolution(
        String tableName,
        String className,
        Map<String, String> propertyToColumn,
        Map<String, JoinResolution> joins,
        ExtendNodeCols extendNodeCols) {

    /** Physical store resolution without an extend-node pruning marker. */
    public StoreResolution(
            String tableName,
            String className,
            Map<String, String> propertyToColumn,
            Map<String, JoinResolution> joins) {
        this(tableName, className, propertyToColumn, joins, null);
    }

    /**
     * Pruning marker stamped on a {@link com.gs.legend.compiler.typed.TypedExtend}
     * node — the set of column aliases the query actually reads, after
     * intersecting declared aliases with {@code classPropertyAccesses}.
     *
     * <p>Produced by {@code MappingResolver.pruneUnusedExtendCols} for
     * synth-body extends whose declared columns are a strict superset of
     * what the query reads. Read by {@code ExtendLowering} to skip unused
     * columns (and their traversal joins, for synth-body
     * association/embedded extends).
     *
     * <p>Semantics:
     * <ul>
     *   <li>marker absent ({@code null}) — no pruning; render every column.</li>
     *   <li>{@link #activeAliases} empty — whole extend node is skippable
     *       ({@link #isFullyCancelled}).</li>
     *   <li>non-empty — only the listed aliases are active; skip the rest
     *       ({@link #isActiveCol}).</li>
     * </ul>
     */
    public record ExtendNodeCols(java.util.Set<String> activeAliases) {
        public boolean isFullyCancelled() { return activeAliases.isEmpty(); }
        public boolean isActiveCol(String alias) { return activeAliases.contains(alias); }
    }

    /**
     * How an association property navigates to its target. Sealed by physical
     * shape so consumers switch on a typed variant rather than null-checking
     * fields (architecture doc §6.2 — one sealed-type dispatch per axis).
     *
     * <p>Three variants:
     * <ul>
     *   <li>{@link FkJoin} — physical FK join: {@code LEFT JOIN target ON cond}.</li>
     *   <li>{@link Embedded} — sub-mapping on parent table; no JOIN, no alias.</li>
     *   <li>{@link StructArrayUnnest} — inline struct-array: lateral UNNEST hop
     *       (architecture doc §1: dialect projects element fields up so the
     *       row-shape invariant {@code alias."<f>"} holds).</li>
     * </ul>
     */
    public sealed interface JoinResolution {
        boolean isToMany();
        StoreResolution targetResolution();

        /**
         * Otherwise mapping: the property has BOTH an embedded sub-mapping
         * (sub-properties live as columns on the PARENT table — no JOIN) AND
         * an FK fallback (used for any sub-property not covered by the embedded
         * sub-cols). Pure syntax:
         * <pre>
         *   firm (
         *       legalName: T_PERSON.FIRM_NAME    // embedded
         *   ) Otherwise([firm_set1]: @Person_Firm)  // FK fallback
         * </pre>
         *
         * <p>Property access dispatches at the hop site: if the leaf property
         * name is in {@link #embeddedSubCols}, emit a column on the parent
         * alias directly (no navigation, no JOIN). Otherwise unwrap to
         * {@link #fallback} and continue the FK navigation. This makes JOIN
         * emission lazy: when only embedded sub-properties are accessed, no
         * physical JOIN is ever installed (NavScope stays empty).
         *
         * @param embeddedSubCols Sub-property name → physical column on the
         *                        parent table.
         * @param fallback        FK join used for non-embedded sub-properties.
         */
        record Otherwise(
                java.util.Map<String, String> embeddedSubCols,
                FkJoin fallback) implements JoinResolution {
            @Override public boolean isToMany() { return fallback.isToMany(); }
            @Override public StoreResolution targetResolution() { return fallback.targetResolution(); }
        }

        /**
         * Physical FK join: rendered as {@code LEFT JOIN <targetTable> ON <joinCondition>}.
         *
         * @param targetTable      Physical table to join to.
         * @param sourceParam      Variable name for source side in the join condition.
         * @param targetParam      Variable name for target side in the join condition.
         * @param isToMany         True if association has [*] multiplicity.
         * @param joinCondition    Typed join predicate (2-param TypedLambda body).
         * @param targetResolution {@link StoreResolution} for the target table.
         */
        record FkJoin(
                String targetTable,
                String sourceParam,
                String targetParam,
                boolean isToMany,
                TypedSpec joinCondition,
                StoreResolution targetResolution) implements JoinResolution {}

        /**
         * Embedded sub-mapping: sub-properties live on the parent table — no
         * JOIN, no alias change. Callers advance the active store and continue
         * resolving against the parent row.
         */
        record Embedded(
                boolean isToMany,
                StoreResolution targetResolution) implements JoinResolution {}

        /**
         * Inline struct-array property (e.g. {@code Class.<Class[*]>}). Lowered
         * by {@link com.gs.legend.plan.lowering.Relations#install} as a
         * {@code LEFT JOIN <LateralUnnest> ON TRUE}. The dialect projects
         * {@code targetResolution.propertyToColumn().values()} up so downstream
         * property access is uniform {@code alias."<col>"}.
         *
         * @param arrayProperty    Pure property name on the parent row that
         *                         holds the struct array (also the column name
         *                         for identity stores).
         */
        record StructArrayUnnest(
                String arrayProperty,
                boolean isToMany,
                StoreResolution targetResolution) implements JoinResolution {}
    }

    // ===== Convenience =====

    /** Resolves a property to its column name. Returns null if not a simple column mapping. */
    public String columnFor(String propertyName) {
        return propertyToColumn.get(propertyName);
    }

    /** True if this resolution has any association joins. */
    public boolean hasJoins() {
        return joins != null && !joins.isEmpty();
    }


}
