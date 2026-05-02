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
 * @param properties       Per-property resolution descriptors (handles expression, enum, M2M).
 * @param joins            Association property → join resolution.
 * @param nested           True for struct-literal identity mappings (nested field access).
 * @param extendOverride   Cancellation info for extend nodes (null = not an extend node / all active).
 */
public record StoreResolution(
        String tableName,
        String className,
        Map<String, String> propertyToColumn,
        Map<String, PropertyResolution> properties,
        Map<String, JoinResolution> joins,
        boolean nested,
        ExtendOverride extendOverride) {

    /** Physical store resolution without extendOverride. */
    public StoreResolution(
            String tableName,
            String className,
            Map<String, String> propertyToColumn,
            Map<String, PropertyResolution> properties,
            Map<String, JoinResolution> joins,
            boolean nested) {
        this(tableName, className, propertyToColumn, properties, joins, nested, null);
    }

    /** Factory for extend-only StoreResolutions (carries only cancellation info). */
    public static StoreResolution forExtend(ExtendOverride override) {
        return new StoreResolution(null, null, Map.of(), Map.of(), Map.of(), false, override);
    }

    /**
     * Column-level cancellation info for extend nodes.
     * Stamped by MappingResolver on sourceSpec extend nodes;
     * read by PlanGenerator in generateExtend.
     *
     * @param activeColumns Columns to keep (null = all active, empty = skip entire node)
     */
    public record ExtendOverride(java.util.Set<String> activeColumns) {
        public boolean isFullyCancelled() { return activeColumns != null && activeColumns.isEmpty(); }
        public boolean isActive(String col) { return activeColumns == null || activeColumns.contains(col); }
    }

    /**
     * How a single property resolves to a physical store element.
     * PlanGenerator switches on this to emit the right SqlExpr —
     * no ClassMapping dispatch needed.
     */
    public sealed interface PropertyResolution {
        /** Simple column: property maps directly to a column name. */
        record Column(String columnName) implements PropertyResolution {}

        /**
         * DynaFunction expression: property computed from a typed relational
         * expression (e.g., {@code concat($row.FIRST, ' ', $row.LAST)}).
         *
         * @param expression Typed HIR for the computed expression.
         */
        record DynaFunction(TypedSpec expression) implements PropertyResolution {}

        /**
         * Embedded column: sub-property resolves to a column on the PARENT table (no JOIN).
         * PlanGenerator uses the parent alias to reference the column directly.
         *
         * @param columnName Physical column on the parent table
         */
        record EmbeddedColumn(String columnName) implements PropertyResolution {}
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

    /** Gets the full PropertyResolution for a property. */
    public PropertyResolution resolveProperty(String propertyName) {
        return properties.get(propertyName);
    }

    /** True if this resolution has any association joins. */
    public boolean hasJoins() {
        return joins != null && !joins.isEmpty();
    }

    /** True if this is an extend node with cancellation info. */
    public boolean hasExtendOverride() {
        return extendOverride != null;
    }

}
