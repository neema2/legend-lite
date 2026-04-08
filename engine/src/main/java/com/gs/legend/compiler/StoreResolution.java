package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;

import java.util.Map;

/**
 * Per-node store resolution produced by {@link MappingResolver}.
 *
 * <p>Same sidecar pattern as {@link TypeInfo}: an
 * {@code IdentityHashMap<ValueSpecification, StoreResolution>} maps AST nodes
 * to their resolved store info. PlanGenerator reads this instead of
 * {@link com.gs.legend.model.mapping.ClassMapping}.
 *
 * <p>MappingResolver produces high-level descriptors (WHAT);
 * PlanGenerator renders SQL (HOW).
 *
 * @param tableName        Physical table name for the root source (e.g., "T_RAW_PERSON")
 * @param propertyToColumn Pure property name → physical column name (simple mappings)
 * @param properties       Per-property resolution descriptors (handles expression, enum, M2M)
 * @param joins            Association property → join resolution
 * @param filterExpr       Pre-compiled ~filter expression (null if none). Used by M2M filters only.
 * @param nested           True for struct-literal identity mappings (nested field access)
 * @param sourceRelation   Synthesized source Relation ValueSpec for relational mappings (null for M2M/identity).
 *                         Encapsulates tableReference + filter + distinct + join chains.
 * @param extendOverride   Cancellation info for extend nodes (null = not an extend node / all active).
 */
public record StoreResolution(
        String tableName,
        Map<String, String> propertyToColumn,
        Map<String, PropertyResolution> properties,
        Map<String, JoinResolution> joins,
        ValueSpecification filterExpr,
        boolean nested,
        ValueSpecification sourceRelation,
        ExtendOverride extendOverride) {

    /** Constructor without extendOverride (default for all non-extend nodes). */
    public StoreResolution(
            String tableName,
            Map<String, String> propertyToColumn,
            Map<String, PropertyResolution> properties,
            Map<String, JoinResolution> joins,
            ValueSpecification filterExpr,
            boolean nested,
            ValueSpecification sourceRelation) {
        this(tableName, propertyToColumn, properties, joins, filterExpr, nested, sourceRelation, null);
    }

    /** Constructor without sourceRelation or extendOverride (for M2M and identity mappings). */
    public StoreResolution(
            String tableName,
            Map<String, String> propertyToColumn,
            Map<String, PropertyResolution> properties,
            Map<String, JoinResolution> joins,
            ValueSpecification filterExpr,
            boolean nested) {
        this(tableName, propertyToColumn, properties, joins, filterExpr, nested, null, null);
    }

    /** Factory for extend-only StoreResolutions (carries only cancellation info). */
    public static StoreResolution forExtend(ExtendOverride override) {
        return new StoreResolution(null, Map.of(), Map.of(), Map.of(), null, false, null, override);
    }

    /**
     * Column-level cancellation info for extend nodes.
     * Stamped by MappingResolver on sourceRelation extend nodes;
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
         * M2M expression: property computed from source via a Pure expression.
         * The expression AST is pre-compiled by TypeChecker; PlanGenerator
         * calls generateScalar on it with the sourceResolution for context.
         */
        record M2MExpression(ValueSpecification expression, StoreResolution sourceResolution)
                implements PropertyResolution {}

        /**
         * DynaFunction expression: property computed from a relational expression.
         * PlanGenerator calls generateScalar on the expression instead of simple column lookup.
         *
         * @param expression Pre-compiled ValueSpecification expression tree
         */
        record DynaFunction(ValueSpecification expression) implements PropertyResolution {}

        /**
         * Embedded column: sub-property resolves to a column on the PARENT table (no JOIN).
         * PlanGenerator uses the parent alias to reference the column directly.
         *
         * @param columnName Physical column on the parent table
         */
        record EmbeddedColumn(String columnName) implements PropertyResolution {}
    }

    /**
     * Resolved association navigation (property → JOIN).
     *
     * @param targetTable      Physical table to join to
     * @param sourceParam      Variable name for source side in joinCondition ValueSpec
     * @param targetParam      Variable name for target side in joinCondition ValueSpec
     * @param isToMany         True if association has [*] multiplicity
     * @param joinCondition    Pre-converted join condition as ValueSpecification
     * @param sourceColumns    Source-side column names referenced by the join condition (for graphFetch projection)
     * @param targetResolution StoreResolution for the target table (for nested property access)
     * @param embedded         True if this is an embedded mapping (sub-properties on parent table, no JOIN)
     */
    public record JoinResolution(
            String targetTable,
            String sourceParam,
            String targetParam,
            boolean isToMany,
            ValueSpecification joinCondition,
            java.util.Set<String> sourceColumns,
            StoreResolution targetResolution,
            boolean embedded) {

        /** Convenience constructor for non-embedded joins. */
        public JoinResolution(
                String targetTable, String sourceParam, String targetParam,
                boolean isToMany, ValueSpecification joinCondition,
                java.util.Set<String> sourceColumns, StoreResolution targetResolution) {
            this(targetTable, sourceParam, targetParam, isToMany,
                    joinCondition, sourceColumns, targetResolution, false);
        }
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

    /** True if this resolution has a mapping filter. */
    public boolean hasFilter() {
        return filterExpr != null;
    }

    /** True if this is an extend node with cancellation info. */
    public boolean hasExtendOverride() {
        return extendOverride != null;
    }

    /** Extracts the source StoreResolution from M2M property expressions (for filter context). */
    public StoreResolution sourceResolution() {
        for (var res : properties.values()) {
            if (res instanceof PropertyResolution.M2MExpression m2m && m2m.sourceResolution() != null) {
                return m2m.sourceResolution();
            }
        }
        return null;
    }
}
