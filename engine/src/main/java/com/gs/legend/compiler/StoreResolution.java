package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.store.Join;

import java.util.List;
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
 * @param filterExpr       Pre-parsed ~filter expression from mapping (null if none)
 * @param nested           True for struct-literal identity mappings (nested field access)
 */
public record StoreResolution(
        String tableName,
        Map<String, String> propertyToColumn,
        Map<String, PropertyResolution> properties,
        Map<String, JoinResolution> joins,
        ValueSpecification filterExpr,
        boolean nested) {

    /**
     * How a single property resolves to a physical store element.
     * PlanGenerator switches on this to emit the right SqlExpr —
     * no ClassMapping dispatch needed.
     */
    public sealed interface PropertyResolution {
        /** Simple column: property maps directly to a column name. */
        record Column(String columnName) implements PropertyResolution {}

        /** Expression access: variant/JSON column with key extraction + optional cast. */
        record Expression(String columnName, String jsonKey, String castType)
                implements PropertyResolution {}

        /** Enum mapping: column value translated via CASE WHEN. */
        record Enum(String columnName, Map<String, List<Object>> enumMap)
                implements PropertyResolution {}

        /**
         * M2M expression: property computed from source via a Pure expression.
         * The expression AST is pre-compiled by TypeChecker; PlanGenerator
         * calls generateScalar on it with the sourceResolution for context.
         */
        record M2MExpression(ValueSpecification expression, StoreResolution sourceResolution)
                implements PropertyResolution {}
    }

    /**
     * Resolved association navigation (property → JOIN).
     *
     * @param targetTable      Physical table to join to
     * @param sourceColumn     Column on the source side of the join
     * @param targetColumn     Column on the target side of the join
     * @param isToMany         True if association has [*] multiplicity
     * @param join             The resolved Join object (for complex join conditions)
     * @param targetResolution StoreResolution for the target table (for nested property access)
     */
    public record JoinResolution(
            String targetTable,
            String sourceColumn,
            String targetColumn,
            boolean isToMany,
            Join join,
            StoreResolution targetResolution) {
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
