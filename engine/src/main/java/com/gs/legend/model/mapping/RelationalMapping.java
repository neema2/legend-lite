package com.gs.legend.model.mapping;

import com.gs.legend.ast.AppliedProperty;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.ast.Variable;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.store.Column;
import com.gs.legend.model.store.PropertyMapping;
import com.gs.legend.model.store.SqlDataType;
import com.gs.legend.model.store.Table;
import com.gs.legend.model.store.View;
import com.gs.legend.model.m3.Type;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Links a Pure Class to a relational Table with property-to-column mappings.
 * 
 * @param pureClass        The Pure class being mapped
 * @param table            The target relational table
 * @param propertyMappings Mappings from Pure properties to table columns
 * @param nested           If true, properties are accessed via nested struct field paths
 *                         (e.g., {@code t0."alias"."field"}) rather than flat column refs.
 *                         Set for struct literals (identity mappings) and future variant-column mappings.
 * @param setId            Optional set implementation ID (null if default)
 * @param isRoot           Whether this is the root mapping for the class
 * @param distinct           Whether ~distinct is specified on this mapping
 * @param filterFqn          FQN of the ~filter (e.g., "store::DB.ActiveFilter"). Null if none.
 * @param embeddedMappings   Map from embedded property name to sub-property mappings.
 *                           e.g., firm → [legalName→FIRM_NAME, revenue→FIRM_REVENUE].
 *                           Empty if no embedded properties.
 * @param groupByColumns     Column names for ~groupBy (empty if none)
 * @param view               The raw View object when ~mainTable references a view (null for regular tables).
 *                           MappingNormalizer uses this to synthesize the view's source relation.
 * @param sourceUrl          URL for external data sources (e.g., JSON data: or file: URI). Null for regular tables.
 */
public record RelationalMapping(
        PureClass pureClass,
        Table table,
        List<PropertyMapping> propertyMappings,
        boolean nested,
        String setId,
        boolean isRoot,
        boolean distinct,
        String filterFqn,
        Map<String, List<PropertyMapping>> embeddedMappings,
        List<String> groupByColumns,
        View view,
        String sourceUrl) implements ClassMapping {

    public RelationalMapping(PureClass pureClass, Table table, List<PropertyMapping> propertyMappings) {
        this(pureClass, table, propertyMappings, false, null, false, false, null, Map.of(), List.of(), null, null);
    }

    public RelationalMapping(PureClass pureClass, Table table, List<PropertyMapping> propertyMappings, boolean nested) {
        this(pureClass, table, propertyMappings, nested, null, false, false, null, Map.of(), List.of(), null, null);
    }

    public RelationalMapping {
        Objects.requireNonNull(pureClass, "Pure class cannot be null");
        Objects.requireNonNull(table, "Table cannot be null");
        Objects.requireNonNull(propertyMappings, "Property mappings cannot be null");

        // Ensure immutability
        propertyMappings = List.copyOf(propertyMappings);
        if (embeddedMappings == null) embeddedMappings = Map.of();
        if (groupByColumns == null) groupByColumns = List.of();
    }

    /**
     * Returns a copy with the given property mappings (all other fields unchanged).
     * Used by MappingNormalizer to store view-resolved PMs.
     */
    public RelationalMapping withPropertyMappings(List<PropertyMapping> resolvedPMs) {
        return new RelationalMapping(pureClass, table, resolvedPMs, nested, setId, isRoot,
                distinct, filterFqn, embeddedMappings, groupByColumns, view, sourceUrl);
    }

    public RelationalMapping withGroupByColumns(List<String> resolvedGroupBy) {
        return new RelationalMapping(pureClass, table, propertyMappings, nested, setId, isRoot,
                distinct, filterFqn, embeddedMappings, resolvedGroupBy, view, sourceUrl);
    }

    /**
     * Gets the column name for a given property name.
     * 
     * @param propertyName The Pure property name
     * @return Optional containing the column name if mapped
     */
    public Optional<String> getColumnForProperty(String propertyName) {
        return propertyMappings.stream()
                .filter(pm -> pm.propertyName().equals(propertyName))
                .map(PropertyMapping::columnName)
                .findFirst();
    }

    /**
     * Gets the property name for a given column name.
     * 
     * @param columnName The relational column name
     * @return Optional containing the property name if mapped
     */
    public Optional<String> getPropertyForColumn(String columnName) {
        return propertyMappings.stream()
                .filter(pm -> pm.columnName().equals(columnName))
                .map(PropertyMapping::propertyName)
                .findFirst();
    }

    /**
     * @return A map from property names to column names
     */
    public Map<String, String> propertyToColumnMap() {
        return propertyMappings.stream()
                .collect(Collectors.toMap(
                        PropertyMapping::propertyName,
                        PropertyMapping::columnName));
    }

    /**
     * @return A map from column names to property names
     */
    public Map<String, String> columnToPropertyMap() {
        return propertyMappings.stream()
                .collect(Collectors.toMap(
                        PropertyMapping::columnName,
                        PropertyMapping::propertyName));
    }

    /**
     * Gets the property mapping for a given property name.
     * 
     * @param propertyName The Pure property name
     * @return Optional containing the PropertyMapping if found
     */
    public Optional<PropertyMapping> getPropertyMapping(String propertyName) {
        return propertyMappings.stream()
                .filter(pm -> pm.propertyName().equals(propertyName))
                .findFirst();
    }

    /**
     * Gets the Type for a given property name by looking up the property's
     * generic type in the Pure class definition.
     * 
     * @param propertyName The Pure property name
     * @return The Type for this property
     * @throws IllegalArgumentException if property not found in Pure class
     */
    public com.gs.legend.model.m3.Type pureTypeForProperty(String propertyName,
            com.gs.legend.model.ModelContext ctx) {
        return pureClass.findProperty(propertyName, ctx)
                .map(p -> p.type())
                .orElseThrow(() -> new IllegalArgumentException(
                        "Property '" + propertyName + "' not found in class " + pureClass.name()));
    }

    /**
     * Creates an identity mapping for a struct literal: each scalar primitive
     * property maps to a virtual column with the same name.
     *
     * <p>Class-typed and to-many properties are excluded from the mapping
     * because they are modeled as associations (resolved via TypeInfo).
     * PlanGenerator uses the absence of a Join in the AssociationTarget
     * to emit UNNEST instead of JOIN.
     *
     * @param pureClass The Pure class to create an identity mapping for
     * @return A RelationalMapping with a synthetic Table and identity PropertyMappings
     */
    public static RelationalMapping identity(PureClass pureClass, com.gs.legend.model.ModelContext ctx) {
        var columns = new java.util.ArrayList<Column>();
        var mappings = new java.util.ArrayList<PropertyMapping>();

        for (var prop : pureClass.allProperties(ctx)) {
            // Map ALL properties — for struct literals, every property is a physical column,
            // including collections (arrays needing UNNEST) and class-typed (nested structs).
            SqlDataType sqlType;
            if (prop.type() instanceof com.gs.legend.model.m3.Type.Primitive pr) {
                sqlType = SqlDataType.fromPrimitive(pr);
            } else {
                // Class-typed or enum-typed property → use VARIANT (JSON/struct)
                sqlType = SqlDataType.SEMISTRUCTURED;
            }
            columns.add(Column.nullable(prop.name(), sqlType));
            mappings.add(PropertyMapping.column(prop.name(), prop.name()));
        }

        // Synthetic virtual table — name is the lowercased class name
        String simpleName = pureClass.name();
        String tableName = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
        var table = new Table("__identity__", tableName, columns);

        return new RelationalMapping(pureClass, table, mappings, true);
    }

    /**
     * Creates a variant identity mapping for a JSON-backed source class.
     * The table has a single SEMISTRUCTURED {@code data} column; each primitive
     * property maps via expression access: {@code data->get('propName', @Type)}.
     *
     * <p>The {@code sourceUrl} is threaded through to StoreResolution so the
     * dialect can render the appropriate inline VARIANT subquery in the FROM clause.
     *
     * @param pureClass The Pure class whose properties define the mapping
     * @param sourceUrl The data URL (data: URI, file:, or http:)
     * @return A RelationalMapping with expression-access PropertyMappings and sourceUrl
     */
    public static RelationalMapping variantIdentity(PureClass pureClass, String sourceUrl,
            com.gs.legend.model.ModelContext ctx) {
        var columns = java.util.List.of(Column.nullable("data", SqlDataType.SEMISTRUCTURED));
        // Internal table name — not registered, only used for mapping structure
        String simpleName = pureClass.name();
        String internalName = "_json_" + simpleName;
        var table = new Table("__json__", internalName, columns);

        var mappings = new java.util.ArrayList<PropertyMapping>();
        for (var prop : pureClass.allProperties(ctx)) {
            String pureType = null;
            if (prop.type() instanceof com.gs.legend.model.m3.Type.Primitive pr) {
                pureType = pr.pureName();
            }
            String expr = "->get('" + prop.name() + "'" +
                    (pureType != null ? ", @" + pureType : "") + ")";
            mappings.add(PropertyMapping.expression(prop.name(), "data", expr));
        }

        return new RelationalMapping(pureClass, table, mappings, false, null, false, false,
                null, Map.of(), List.of(), null, sourceUrl);
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("Mapping ").append(pureClass.qualifiedName())
                .append(" -> ").append(table.dbName()).append(" {\n");
        for (PropertyMapping pm : propertyMappings) {
            sb.append("    ").append(pm).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }

    // ========== ClassMapping interface ==========

    @Override
    public PureClass targetClass() {
        return pureClass;
    }

    @Override
    public Table sourceTable() {
        return table;
    }

    @Override
    public ValueSpecification expressionForProperty(String propertyName) {
        var pmOpt = getPropertyMapping(propertyName);
        if (pmOpt.isEmpty()) {
            throw new IllegalArgumentException(
                    "Property '" + propertyName + "' not found in mapping for " + pureClass.name());
        }
        var pm = pmOpt.get();
        // Simple column reference: $src.COLUMN_NAME
        return new AppliedProperty(pm.columnName(), List.of(new Variable("src")));
    }

    @Override
    public Type typeForProperty(String propertyName, com.gs.legend.model.ModelContext ctx) {
        return pureTypeForProperty(propertyName, ctx);
    }

    @Override
    public boolean hasProperty(String propertyName) {
        return propertyMappings.stream()
                .anyMatch(pm -> pm.propertyName().equals(propertyName));
    }

    @Override
    public String resolveColumn(String propertyName) {
        if (propertyName == null) return null;
        return getColumnForProperty(propertyName).orElse(null);
    }
}
