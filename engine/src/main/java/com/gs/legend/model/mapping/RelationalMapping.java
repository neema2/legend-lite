package com.gs.legend.model.mapping;

import com.gs.legend.ast.AppliedProperty;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.ast.Variable;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.store.Column;
import com.gs.legend.model.store.PropertyMapping;
import com.gs.legend.model.store.SqlDataType;
import com.gs.legend.model.store.Table;
import com.gs.legend.plan.GenericType;

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
 * @param distinct         Whether ~distinct is specified on this mapping
 * @param filterName       Name of the ~filter (null if none)
 * @param filterDbName     Database name qualifying the filter (null if none)
 */
public record RelationalMapping(
        PureClass pureClass,
        Table table,
        List<PropertyMapping> propertyMappings,
        boolean nested,
        String setId,
        boolean isRoot,
        boolean distinct,
        String filterName,
        String filterDbName) implements ClassMapping {

    public RelationalMapping(PureClass pureClass, Table table, List<PropertyMapping> propertyMappings) {
        this(pureClass, table, propertyMappings, false, null, false, false, null, null);
    }

    public RelationalMapping(PureClass pureClass, Table table, List<PropertyMapping> propertyMappings, boolean nested) {
        this(pureClass, table, propertyMappings, nested, null, false, false, null, null);
    }

    public RelationalMapping {
        Objects.requireNonNull(pureClass, "Pure class cannot be null");
        Objects.requireNonNull(table, "Table cannot be null");
        Objects.requireNonNull(propertyMappings, "Property mappings cannot be null");

        // Ensure immutability
        propertyMappings = List.copyOf(propertyMappings);
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
     * Gets the GenericType for a given property name by looking up the property's
     * generic type in the Pure class definition.
     * 
     * @param propertyName The Pure property name
     * @return The GenericType for this property
     * @throws IllegalArgumentException if property not found in Pure class
     */
    public com.gs.legend.plan.GenericType pureTypeForProperty(String propertyName) {
        return pureClass.findProperty(propertyName)
                .map(p -> com.gs.legend.plan.GenericType.fromType(p.genericType()))
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
    public static RelationalMapping identity(PureClass pureClass) {
        var columns = new java.util.ArrayList<Column>();
        var mappings = new java.util.ArrayList<PropertyMapping>();

        for (var prop : pureClass.allProperties()) {
            // Map ALL properties — for struct literals, every property is a physical column,
            // including collections (arrays needing UNNEST) and class-typed (nested structs).
            SqlDataType sqlType;
            if (prop.genericType() instanceof com.gs.legend.model.m3.PrimitiveType pt) {
                sqlType = SqlDataType.fromPrimitiveType(pt);
            } else {
                // Class-typed property → use VARIANT (JSON/struct)
                sqlType = SqlDataType.SEMISTRUCTURED;
            }
            columns.add(Column.nullable(prop.name(), sqlType));
            mappings.add(PropertyMapping.column(prop.name(), prop.name()));
        }

        // Synthetic virtual table — name is the lowercased class name
        String simpleName = pureClass.name();
        String tableName = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
        var table = new Table(tableName, columns);

        return new RelationalMapping(pureClass, table, mappings, true);
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append("Mapping ").append(pureClass.qualifiedName())
                .append(" -> ").append(table.qualifiedName()).append(" {\n");
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
    public GenericType typeForProperty(String propertyName) {
        return pureTypeForProperty(propertyName);
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
