package com.gs.legend.model.store;

import com.gs.legend.model.def.RelationalOperation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a View within a Database definition.
 * Views are named queries usable like tables in mappings.
 * 
 * Pure syntax:
 * <pre>
 * View ViewName
 * (
 *     ~filter filterExpr
 *     ~groupBy (col1, col2)
 *     ~distinct
 *     columnName : dbOperation PRIMARY_KEY?,
 *     ...
 * )
 * </pre>
 * 
 * @param db             The parent database FQN (e.g., "store::DB"). Never empty.
 * @param schema         The schema name (empty for default)
 * @param name           The view name
 * @param filterMapping  Optional filter condition (null if none)
 * @param groupBy        GroupBy expressions (empty if none)
 * @param distinct       Whether ~distinct is specified
 * @param columnMappings The view columns (name -> expression)
 */
public record View(
        String db,
        String schema,
        String name,
        RelationalOperation filterMapping,
        List<RelationalOperation> groupBy,
        boolean distinct,
        List<ViewColumn> columnMappings
) {
    public View {
        Objects.requireNonNull(db, "Database FQN cannot be null");
        if (db.isBlank()) throw new IllegalArgumentException("Database FQN cannot be blank");
        Objects.requireNonNull(schema, "Schema cannot be null (use empty string for default)");
        Objects.requireNonNull(name, "View name cannot be null");
        groupBy = groupBy != null ? List.copyOf(groupBy) : List.of();
        columnMappings = columnMappings != null ? List.copyOf(columnMappings) : List.of();
    }

    /**
     * Creates a view in the default schema.
     */
    public View(String db, String name, RelationalOperation filterMapping, List<RelationalOperation> groupBy,
                boolean distinct, List<ViewColumn> columnMappings) {
        this(db, "", name, filterMapping, groupBy, distinct, columnMappings);
    }

    /**
     * @return The database-local name (schema.view or just view). Used in SQL.
     */
    public String dbName() {
        return schema.isEmpty() ? name : schema + "." + name;
    }

    /**
     * @return The fully qualified name: db + "." + dbName(). Used as SymbolTable key.
     */
    public String qualifiedName() {
        return db + "." + dbName();
    }

    /**
     * Finds a column mapping by name.
     */
    public Optional<ViewColumn> findColumn(String columnName) {
        return columnMappings.stream()
                .filter(c -> c.name().equals(columnName))
                .findFirst();
    }

    /**
     * A column within a view.
     * 
     * @param name       The column name
     * @param expression The expression defining this column's value
     * @param primaryKey Whether this is a PRIMARY KEY column
     */
    public record ViewColumn(
            String name,
            RelationalOperation expression,
            boolean primaryKey
    ) {
        public ViewColumn {
            Objects.requireNonNull(name, "Column name cannot be null");
            Objects.requireNonNull(expression, "Expression cannot be null");
        }
    }
}
