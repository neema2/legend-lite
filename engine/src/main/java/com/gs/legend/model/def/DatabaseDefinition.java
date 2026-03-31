package com.gs.legend.model.def;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Pure Database (Store) definition.
 * 
 * Pure syntax:
 * <pre>
 * Database package::DatabaseName
 * (
 *     include OtherDB
 *     Schema S ( Table T (...) View V (...) )
 *     Table TABLE_NAME ( COLUMN_NAME DATA_TYPE, ... )
 *     View VIEW_NAME ( col : expression, ... )
 *     Join JoinName(TABLE_A.COLUMN_A = TABLE_B.COLUMN_B)
 *     Filter FilterName(TABLE.COL = value)
 * )
 * </pre>
 * 
 * @param qualifiedName     The fully qualified database name
 * @param includes          Included database paths
 * @param schemas           Named schemas containing tables and views
 * @param tables            Top-level table definitions
 * @param views             Top-level view definitions
 * @param joins             Join definitions with full expression trees
 * @param filters           Named filter definitions
 * @param multiGrainFilters Multi-grain filter definitions
 */
public record DatabaseDefinition(
        String qualifiedName,
        List<String> includes,
        List<SchemaDefinition> schemas,
        List<TableDefinition> tables,
        List<ViewDefinition> views,
        List<JoinDefinition> joins,
        List<FilterDefinition> filters,
        List<FilterDefinition> multiGrainFilters
) implements PackageableElement {
    
    public DatabaseDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes = includes != null ? List.copyOf(includes) : List.of();
        schemas = schemas != null ? List.copyOf(schemas) : List.of();
        Objects.requireNonNull(tables, "Tables cannot be null");
        tables = List.copyOf(tables);
        views = views != null ? List.copyOf(views) : List.of();
        Objects.requireNonNull(joins, "Joins cannot be null");
        joins = List.copyOf(joins);
        filters = filters != null ? List.copyOf(filters) : List.of();
        multiGrainFilters = multiGrainFilters != null ? List.copyOf(multiGrainFilters) : List.of();
    }
    
    /**
     * Backward-compatible constructor: tables + joins only.
     */
    public DatabaseDefinition(String qualifiedName, List<TableDefinition> tables, List<JoinDefinition> joins) {
        this(qualifiedName, List.of(), List.of(), tables, List.of(), joins, List.of(), List.of());
    }

    /**
     * Backward-compatible constructor: tables only.
     */
    public DatabaseDefinition(String qualifiedName, List<TableDefinition> tables) {
        this(qualifiedName, List.of(), List.of(), tables, List.of(), List.of(), List.of(), List.of());
    }
    
    /**
     * @return The simple database name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }
    
    /**
     * Finds a table by name (searches top-level tables and all schemas).
     */
    public Optional<TableDefinition> findTable(String tableName) {
        // Search top-level tables first
        var found = tables.stream()
                .filter(t -> t.name().equals(tableName))
                .findFirst();
        if (found.isPresent()) return found;
        // Then search schemas
        return schemas.stream()
                .flatMap(s -> s.tables().stream())
                .filter(t -> t.name().equals(tableName))
                .findFirst();
    }
    
    /**
     * Finds a join by name.
     */
    public Optional<JoinDefinition> findJoin(String joinName) {
        return joins.stream()
                .filter(j -> j.name().equals(joinName))
                .findFirst();
    }

    /**
     * Finds a view by name (searches top-level views and all schemas).
     */
    public Optional<ViewDefinition> findView(String viewName) {
        var found = views.stream()
                .filter(v -> v.name().equals(viewName))
                .findFirst();
        if (found.isPresent()) return found;
        return schemas.stream()
                .flatMap(s -> s.views().stream())
                .filter(v -> v.name().equals(viewName))
                .findFirst();
    }

    /**
     * Finds a filter by name.
     */
    public Optional<FilterDefinition> findFilter(String filterName) {
        return filters.stream()
                .filter(f -> f.name().equals(filterName))
                .findFirst();
    }

    // ==================== Nested Records ====================
    
    /**
     * A named schema containing tables and views.
     * 
     * Pure syntax: {@code Schema S ( Table T (...) View V (...) )}
     * 
     * @param name   The schema name
     * @param tables Tables within this schema
     * @param views  Views within this schema
     */
    public record SchemaDefinition(
            String name,
            List<TableDefinition> tables,
            List<ViewDefinition> views
    ) {
        public SchemaDefinition {
            Objects.requireNonNull(name, "Schema name cannot be null");
            tables = tables != null ? List.copyOf(tables) : List.of();
            views = views != null ? List.copyOf(views) : List.of();
        }
    }

    /**
     * A table definition within a database.
     * 
     * @param name    The table name
     * @param columns The list of column definitions
     */
    public record TableDefinition(
            String name,
            List<ColumnDefinition> columns
    ) {
        public TableDefinition {
            Objects.requireNonNull(name, "Table name cannot be null");
            Objects.requireNonNull(columns, "Columns cannot be null");
            columns = List.copyOf(columns);
        }
        
        /**
         * Finds a column by name.
         */
        public Optional<ColumnDefinition> findColumn(String columnName) {
            return columns.stream()
                    .filter(c -> c.name().equals(columnName))
                    .findFirst();
        }
    }
    
    /**
     * A column definition within a table.
     * 
     * @param name       The column name
     * @param dataType   The SQL data type (e.g., "INTEGER", "VARCHAR(100)")
     * @param primaryKey Whether this column is part of the primary key
     * @param notNull    Whether this column is NOT NULL
     */
    public record ColumnDefinition(
            String name,
            String dataType,
            boolean primaryKey,
            boolean notNull
    ) {
        public ColumnDefinition {
            Objects.requireNonNull(name, "Column name cannot be null");
            Objects.requireNonNull(dataType, "Data type cannot be null");
        }
        
        public static ColumnDefinition of(String name, String dataType) {
            return new ColumnDefinition(name, dataType, false, false);
        }
        
        public static ColumnDefinition primaryKey(String name, String dataType) {
            return new ColumnDefinition(name, dataType, true, true);
        }
        
        public static ColumnDefinition notNull(String name, String dataType) {
            return new ColumnDefinition(name, dataType, false, true);
        }
    }

    /**
     * A view definition — a named query inside a Database, usable like a table in mappings.
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
     * @param name           The view name
     * @param filterMapping  Optional filter expression (null if none)
     * @param groupByColumns Optional groupBy expressions (empty if none)
     * @param distinct       Whether ~distinct is specified
     * @param columnMappings The view's column definitions (name → expression)
     */
    public record ViewDefinition(
            String name,
            RelationalOperation filterMapping,
            List<RelationalOperation> groupByColumns,
            boolean distinct,
            List<ViewColumnMapping> columnMappings
    ) {
        public ViewDefinition {
            Objects.requireNonNull(name, "View name cannot be null");
            groupByColumns = groupByColumns != null ? List.copyOf(groupByColumns) : List.of();
            columnMappings = columnMappings != null ? List.copyOf(columnMappings) : List.of();
        }

        /**
         * A column within a view.
         * 
         * @param name       The column name
         * @param targetSetId Optional target set ID (from {@code [id]} bracket, null if none)
         * @param expression The expression defining this column's value
         * @param primaryKey Whether this is a PRIMARY KEY column
         */
        public record ViewColumnMapping(
                String name,
                String targetSetId,
                RelationalOperation expression,
                boolean primaryKey
        ) {
            public ViewColumnMapping {
                Objects.requireNonNull(name, "Column name cannot be null");
                Objects.requireNonNull(expression, "Expression cannot be null");
            }
        }
    }
    
    /**
     * A join definition with a full expression tree condition.
     * 
     * Pure syntax: {@code Join JoinName(dbOperation)}
     * 
     * Examples:
     * <pre>
     * Join Simple(T1.ID = T2.FK_ID)
     * Join Multi(T1.A = T2.A and T1.B = T2.B)
     * Join SelfJoin(T1.PARENT_ID = {target}.ID)
     * Join FuncJoin(concat('pfx_', T1.NAME) = T2.PREFIXED_NAME)
     * </pre>
     * 
     * @param name      The join name
     * @param operation The full expression tree for the join condition
     */
    public record JoinDefinition(
            String name,
            RelationalOperation operation
    ) {
        public JoinDefinition {
            Objects.requireNonNull(name, "Join name cannot be null");
            Objects.requireNonNull(operation, "Operation cannot be null");
        }

        /**
         * Backward-compatible constructor for simple equi-joins.
         * Builds a {@code Comparison(ColumnRef(lT,lC), "=", ColumnRef(rT,rC))} operation.
         */
        public JoinDefinition(String name, String leftTable, String leftColumn, String rightTable, String rightColumn) {
            this(name, RelationalOperation.Comparison.eq(
                    RelationalOperation.ColumnRef.of(leftTable, leftColumn),
                    RelationalOperation.ColumnRef.of(rightTable, rightColumn)));
        }

        /**
         * Extracts left table name (backward compat). Throws if not a simple equi-join.
         */
        public String leftTable() {
            var eq = operation.asSimpleEquiJoin();
            if (eq == null) throw new UnsupportedOperationException("Not a simple equi-join: " + name);
            return eq.leftTable();
        }

        /**
         * Extracts left column name (backward compat). Throws if not a simple equi-join.
         */
        public String leftColumn() {
            var eq = operation.asSimpleEquiJoin();
            if (eq == null) throw new UnsupportedOperationException("Not a simple equi-join: " + name);
            return eq.leftColumn();
        }

        /**
         * Extracts right table name (backward compat). Throws if not a simple equi-join.
         */
        public String rightTable() {
            var eq = operation.asSimpleEquiJoin();
            if (eq == null) throw new UnsupportedOperationException("Not a simple equi-join: " + name);
            return eq.rightTable();
        }

        /**
         * Extracts right column name (backward compat). Throws if not a simple equi-join.
         */
        public String rightColumn() {
            var eq = operation.asSimpleEquiJoin();
            if (eq == null) throw new UnsupportedOperationException("Not a simple equi-join: " + name);
            return eq.rightColumn();
        }
        
        /**
         * Checks if this join involves the given table.
         * For simple equi-joins only; complex joins should inspect the operation tree directly.
         */
        public boolean involvesTable(String tableName) {
            var eq = operation.asSimpleEquiJoin();
            if (eq == null) return false;
            return eq.leftTable().equals(tableName) || eq.rightTable().equals(tableName);
        }
        
        /**
         * Gets the other table in a simple equi-join. Throws if not a simple equi-join.
         */
        public String getOtherTable(String tableName) {
            var eq = operation.asSimpleEquiJoin();
            if (eq == null) throw new UnsupportedOperationException("Not a simple equi-join: " + name);
            if (eq.leftTable().equals(tableName)) return eq.rightTable();
            if (eq.rightTable().equals(tableName)) return eq.leftTable();
            throw new IllegalArgumentException("Table " + tableName + " not in join " + name);
        }
    }

    /**
     * A named filter definition.
     * 
     * Pure syntax: {@code Filter FilterName(dbOperation)}
     * Also used for MultiGrainFilter.
     * 
     * @param name      The filter name
     * @param condition The filter condition expression tree
     */
    public record FilterDefinition(
            String name,
            RelationalOperation condition
    ) {
        public FilterDefinition {
            Objects.requireNonNull(name, "Filter name cannot be null");
            Objects.requireNonNull(condition, "Condition cannot be null");
        }
    }
}
