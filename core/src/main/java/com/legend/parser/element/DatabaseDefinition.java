package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure {@code Database} declaration &mdash; the relational store.
 *
 * <p>Pure syntax:
 * <pre>
 *   Database package::DatabaseName
 *   (
 *     include other::Database
 *     Schema S ( Table T ( COL TYPE PRIMARY KEY, ... ) View V ( ... ) )
 *     Table T ( COL TYPE NOT NULL, ... )
 *     View V ( ~filter F ~groupBy(...) ~distinct  col: expr PRIMARY KEY, ... )
 *     Join JoinName(T1.A = T2.A and T1.B = T2.B)
 *     Filter F(T.col = 'x')
 *     MultiGrainFilter MGF(...)
 *   )
 * </pre>
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.DatabaseDefinition}
 * minus {@code simpleName()} / {@code packagePath()} / convenience finders
 * (drop-for-clarity policy &mdash; see {@link PackageableElement}).
 *
 * <h2>View filter encoding</h2>
 * {@link ViewDefinition#filter()} is a sealed {@link FilterMapping} carrying
 * the parsed shape of the view's {@code ~filter ...} clause. See that type
 * for the rationale; the short version is that the four legal grammar forms
 * collapse onto two sealed pairs ({@link FilterPointer.Local} vs
 * {@link FilterPointer.Cross}; {@link FilterMapping.Direct} vs
 * {@link FilterMapping.JoinMediated}) so every consumer dispatches via
 * pattern match instead of inspecting nullable fields.
 */
public record DatabaseDefinition(
        String qualifiedName,
        List<String> includes,
        List<SchemaDefinition> schemas,
        List<TableDefinition> tables,
        List<ViewDefinition> views,
        List<JoinDefinition> joins,
        List<FilterDefinition> filters,
        List<FilterDefinition> multiGrainFilters) implements PackageableElement {

    public DatabaseDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes          = includes          == null ? List.of() : List.copyOf(includes);
        schemas           = schemas           == null ? List.of() : List.copyOf(schemas);
        tables            = tables            == null ? List.of() : List.copyOf(tables);
        views             = views             == null ? List.of() : List.copyOf(views);
        joins             = joins             == null ? List.of() : List.copyOf(joins);
        filters           = filters           == null ? List.of() : List.copyOf(filters);
        multiGrainFilters = multiGrainFilters == null ? List.of() : List.copyOf(multiGrainFilters);
    }

    /** A named schema containing tables and views. */
    public record SchemaDefinition(
            String name,
            List<TableDefinition> tables,
            List<ViewDefinition> views) {
        public SchemaDefinition {
            Objects.requireNonNull(name, "Schema name cannot be null");
            tables = tables == null ? List.of() : List.copyOf(tables);
            views  = views  == null ? List.of() : List.copyOf(views);
        }
    }

    /** A table with ordered column declarations. */
    public record TableDefinition(String name, List<ColumnDefinition> columns) {
        public TableDefinition {
            Objects.requireNonNull(name, "Table name cannot be null");
            Objects.requireNonNull(columns, "Columns cannot be null");
            columns = List.copyOf(columns);
        }
    }

    /**
     * A column declaration.
     *
     * @param name       column name as written (post-quote-strip)
     * @param dataType   SQL data type as written, including any size or
     *                   precision spec (e.g. {@code "INTEGER"},
     *                   {@code "VARCHAR(100)"}, {@code "DECIMAL(10,2)"})
     * @param primaryKey {@code true} if {@code PRIMARY KEY} appeared
     * @param notNull    {@code true} if {@code NOT NULL} appeared
     *                   (implied by {@code PRIMARY KEY} in engine; we
     *                   preserve the same semantics)
     */
    public record ColumnDefinition(String name, String dataType, boolean primaryKey, boolean notNull) {
        public ColumnDefinition {
            Objects.requireNonNull(name, "Column name cannot be null");
            Objects.requireNonNull(dataType, "Data type cannot be null");
        }
    }

    /**
     * A view definition &mdash; a named relational expression usable like
     * a table in mappings.
     *
     * @param name           view name
     * @param filter         parsed {@code ~filter ...} clause, or {@code null}
     *                       when no {@code ~filter} was written. When
     *                       present, the sealed {@link FilterMapping}
     *                       distinguishes direct vs join-mediated forms.
     * @param groupByColumns optional {@code ~groupBy} expressions
     * @param distinct       whether {@code ~distinct} was written
     * @param columnMappings the view's projected columns in declaration order
     */
    public record ViewDefinition(
            String name,
            FilterMapping filter,
            List<RelationalOperation> groupByColumns,
            boolean distinct,
            List<ViewColumnMapping> columnMappings) {
        public ViewDefinition {
            Objects.requireNonNull(name, "View name cannot be null");
            // filter intentionally nullable: absence of any ~filter clause
            // is a meaningful state (most views don't have one).
            groupByColumns = groupByColumns == null ? List.of() : List.copyOf(groupByColumns);
            columnMappings = columnMappings == null ? List.of() : List.copyOf(columnMappings);
        }

        /**
         * One projected column of a view.
         *
         * @param name        column name
         * @param targetSetId optional {@code [id]} bracket marker; {@code null} if absent
         * @param expression  the value expression
         * @param primaryKey  whether trailing {@code PRIMARY KEY} marker was written
         */
        public record ViewColumnMapping(
                String name,
                String targetSetId,
                RelationalOperation expression,
                boolean primaryKey) {
            public ViewColumnMapping {
                Objects.requireNonNull(name, "Column name cannot be null");
                Objects.requireNonNull(expression, "Expression cannot be null");
            }
        }
    }

    /**
     * A named join definition. The {@code operation} is the full relational
     * expression tree forming the join condition.
     */
    public record JoinDefinition(String name, RelationalOperation operation) {
        public JoinDefinition {
            Objects.requireNonNull(name, "Join name cannot be null");
            Objects.requireNonNull(operation, "Operation cannot be null");
        }
    }

    /**
     * A named filter definition. Also used to model {@code MultiGrainFilter}
     * since the shapes are structurally identical.
     */
    public record FilterDefinition(String name, RelationalOperation condition) {
        public FilterDefinition {
            Objects.requireNonNull(name, "Filter name cannot be null");
            Objects.requireNonNull(condition, "Condition cannot be null");
        }
    }
}
