package org.finos.legend.engine.transpiler;

import org.finos.legend.engine.plan.*;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transpiles a RelationNode tree into a SQL string.
 * This is the core transpiler that implements the "Database-as-Runtime"
 * philosophy.
 * 
 * The generator traverses the logical plan and produces dialect-specific SQL.
 */
public final class SQLGenerator implements RelationNodeVisitor<String>, ExpressionVisitor<String> {

    private final SQLDialect dialect;

    // Track lambda parameters to avoid quoting them in SQL generation
    private final java.util.Set<String> lambdaParams = new java.util.HashSet<>();

    public SQLGenerator(SQLDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "Dialect cannot be null");
    }

    /**
     * Generates SQL from a relation node tree.
     * 
     * @param node The root node of the logical plan
     * @return The generated SQL string
     */
    public String generate(RelationNode node) {
        return node.accept(this);
    }

    /**
     * Generates SQL from an expression.
     * 
     * @param expression The expression to generate SQL for
     * @return The generated SQL string
     */
    public String generateExpression(Expression expression) {
        return expression.accept(this);
    }

    // ==================== RelationNode Visitors ====================

    @Override
    public String visit(TableNode table) {
        // Simple table scan: FROM "table" AS "alias"
        return "SELECT * FROM " + dialect.quoteIdentifier(table.table().name())
                + " AS " + dialect.quoteIdentifier(table.alias());
    }

    @Override
    public String visit(FilterNode filter) {
        // We need to handle the case where filter is on top of table or on top of
        // project
        RelationNode source = filter.source();
        String whereClause = filter.condition().accept(this);

        return switch (source) {
            case TableNode table -> {
                yield "SELECT * FROM " + dialect.quoteIdentifier(table.table().name())
                        + " AS " + dialect.quoteIdentifier(table.alias())
                        + " WHERE " + whereClause;
            }
            case ProjectNode project -> {
                // Push filter down into the project
                yield visitProjectWithFilter(project, filter);
            }
            case

                    FilterNode nestedFilter -> {
                // Combine filters with AND
                String innerSql = nestedFilter.accept(this);
                yield innerSql + " AND " + whereClause;
            }
            case
                    JoinNode join -> {
                // Filter on top of join
                String innerSql = join.accept(this);
                yield innerSql + " WHERE " + whereClause;
            }
            case
                    GroupByNode groupBy -> {
                // Filter on top of group by
                String innerSql = groupBy.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS grp WHERE " + whereClause;
            }
            case
                    SortNode sort -> {
                // Filter on top of sort
                String innerSql = sort.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS srt WHERE " + whereClause;
            }
            case
                    LimitNode limit -> {
                // Filter on top of limit
                String innerSql = limit.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS lim WHERE " + whereClause;
            }
            case
                    FromNode from -> {
                // Filter on top of from (unwrap and process)
                String innerSql = from.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS frm WHERE " + whereClause;
            }
            case
                    ExtendNode extend -> {
                // Filter on top of extend (window functions)
                String innerSql = extend.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS ext WHERE " + whereClause;
            }
            case
                    LateralJoinNode lateral -> {
                // Filter on top of lateral join (unnest)
                String innerSql = lateral.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS lat WHERE " + whereClause;
            }
            default -> {
                // Handle new node types (DistinctNode, RenameNode, ConcatenateNode)
                String innerSql = source.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS src WHERE " + whereClause;
            }
        };
    }

    @Override
    public String visit(ProjectNode project) {
        // Check the source type to construct appropriate SQL
        return switch (project.source()) {
            case TableNode table -> generateSelectFromTable(project, table, null);
            case FilterNode filter -> visitProjectWithFilter(project, filter);
            case JoinNode join -> visitProjectWithJoin(project, join, null);
            case ProjectNode nested -> {
                // Nested projections - generate subquery
                String innerSql = nested.accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS subq";
            }
            case GroupByNode groupBy -> {
                // Project on top of group by
                String innerSql = groupBy.accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS groupby_result";
            }
            case SortNode sort -> {
                String innerSql = sort.accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS sort_result";
            }
            case LimitNode limit -> {
                String innerSql = limit.accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS limit_result";
            }
            case FromNode from -> {
                String innerSql = from.accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS from_result";
            }
            case ExtendNode extend -> {
                String innerSql = extend.accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS extend_result";
            }
            case LateralJoinNode lateral -> {
                String innerSql = lateral.accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS lateral_result";
            }
            default -> {
                // Handle new node types (DistinctNode, RenameNode, ConcatenateNode)
                String innerSql = project.source().accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS src_result";
            }
        };
    }

    @Override
    public String visit(JoinNode join) {
        var sb = new StringBuilder();

        // Check if sources are complex (need subqueries) or simple tables
        String leftFrom = generateJoinSource(join.left(), "left_src");
        String rightFrom = generateJoinSource(join.right(), "right_src");

        sb.append("SELECT * FROM ");
        sb.append(leftFrom);

        sb.append(" ");
        sb.append(join.joinType().toSql());
        sb.append(" ");

        sb.append(rightFrom);

        sb.append(" ON ");
        sb.append(join.condition().accept(this));

        return sb.toString();
    }

    /**
     * Generates a source clause for a JOIN, handling complex sources with
     * subqueries.
     */
    private String generateJoinSource(RelationNode node, String defaultAlias) {
        return switch (node) {
            case TableNode table ->
                dialect.quoteIdentifier(table.table().name()) + " AS " + dialect.quoteIdentifier(table.alias());
            case ProjectNode project -> {
                String innerSql = project.accept(this);
                yield "(" + innerSql + ") AS " + dialect.quoteIdentifier(defaultAlias);
            }
            case GroupByNode groupBy -> {
                String innerSql = groupBy.accept(this);
                yield "(" + innerSql + ") AS " + dialect.quoteIdentifier(defaultAlias);
            }
            case SortNode sort -> {
                String innerSql = sort.accept(this);
                yield "(" + innerSql + ") AS " + dialect.quoteIdentifier(defaultAlias);
            }
            case LimitNode limit -> {
                String innerSql = limit.accept(this);
                yield "(" + innerSql + ") AS " + dialect.quoteIdentifier(defaultAlias);
            }
            case ExtendNode extend -> {
                String innerSql = extend.accept(this);
                yield "(" + innerSql + ") AS " + dialect.quoteIdentifier(defaultAlias);
            }
            case FilterNode filter -> {
                TableNode table = extractTableNode(filter);
                yield dialect.quoteIdentifier(table.table().name()) + " AS " + dialect.quoteIdentifier(table.alias());
            }
            case JoinNode nestedJoin -> {
                String innerSql = nestedJoin.accept(this);
                yield "(" + innerSql + ") AS " + dialect.quoteIdentifier(defaultAlias);
            }
            case LateralJoinNode lateral -> {
                String innerSql = lateral.accept(this);
                yield "(" + innerSql + ") AS " + dialect.quoteIdentifier(defaultAlias);
            }
            case FromNode from -> generateJoinSource(from.source(), defaultAlias);
            default -> {
                // Handle new node types (DistinctNode, RenameNode, ConcatenateNode)
                String innerSql = node.accept(this);
                yield "(" + innerSql + ") AS " + dialect.quoteIdentifier(defaultAlias);
            }
        };
    }

    @Override
    public String visit(GroupByNode groupBy) {
        var sb = new StringBuilder();

        // Generate the source subquery
        String sourceSql = groupBy.source().accept(this);

        // Build SELECT clause with grouping columns and aggregations
        sb.append("SELECT ");

        // Add grouping columns
        String groupCols = groupBy.groupingColumns().stream()
                .map(dialect::quoteIdentifier)
                .collect(Collectors.joining(", "));
        sb.append(groupCols);

        // Add aggregate projections
        for (GroupByNode.AggregateProjection agg : groupBy.aggregations()) {
            sb.append(", ");

            // Handle percentile functions with WITHIN GROUP syntax
            if (agg.isPercentile()) {
                // PERCENTILE_CONT(value) WITHIN GROUP (ORDER BY column)
                sb.append(agg.function().sql());
                sb.append("(");
                sb.append(agg.optionalPercentileValue().orElse(0.5)); // default to median
                sb.append(") WITHIN GROUP (ORDER BY ");
                sb.append(dialect.quoteIdentifier(agg.sourceColumn()));
                sb.append(")");
            } else {
                // Standard aggregate functions
                sb.append(agg.function().sql());
                sb.append("(");
                sb.append(dialect.quoteIdentifier(agg.sourceColumn()));
                // Add second column for bi-variate functions (CORR, COVAR_SAMP, COVAR_POP)
                if (agg.isBivariate()) {
                    sb.append(", ");
                    sb.append(dialect.quoteIdentifier(agg.secondColumn()));
                }
                sb.append(")");
                if (agg.function() == AggregateExpression.AggregateFunction.COUNT_DISTINCT) {
                    sb.append(")"); // close the extra paren for COUNT(DISTINCT ...)
                }
            }
            sb.append(" AS ");
            sb.append(dialect.quoteIdentifier(agg.alias()));
        }

        // FROM subquery
        sb.append(" FROM (");
        sb.append(sourceSql);
        sb.append(") AS groupby_src");

        // GROUP BY clause
        sb.append(" GROUP BY ");
        sb.append(groupCols);

        return sb.toString();
    }

    @Override
    public String visit(AggregateNode aggregate) {
        var sb = new StringBuilder();

        // Generate the source subquery
        String sourceSql = aggregate.source().accept(this);

        // Build SELECT clause with only aggregations (no grouping columns)
        sb.append("SELECT ");

        // Add aggregate projections
        boolean first = true;
        for (GroupByNode.AggregateProjection agg : aggregate.aggregations()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;

            // Handle percentile functions with WITHIN GROUP syntax
            if (agg.isPercentile()) {
                // PERCENTILE_CONT(value) WITHIN GROUP (ORDER BY column)
                sb.append(agg.function().sql());
                sb.append("(");
                sb.append(agg.optionalPercentileValue().orElse(0.5)); // default to median
                sb.append(") WITHIN GROUP (ORDER BY ");
                sb.append(dialect.quoteIdentifier(agg.sourceColumn()));
                sb.append(")");
            } else if (agg.isStringAgg()) {
                // STRING_AGG(column, separator)
                sb.append(agg.function().sql());
                sb.append("(");
                sb.append(dialect.quoteIdentifier(agg.sourceColumn()));
                sb.append(", ");
                sb.append("'");
                sb.append(agg.optionalSeparator().orElse(","));
                sb.append("'");
                sb.append(")");
            } else {
                // Standard aggregate functions
                sb.append(agg.function().sql());
                sb.append("(");
                sb.append(dialect.quoteIdentifier(agg.sourceColumn()));
                // Add second column for bi-variate functions (CORR, COVAR_SAMP, COVAR_POP)
                if (agg.isBivariate()) {
                    sb.append(", ");
                    sb.append(dialect.quoteIdentifier(agg.secondColumn()));
                }
                sb.append(")");
                if (agg.function() == AggregateExpression.AggregateFunction.COUNT_DISTINCT) {
                    sb.append(")"); // close the extra paren for COUNT(DISTINCT ...)
                }
            }
            sb.append(" AS ");
            sb.append(dialect.quoteIdentifier(agg.alias()));
        }

        // FROM subquery (no GROUP BY clause - aggregates entire relation)
        sb.append(" FROM (");
        sb.append(sourceSql);
        sb.append(") AS agg_src");

        return sb.toString();
    }

    @Override
    public String visit(SortNode sort) {
        var sb = new StringBuilder();

        // Generate the source SQL
        String sourceSql = sort.source().accept(this);

        // Wrap source in subquery and add ORDER BY
        sb.append("SELECT * FROM (");
        sb.append(sourceSql);
        sb.append(") AS sort_src ORDER BY ");

        // Add sort columns
        String orderCols = sort.columns().stream()
                .map(col -> dialect.quoteIdentifier(col.column()) + " " + col.direction().name())
                .collect(Collectors.joining(", "));
        sb.append(orderCols);

        return sb.toString();
    }

    @Override
    public String visit(LimitNode limit) {
        var sb = new StringBuilder();

        // Generate the source SQL
        String sourceSql = limit.source().accept(this);

        // Wrap source and add LIMIT/OFFSET
        sb.append("SELECT * FROM (");
        sb.append(sourceSql);
        sb.append(") AS limit_src");

        // Add LIMIT if specified
        if (limit.limit() != null) {
            sb.append(" LIMIT ");
            sb.append(limit.limit());
        }

        // Add OFFSET if specified
        if (limit.offset() > 0) {
            sb.append(" OFFSET ");
            sb.append(limit.offset());
        }

        return sb.toString();
    }

    @Override
    public String visit(FromNode from) {
        // FromNode is a wrapper that carries runtime binding.
        // For SQL generation, we simply generate SQL from the source.
        // The runtime reference is used during execution to get the connection.
        return from.source().accept(this);
    }

    @Override
    public String visit(ExtendNode extend) {
        // Check if all projections are simple (no window functions)
        boolean allSimple = extend.projections().stream()
                .allMatch(p -> p instanceof ExtendNode.SimpleProjection);

        // Force subquery wrapping if source is LateralJoinNode (UNNEST)
        // because we can't reference UNNEST aliases in the same SELECT
        boolean needsSubquery = extend.source() instanceof LateralJoinNode;

        if (allSimple && !needsSubquery) {
            // Simple extends: inline the calculated column without subquery
            String sourceSql = extend.source().accept(this);

            // Build extend column expressions
            var extendCols = new StringBuilder();
            for (ExtendNode.ExtendProjection proj : extend.projections()) {
                extendCols.append(", ");
                ExtendNode.SimpleProjection sp = (ExtendNode.SimpleProjection) proj;
                extendCols.append(sp.expression().accept(this));
                extendCols.append(" AS ");
                extendCols.append(dialect.quoteIdentifier(proj.alias()));
            }

            // For simple extends, inject columns directly
            // sourceSql is like "SELECT ... FROM table", we need to add columns after
            // SELECT
            if (sourceSql.startsWith("SELECT ")) {
                int fromIdx = sourceSql.toUpperCase().indexOf(" FROM ");
                if (fromIdx > 0) {
                    return sourceSql.substring(0, fromIdx) + extendCols + sourceSql.substring(fromIdx);
                }
            }
            // Fallback: wrap in subquery
            return "SELECT *" + extendCols + " FROM (" + sourceSql + ") AS extend_src";
        } else {
            // Window functions need subquery for proper semantics
            String sourceSql = extend.source().accept(this);

            var extendCols = new StringBuilder();
            for (ExtendNode.ExtendProjection proj : extend.projections()) {
                extendCols.append(", ");
                switch (proj) {
                    case ExtendNode.WindowProjection wp -> {
                        extendCols.append(formatWindowExpression(wp.expression()));
                    }
                    case ExtendNode.SimpleProjection sp -> {
                        extendCols.append(sp.expression().accept(this));
                    }
                    default -> throw new IllegalStateException("Unknown projection type: " + proj);
                }
                extendCols.append(" AS ");
                extendCols.append(dialect.quoteIdentifier(proj.alias()));
            }

            return "SELECT *" + extendCols + " FROM (" + sourceSql + ") AS window_src";
        }
    }

    @Override
    public String visit(LateralJoinNode lateralJoin) {
        // Generate SQL to flatten a JSON array column using UNNEST
        // Since get() now returns JSON, we cast it to JSON[] for UNNEST
        String sourceSql = lateralJoin.source().accept(this);
        String arrayColSql = lateralJoin.arrayColumn().accept(this);
        String outputCol = dialect.quoteIdentifier(lateralJoin.outputColumnName());

        // DuckDB: UNNEST expects an array type, so we cast JSON to JSON[]
        // Use SELECT * EXCLUDE to avoid duplicate column name (original array vs
        // unnested)
        return "SELECT * EXCLUDE(" + outputCol + "), UNNEST(CAST(" + arrayColSql + " AS JSON[])) AS " + outputCol +
                " FROM (" + sourceSql + ") AS t";
    }

    @Override
    public String visit(DistinctNode distinct) {
        String sourceSql = distinct.source().accept(this);

        if (distinct.columns().isEmpty()) {
            // distinct() - all columns: SELECT DISTINCT * FROM (source)
            // Replace SELECT * or SELECT cols with SELECT DISTINCT
            if (sourceSql.startsWith("SELECT ")) {
                return "SELECT DISTINCT" + sourceSql.substring(6);
            }
            return "SELECT DISTINCT * FROM (" + sourceSql + ") AS distinct_src";
        } else {
            // distinct(~[col1, col2]) - specific columns only
            String cols = distinct.columns().stream()
                    .map(dialect::quoteIdentifier)
                    .collect(Collectors.joining(", "));
            return "SELECT DISTINCT " + cols + " FROM (" + sourceSql + ") AS distinct_src";
        }
    }

    @Override
    public String visit(RenameNode rename) {
        String sourceSql = rename.source().accept(this);
        String oldCol = dialect.quoteIdentifier(rename.oldColumnName());
        String newCol = dialect.quoteIdentifier(rename.newColumnName());

        // Generate: SELECT * EXCLUDE(oldCol), oldCol AS newCol FROM (source)
        // DuckDB's EXCLUDE allows us to remove the old column and add it with new name
        return "SELECT * EXCLUDE(" + oldCol + "), " + oldCol + " AS " + newCol +
                " FROM (" + sourceSql + ") AS rename_src";
    }

    @Override
    public String visit(ConcatenateNode concatenate) {
        String leftSql = concatenate.left().accept(this);
        String rightSql = concatenate.right().accept(this);

        // UNION ALL combines the two relations
        return "(" + leftSql + ") UNION ALL (" + rightSql + ")";
    }

    @Override
    public String visit(PivotNode pivot) {
        // DuckDB PIVOT syntax:
        // PIVOT (source) ON pivotCol USING agg(valueCol) AS aggName
        var sb = new StringBuilder();

        // Render source as subquery
        String sourceSql = pivot.source().accept(this);
        sb.append("PIVOT (").append(sourceSql).append(")");

        // ON clause - pivot columns
        sb.append(" ON ");
        sb.append(pivot.pivotColumns().stream()
                .map(dialect::quoteIdentifier)
                .reduce((a, b) -> a + ", " + b)
                .orElse(""));

        // IN clause for static pivot
        if (pivot.isStatic() && !pivot.staticValues().isEmpty()) {
            sb.append(" IN (");
            sb.append(pivot.staticValues().stream()
                    .map(v -> v instanceof String ? "'" + v + "'" : String.valueOf(v))
                    .reduce((a, b) -> a + ", " + b)
                    .orElse(""));
            sb.append(")");
        }

        // USING clause - aggregates
        sb.append(" USING ");
        boolean first = true;
        for (var agg : pivot.aggregates()) {
            if (!first)
                sb.append(", ");
            first = false;
            sb.append(agg.aggFunction().toUpperCase());
            sb.append("(");
            if (agg.valueColumn() != null) {
                sb.append(dialect.quoteIdentifier(agg.valueColumn()));
            }
            sb.append(") AS ");
            sb.append(dialect.quoteIdentifier(agg.name()));
        }

        return sb.toString();
    }

    @Override
    public String visit(TdsLiteralNode tdsLiteral) {
        // Generate SQL using DuckDB VALUES syntax:
        // SELECT * FROM (VALUES (v1, v2), (v3, v4)) AS _tds(col1, col2)
        var sb = new StringBuilder();
        sb.append("SELECT * FROM (VALUES ");

        boolean firstRow = true;
        for (var row : tdsLiteral.rows()) {
            if (!firstRow)
                sb.append(", ");
            firstRow = false;

            sb.append("(");
            boolean firstVal = true;
            for (var val : row) {
                if (!firstVal)
                    sb.append(", ");
                firstVal = false;
                sb.append(formatTdsValue(val));
            }
            sb.append(")");
        }

        sb.append(") AS _tds(");
        sb.append(tdsLiteral.columnNames().stream()
                .map(dialect::quoteIdentifier)
                .collect(Collectors.joining(", ")));
        sb.append(")");

        return sb.toString();
    }

    @Override
    public String visit(ConstantNode constant) {
        // Generate SELECT <expression> without a FROM clause
        // This supports Pure expressions like |1+1 which translate to SELECT 1+1
        return "SELECT " + constant.expression().accept(this) + " AS \"value\"";
    }

    /**
     * Formats a TDS cell value for SQL insertion.
     */
    private String formatTdsValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String s) {
            return dialect.quoteStringLiteral(s);
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean b) {
            return dialect.formatBoolean(b);
        }
        return dialect.quoteStringLiteral(value.toString());
    }

    /**
     * Formats a window expression to SQL.
     * Example: ROW_NUMBER() OVER (PARTITION BY "dept" ORDER BY "salary" DESC)
     * Example with frame: SUM("val") OVER (PARTITION BY "dept" ORDER BY "date" ROWS
     * BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
     */
    private String formatWindowExpression(WindowExpression w) {
        var sb = new StringBuilder();

        // Function name
        sb.append(w.function().name());
        sb.append("(");
        if (w.aggregateColumn() != null) {
            // Don't quote '*' for COUNT(*)
            if ("*".equals(w.aggregateColumn())) {
                sb.append("*");
            } else {
                sb.append(dialect.quoteIdentifier(w.aggregateColumn()));
            }
            // Add offset for LAG/LEAD
            if (w.offset() != null && (w.function() == WindowExpression.WindowFunction.LAG
                    || w.function() == WindowExpression.WindowFunction.LEAD)) {
                sb.append(", ");
                sb.append(w.offset());
            }
        } else if (w.function() == WindowExpression.WindowFunction.NTILE && w.offset() != null) {
            // NTILE(bucket_count)
            sb.append(w.offset());
        }
        sb.append(") OVER (");

        boolean hasPartition = !w.partitionBy().isEmpty();
        boolean hasOrder = !w.orderBy().isEmpty();
        boolean hasFrame = w.hasFrame();

        // PARTITION BY clause
        if (hasPartition) {
            sb.append("PARTITION BY ");
            sb.append(w.partitionBy().stream()
                    .map(dialect::quoteIdentifier)
                    .collect(Collectors.joining(", ")));
        }

        // ORDER BY clause
        if (hasOrder) {
            if (hasPartition) {
                sb.append(" ");
            }
            sb.append("ORDER BY ");
            sb.append(w.orderBy().stream()
                    .map(s -> dialect.quoteIdentifier(s.column()) + " " + s.direction().name())
                    .collect(Collectors.joining(", ")));
        }

        // FRAME clause (ROWS/RANGE BETWEEN ... AND ...)
        if (hasFrame) {
            if (hasPartition || hasOrder) {
                sb.append(" ");
            }
            sb.append(formatFrameSpec(w.frame()));
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * Formats a frame specification to SQL.
     * Example: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
     */
    private String formatFrameSpec(WindowExpression.FrameSpec frame) {
        return frame.type().name() + " BETWEEN " +
                formatFrameBound(frame.start(), true) + " AND " +
                formatFrameBound(frame.end(), false);
    }

    /**
     * Formats a frame bound to SQL.
     * 
     * @param isStart true if this is the start bound, false if end bound
     */
    private String formatFrameBound(WindowExpression.FrameBound bound, boolean isStart) {
        return switch (bound.type()) {
            case UNBOUNDED -> "UNBOUNDED " + (isStart ? "PRECEDING" : "FOLLOWING");
            case CURRENT_ROW -> "CURRENT ROW";
            case PRECEDING -> bound.offset() + " PRECEDING";
            case FOLLOWING -> bound.offset() + " FOLLOWING";
        };
    }

    /**
     * Handles the relationship between ProjectNode and FilterNode.
     * 
     * Called from two paths:
     * 1. visit(FilterNode): filter.source() == project → push filter INTO project
     * 2. visit(ProjectNode): project.source() == filter → apply projections OVER
     * filter
     */
    private String visitProjectWithFilter(ProjectNode project, FilterNode filter) {
        // Detect which call path we're on
        boolean filterWrapsProject = filter.source() == project;
        boolean projectWrapsFilter = project.source() == filter;

        if (projectWrapsFilter) {
            // Case 2: Project→Filter structure (normal SQL: SELECT cols FROM table WHERE
            // cond)
            // Check what the filter is on top of
            return switch (filter.source()) {
                case TableNode table -> generateSelectFromTable(project, table, filter.condition());
                case JoinNode join -> visitProjectWithJoin(project, join, filter.condition());
                default -> {
                    // Complex source: wrap in subquery
                    String innerSql = filter.source().accept(this);
                    String projections = formatProjections(project);
                    String whereClause = filter.condition().accept(this);
                    yield "SELECT " + projections + " FROM (" + innerSql + ") AS subq WHERE " + whereClause;
                }
            };
        } else if (filterWrapsProject) {
            // Case 1: Filter→Project structure (service case: Filter(ServiceProjectNode))
            // The project is from service(), we need to wrap it and apply filter
            return switch (project.source()) {
                case TableNode table -> {
                    // Simple case: SELECT + WHERE on table
                    String projections = formatProjections(project);
                    String whereClause = filter.condition().accept(this);
                    yield "SELECT " + projections + " FROM "
                            + dialect.quoteIdentifier(table.table().name()) + " AS "
                            + dialect.quoteIdentifier(table.alias())
                            + " WHERE " + whereClause;
                }
                default -> {
                    // Complex: wrap project in subquery, apply filter
                    String innerSql = project.accept(this);
                    String whereClause = filter.condition().accept(this);
                    yield "SELECT * FROM (" + innerSql + ") AS filter_subq WHERE " + whereClause;
                }
            };
        } else {
            // Fallback: should not happen but be safe
            String innerSql = project.accept(this);
            String whereClause = filter.condition().accept(this);
            return "SELECT * FROM (" + innerSql + ") AS fallback WHERE " + whereClause;
        }
    }

    private String visitProjectWithJoin(ProjectNode project, JoinNode join, Expression whereCondition) {
        var sb = new StringBuilder();

        // Use generateJoinSource to handle both table and TDS sources
        String leftFrom = generateJoinSource(join.left(), "left_src");
        String rightFrom = generateJoinSource(join.right(), "right_src");

        // Check if the left side has a filter condition (e.g., EXISTS from association
        // filter)
        Expression leftFilterCondition = extractFilterCondition(join.left());

        // SELECT clause
        sb.append("SELECT ");
        sb.append(formatProjections(project));

        // FROM clause with JOIN
        sb.append(" FROM ");
        sb.append(leftFrom);

        sb.append(" ");
        sb.append(join.joinType().toSql());
        sb.append(" ");

        sb.append(rightFrom);

        sb.append(" ON ");
        sb.append(join.condition().accept(this));

        // WHERE clause: combine any filter from left side + explicit whereCondition
        Expression combinedWhere = combineConditions(leftFilterCondition, whereCondition);
        if (combinedWhere != null) {
            sb.append(" WHERE ");
            sb.append(combinedWhere.accept(this));
        }

        return sb.toString();
    }

    /**
     * Extracts a filter condition from a relation node, if present.
     * Used to preserve EXISTS conditions from filters when building JOINs.
     */
    private Expression extractFilterCondition(RelationNode node) {
        return switch (node) {
            case FilterNode filter -> filter.condition();
            case ProjectNode project -> extractFilterCondition(project.source());
            case JoinNode join -> extractFilterCondition(join.left());
            case TableNode table -> null;
            case GroupByNode groupBy -> extractFilterCondition(groupBy.source());
            case SortNode sort -> extractFilterCondition(sort.source());
            case LimitNode limit -> extractFilterCondition(limit.source());
            case FromNode from -> extractFilterCondition(from.source());
            case ExtendNode extend -> extractFilterCondition(extend.source());
            case LateralJoinNode lateral -> extractFilterCondition(lateral.source());
            case DistinctNode distinct -> extractFilterCondition(distinct.source());
            case RenameNode rename -> extractFilterCondition(rename.source());
            case ConcatenateNode concat -> null; // No filter to extract from UNION
            case PivotNode pivot -> null; // No filter to extract from PIVOT
            case TdsLiteralNode tds -> null; // No filter in TDS literal
            case ConstantNode constant -> null; // No filter in constant expression
            case AggregateNode agg -> extractFilterCondition(agg.source());
        };
    }

    /**
     * Combines two conditions with AND, handling nulls.
     */
    private Expression combineConditions(Expression cond1, Expression cond2) {
        if (cond1 == null)
            return cond2;
        if (cond2 == null)
            return cond1;
        return LogicalExpression.and(cond1, cond2);
    }

    private TableNode extractTableNode(RelationNode node) {
        return switch (node) {
            case TableNode table -> table;
            case FilterNode filter -> extractTableNode(filter.source());
            case ProjectNode project -> extractTableNode(project.source());
            case JoinNode join -> extractTableNode(join.left());
            case GroupByNode groupBy -> extractTableNode(groupBy.source());
            case SortNode sort -> extractTableNode(sort.source());
            case LimitNode limit -> extractTableNode(limit.source());
            case FromNode from -> extractTableNode(from.source());
            case ExtendNode extend -> extractTableNode(extend.source());
            case LateralJoinNode lateral -> extractTableNode(lateral.source());
            case DistinctNode distinct -> extractTableNode(distinct.source());
            case RenameNode rename -> extractTableNode(rename.source());
            case ConcatenateNode concat -> extractTableNode(concat.left()); // Use left side
            case PivotNode pivot -> extractTableNode(pivot.source());
            case AggregateNode agg -> extractTableNode(agg.source());
            case TdsLiteralNode tds -> throw new IllegalArgumentException("TDS literal has no source table");
            case ConstantNode constant -> throw new IllegalArgumentException("Constant expression has no source table");
        };
    }

    private String generateSelectFromTable(ProjectNode project, TableNode table, Expression whereCondition) {
        var sb = new StringBuilder();

        // SELECT clause
        sb.append("SELECT ");
        sb.append(formatProjections(project));

        // FROM clause
        sb.append(" FROM ");
        sb.append(dialect.quoteIdentifier(table.table().name()));
        sb.append(" AS ");
        sb.append(dialect.quoteIdentifier(table.alias()));

        // WHERE clause (optional)
        if (whereCondition != null) {
            sb.append(" WHERE ");
            sb.append(whereCondition.accept(this));
        }

        return sb.toString();
    }

    private String formatProjections(ProjectNode project) {
        return project.projections().stream()
                .map(this::formatProjection)
                .collect(Collectors.joining(", "));
    }

    private String formatProjection(Projection projection) {
        String expr = projection.expression().accept(this);
        return expr + " AS " + dialect.quoteIdentifier(projection.alias());
    }

    // ==================== Expression Visitors ====================

    @Override
    public String visitColumnReference(ColumnReference columnRef) {
        // Lambda parameters should NOT be quoted in DuckDB
        if (lambdaParams.contains(columnRef.columnName())) {
            return columnRef.columnName();
        }
        if (columnRef.tableAlias().isEmpty()) {
            return dialect.quoteIdentifier(columnRef.columnName());
        }
        return dialect.quoteIdentifier(columnRef.tableAlias())
                + "." + dialect.quoteIdentifier(columnRef.columnName());
    }

    @Override
    public String visitLiteral(Literal literal) {
        return switch (literal.literalType()) {
            case STRING -> dialect.quoteStringLiteral((String) literal.value());
            case INTEGER -> String.valueOf(literal.value());
            case BOOLEAN -> dialect.formatBoolean((Boolean) literal.value());
            case DOUBLE -> String.valueOf(literal.value());
            case NULL -> dialect.formatNull();
            case DATE -> dialect.formatDate((String) literal.value());
            case TIME -> dialect.formatTime((String) literal.value());
        };
    }

    @Override
    public String visitComparison(ComparisonExpression comparison) {
        String left = comparison.left().accept(this);
        String op = comparison.operator().toSql();

        // Handle IS NULL / IS NOT NULL (unary operators)
        if (comparison.operator() == ComparisonExpression.ComparisonOperator.IS_NULL ||
                comparison.operator() == ComparisonExpression.ComparisonOperator.IS_NOT_NULL) {
            return left + " " + op;
        }

        String right = comparison.right().accept(this);
        return left + " " + op + " " + right;
    }

    @Override
    public String visitLogical(LogicalExpression logical) {
        return switch (logical.operator()) {
            case NOT -> "NOT (" + logical.operands().getFirst().accept(this) + ")";
            case AND -> logical.operands().stream()
                    .map(e -> e.accept(this))
                    .collect(Collectors.joining(" AND ", "(", ")"));
            case OR -> logical.operands().stream()
                    .map(e -> e.accept(this))
                    .collect(Collectors.joining(" OR ", "(", ")"));
        };
    }

    @Override
    public String visitExists(ExistsExpression exists) {
        // Generate the correlated subquery - but strip outer SELECT * and make it
        // SELECT 1
        String subquerySql = generateExistsSubquery(exists.subquery());

        if (exists.negated()) {
            return "NOT EXISTS (" + subquerySql + ")";
        }
        return "EXISTS (" + subquerySql + ")";
    }

    @Override
    public String visitConcat(ConcatExpression concat) {
        // Use || for concatenation (works in DuckDB, SQLite, PostgreSQL)
        return concat.parts().stream()
                .map(e -> e.accept(this))
                .collect(Collectors.joining(" || ", "(", ")"));
    }

    @Override
    public String visitFunctionCall(SqlFunctionCall functionCall) {
        String funcName = functionCall.sqlFunctionName();
        String target = functionCall.target().accept(this);

        // Handle variant/JSON functions specially via dialect
        return switch (functionCall.functionName()) {
            case "fromjson" -> dialect.getJsonDialect().variantFromJson(target);
            case "tojson" -> dialect.getJsonDialect().variantToJson(target);
            case "get" -> {
                if (functionCall.arguments().isEmpty()) {
                    throw new IllegalArgumentException("get() requires a key or index argument");
                }
                Expression arg = functionCall.arguments().getFirst();

                // Check if there's a type argument (second argument)
                // get('key') -> returns JSON using ->
                // get('key', @Type) -> returns typed value using ->> + CAST
                boolean hasTypeArg = functionCall.returnType() != SqlType.UNKNOWN;

                if (arg instanceof Literal lit && lit.literalType() == Literal.LiteralType.INTEGER) {
                    // Array index access - always returns JSON
                    yield dialect.getJsonDialect().variantGetIndex(target, ((Number) lit.value()).intValue());
                } else if (arg instanceof Literal lit && lit.literalType() == Literal.LiteralType.STRING) {
                    String key = (String) lit.value();
                    if (hasTypeArg) {
                        // get('key', @Type) - extract as text and cast
                        String textValue = dialect.getJsonDialect().variantGet(target, key);
                        SqlType type = functionCall.returnType();
                        String sqlType = switch (type) {
                            case INTEGER -> "INTEGER";
                            case DOUBLE -> "DOUBLE";
                            case VARCHAR -> "VARCHAR";
                            case BOOLEAN -> "BOOLEAN";
                            default -> "VARCHAR";
                        };
                        yield "CAST(" + textValue + " AS " + sqlType + ")";
                    } else {
                        // get('key') - return JSON structure
                        yield dialect.getJsonDialect().variantGetJson(target, key);
                    }
                } else {
                    // Dynamic key - fallback to JSON extraction
                    String keyExpr = arg.accept(this);
                    yield dialect.getJsonDialect().variantGetJson(target, keyExpr);
                }
            }
            case "cast" -> {
                // Generate SQL CAST based on return type
                SqlType type = functionCall.returnType();
                String sqlType = switch (type) {
                    case INTEGER -> "INTEGER";
                    case DOUBLE -> "DOUBLE";
                    case VARCHAR -> "VARCHAR";
                    case BOOLEAN -> "BOOLEAN";
                    default -> "VARCHAR"; // fallback
                };
                yield "CAST(" + target + " AS " + sqlType + ")";
            }
            default -> {
                // Standard function call
                if (functionCall.arguments().isEmpty()) {
                    yield funcName + "(" + target + ")";
                } else {
                    String args = functionCall.arguments().stream()
                            .map(e -> e.accept(this))
                            .collect(Collectors.joining(", "));
                    yield funcName + "(" + target + ", " + args + ")";
                }
            }
        };
    }

    @Override
    public String visitCase(CaseExpression caseExpr) {
        var sb = new StringBuilder();
        sb.append("CASE");

        // Flatten nested CASE expressions for cleaner SQL
        appendCaseWhen(sb, caseExpr);

        sb.append(" END");
        return sb.toString();
    }

    /**
     * Helper to append CASE WHEN clauses, flattening nested CaseExpressions.
     */
    private void appendCaseWhen(StringBuilder sb, CaseExpression caseExpr) {
        sb.append(" WHEN ");
        sb.append(caseExpr.condition().accept(this));
        sb.append(" THEN ");
        sb.append(caseExpr.thenValue().accept(this));

        if (caseExpr.elseValue() instanceof CaseExpression nested) {
            // Flatten nested if into multiple WHEN clauses
            appendCaseWhen(sb, nested);
        } else {
            sb.append(" ELSE ");
            sb.append(caseExpr.elseValue().accept(this));
        }
    }

    @Override
    public String visitArithmetic(ArithmeticExpression arithmetic) {
        String left = wrapNumericCast(arithmetic.left());
        String right = wrapNumericCast(arithmetic.right());
        return "(" + left + " " + arithmetic.sqlOperator() + " " + right + ")";
    }

    /**
     * Wraps an expression with CAST(... AS DOUBLE) if it's not already numeric.
     * This is needed for JSON-extracted values which come as VARCHAR or JSON.
     */
    private String wrapNumericCast(Expression expr) {
        String sql = expr.accept(this);
        if (expr.type().needsNumericCast()) {
            return "CAST(" + sql + " AS DOUBLE)";
        }
        return sql;
    }

    @Override
    public String visitAggregate(AggregateExpression aggregate) {
        String arg = aggregate.argument().accept(this);
        String funcName = aggregate.function().sql();

        if (aggregate.function() == AggregateExpression.AggregateFunction.COUNT_DISTINCT) {
            return funcName + " " + arg + ")"; // COUNT(DISTINCT col)
        }

        // Handle ordered-set aggregate functions (PERCENTILE_CONT, PERCENTILE_DISC)
        if (aggregate.function() == AggregateExpression.AggregateFunction.PERCENTILE_CONT
                || aggregate.function() == AggregateExpression.AggregateFunction.PERCENTILE_DISC) {
            // Syntax: PERCENTILE_CONT(percentile_value) WITHIN GROUP (ORDER BY column)
            // argument = the column to order by
            // secondArgument = the percentile value (0.0 - 1.0)
            String percentileValue = aggregate.optionalSecondArgument()
                    .map(e -> e.accept(this))
                    .orElse("0.5"); // default to median
            return funcName + "(" + percentileValue + ") WITHIN GROUP (ORDER BY " + arg + ")";
        }

        return funcName + "(" + arg + ")";
    }

    @Override
    public String visit(DateFunctionExpression dateFunction) {
        // EXTRACT(YEAR FROM column)
        String arg = dateFunction.argument().accept(this);

        // Special handling for DAY_OF_WEEK: DuckDB DOW returns 0-6 (Sunday=0)
        // Pure expects 1-7 where Monday=1, Sunday=7
        if (dateFunction.function() == DateFunctionExpression.DateFunction.DAY_OF_WEEK) {
            // DuckDB ISODOW returns 1-7 with Monday=1 (ISO standard)
            return "EXTRACT(ISODOW FROM " + arg + ")";
        }

        return "EXTRACT(" + dateFunction.function().sql() + " FROM " + arg + ")";
    }

    @Override
    public String visit(CurrentDateExpression currentDate) {
        // now() -> CURRENT_TIMESTAMP
        // today() -> CURRENT_DATE
        return currentDate.function().sql();
    }

    @Override
    public String visit(DateDiffExpression dateDiff) {
        // dateDiff(d1, d2, DAYS) -> DATE_DIFF('day', d1, d2)
        String d1 = dateDiff.date1().accept(this);
        String d2 = dateDiff.date2().accept(this);
        return "DATE_DIFF('" + dateDiff.unit().sql() + "', " + d1 + ", " + d2 + ")";
    }

    @Override
    public String visit(DateAdjustExpression dateAdjust) {
        // adjust(date, 5, DAYS) -> date + INTERVAL '5' DAY
        String date = dateAdjust.date().accept(this);
        String amount = dateAdjust.amount().accept(this);
        String unitSql = dateAdjust.unit().sql().toUpperCase();
        return "(" + date + " + INTERVAL '" + amount + "' " + unitSql + ")";
    }

    @Override
    public String visit(DateTruncExpression dateTrunc) {
        // firstDayOfMonth(date) -> DATE_TRUNC('month', date)
        String arg = dateTrunc.argument().accept(this);
        return "DATE_TRUNC('" + dateTrunc.part().sql() + "', " + arg + ")";
    }

    @Override
    public String visitEpochExpression(EpochExpression epoch) {
        String value = epoch.value().accept(this);
        if (epoch.direction() == EpochExpression.Direction.FROM_EPOCH) {
            // fromEpochValue(seconds) -> EPOCH_MS(seconds * 1000) for DuckDB
            // or: TO_TIMESTAMP(seconds) for seconds
            return switch (epoch.unit()) {
                case SECONDS -> "TO_TIMESTAMP(" + value + ")";
                case MILLISECONDS -> "EPOCH_MS(" + value + ")";
                default -> "TO_TIMESTAMP(" + value + ")"; // Default to seconds
            };
        } else {
            // toEpochValue(date) -> EPOCH(date) for DuckDB
            // Returns seconds since 1970-01-01
            return switch (epoch.unit()) {
                case SECONDS -> "EPOCH(" + value + ")";
                case MILLISECONDS -> "(EPOCH(" + value + ") * 1000)";
                default -> "EPOCH(" + value + ")";
            };
        }
    }

    @Override
    public String visitDateComparisonExpression(DateComparisonExpression dateComp) {
        String left = dateComp.left().accept(this);
        String right = dateComp.right().accept(this);
        // Use DATE_TRUNC('day', ...) to compare at day level
        String leftDay = "DATE_TRUNC('day', " + left + ")";
        String rightDay = "DATE_TRUNC('day', " + right + ")";

        return switch (dateComp.operation()) {
            case IS_ON_DAY -> "(" + leftDay + " = " + rightDay + ")";
            case IS_AFTER_DAY -> "(" + leftDay + " > " + rightDay + ")";
            case IS_BEFORE_DAY -> "(" + leftDay + " < " + rightDay + ")";
            case IS_ON_OR_AFTER_DAY -> "(" + leftDay + " >= " + rightDay + ")";
            case IS_ON_OR_BEFORE_DAY -> "(" + leftDay + " <= " + rightDay + ")";
        };
    }

    @Override
    public String visit(TimeBucketExpression timeBucket) {
        String dateExpr = timeBucket.dateExpression().accept(this);
        String quantity = timeBucket.quantity().accept(this);
        String unitSql = timeBucket.unit().toSQL();

        // DuckDB: TIME_BUCKET(INTERVAL 'N unit', date_expression)
        return String.format("TIME_BUCKET(INTERVAL '%s %s', %s)", quantity, unitSql, dateExpr);
    }

    @Override
    public String visit(MinMaxExpression minMax) {
        // GREATEST(a, b, ...) or LEAST(a, b, ...)
        String functionName = minMax.function().sql();
        var args = minMax.arguments().stream()
                .map(arg -> arg.accept(this))
                .toList();
        return functionName + "(" + String.join(", ", args) + ")";
    }

    @Override
    public String visit(InExpression inExpr) {
        // expr IN (val1, val2, ...) or expr NOT IN (val1, val2, ...)
        String operand = inExpr.operand().accept(this);
        String values = inExpr.values().stream()
                .map(v -> v.accept(this))
                .collect(Collectors.joining(", "));
        String notPart = inExpr.negated() ? " NOT" : "";
        return operand + notPart + " IN (" + values + ")";
    }

    @Override
    public String visit(ListLiteral listLiteral) {
        // DuckDB list literal: [elem1, elem2, ...]
        String elements = listLiteral.elements().stream()
                .map(e -> e.accept(this))
                .collect(Collectors.joining(", "));
        return "[" + elements + "]";
    }

    @Override
    public String visit(JsonObjectExpression jsonObject) {
        // Render nested json_object() for deep fetch
        // Example: json_object('city', t1.CITY, 'street', t1.STREET)
        var sb = new StringBuilder();
        sb.append("json_object(");

        boolean first = true;
        for (Projection p : jsonObject.projections()) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append("'").append(p.alias()).append("', ");
            sb.append(p.expression().accept(this));
        }

        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visit(SubqueryExpression subquery) {
        // Render as a correlated subquery: (SELECT expr FROM table WHERE condition)
        String selectExpr = subquery.selectExpression().accept(this);
        String fromClause = generateSubqueryFromClause(subquery.source());

        return "(" + "SELECT " + selectExpr + " " + fromClause + ")";
    }

    /**
     * Generates FROM clause for a subquery, handling filters.
     */
    private String generateSubqueryFromClause(RelationNode node) {
        return switch (node) {
            case TableNode table ->
                "FROM " + dialect.quoteIdentifier(table.table().name())
                        + " AS " + dialect.quoteIdentifier(table.alias());
            case FilterNode filter -> {
                String whereClause = filter.condition().accept(this);
                String innerFrom = generateSubqueryFromClause(filter.source());
                yield innerFrom + " WHERE " + whereClause;
            }
            default ->
                "FROM (" + node.accept(this) + ") AS subq";
        };
    }

    @Override
    public String visit(JsonArrayExpression jsonArray) {
        // Render as json_group_array(element)
        String element = jsonArray.elementExpression().accept(this);
        return "json_group_array(" + element + ")";
    }

    /**
     * Generates a subquery suitable for EXISTS.
     * The subquery uses SELECT 1 instead of SELECT * for efficiency.
     */
    private String generateExistsSubquery(RelationNode node) {
        return switch (node) {
            case TableNode table -> {
                yield "SELECT 1 FROM " + dialect.quoteIdentifier(table.table().name())
                        + " AS " + dialect.quoteIdentifier(table.alias());
            }
            case FilterNode filter -> {
                String whereClause = filter.condition().accept(this);
                yield switch (filter.source()) {
                    case TableNode table -> {
                        yield "SELECT 1 FROM " + dialect.quoteIdentifier(table.table().name())
                                + " AS " + dialect.quoteIdentifier(table.alias())
                                + " WHERE " + whereClause;
                    }
                    case JoinNode join -> {
                        TableNode leftTable = extractTableNode(join.left());
                        TableNode rightTable = extractTableNode(join.right());
                        yield "SELECT 1 FROM " + dialect.quoteIdentifier(leftTable.table().name())
                                + " AS " + dialect.quoteIdentifier(leftTable.alias())
                                + " " + join.joinType().toSql() + " "
                                + dialect.quoteIdentifier(rightTable.table().name())
                                + " AS " + dialect.quoteIdentifier(rightTable.alias())
                                + " ON " + join.condition().accept(this)
                                + " WHERE " + whereClause;
                    }
                    default -> "SELECT 1 FROM (" + filter.source().accept(this) + ") WHERE " + whereClause;
                };
            }
            case JoinNode join -> {
                TableNode leftTable = extractTableNode(join.left());
                TableNode rightTable = extractTableNode(join.right());
                yield "SELECT 1 FROM " + dialect.quoteIdentifier(leftTable.table().name())
                        + " AS " + dialect.quoteIdentifier(leftTable.alias())
                        + " " + join.joinType().toSql() + " "
                        + dialect.quoteIdentifier(rightTable.table().name())
                        + " AS " + dialect.quoteIdentifier(rightTable.alias())
                        + " ON " + join.condition().accept(this);
            }
            case ProjectNode project -> {
                // For EXISTS, we don't need the projections, just the source with filter
                yield generateExistsSubquery(project.source());
            }
            case

                    GroupByNode groupBy -> {
                // For EXISTS, we don't need aggregations, just the source
                yield generateExistsSubquery(groupBy.source());
            }
            case

                    SortNode sort -> {
                // For EXISTS, sorting doesn't matter, just use the source
                yield generateExistsSubquery(sort.source());
            }
            case

                    LimitNode limit -> {
                // For EXISTS with limit, we need to preserve the limit
                yield "SELECT 1 FROM (" + limit.accept(this) + ") AS exists_src";
            }
            case
                    FromNode from -> {
                // For EXISTS, unwrap the from and process the source
                yield generateExistsSubquery(from.source());
            }
            case

                    ExtendNode extend -> {
                // For EXISTS with window functions, just use the source
                yield generateExistsSubquery(extend.source());
            }
            case

                    LateralJoinNode lateral -> {
                // For EXISTS with lateral join, just use the source
                yield generateExistsSubquery(lateral.source());
            }
            case DistinctNode distinct -> {
                // For EXISTS, distinct doesn't matter, just use the source
                yield generateExistsSubquery(distinct.source());
            }
            case RenameNode rename -> {
                // For EXISTS, rename doesn't matter, just use the source
                yield generateExistsSubquery(rename.source());
            }
            case ConcatenateNode concat -> {
                // For EXISTS with concatenate, wrap the whole thing
                yield "SELECT 1 FROM (" + concat.accept(this) + ") AS exists_src";
            }
            case PivotNode pivot -> {
                // For EXISTS with pivot, wrap the whole thing
                yield "SELECT 1 FROM (" + pivot.accept(this) + ") AS exists_src";
            }
            case TdsLiteralNode tds -> {
                // For EXISTS with TDS literal, wrap the whole thing
                yield "SELECT 1 FROM (" + tds.accept(this) + ") AS exists_src";
            }
            case ConstantNode constant -> {
                // For EXISTS with constant, wrap the expression
                yield "SELECT 1 FROM (" + constant.accept(this) + ") AS exists_src";
            }
            default -> throw new IllegalStateException(
                    "Unknown node type in generateExistsSubquery: " + node.getClass().getSimpleName());
        };
    }

    // ==================== Collection Function Support ====================

    @Override
    public String visitCollectionCall(SqlCollectionCall call) {
        // get() now returns JSON by default, so arrays work correctly
        String source = call.source().accept(this);

        // Wrap JSON array source with CAST to JSON[] for DuckDB list functions
        String listSource = "CAST(" + source + " AS JSON[])";

        return switch (call.function()) {
            case MAP -> {
                String param = call.lambdaParam();
                lambdaParams.add(param); // Register lambda param
                String lambdaBody = call.lambdaBody().accept(this);
                lambdaParams.remove(param); // Unregister after use
                yield "list_transform(" + listSource + ", " + param + " -> " + lambdaBody + ")";
            }
            case FILTER -> {
                String param = call.lambdaParam();
                lambdaParams.add(param);
                String lambdaBody = call.lambdaBody().accept(this);
                lambdaParams.remove(param);
                yield "list_filter(" + listSource + ", " + param + " -> " + lambdaBody + ")";
            }
            case FOLD -> {
                String accParam = call.lambdaParam2(); // accumulator
                String elemParam = call.lambdaParam(); // element
                lambdaParams.add(accParam);
                lambdaParams.add(elemParam);
                String lambdaBody = call.lambdaBody().accept(this);
                lambdaParams.remove(accParam);
                lambdaParams.remove(elemParam);
                String initVal = call.initialValue().accept(this);
                // DuckDB list_reduce only takes 2 args: list_reduce(list, lambda)
                // Use COALESCE to handle empty list with initial value fallback
                yield "COALESCE(list_reduce(" + listSource + ", (" + accParam + ", " + elemParam + ") -> "
                        + lambdaBody + "), " + initVal + ")";
            }
            case FLATTEN -> "flatten(" + listSource + ")";
        };
    }
}
