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
            case GroupByNode groupBy -> {
                // Filter on top of group by
                String innerSql = groupBy.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS grp WHERE " + whereClause;
            }
            case SortNode sort -> {
                // Filter on top of sort
                String innerSql = sort.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS srt WHERE " + whereClause;
            }
            case LimitNode limit -> {
                // Filter on top of limit
                String innerSql = limit.accept(this);
                yield "SELECT * FROM (" + innerSql + ") AS lim WHERE " + whereClause;
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
        };
    }

    @Override
    public String visit(JoinNode join) {
        var sb = new StringBuilder();

        // Get left table info
        TableNode leftTable = extractTableNode(join.left());
        TableNode rightTable = extractTableNode(join.right());

        sb.append("SELECT * FROM ");
        sb.append(dialect.quoteIdentifier(leftTable.table().name()));
        sb.append(" AS ");
        sb.append(dialect.quoteIdentifier(leftTable.alias()));

        sb.append(" ");
        sb.append(join.joinType().toSql());
        sb.append(" ");

        sb.append(dialect.quoteIdentifier(rightTable.table().name()));
        sb.append(" AS ");
        sb.append(dialect.quoteIdentifier(rightTable.alias()));

        sb.append(" ON ");
        sb.append(join.condition().accept(this));

        return sb.toString();
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
            sb.append(agg.function().sql());
            sb.append("(");
            sb.append(dialect.quoteIdentifier(agg.sourceColumn()));
            sb.append(")");
            if (agg.function() == AggregateExpression.AggregateFunction.COUNT_DISTINCT) {
                sb.append(")"); // close the extra paren for COUNT(DISTINCT ...)
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

    private String visitProjectWithFilter(ProjectNode project, FilterNode filter) {
        // Common case: Project on top of Filter on top of Table
        return switch (filter.source()) {
            case TableNode table -> generateSelectFromTable(project, table, filter.condition());
            case JoinNode join -> visitProjectWithJoin(project, join, filter.condition());
            default -> {
                // Complex case: generate subquery
                String innerSql = filter.accept(this);
                String projections = formatProjections(project);
                yield "SELECT " + projections + " FROM (" + innerSql + ") AS subq";
            }
        };
    }

    private String visitProjectWithJoin(ProjectNode project, JoinNode join, Expression whereCondition) {
        var sb = new StringBuilder();

        // Extract table info and any filter conditions from the left side
        TableNode leftTable = extractTableNode(join.left());
        TableNode rightTable = extractTableNode(join.right());

        // Check if the left side has a filter condition (e.g., EXISTS from association
        // filter)
        Expression leftFilterCondition = extractFilterCondition(join.left());

        // SELECT clause
        sb.append("SELECT ");
        sb.append(formatProjections(project));

        // FROM clause with JOIN
        sb.append(" FROM ");
        sb.append(dialect.quoteIdentifier(leftTable.table().name()));
        sb.append(" AS ");
        sb.append(dialect.quoteIdentifier(leftTable.alias()));

        sb.append(" ");
        sb.append(join.joinType().toSql());
        sb.append(" ");

        sb.append(dialect.quoteIdentifier(rightTable.table().name()));
        sb.append(" AS ");
        sb.append(dialect.quoteIdentifier(rightTable.alias()));

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
        if (columnRef.tableAlias().isEmpty()) {
            return dialect.quoteIdentifier(columnRef.columnName());
        }
        return dialect.quoteIdentifier(columnRef.tableAlias())
                + "." + dialect.quoteIdentifier(columnRef.columnName());
    }

    @Override
    public String visitLiteral(Literal literal) {
        return switch (literal.type()) {
            case STRING -> dialect.quoteStringLiteral((String) literal.value());
            case INTEGER -> String.valueOf(literal.value());
            case BOOLEAN -> dialect.formatBoolean((Boolean) literal.value());
            case DOUBLE -> String.valueOf(literal.value());
            case NULL -> dialect.formatNull();
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

        if (functionCall.arguments().isEmpty()) {
            return funcName + "(" + target + ")";
        } else {
            String args = functionCall.arguments().stream()
                    .map(e -> e.accept(this))
                    .collect(Collectors.joining(", "));
            return funcName + "(" + target + ", " + args + ")";
        }
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
        String left = arithmetic.left().accept(this);
        String right = arithmetic.right().accept(this);
        return "(" + left + " " + arithmetic.sqlOperator() + " " + right + ")";
    }

    @Override
    public String visitAggregate(AggregateExpression aggregate) {
        String arg = aggregate.argument().accept(this);
        String funcName = aggregate.function().sql();
        if (aggregate.function() == AggregateExpression.AggregateFunction.COUNT_DISTINCT) {
            return funcName + " " + arg + ")"; // COUNT(DISTINCT col)
        }
        return funcName + "(" + arg + ")";
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
            case GroupByNode groupBy -> {
                // For EXISTS, we don't need aggregations, just the source
                yield generateExistsSubquery(groupBy.source());
            }
            case SortNode sort -> {
                // For EXISTS, sorting doesn't matter, just use the source
                yield generateExistsSubquery(sort.source());
            }
            case LimitNode limit -> {
                // For EXISTS with limit, we need to preserve the limit
                yield "SELECT 1 FROM (" + limit.accept(this) + ") AS exists_src";
            }
        };
    }
}
