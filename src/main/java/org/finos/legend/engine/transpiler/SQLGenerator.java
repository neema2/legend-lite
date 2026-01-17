package org.finos.legend.engine.transpiler;

import org.finos.legend.engine.plan.*;

import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transpiles a RelationNode tree into a SQL string.
 * This is the core transpiler that implements the "Database-as-Runtime" philosophy.
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
        // We need to handle the case where filter is on top of table or on top of project
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
            case FilterNode nestedFilter -> {
                // Combine filters with AND
                String innerSql = nestedFilter.accept(this);
                yield innerSql + " AND " + whereClause;
            }
            case JoinNode join -> {
                // Filter on top of join
                String innerSql = join.accept(this);
                yield innerSql + " WHERE " + whereClause;
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
        
        // Check if the left side has a filter condition (e.g., EXISTS from association filter)
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
            case JoinNode join -> extractFilterCondition(join.left()); // Recurse into left side
            case TableNode table -> null; // No filter
        };
    }
    
    /**
     * Combines two conditions with AND, handling nulls.
     */
    private Expression combineConditions(Expression cond1, Expression cond2) {
        if (cond1 == null) return cond2;
        if (cond2 == null) return cond1;
        return LogicalExpression.and(cond1, cond2);
    }
    
    private TableNode extractTableNode(RelationNode node) {
        return switch (node) {
            case TableNode table -> table;
            case FilterNode filter -> extractTableNode(filter.source());
            case ProjectNode project -> extractTableNode(project.source());
            case JoinNode join -> extractTableNode(join.left()); // For nested joins, take left
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
        // Generate the correlated subquery - but strip outer SELECT * and make it SELECT 1
        String subquerySql = generateExistsSubquery(exists.subquery());
        
        if (exists.negated()) {
            return "NOT EXISTS (" + subquerySql + ")";
        }
        return "EXISTS (" + subquerySql + ")";
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
        };
    }
}
