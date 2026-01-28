package org.finos.legend.engine.sql;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.sql.ast.SelectStatement;
import org.finos.legend.engine.sql.ast.OrderSpec;
import org.finos.legend.engine.sql.ast.SelectItem;
import org.finos.legend.engine.sql.ast.FromItem;
import org.finos.legend.engine.store.Table;
import org.finos.legend.pure.dsl.m2m.BinaryArithmeticExpr;

import java.util.*;

/**
 * Compiles SQL AST into RelationNode IR.
 * 
 * Architecture:
 * - SQL AST (from SelectParser) → Compiler → RelationNode IR → SQLGenerator → SQL String
 * 
 * The compiler builds the IR bottom-up:
 * 1. FROM clause → TableNode / JoinNode
 * 2. WHERE clause → FilterNode
 * 3. GROUP BY → GroupByNode
 * 4. SELECT clause → ProjectNode
 * 5. ORDER BY → SortNode
 * 6. LIMIT/OFFSET → LimitNode
 */
public final class SQLCompiler {

    private final TableResolver tableResolver;
    private final ClassResolver classResolver;
    private final ServiceResolver serviceResolver;
    private int aliasCounter = 0;

    /**
     * Interface for resolving table names to Table metadata.
     */
    @FunctionalInterface
    public interface TableResolver {
        Table resolve(String schema, String tableName);
    }

    /**
     * Interface for resolving class names to Table via mappings.
     * Returns the Table that the class is mapped to.
     */
    @FunctionalInterface
    public interface ClassResolver {
        Table resolve(String className);
    }

    /**
     * Interface for resolving service paths to compiled IR.
     * Returns the RelationNode representing the service execution.
     */
    @FunctionalInterface
    public interface ServiceResolver {
        RelationNode resolve(String servicePath);
    }

    /**
     * Creates a SQLCompiler with only table resolution.
     */
    public SQLCompiler(TableResolver tableResolver) {
        this(tableResolver, null, null);
    }

    /**
     * Creates a SQLCompiler with full resolution capabilities.
     */
    public SQLCompiler(TableResolver tableResolver, ClassResolver classResolver, ServiceResolver serviceResolver) {
        this.tableResolver = Objects.requireNonNull(tableResolver);
        this.classResolver = classResolver;
        this.serviceResolver = serviceResolver;
    }

    /**
     * Compiles a SELECT statement to RelationNode IR.
     */
    public RelationNode compile(SelectStatement stmt) {
        // Build from bottom up
        RelationNode node = compileFrom(stmt.from());
        
        if (stmt.hasWhere()) {
            node = new FilterNode(node, compileExpression(stmt.where()));
        }
        
        // Handle GROUP BY with aggregates
        if (stmt.hasGroupBy()) {
            node = compileGroupBy(node, stmt);
        } else {
            // Regular projection
            node = compileProjection(node, stmt.selectItems());
        }
        
        if (stmt.hasOrderBy()) {
            node = compileSortNode(node, stmt.orderBy());
        }
        
        if (stmt.hasLimit() || stmt.offset() != null) {
            node = new LimitNode(node, stmt.limit(), stmt.offset() != null ? stmt.offset() : 0);
        }
        
        return node;
    }

    // ==================== FROM Clause ====================

    private RelationNode compileFrom(List<FromItem> fromItems) {
        if (fromItems.isEmpty()) {
            throw new SQLCompileException("FROM clause is required");
        }

        RelationNode result = compileFromItem(fromItems.get(0));
        
        // Handle implicit cross joins (FROM t1, t2)
        for (int i = 1; i < fromItems.size(); i++) {
            RelationNode right = compileFromItem(fromItems.get(i));
            // Implicit cross join
            result = new JoinNode(result, right, Literal.bool(true), JoinNode.JoinType.INNER);
        }
        
        return result;
    }

    private RelationNode compileFromItem(FromItem item) {
        return switch (item) {
            case FromItem.TableRef ref -> compileTableRef(ref);
            case FromItem.JoinedTable join -> compileJoin(join);
            case FromItem.SubQuery subQuery -> compileSubQuery(subQuery);
            case FromItem.TableFunction func -> compileTableFunction(func);
        };
    }

    private RelationNode compileTableRef(FromItem.TableRef ref) {
        Table table = tableResolver.resolve(ref.schema(), ref.table());
        if (table == null) {
            throw new SQLCompileException("Table not found: " + 
                (ref.schema() != null ? ref.schema() + "." : "") + ref.table());
        }
        String alias = ref.alias() != null ? ref.alias() : generateAlias();
        return new TableNode(table, alias);
    }

    private RelationNode compileJoin(FromItem.JoinedTable join) {
        RelationNode left = compileFromItem(join.left());
        RelationNode right = compileFromItem(join.right());
        Expression condition = join.condition() != null 
            ? compileExpression(join.condition()) 
            : Literal.bool(true);
        
        JoinNode.JoinType joinType = switch (join.joinType()) {
            case INNER -> JoinNode.JoinType.INNER;
            case LEFT_OUTER -> JoinNode.JoinType.LEFT_OUTER;
            case RIGHT_OUTER -> JoinNode.JoinType.RIGHT_OUTER;
            case FULL_OUTER -> JoinNode.JoinType.FULL_OUTER;
            case CROSS -> JoinNode.JoinType.INNER;
        };
        
        return new JoinNode(left, right, condition, joinType);
    }

    private RelationNode compileSubQuery(FromItem.SubQuery subQuery) {
        return compile(subQuery.query());
    }

    private RelationNode compileTableFunction(FromItem.TableFunction func) {
        String funcName = func.functionName().toLowerCase();
        
        return switch (funcName) {
            case "table" -> compileTableFunctionCall(func);
            case "class" -> compileClassFunctionCall(func);
            case "service" -> compileServiceFunctionCall(func);
            default -> throw new SQLCompileException("Unknown table function: " + funcName);
        };
    }

    /**
     * Compiles table('store::DB.T_TABLE') function.
     * Parses the qualified name and resolves via TableResolver.
     */
    private RelationNode compileTableFunctionCall(FromItem.TableFunction func) {
        if (func.arguments().isEmpty()) {
            throw new SQLCompileException("table() requires 1 argument: table('store::DB.T_TABLE')");
        }
        
        // Get the qualified table path, e.g. 'store::PersonDatabase.T_PERSON'
        var arg = func.arguments().get(0);
        if (!(arg instanceof org.finos.legend.engine.sql.ast.Expression.Literal lit)) {
            throw new SQLCompileException("table() argument must be a string literal");
        }
        
        String qualifiedPath = lit.value().toString();
        
        // Parse the path: 'store::DB.T_TABLE' -> schema='store::DB', table='T_TABLE'
        int dotIndex = qualifiedPath.lastIndexOf('.');
        if (dotIndex == -1) {
            throw new SQLCompileException("table() path must be 'store::DB.TABLE_NAME', got: " + qualifiedPath);
        }
        
        String schema = qualifiedPath.substring(0, dotIndex); // 'store::PersonDatabase'
        String tableName = qualifiedPath.substring(dotIndex + 1); // 'T_PERSON'
        
        Table table = tableResolver.resolve(schema, tableName);
        if (table == null) {
            throw new SQLCompileException("Table not found: " + qualifiedPath);
        }
        
        String alias = func.alias() != null ? func.alias() : generateAlias();
        return new TableNode(table, alias);
    }

    /**
     * Compiles class('package::ClassName') function.
     * Resolves the class via Pure mappings to get the underlying table.
     */
    private RelationNode compileClassFunctionCall(FromItem.TableFunction func) {
        if (func.arguments().isEmpty()) {
            throw new SQLCompileException("class() requires 1 argument: class('package::ClassName')");
        }
        
        var arg = func.arguments().get(0);
        if (!(arg instanceof org.finos.legend.engine.sql.ast.Expression.Literal lit)) {
            throw new SQLCompileException("class() argument must be a string literal");
        }
        
        String className = lit.value().toString();
        
        if (classResolver == null) {
            throw new SQLCompileException("class() function requires ClassResolver to be configured");
        }
        
        Table table = classResolver.resolve(className);
        if (table == null) {
            throw new SQLCompileException("No mapping found for class: " + className);
        }
        
        String alias = func.alias() != null ? func.alias() : generateAlias();
        return new TableNode(table, alias);
    }

    /**
     * Compiles service('/servicePath') function.
     * Resolves to a Legend service and returns its compiled query.
     */
    private RelationNode compileServiceFunctionCall(FromItem.TableFunction func) {
        if (func.arguments().isEmpty()) {
            throw new SQLCompileException("service() requires 1 argument: service('/path')");
        }
        
        var arg = func.arguments().get(0);
        if (!(arg instanceof org.finos.legend.engine.sql.ast.Expression.Literal lit)) {
            throw new SQLCompileException("service() argument must be a string literal");
        }
        
        String servicePath = lit.value().toString();
        
        if (serviceResolver == null) {
            throw new SQLCompileException("service() function requires ServiceResolver to be configured");
        }
        
        RelationNode serviceResult = serviceResolver.resolve(servicePath);
        if (serviceResult == null) {
            throw new SQLCompileException("Service not found: " + servicePath);
        }
        
        return serviceResult;
    }

    // ==================== Projections ====================

    private RelationNode compileProjection(RelationNode source, List<SelectItem> items) {
        List<Projection> projections = new ArrayList<>();
        
        for (SelectItem item : items) {
            switch (item) {
                case SelectItem.AllColumns all -> {
                    projections.add(new Projection(new ColumnReference("", "*"), "*"));
                }
                case SelectItem.ExpressionItem expr -> {
                    Expression compiled = compileExpression(expr.expression());
                    String alias = expr.alias() != null ? expr.alias() : inferAlias(expr.expression());
                    projections.add(new Projection(compiled, alias));
                }
            }
        }
        
        return new ProjectNode(source, projections);
    }

    private String inferAlias(org.finos.legend.engine.sql.ast.Expression expr) {
        return switch (expr) {
            case org.finos.legend.engine.sql.ast.Expression.ColumnRef col -> col.columnName();
            case org.finos.legend.engine.sql.ast.Expression.FunctionCall func -> func.functionName().toLowerCase();
            default -> "expr" + (aliasCounter++);
        };
    }

    // ==================== GROUP BY ====================

    private RelationNode compileGroupBy(RelationNode source, SelectStatement stmt) {
        List<String> groupingColumns = new ArrayList<>();
        
        for (org.finos.legend.engine.sql.ast.Expression expr : stmt.groupBy()) {
            if (expr instanceof org.finos.legend.engine.sql.ast.Expression.ColumnRef col) {
                groupingColumns.add(col.columnName());
            } else {
                throw new SQLCompileException("GROUP BY only supports column references");
            }
        }
        
        List<GroupByNode.AggregateProjection> aggregations = new ArrayList<>();
        
        for (SelectItem item : stmt.selectItems()) {
            if (item instanceof SelectItem.ExpressionItem exprItem) {
                if (exprItem.expression() instanceof org.finos.legend.engine.sql.ast.Expression.FunctionCall func) {
                    AggregateExpression.AggregateFunction aggFunc = parseAggregateFunction(func.functionName());
                    if (aggFunc != null && !func.arguments().isEmpty()) {
                        String sourceCol = extractColumnName(func.arguments().get(0));
                        String alias = exprItem.alias() != null ? exprItem.alias() : func.functionName().toLowerCase();
                        aggregations.add(new GroupByNode.AggregateProjection(alias, sourceCol, aggFunc));
                    }
                }
            }
        }
        
        if (aggregations.isEmpty()) {
            throw new SQLCompileException("GROUP BY requires at least one aggregate function in SELECT");
        }
        
        return new GroupByNode(source, groupingColumns, aggregations);
    }

    private AggregateExpression.AggregateFunction parseAggregateFunction(String name) {
        return switch (name.toUpperCase()) {
            case "COUNT" -> AggregateExpression.AggregateFunction.COUNT;
            case "SUM" -> AggregateExpression.AggregateFunction.SUM;
            case "AVG" -> AggregateExpression.AggregateFunction.AVG;
            case "MIN" -> AggregateExpression.AggregateFunction.MIN;
            case "MAX" -> AggregateExpression.AggregateFunction.MAX;
            default -> null;
        };
    }

    private String extractColumnName(org.finos.legend.engine.sql.ast.Expression expr) {
        if (expr instanceof org.finos.legend.engine.sql.ast.Expression.ColumnRef col) {
            return col.columnName();
        }
        throw new SQLCompileException("Expected column reference in aggregate function");
    }

    // ==================== ORDER BY ====================

    private RelationNode compileSortNode(RelationNode source, List<OrderSpec> orderSpecs) {
        List<SortNode.SortColumn> columns = new ArrayList<>();
        
        for (OrderSpec spec : orderSpecs) {
            String column = extractColumnName(spec.expression());
            SortNode.SortDirection dir = spec.direction() == OrderSpec.Direction.DESC 
                ? SortNode.SortDirection.DESC 
                : SortNode.SortDirection.ASC;
            columns.add(new SortNode.SortColumn(column, dir));
        }
        
        return new SortNode(source, columns);
    }

    // ==================== Expression Compilation ====================

    private Expression compileExpression(org.finos.legend.engine.sql.ast.Expression expr) {
        return switch (expr) {
            case org.finos.legend.engine.sql.ast.Expression.ColumnRef col -> compileColumnRef(col);
            case org.finos.legend.engine.sql.ast.Expression.Literal lit -> compileLiteral(lit);
            case org.finos.legend.engine.sql.ast.Expression.BinaryOp binOp -> compileBinaryOp(binOp);
            case org.finos.legend.engine.sql.ast.Expression.UnaryOp unaryOp -> compileUnaryOp(unaryOp);
            case org.finos.legend.engine.sql.ast.Expression.FunctionCall func -> compileFunctionCall(func);
            case org.finos.legend.engine.sql.ast.Expression.IsNullExpr isNull -> compileIsNull(isNull);
            case org.finos.legend.engine.sql.ast.Expression.BetweenExpr between -> compileBetween(between);
            case org.finos.legend.engine.sql.ast.Expression.InExpr inExpr -> compileIn(inExpr);
            case org.finos.legend.engine.sql.ast.Expression.CaseExpr caseExpr -> compileCase(caseExpr);
            case org.finos.legend.engine.sql.ast.Expression.CastExpr cast -> compileCast(cast);
            case org.finos.legend.engine.sql.ast.Expression.ExistsExpr exists -> compileExists(exists);
            case org.finos.legend.engine.sql.ast.Expression.SubqueryExpr subquery -> compileSubqueryExpr(subquery);
            case org.finos.legend.engine.sql.ast.Expression.WindowExpr window -> compileWindow(window);
        };
    }

    private Expression compileColumnRef(org.finos.legend.engine.sql.ast.Expression.ColumnRef col) {
        String tableAlias = col.tableAlias() != null ? col.tableAlias() : "";
        return new ColumnReference(tableAlias, col.columnName());
    }

    private Expression compileLiteral(org.finos.legend.engine.sql.ast.Expression.Literal lit) {
        return switch (lit.type()) {
            case STRING -> Literal.string((String) lit.value());
            case INTEGER -> Literal.integer(((Number) lit.value()).longValue());
            case DECIMAL -> new Literal(((Number) lit.value()).doubleValue(), Literal.LiteralType.DOUBLE);
            case BOOLEAN -> Literal.bool((Boolean) lit.value());
            case NULL -> Literal.nullValue();
        };
    }

    private Expression compileBinaryOp(org.finos.legend.engine.sql.ast.Expression.BinaryOp binOp) {
        Expression left = compileExpression(binOp.left());
        Expression right = compileExpression(binOp.right());
        
        return switch (binOp.operator()) {
            // Comparison
            case EQ -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.EQUALS, right);
            case NE -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.NOT_EQUALS, right);
            case LT -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.LESS_THAN, right);
            case LE -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS, right);
            case GT -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.GREATER_THAN, right);
            case GE -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, right);
            
            // Logical
            case AND -> LogicalExpression.and(left, right);
            case OR -> LogicalExpression.or(left, right);
            
            // String
            case LIKE, ILIKE -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.LIKE, right);
            case CONCAT -> ConcatExpression.of(left, right);
            
            // Arithmetic
            case PLUS -> new ArithmeticExpression(left, BinaryArithmeticExpr.Operator.ADD, right);
            case MINUS -> new ArithmeticExpression(left, BinaryArithmeticExpr.Operator.SUBTRACT, right);
            case MULTIPLY -> new ArithmeticExpression(left, BinaryArithmeticExpr.Operator.MULTIPLY, right);
            case DIVIDE -> new ArithmeticExpression(left, BinaryArithmeticExpr.Operator.DIVIDE, right);
            case MODULO -> SqlFunctionCall.of("MOD", left, right);
        };
    }

    private Expression compileUnaryOp(org.finos.legend.engine.sql.ast.Expression.UnaryOp unaryOp) {
        Expression operand = compileExpression(unaryOp.operand());
        
        return switch (unaryOp.operator()) {
            case NOT -> LogicalExpression.not(operand);
            case MINUS -> ArithmeticExpression.subtract(Literal.integer(0), operand);
            case PLUS -> operand;
        };
    }

    private Expression compileFunctionCall(org.finos.legend.engine.sql.ast.Expression.FunctionCall func) {
        List<Expression> args = func.arguments().stream()
            .map(this::compileExpression)
            .toList();
        
        // Check for aggregate functions
        AggregateExpression.AggregateFunction aggFunc = parseAggregateFunction(func.functionName());
        if (aggFunc != null) {
            Expression arg = args.isEmpty() ? new ColumnReference("", "*") : args.get(0);
            return new AggregateExpression(aggFunc, arg);
        }
        
        // Use first arg as target, rest as additional args
        if (args.isEmpty()) {
            return SqlFunctionCall.of(func.functionName(), Literal.nullValue());
        }
        return SqlFunctionCall.of(func.functionName(), args.get(0), args.subList(1, args.size()).toArray(new Expression[0]));
    }

    private Expression compileIsNull(org.finos.legend.engine.sql.ast.Expression.IsNullExpr isNull) {
        Expression expr = compileExpression(isNull.expr());
        ComparisonExpression cmp = new ComparisonExpression(
            expr, ComparisonExpression.ComparisonOperator.IS_NULL, null
        );
        return isNull.negated() 
            ? LogicalExpression.not(cmp)
            : cmp;
    }

    private Expression compileBetween(org.finos.legend.engine.sql.ast.Expression.BetweenExpr between) {
        Expression expr = compileExpression(between.expr());
        Expression low = compileExpression(between.low());
        Expression high = compileExpression(between.high());
        
        Expression condition = LogicalExpression.and(
            new ComparisonExpression(expr, ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, low),
            new ComparisonExpression(expr, ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS, high)
        );
        
        return between.negated() 
            ? LogicalExpression.not(condition)
            : condition;
    }

    private Expression compileIn(org.finos.legend.engine.sql.ast.Expression.InExpr inExpr) {
        Expression left = compileExpression(inExpr.expr());
        
        if (inExpr.values() != null && !inExpr.values().isEmpty()) {
            List<Expression> conditions = new ArrayList<>();
            for (var v : inExpr.values()) {
                conditions.add(new ComparisonExpression(
                    left, ComparisonExpression.ComparisonOperator.EQUALS, compileExpression(v)
                ));
            }
            
            Expression condition = conditions.size() == 1 
                ? conditions.get(0)
                : new LogicalExpression(LogicalExpression.LogicalOperator.OR, conditions);
            
            return inExpr.negated() 
                ? LogicalExpression.not(condition)
                : condition;
        }
        
        throw new SQLCompileException("IN with subquery not yet supported");
    }

    private Expression compileCase(org.finos.legend.engine.sql.ast.Expression.CaseExpr caseExpr) {
        // Build nested CaseExpression from the when clauses
        // CASE WHEN c1 THEN v1 WHEN c2 THEN v2 ELSE v3 END
        // becomes: CaseExpression(c1, v1, CaseExpression(c2, v2, v3))
        
        Expression elseExpr = caseExpr.elseExpr() != null 
            ? compileExpression(caseExpr.elseExpr())
            : Literal.nullValue();
        
        // Build from right to left
        var whenClauses = caseExpr.whenClauses();
        for (int i = whenClauses.size() - 1; i >= 0; i--) {
            var when = whenClauses.get(i);
            elseExpr = new CaseExpression(
                compileExpression(when.condition()),
                compileExpression(when.result()),
                elseExpr
            );
        }
        
        return elseExpr;
    }

    private Expression compileCast(org.finos.legend.engine.sql.ast.Expression.CastExpr cast) {
        return compileExpression(cast.expr());
    }

    private Expression compileExists(org.finos.legend.engine.sql.ast.Expression.ExistsExpr exists) {
        RelationNode subquery = compile(exists.subquery());
        return exists.negated() ? ExistsExpression.notExists(subquery) : ExistsExpression.exists(subquery);
    }

    private Expression compileSubqueryExpr(org.finos.legend.engine.sql.ast.Expression.SubqueryExpr subquery) {
        throw new SQLCompileException("Scalar subqueries not yet supported");
    }

    private Expression compileWindow(org.finos.legend.engine.sql.ast.Expression.WindowExpr window) {
        throw new SQLCompileException("Window functions not yet supported in IR");
    }

    // ==================== Helpers ====================

    private String generateAlias() {
        return "t" + (aliasCounter++);
    }
}
