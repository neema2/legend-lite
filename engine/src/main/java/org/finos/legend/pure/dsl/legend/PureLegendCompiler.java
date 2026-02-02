package org.finos.legend.pure.dsl.legend;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.pure.dsl.ModelContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * "Smart" compiler that interprets Expression AST into RelationNode execution
 * plans.
 * 
 * This is the counterpart to PureLegendParser (the "dumb" parser).
 * ALL semantic interpretation happens here:
 * - Function("filter", [source, lambda]) → FilterNode
 * - Function("groupBy", [source, cols, aggs]) → GroupByNode
 * - Function("project", [source, cols]) → ProjectNode
 * - etc.
 * 
 * The parser produces generic Function nodes; this compiler understands what
 * they mean.
 */
public final class PureLegendCompiler {

    private final MappingRegistry mappingRegistry;
    private final ModelContext modelContext;
    private int aliasCounter = 0;

    public PureLegendCompiler(MappingRegistry mappingRegistry) {
        this(mappingRegistry, null);
    }

    public PureLegendCompiler(MappingRegistry mappingRegistry, ModelContext modelContext) {
        this.mappingRegistry = Objects.requireNonNull(mappingRegistry, "Mapping registry cannot be null");
        this.modelContext = modelContext;
    }

    /**
     * Compiles a Pure query string to RelationNode.
     */
    public RelationNode compile(String pureQuery) {
        Expression ast = PureLegendParser.parse(pureQuery);
        return compileToRelation(ast, new CompilationContext());
    }

    /**
     * Compiles an Expression AST to RelationNode.
     */
    public RelationNode compile(Expression ast) {
        return compileToRelation(ast, new CompilationContext());
    }

    /**
     * Compiles an Expression AST to RelationNode.
     */
    public RelationNode compileToRelation(Expression expr, CompilationContext ctx) {
        return switch (expr) {
            case Function f -> compileFunction(f, ctx);
            case TdsLiteral tds -> new TdsLiteralNode(tds.columnNames(), tds.rows());
            case Variable v -> throw new CompileException("Cannot compile variable $" + v.name() + " as relation");
            case Property p -> throw new CompileException("Cannot compile property access as relation");
            case Literal l -> throw new CompileException("Cannot compile literal as relation");
            case Lambda l -> throw new CompileException("Cannot compile lambda as relation");
            case Collection c -> throw new CompileException("Cannot compile collection as relation");
        };
    }

    /**
     * Compiles a Function node - the main semantic dispatch.
     */
    private RelationNode compileFunction(Function f, CompilationContext ctx) {
        String name = f.function();
        List<Expression> params = f.parameters();

        return switch (name) {
            // Class operations
            case "all" -> compileAll(params, ctx);

            // Relation transformations
            case "filter" -> compileFilter(params, ctx);
            case "project" -> compileProject(params, ctx);
            case "groupBy" -> compileGroupBy(params, ctx);
            case "extend" -> compileExtend(params, ctx);
            case "select" -> compileSelect(params, ctx);
            case "rename" -> compileRename(params, ctx);
            case "distinct" -> compileDistinct(params, ctx);
            case "concatenate" -> compileConcatenate(params, ctx);

            // Sorting
            case "sort" -> compileSort(params, ctx);
            case "sortBy" -> compileSortBy(params, ctx);

            // Limiting
            case "limit", "take" -> compileLimit(params, ctx);
            case "drop" -> compileDrop(params, ctx);
            case "slice" -> compileSlice(params, ctx);
            case "first" -> compileFirst(params, ctx);
            case "nth" -> compileNth(params, ctx);

            // Joins
            case "join" -> compileJoin(params, ctx);
            case "asOfJoin" -> compileAsOfJoin(params, ctx);

            // Flatten (for JSON/nested data)
            case "flatten" -> compileFlatten(params, ctx);

            // Pivot
            case "pivot" -> compilePivot(params, ctx);

            // From clause (runtime binding)
            case "from" -> compileFrom(params, ctx);

            // Unknown function - could be UDF or error
            default -> throw new CompileException("Unknown function: " + name);
        };
    }

    // ========================================
    // CLASS OPERATIONS
    // ========================================

    private RelationNode compileAll(List<Expression> params, CompilationContext ctx) {
        if (params.isEmpty()) {
            throw new CompileException("all() requires a class reference");
        }

        // params[0] should be Function("class", [Literal("ClassName")])
        String className = extractClassName(params.get(0));

        // Look up mapping
        RelationalMapping mapping = mappingRegistry.getByClassName(className);
        if (mapping == null) {
            throw new CompileException("No mapping found for class: " + className);
        }

        String alias = nextAlias();
        return new TableNode(mapping.table(), alias);
    }

    private String extractClassName(Expression expr) {
        if (expr instanceof Function f && "class".equals(f.function())) {
            if (!f.parameters().isEmpty() && f.parameters().get(0) instanceof Literal lit) {
                return String.valueOf(lit.value());
            }
        }
        throw new CompileException("Expected class reference, got: " + expr);
    }

    // ========================================
    // RELATION TRANSFORMATIONS
    // ========================================

    private RelationNode compileFilter(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("filter() requires source and predicate");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        Expression predicate = params.get(1);

        // Enrich context with mapping info (same pattern as compileProject)
        CompilationContext sourceCtx = ctx.withSource(source);
        sourceCtx = enrichContextWithMapping(params.get(0), sourceCtx);

        // Compile predicate lambda to SQL expression
        org.finos.legend.engine.plan.Expression condition = compilePredicate(predicate, sourceCtx);

        return new FilterNode(source, condition);
    }

    private RelationNode compileProject(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("project() requires source and columns");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);

        // Extract mapping context from source if available
        CompilationContext sourceCtx = ctx.withSource(source);
        sourceCtx = enrichContextWithMapping(params.get(0), sourceCtx);

        // Handle both: project([cols]) and project(lambda1, lambda2, ...)
        List<Projection> projections = new ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            Expression arg = params.get(i);
            if (arg instanceof Collection coll) {
                // Collection of columns: [{p|$p.a}, {p|$p.b}]
                for (Expression e : coll.values()) {
                    projections.add(compileOneProjection(e, sourceCtx));
                }
            } else {
                // Individual projection (vararg style)
                projections.add(compileOneProjection(arg, sourceCtx));
            }
        }

        return new ProjectNode(source, projections);
    }

    /**
     * Enriches the compilation context with mapping info from the source
     * expression.
     * Following the legacy PureCompiler pattern - extracts mapping and class name
     * from the source expression chain.
     */
    private CompilationContext enrichContextWithMapping(Expression sourceExpr, CompilationContext ctx) {
        RelationalMapping mapping = getMappingFromSource(sourceExpr);
        String className = getClassNameFromSource(sourceExpr);
        String tableAlias = ctx.getTableAlias();

        if (mapping != null && tableAlias != null) {
            return ctx.withMapping(mapping, tableAlias, className);
        }
        return ctx;
    }

    private RelationNode compileGroupBy(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 3) {
            throw new CompileException("groupBy() requires source, grouping columns, and aggregation columns");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        List<String> groupCols = extractColumnNames(params.get(1));
        List<GroupByNode.AggregateProjection> aggCols = compileAggregations(params.get(2), ctx.withSource(source));

        return new GroupByNode(source, groupCols, aggCols);
    }

    private RelationNode compileExtend(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("extend() requires source and columns");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        List<ExtendNode.ExtendProjection> projections = compileExtendProjections(params.get(1), ctx.withSource(source));

        return new ExtendNode(source, projections);
    }

    private RelationNode compileSelect(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("select() requires source and columns");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        List<String> columns = extractColumnNames(params.get(1));

        // select() is like project() but only with column names (no transforms)
        List<Projection> projections = columns.stream()
                .map(col -> new Projection(ColumnReference.of(col), col))
                .toList();

        return new ProjectNode(source, projections);
    }

    private RelationNode compileRename(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 3) {
            throw new CompileException("rename() requires source, old name, and new name");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        String oldName = extractString(params.get(1));
        String newName = extractString(params.get(2));

        return new RenameNode(source, oldName, newName);
    }

    private RelationNode compileDistinct(List<Expression> params, CompilationContext ctx) {
        if (params.isEmpty()) {
            throw new CompileException("distinct() requires source");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        List<String> columns = params.size() > 1 ? extractColumnNames(params.get(1)) : List.of();

        return new DistinctNode(source, columns);
    }

    private RelationNode compileConcatenate(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("concatenate() requires two sources");
        }

        RelationNode left = compileToRelation(params.get(0), ctx);
        RelationNode right = compileToRelation(params.get(1), ctx);

        return new ConcatenateNode(left, right);
    }

    // ========================================
    // SORTING & LIMITING
    // ========================================

    private RelationNode compileSort(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("sort() requires source and sort specs");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        List<SortNode.SortColumn> sortCols = compileSortColumns(params.get(1));

        return new SortNode(source, sortCols);
    }

    private RelationNode compileLimit(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("limit() requires source and count");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        int count = extractInt(params.get(1));

        return LimitNode.limit(source, count);
    }

    private RelationNode compileDrop(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("drop() requires source and count");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        int count = extractInt(params.get(1));

        // drop(n) = offset(n) with no limit
        return LimitNode.offset(source, count);
    }

    private RelationNode compileSlice(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 3) {
            throw new CompileException("slice() requires source, start, and end");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        int start = extractInt(params.get(1));
        int end = extractInt(params.get(2));

        return LimitNode.slice(source, start, end);
    }

    private RelationNode compileFirst(List<Expression> params, CompilationContext ctx) {
        if (params.isEmpty()) {
            throw new CompileException("first() requires source");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        return LimitNode.limit(source, 1);
    }

    private RelationNode compileNth(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("nth() requires source and index");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        int index = extractInt(params.get(1));

        // nth(n) means get item at index n (0-based)
        // This translates to LIMIT 1 OFFSET n
        return new LimitNode(source, 1, index);
    }

    private RelationNode compileSortBy(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("sortBy() requires source and sort column");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);

        // Extract column(s) from remaining params (lambdas or column specs)
        List<SortNode.SortColumn> sortCols = new ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            Expression param = params.get(i);
            String colName = extractSortColumn(param);
            SortNode.SortDirection direction = extractSortDirection(param);
            sortCols.add(new SortNode.SortColumn(colName, direction));
        }

        return new SortNode(source, sortCols);
    }

    /**
     * Extracts column name from a sort expression.
     * Handles: Lambda {p | $p.col}, ~col, ~col->desc()
     */
    private String extractSortColumn(Expression expr) {
        if (expr instanceof Lambda lambda) {
            // {p | $p.col} - extract property name
            if (lambda.body() instanceof Property p) {
                return p.property();
            }
            // {p | $p.col->desc()} - wrapped in desc()
            if (lambda.body() instanceof Function f
                    && ("desc".equals(f.function()) || "asc".equals(f.function()) || "ascending".equals(f.function())
                            || "descending".equals(f.function()))) {
                if (!f.parameters().isEmpty() && f.parameters().get(0) instanceof Property p) {
                    return p.property();
                }
            }
        }
        if (expr instanceof Function f) {
            // ~col or ~col->desc()
            if ("column".equals(f.function()) && !f.parameters().isEmpty()) {
                return extractString(f.parameters().get(0));
            }
            if ("desc".equals(f.function()) || "asc".equals(f.function())
                    || "ascending".equals(f.function()) || "descending".equals(f.function())) {
                if (!f.parameters().isEmpty()) {
                    return extractSortColumn(f.parameters().get(0));
                }
            }
        }
        throw new CompileException("Cannot extract sort column from: " + expr);
    }

    /**
     * Extracts sort direction from a sort expression.
     */
    private SortNode.SortDirection extractSortDirection(Expression expr) {
        if (expr instanceof Lambda lambda) {
            if (lambda.body() instanceof Function f) {
                if ("desc".equals(f.function()) || "descending".equals(f.function())) {
                    return SortNode.SortDirection.DESC;
                }
            }
        }
        if (expr instanceof Function f) {
            if ("desc".equals(f.function()) || "descending".equals(f.function())) {
                return SortNode.SortDirection.DESC;
            }
        }
        return SortNode.SortDirection.ASC; // default
    }

    // ========================================
    // JOINS
    // ========================================

    private RelationNode compileJoin(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 3) {
            throw new CompileException("join() requires left, right, and condition");
        }

        RelationNode left = compileToRelation(params.get(0), ctx);
        RelationNode right = compileToRelation(params.get(1), ctx);
        Expression conditionExpr = params.get(2);
        JoinNode.JoinType joinType = params.size() > 3 ? extractJoinType(params.get(3)) : JoinNode.JoinType.INNER;

        org.finos.legend.engine.plan.Expression joinCondition = compilePredicate(conditionExpr,
                ctx.withSources(left, right));

        return new JoinNode(left, right, joinCondition, joinType);
    }

    /**
     * Compiles asOfJoin - a temporal join that matches rows based on a time
     * condition.
     * 
     * asOfJoin(left, right, matchLambda, keyLambda?)
     * - matchLambda: {l, r | $l.time > $r.time} - the ASOF match condition
     * - keyLambda (optional): {l, r | $l.id == $r.id} - additional equality keys
     */
    private RelationNode compileAsOfJoin(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 3) {
            throw new CompileException("asOfJoin() requires left, right, and match condition");
        }

        RelationNode left = compileToRelation(params.get(0), ctx);
        RelationNode right = compileToRelation(params.get(1), ctx);

        // Match condition lambda
        Lambda matchLambda = extractLambda(params.get(2));
        List<String> lambdaParams = matchLambda.parameters();

        // Build context with bindings for both sides
        String leftAlias = "left_src";
        String rightAlias = "right_src";
        CompilationContext joinCtx = ctx
                .withLambdaParam(lambdaParams.size() > 0 ? lambdaParams.get(0) : "l")
                .withSource(left);

        // Compile match condition
        org.finos.legend.engine.plan.Expression matchCondition = compilePredicate(matchLambda.body(), joinCtx);

        // If there's a key condition (4th param), combine with AND
        org.finos.legend.engine.plan.Expression finalCondition;
        if (params.size() > 3 && params.get(3) instanceof Lambda keyLambda) {
            org.finos.legend.engine.plan.Expression keyCondition = compilePredicate(keyLambda.body(), joinCtx);
            finalCondition = LogicalExpression.and(keyCondition, matchCondition);
        } else {
            finalCondition = matchCondition;
        }

        return new JoinNode(left, right, finalCondition, JoinNode.JoinType.ASOF_LEFT);
    }

    private Lambda extractLambda(Expression expr) {
        if (expr instanceof Lambda l) {
            return l;
        }
        if (expr instanceof Function f && f.parameters().size() > 0 && f.parameters().get(0) instanceof Lambda l) {
            return l;
        }
        throw new CompileException("Expected lambda, got: " + expr);
    }

    // ========================================
    // OTHER OPERATIONS
    // ========================================

    private RelationNode compileFlatten(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("flatten() requires source and column");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);
        String column = extractColumnName(params.get(1));

        return new LateralJoinNode(source, ColumnReference.of(column), nextAlias());
    }

    private RelationNode compileFrom(List<Expression> params, CompilationContext ctx) {
        if (params.isEmpty()) {
            throw new CompileException("from() requires source");
        }

        // from() adds mapping/runtime context but doesn't change the relation structure
        // The mapping/runtime would be extracted and used by the execution layer
        return compileToRelation(params.get(0), ctx);
    }

    /**
     * Compiles pivot() - rotates rows into columns.
     * 
     * Pure syntax: pivot(pivotCols, aggSpecs, staticValues?)
     * - pivotCols: columns whose values become new column names
     * - aggSpecs: list of (name, column, function) triples
     * - staticValues (optional): explicit list of pivot values
     */
    private RelationNode compilePivot(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 3) {
            throw new CompileException("pivot() requires source, pivot columns, and aggregate specs");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);

        // Extract pivot columns from params[1] - can be ~col or [~col1, ~col2]
        List<String> pivotColumns = extractColumnNames(params.get(1));

        // Extract aggregate specs from params[2]
        // Format: [~name:{x|$x.col->agg()}] or collection of agg specs
        List<PivotNode.AggregateSpec> aggregates = compileAggregateSpecs(params.get(2), ctx);

        // Check for static values (optional 4th param)
        if (params.size() > 3) {
            List<String> staticValues = extractStringList(params.get(3));
            return PivotNode.withValues(source, pivotColumns, staticValues, aggregates);
        } else {
            return PivotNode.dynamic(source, pivotColumns, aggregates);
        }
    }

    private List<PivotNode.AggregateSpec> compileAggregateSpecs(Expression expr, CompilationContext ctx) {
        List<PivotNode.AggregateSpec> specs = new ArrayList<>();

        if (expr instanceof Collection coll) {
            for (Expression item : coll.values()) {
                specs.add(compileOneAggregateSpec(item, ctx));
            }
        } else {
            specs.add(compileOneAggregateSpec(expr, ctx));
        }

        return specs;
    }

    private PivotNode.AggregateSpec compileOneAggregateSpec(Expression expr, CompilationContext ctx) {
        // Format: agg(~name, {x|$x.col}, 'SUM')
        // or: ~name:{x|$x.col->sum()}
        if (expr instanceof Function f && "agg".equals(f.function())) {
            List<Expression> args = f.parameters();
            String name = extractColumnName(args.get(0));
            String valueColumn = extractColumnFromLambda(extractLambda(args.get(1)));
            String aggFuncName = args.size() > 2
                    ? extractAggFunctionName(args.get(2))
                    : "SUM";
            return new PivotNode.AggregateSpec(name, valueColumn, aggFuncName);
        }

        // Named lambda format: ~name:{x|$x.col->sum()}
        if (expr instanceof Function f && "column".equals(f.function())) {
            String name = extractString(f.parameters().get(0));
            Lambda lambda = extractLambda(f.parameters().get(1));
            String valueColumn = extractPivotColumnFromLambda(lambda);
            String aggFuncName = extractAggFunctionNameFromLambda(lambda);
            return new PivotNode.AggregateSpec(name, valueColumn, aggFuncName);
        }

        throw new CompileException("Invalid aggregate spec: " + expr);
    }

    private String extractPivotColumnFromLambda(Lambda lambda) {
        Expression body = lambda.body();
        if (body instanceof Function f) {
            // {x | $x.col->sum()} - extract col from first param
            if (!f.parameters().isEmpty()) {
                Expression first = f.parameters().get(0);
                if (first instanceof Property p) {
                    return p.property();
                }
            }
        }
        if (body instanceof Property p) {
            return p.property();
        }
        throw new CompileException("Cannot extract column from pivot lambda: " + body);
    }

    private String extractAggFunctionNameFromLambda(Lambda lambda) {
        Expression body = lambda.body();
        if (body instanceof Function f) {
            return f.function().toUpperCase();
        }
        return "SUM";
    }

    private String extractAggFunctionName(Expression expr) {
        String funcName = extractString(expr);
        return switch (funcName.toUpperCase()) {
            case "SUM" -> "SUM";
            case "AVG", "AVERAGE" -> "AVG";
            case "MIN" -> "MIN";
            case "MAX" -> "MAX";
            case "COUNT" -> "COUNT";
            default -> "SUM";
        };
    }

    private List<String> extractStringList(Expression expr) {
        List<String> result = new ArrayList<>();
        if (expr instanceof Collection coll) {
            for (Expression item : coll.values()) {
                result.add(extractString(item));
            }
        } else {
            result.add(extractString(expr));
        }
        return result;
    }

    // ========================================
    // EXPRESSION COMPILATION
    // ========================================

    /**
     * Compiles an AST Expression to a plan Expression (for WHERE, computed columns,
     * etc.)
     */
    private org.finos.legend.engine.plan.Expression compilePredicate(Expression expr, CompilationContext ctx) {
        return switch (expr) {
            case Lambda lambda -> compilePredicateBody(lambda.body(), ctx.withLambdaParam(lambda.parameters().get(0)));
            case Function f -> compilePredicateFunction(f, ctx);
            case Variable v -> compileVariable(v, ctx);
            case Property p -> compilePropertyAccess(p, ctx);
            case Literal l -> compileLiteral(l);
            case TdsLiteral t -> throw new CompileException("Cannot use TDS literal as predicate");
            case Collection c -> throw new CompileException("Cannot use collection as predicate");
        };
    }

    private org.finos.legend.engine.plan.Expression compilePredicateBody(Expression body, CompilationContext ctx) {
        return compilePredicate(body, ctx);
    }

    private org.finos.legend.engine.plan.Expression compilePredicateFunction(Function f, CompilationContext ctx) {
        String name = f.function();
        List<Expression> params = f.parameters();

        return switch (name) {
            // Comparison operators
            case "equal" -> ComparisonExpression.equals(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));
            case "notEqual" -> new ComparisonExpression(
                    compilePredicate(params.get(0), ctx),
                    ComparisonExpression.ComparisonOperator.NOT_EQUALS,
                    compilePredicate(params.get(1), ctx));
            case "lessThan" -> ComparisonExpression.lessThan(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));
            case "greaterThan" -> ComparisonExpression.greaterThan(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));
            case "lessThanEqual" -> new ComparisonExpression(
                    compilePredicate(params.get(0), ctx),
                    ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS,
                    compilePredicate(params.get(1), ctx));
            case "greaterThanEqual" -> new ComparisonExpression(
                    compilePredicate(params.get(0), ctx),
                    ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS,
                    compilePredicate(params.get(1), ctx));

            // Logical operators
            case "and" -> LogicalExpression.and(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));
            case "or" -> LogicalExpression.or(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));
            case "not" -> LogicalExpression.not(
                    compilePredicate(params.get(0), ctx));

            // Arithmetic
            case "plus" -> ArithmeticExpression.add(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));
            case "minus" -> ArithmeticExpression.subtract(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));
            case "times" -> ArithmeticExpression.multiply(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));
            case "divide" -> ArithmeticExpression.divide(
                    compilePredicate(params.get(0), ctx),
                    compilePredicate(params.get(1), ctx));

            // Column reference from ~col syntax
            case "column" -> {
                String colName = extractString(params.get(0));
                yield ColumnReference.of(colName);
            }

            // ========================================
            // DATE FUNCTIONS
            // ========================================
            case "year" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.YEAR,
                    compilePredicate(params.get(0), ctx));
            case "month" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.MONTH,
                    compilePredicate(params.get(0), ctx));
            case "day", "dayOfMonth" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.DAY,
                    compilePredicate(params.get(0), ctx));
            case "hour" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.HOUR,
                    compilePredicate(params.get(0), ctx));
            case "minute" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.MINUTE,
                    compilePredicate(params.get(0), ctx));
            case "second" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.SECOND,
                    compilePredicate(params.get(0), ctx));
            case "dayOfWeek" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.DAY_OF_WEEK,
                    compilePredicate(params.get(0), ctx));
            case "dayOfYear" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.DAY_OF_YEAR,
                    compilePredicate(params.get(0), ctx));
            case "weekOfYear" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.WEEK_OF_YEAR,
                    compilePredicate(params.get(0), ctx));
            case "quarter" -> new DateFunctionExpression(
                    DateFunctionExpression.DateFunction.QUARTER,
                    compilePredicate(params.get(0), ctx));

            // ========================================
            // STRING FUNCTIONS
            // ========================================
            case "toUpper", "toUpperCase" -> SqlFunctionCall.of("toupper",
                    compilePredicate(params.get(0), ctx), SqlType.VARCHAR);
            case "toLower", "toLowerCase" -> SqlFunctionCall.of("tolower",
                    compilePredicate(params.get(0), ctx), SqlType.VARCHAR);
            case "trim" -> SqlFunctionCall.of("trim",
                    compilePredicate(params.get(0), ctx), SqlType.VARCHAR);
            case "length" -> SqlFunctionCall.of("length",
                    compilePredicate(params.get(0), ctx), SqlType.INTEGER);
            case "substring" -> {
                var str = compilePredicate(params.get(0), ctx);
                var start = compilePredicate(params.get(1), ctx);
                if (params.size() > 2) {
                    yield SqlFunctionCall.of("substring", str, SqlType.VARCHAR,
                            start, compilePredicate(params.get(2), ctx));
                }
                yield SqlFunctionCall.of("substring", str, SqlType.VARCHAR, start);
            }
            case "startsWith" -> SqlFunctionCall.of("startswith",
                    compilePredicate(params.get(0), ctx), SqlType.BOOLEAN,
                    compilePredicate(params.get(1), ctx));
            case "endsWith" -> SqlFunctionCall.of("endswith",
                    compilePredicate(params.get(0), ctx), SqlType.BOOLEAN,
                    compilePredicate(params.get(1), ctx));
            case "contains" -> {
                // String contains: $str->contains('sub')
                if (params.size() >= 2 && isStringLiteral(params.get(1))) {
                    yield SqlFunctionCall.of("contains",
                            compilePredicate(params.get(0), ctx), SqlType.BOOLEAN,
                            compilePredicate(params.get(1), ctx));
                }
                // Collection contains: [1,2,3]->contains($x) = $x IN (1,2,3)
                throw new CompileException("Collection contains() not yet supported");
            }

            // ========================================
            // COLLECTION OPERATIONS
            // ========================================
            case "in" -> {
                // $x->in(['a', 'b', 'c'])
                var operand = compilePredicate(params.get(0), ctx);
                var valuesExpr = params.get(1);
                if (valuesExpr instanceof Collection coll) {
                    List<org.finos.legend.engine.plan.Expression> values = coll.values().stream()
                            .map(e -> compilePredicate(e, ctx))
                            .toList();
                    yield InExpression.of(operand, values);
                }
                throw new CompileException("in() requires a collection");
            }

            // ========================================
            // NULL CHECKS
            // ========================================
            case "isEmpty" -> new ComparisonExpression(
                    compilePredicate(params.get(0), ctx),
                    ComparisonExpression.ComparisonOperator.IS_NULL,
                    null);
            case "isNotEmpty" -> new ComparisonExpression(
                    compilePredicate(params.get(0), ctx),
                    ComparisonExpression.ComparisonOperator.IS_NOT_NULL,
                    null);

            // ========================================
            // MATH FUNCTIONS
            // ========================================
            case "abs" -> SqlFunctionCall.of("abs",
                    compilePredicate(params.get(0), ctx));
            case "round" -> SqlFunctionCall.of("round",
                    compilePredicate(params.get(0), ctx));
            case "ceiling", "ceil" -> SqlFunctionCall.of("ceiling",
                    compilePredicate(params.get(0), ctx));
            case "floor" -> SqlFunctionCall.of("floor",
                    compilePredicate(params.get(0), ctx));

            default -> throw new CompileException("Unknown function in predicate: " + name);
        };
    }

    private org.finos.legend.engine.plan.Expression compileVariable(Variable v, CompilationContext ctx) {
        // Variable reference - usually resolves to a column reference via lambda param
        String name = v.name();
        if (ctx.lambdaParam != null && ctx.lambdaParam.equals(name)) {
            // This is the lambda parameter referring to the current row
            // Return a placeholder - property access will resolve to actual column
            return ColumnReference.of("*"); // Placeholder
        }
        throw new CompileException("Unknown variable: $" + name);
    }

    private org.finos.legend.engine.plan.Expression compilePropertyAccess(Property p, CompilationContext ctx) {
        // $x.propertyName -> ColumnReference(tableAlias, columnName)
        // Use the context's mapping to resolve property name to column name
        String propName = p.property();
        String columnName = ctx.resolveColumn(propName);
        String tableAlias = ctx.getTableAlias();

        if (tableAlias != null && ctx.hasMapping()) {
            // When we have a table alias and mapping, use qualified reference
            return ColumnReference.of(tableAlias, columnName);
        }
        // Otherwise, use column alias (for relation projections, post-project contexts)
        return ColumnReference.of(columnName);
    }

    private org.finos.legend.engine.plan.Expression compileLiteral(org.finos.legend.pure.dsl.legend.Literal l) {
        Object value = l.value();
        return switch (l.type()) {
            case STRING -> org.finos.legend.engine.plan.Literal.string((String) value);
            case INTEGER -> org.finos.legend.engine.plan.Literal.integer(((Number) value).longValue());
            case DECIMAL -> new org.finos.legend.engine.plan.Literal(value,
                    org.finos.legend.engine.plan.Literal.LiteralType.DOUBLE);
            case BOOLEAN -> org.finos.legend.engine.plan.Literal.bool((Boolean) value);
            case NULL -> org.finos.legend.engine.plan.Literal.nullValue();
            case DATE, DATETIME, STRICT_TIME -> org.finos.legend.engine.plan.Literal.date(String.valueOf(value));
        };
    }

    // ========================================
    // PROJECTION COMPILATION
    // ========================================

    private List<Projection> compileProjections(Expression expr, CompilationContext ctx) {
        List<Projection> result = new ArrayList<>();

        if (expr instanceof Collection coll) {
            for (Expression e : coll.values()) {
                result.add(compileOneProjection(e, ctx));
            }
        } else {
            result.add(compileOneProjection(expr, ctx));
        }

        return result;
    }

    private Projection compileOneProjection(Expression expr, CompilationContext ctx) {
        // Handle ~column syntax
        if (expr instanceof Function f && "column".equals(f.function())) {
            List<Expression> args = f.parameters();
            String name = extractString(args.get(0));

            if (args.size() == 1) {
                // Simple column: ~name
                return new Projection(ColumnReference.of(name), name);
            } else if (args.size() >= 2 && args.get(1) instanceof Lambda lambda) {
                // Computed column: ~name:x|expr
                org.finos.legend.engine.plan.Expression sqlExpr = compilePredicate(lambda.body(),
                        ctx.withLambdaParam(lambda.parameters().get(0)));
                return new Projection(sqlExpr, name);
            }
        }

        // Handle lambda syntax: {p | $p.firstName}
        if (expr instanceof Lambda lambda) {
            String paramName = lambda.parameters().get(0);
            CompilationContext lambdaCtx = ctx.withLambdaParam(paramName);

            // Extract property name as the projection alias
            String alias = extractPropertyNameFromLambdaBody(lambda.body());
            org.finos.legend.engine.plan.Expression sqlExpr = compilePredicate(lambda.body(), lambdaCtx);

            return new Projection(sqlExpr, alias);
        }

        throw new CompileException("Invalid projection: " + expr);
    }

    /**
     * Extracts the property name from a lambda body for use as projection alias.
     * {p | $p.firstName} -> "firstName"
     * {p | $p.firstName + $p.lastName} -> uses first property name
     */
    private String extractPropertyNameFromLambdaBody(Expression body) {
        if (body instanceof Property p) {
            return p.property();
        }
        if (body instanceof Function f) {
            // For compound expressions like $p.col->transform(), try first parameter
            if (!f.parameters().isEmpty()) {
                return extractPropertyNameFromLambdaBody(f.parameters().get(0));
            }
        }
        return "expr"; // fallback alias
    }

    private List<ExtendNode.ExtendProjection> compileExtendProjections(Expression expr, CompilationContext ctx) {
        List<ExtendNode.ExtendProjection> result = new ArrayList<>();

        if (expr instanceof Collection coll) {
            for (Expression e : coll.values()) {
                result.add(compileOneExtendProjection(e, ctx));
            }
        } else {
            result.add(compileOneExtendProjection(expr, ctx));
        }

        return result;
    }

    private ExtendNode.ExtendProjection compileOneExtendProjection(Expression expr, CompilationContext ctx) {
        if (expr instanceof Function f && "column".equals(f.function())) {
            List<Expression> args = f.parameters();
            String name = extractString(args.get(0));

            if (args.size() >= 2 && args.get(1) instanceof Lambda lambda) {
                // Check if the lambda body contains a window function (ends with over())
                if (isWindowFunction(lambda.body())) {
                    return compileWindowProjection(name, lambda.body(), ctx);
                }

                // Simple projection
                org.finos.legend.engine.plan.Expression sqlExpr = compilePredicate(lambda.body(),
                        ctx.withLambdaParam(lambda.parameters().get(0)));
                return new ExtendNode.SimpleProjection(name, sqlExpr);
            }
        }
        throw new CompileException("Invalid extend projection: " + expr);
    }

    /**
     * Checks if an expression is a window function (contains over() call).
     */
    private boolean isWindowFunction(Expression expr) {
        if (expr instanceof Function f && "over".equals(f.function())) {
            return true;
        }
        // Check for pattern: func()->over(...) which becomes Function("over",
        // [Function("func", ...)])
        if (expr instanceof Function f) {
            List<Expression> params = f.parameters();
            if (!params.isEmpty() && params.get(0) instanceof Function inner && "over".equals(f.function())) {
                return true;
            }
            // Check the receiver/first param for over pattern
            for (Expression param : params) {
                if (isWindowFunction(param)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Compiles a window function expression into WindowProjection.
     * 
     * Patterns supported:
     * - row_number()->over(~partition, ~order->asc())
     * - sum(~col)->over(~partition, ~order, rows(...))
     * - rank()->over([])
     */
    private ExtendNode.WindowProjection compileWindowProjection(String alias, Expression expr, CompilationContext ctx) {
        if (!(expr instanceof Function overFunc && "over".equals(overFunc.function()))) {
            throw new CompileException("Expected over() function for window, got: " + expr);
        }

        List<Expression> overParams = overFunc.parameters();
        if (overParams.isEmpty()) {
            throw new CompileException("over() requires at least the window function as first argument");
        }

        // First param is the window function call (e.g., row_number(), sum(~col))
        Expression windowFuncExpr = overParams.get(0);

        // Parse the window function
        WindowExpression.WindowFunction windowFunc;
        String aggregateColumn = null;
        Integer offset = null;

        if (windowFuncExpr instanceof Function wf) {
            String funcName = wf.function();

            windowFunc = switch (funcName.toLowerCase()) {
                case "row_number", "rownumber" -> WindowExpression.WindowFunction.ROW_NUMBER;
                case "rank" -> WindowExpression.WindowFunction.RANK;
                case "dense_rank", "denserank" -> WindowExpression.WindowFunction.DENSE_RANK;
                case "percent_rank", "percentrank" -> WindowExpression.WindowFunction.PERCENT_RANK;
                case "cume_dist", "cumedist" -> WindowExpression.WindowFunction.CUME_DIST;
                case "ntile" -> WindowExpression.WindowFunction.NTILE;
                case "lag" -> WindowExpression.WindowFunction.LAG;
                case "lead" -> WindowExpression.WindowFunction.LEAD;
                case "first_value", "first" -> WindowExpression.WindowFunction.FIRST_VALUE;
                case "last_value", "last" -> WindowExpression.WindowFunction.LAST_VALUE;
                case "sum" -> WindowExpression.WindowFunction.SUM;
                case "avg", "average" -> WindowExpression.WindowFunction.AVG;
                case "min" -> WindowExpression.WindowFunction.MIN;
                case "max" -> WindowExpression.WindowFunction.MAX;
                case "count" -> WindowExpression.WindowFunction.COUNT;
                default -> throw new CompileException("Unknown window function: " + funcName);
            };

            // Extract aggregate column for aggregate window functions
            if (!wf.parameters().isEmpty() && windowFunc.ordinal() >= WindowExpression.WindowFunction.SUM.ordinal()) {
                aggregateColumn = extractColumnName(wf.parameters().get(0));
            }

            // Extract offset for LAG/LEAD
            if ((windowFunc == WindowExpression.WindowFunction.LAG
                    || windowFunc == WindowExpression.WindowFunction.LEAD)
                    && wf.parameters().size() >= 2) {
                aggregateColumn = extractColumnName(wf.parameters().get(0));
                offset = extractInteger(wf.parameters().get(1));
            }

            // Extract bucket count for NTILE
            if (windowFunc == WindowExpression.WindowFunction.NTILE && !wf.parameters().isEmpty()) {
                offset = extractInteger(wf.parameters().get(0));
            }
        } else {
            throw new CompileException("Expected window function call, got: " + windowFuncExpr);
        }

        // Parse partition and order from remaining over() arguments
        List<String> partitionBy = new ArrayList<>();
        List<WindowExpression.SortSpec> orderBy = new ArrayList<>();
        WindowExpression.FrameSpec frame = null;

        for (int i = 1; i < overParams.size(); i++) {
            Expression arg = overParams.get(i);

            if (arg instanceof Collection coll && coll.values().isEmpty()) {
                // Empty collection [] means no partition
                continue;
            }

            if (arg instanceof Function argFunc) {
                String argFuncName = argFunc.function();

                // Order specification: ~col->asc() or ~col->desc()
                if ("asc".equals(argFuncName) || "ascending".equals(argFuncName)) {
                    String col = extractColumnName(argFunc.parameters().get(0));
                    orderBy.add(new WindowExpression.SortSpec(col, WindowExpression.SortDirection.ASC));
                } else if ("desc".equals(argFuncName) || "descending".equals(argFuncName)) {
                    String col = extractColumnName(argFunc.parameters().get(0));
                    orderBy.add(new WindowExpression.SortSpec(col, WindowExpression.SortDirection.DESC));
                } else if ("rows".equals(argFuncName)) {
                    frame = compileFrameSpec(argFunc, WindowExpression.FrameType.ROWS);
                } else if ("range".equals(argFuncName)) {
                    frame = compileFrameSpec(argFunc, WindowExpression.FrameType.RANGE);
                } else if ("column".equals(argFuncName)) {
                    // Partition column: ~colName
                    partitionBy.add(extractColumnName(arg));
                } else {
                    // Assume it's a partition column reference
                    partitionBy.add(extractColumnName(arg));
                }
            } else {
                // Plain column reference for partition
                partitionBy.add(extractColumnName(arg));
            }
        }

        WindowExpression windowExpr;
        if (aggregateColumn != null && offset != null &&
                (windowFunc == WindowExpression.WindowFunction.LAG
                        || windowFunc == WindowExpression.WindowFunction.LEAD)) {
            windowExpr = WindowExpression.lagLead(windowFunc, aggregateColumn, offset, partitionBy, orderBy);
        } else if (windowFunc == WindowExpression.WindowFunction.NTILE && offset != null) {
            windowExpr = WindowExpression.ntile(offset, partitionBy, orderBy);
        } else if (aggregateColumn != null) {
            windowExpr = frame != null
                    ? WindowExpression.aggregate(windowFunc, aggregateColumn, partitionBy, orderBy, frame)
                    : WindowExpression.aggregate(windowFunc, aggregateColumn, partitionBy, orderBy);
        } else {
            windowExpr = frame != null
                    ? WindowExpression.ranking(windowFunc, partitionBy, orderBy, frame)
                    : WindowExpression.ranking(windowFunc, partitionBy, orderBy);
        }

        return new ExtendNode.WindowProjection(alias, windowExpr);
    }

    /**
     * Compiles a frame specification from rows() or range() function call.
     */
    private WindowExpression.FrameSpec compileFrameSpec(Function frameFunc, WindowExpression.FrameType type) {
        List<Expression> args = frameFunc.parameters();
        if (args.size() < 2) {
            throw new CompileException("Frame specification requires start and end bounds");
        }

        WindowExpression.FrameBound start = compileFrameBound(args.get(0));
        WindowExpression.FrameBound end = compileFrameBound(args.get(1));

        return new WindowExpression.FrameSpec(type, start, end);
    }

    /**
     * Compiles a frame bound from:
     * - unbounded() -> UNBOUNDED
     * - 0 -> CURRENT ROW
     * - negative number -> PRECEDING
     * - positive number -> FOLLOWING
     */
    private WindowExpression.FrameBound compileFrameBound(Expression expr) {
        if (expr instanceof Function f && "unbounded".equals(f.function())) {
            return WindowExpression.FrameBound.unbounded();
        }
        if (expr instanceof Literal lit && lit.value() instanceof Number n) {
            return WindowExpression.FrameBound.fromInteger(n.intValue());
        }
        throw new CompileException("Invalid frame bound: " + expr);
    }

    private String extractColumnName(Expression expr) {
        if (expr instanceof Function f && "column".equals(f.function())) {
            return extractString(f.parameters().get(0));
        }
        if (expr instanceof Property p) {
            return p.property();
        }
        if (expr instanceof Variable v) {
            return v.name();
        }
        if (expr instanceof Literal lit && lit.value() instanceof String s) {
            return s;
        }
        throw new CompileException("Cannot extract column name from: " + expr);
    }

    private int extractInteger(Expression expr) {
        if (expr instanceof Literal lit && lit.value() instanceof Number n) {
            return n.intValue();
        }
        throw new CompileException("Expected integer literal, got: " + expr);
    }

    // ========================================
    // AGGREGATION COMPILATION
    // ========================================

    private List<GroupByNode.AggregateProjection> compileAggregations(Expression expr, CompilationContext ctx) {
        List<GroupByNode.AggregateProjection> result = new ArrayList<>();

        if (expr instanceof Collection coll) {
            for (Expression e : coll.values()) {
                result.add(compileOneAggregation(e, ctx));
            }
        } else {
            result.add(compileOneAggregation(expr, ctx));
        }

        return result;
    }

    private GroupByNode.AggregateProjection compileOneAggregation(Expression expr, CompilationContext ctx) {
        if (expr instanceof Function f && "column".equals(f.function())) {
            List<Expression> args = f.parameters();
            String alias = extractString(args.get(0));

            if (args.size() >= 3 && args.get(1) instanceof Lambda mapLambda
                    && args.get(2) instanceof Lambda aggLambda) {
                // ~alias:x|$x.col:y|$y->sum()
                String sourceColumn = extractColumnFromLambda(mapLambda);
                AggregateExpression.AggregateFunction aggFunc = extractAggFunction(aggLambda);

                return new GroupByNode.AggregateProjection(alias, sourceColumn, aggFunc);
            }
        }
        throw new CompileException("Invalid aggregation: " + expr);
    }

    private String extractColumnFromLambda(Lambda lambda) {
        // Lambda body should be Property($x, "colName")
        if (lambda.body() instanceof Property p) {
            return p.property();
        }
        throw new CompileException("Expected property access in map lambda");
    }

    private AggregateExpression.AggregateFunction extractAggFunction(Lambda lambda) {
        // Lambda body should be Function("sum", [Variable])
        if (lambda.body() instanceof Function f) {
            return switch (f.function()) {
                case "sum" -> AggregateExpression.AggregateFunction.SUM;
                case "count" -> AggregateExpression.AggregateFunction.COUNT;
                case "avg", "average" -> AggregateExpression.AggregateFunction.AVG;
                case "min" -> AggregateExpression.AggregateFunction.MIN;
                case "max" -> AggregateExpression.AggregateFunction.MAX;
                default -> throw new CompileException("Unknown aggregate: " + f.function());
            };
        }
        throw new CompileException("Expected aggregate function call in agg lambda");
    }

    // ========================================
    // SORT COMPILATION
    // ========================================

    private List<SortNode.SortColumn> compileSortColumns(Expression expr) {
        List<SortNode.SortColumn> result = new ArrayList<>();

        if (expr instanceof Collection coll) {
            for (Expression e : coll.values()) {
                result.add(compileOneSortColumn(e));
            }
        } else {
            result.add(compileOneSortColumn(expr));
        }

        return result;
    }

    private SortNode.SortColumn compileOneSortColumn(Expression expr) {
        // Handle asc() / desc() wrappers
        if (expr instanceof Function f) {
            String funcName = f.function();
            if ("asc".equals(funcName) || "ascending".equals(funcName)) {
                String colName = extractSortColumnName(f.parameters().get(0));
                return SortNode.SortColumn.asc(colName);
            }
            if ("desc".equals(funcName) || "descending".equals(funcName)) {
                String colName = extractSortColumnName(f.parameters().get(0));
                return SortNode.SortColumn.desc(colName);
            }
            // column('name') - default ascending
            if ("column".equals(funcName)) {
                String colName = extractString(f.parameters().get(0));
                return SortNode.SortColumn.asc(colName);
            }
        }

        // Handle raw column name as string literal
        if (expr instanceof Literal lit && lit.value() instanceof String colName) {
            return SortNode.SortColumn.asc(colName);
        }

        throw new CompileException("Invalid sort column: " + expr);
    }

    /**
     * Extracts column name from a sort column expression.
     * Handles: 'name', ~name, column('name')
     */
    private String extractSortColumnName(Expression expr) {
        if (expr instanceof Literal lit && lit.value() instanceof String s) {
            return s;
        }
        if (expr instanceof Function f && "column".equals(f.function())) {
            return extractString(f.parameters().get(0));
        }
        throw new CompileException("Cannot extract sort column name from: " + expr);
    }

    // ========================================
    // HELPER METHODS
    // ========================================

    private List<String> extractColumnNames(Expression expr) {
        List<String> result = new ArrayList<>();

        if (expr instanceof Collection coll) {
            for (Expression e : coll.values()) {
                result.add(extractColumnName(e));
            }
        } else {
            result.add(extractColumnName(expr));
        }

        return result;
    }

    private String extractString(Expression expr) {
        if (expr instanceof Literal lit && lit.value() instanceof String s) {
            return s;
        }
        throw new CompileException("Expected string, got: " + expr);
    }

    private int extractInt(Expression expr) {
        if (expr instanceof Literal lit && lit.value() instanceof Number n) {
            return n.intValue();
        }
        throw new CompileException("Expected integer, got: " + expr);
    }

    private JoinNode.JoinType extractJoinType(Expression expr) {
        if (expr instanceof Literal lit) {
            String val = String.valueOf(lit.value()).toUpperCase();
            return switch (val) {
                case "INNER" -> JoinNode.JoinType.INNER;
                case "LEFT", "LEFT_OUTER" -> JoinNode.JoinType.LEFT_OUTER;
                case "RIGHT", "RIGHT_OUTER" -> JoinNode.JoinType.RIGHT_OUTER;
                default -> JoinNode.JoinType.INNER;
            };
        }
        return JoinNode.JoinType.INNER;
    }

    private boolean isStringLiteral(Expression expr) {
        return expr instanceof Literal l && l.type() == Literal.Type.STRING;
    }

    /**
     * Extracts the RelationalMapping from a source expression.
     * Recursively traverses the expression chain to find the root class.
     * 
     * Following legacy PureCompiler.getMappingFromSource() pattern.
     */
    private RelationalMapping getMappingFromSource(Expression source) {
        if (source instanceof Function f) {
            String funcName = f.function();
            if ("all".equals(funcName) && !f.parameters().isEmpty()) {
                // all(class('Person')) -> get mapping for Person
                String className = extractClassName(f.parameters().get(0));
                return mappingRegistry.getByClassName(className);
            }
            // For chained functions like filter, project, etc., recurse to first param
            if (!f.parameters().isEmpty()) {
                return getMappingFromSource(f.parameters().get(0));
            }
        }
        return null;
    }

    /**
     * Extracts the class name from a source expression.
     * Recursively traverses the expression chain to find the root class.
     * 
     * Following legacy PureCompiler.getClassNameFromSource() pattern.
     */
    private String getClassNameFromSource(Expression source) {
        if (source instanceof Function f) {
            String funcName = f.function();
            if ("all".equals(funcName) && !f.parameters().isEmpty()) {
                return extractClassName(f.parameters().get(0));
            }
            // For chained functions like filter, project, etc., recurse to first param
            if (!f.parameters().isEmpty()) {
                return getClassNameFromSource(f.parameters().get(0));
            }
        }
        return null;
    }

    private String nextAlias() {
        return "t" + (aliasCounter++);
    }

    // ========================================
    // CONTEXT & EXCEPTIONS
    // ========================================

    public static class CompilationContext {
        private RelationNode source;
        private List<RelationNode> sources = new ArrayList<>();
        private String lambdaParam;
        // Mapping context for property resolution
        private RelationalMapping mapping;
        private String tableAlias;
        private String className;

        public CompilationContext withSource(RelationNode source) {
            CompilationContext ctx = new CompilationContext();
            ctx.source = source;
            ctx.sources = List.of(source);
            ctx.lambdaParam = this.lambdaParam;
            ctx.mapping = this.mapping;
            ctx.tableAlias = extractTableAlias(source);
            ctx.className = this.className;
            return ctx;
        }

        private String extractTableAlias(RelationNode node) {
            if (node instanceof TableNode tn) {
                return tn.alias();
            }
            return null;
        }

        public CompilationContext withSources(RelationNode... sources) {
            CompilationContext ctx = new CompilationContext();
            ctx.sources = List.of(sources);
            ctx.lambdaParam = this.lambdaParam;
            ctx.mapping = this.mapping;
            ctx.tableAlias = this.tableAlias;
            ctx.className = this.className;
            return ctx;
        }

        public CompilationContext withLambdaParam(String param) {
            CompilationContext ctx = new CompilationContext();
            ctx.source = this.source;
            ctx.sources = this.sources;
            ctx.lambdaParam = param;
            ctx.mapping = this.mapping;
            ctx.tableAlias = this.tableAlias;
            ctx.className = this.className;
            return ctx;
        }

        public CompilationContext withMapping(RelationalMapping mapping, String tableAlias, String className) {
            CompilationContext ctx = new CompilationContext();
            ctx.source = this.source;
            ctx.sources = this.sources;
            ctx.lambdaParam = this.lambdaParam;
            ctx.mapping = mapping;
            ctx.tableAlias = tableAlias;
            ctx.className = className;
            return ctx;
        }

        public boolean hasMapping() {
            return mapping != null;
        }

        public String resolveColumn(String propertyName) {
            if (mapping != null) {
                return mapping.getColumnForProperty(propertyName)
                        .orElse(toUpperSnakeCase(propertyName));
            }
            // Fall back to convention: firstName -> FIRST_NAME
            return toUpperSnakeCase(propertyName);
        }

        private static String toUpperSnakeCase(String camelCase) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < camelCase.length(); i++) {
                char c = camelCase.charAt(i);
                if (Character.isUpperCase(c) && i > 0) {
                    result.append('_');
                }
                result.append(Character.toUpperCase(c));
            }
            return result.toString();
        }

        public String getTableAlias() {
            return tableAlias;
        }
    }

    public static class CompileException extends RuntimeException {
        public CompileException(String message) {
            super(message);
        }
    }
}
