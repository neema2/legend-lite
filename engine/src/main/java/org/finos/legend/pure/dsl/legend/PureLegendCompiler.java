package org.finos.legend.pure.dsl.legend;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.pure.dsl.ModelContext;
import org.finos.legend.pure.dsl.m2m.BinaryArithmeticExpr;

import java.util.*;

/**
 * Clean compiler that interprets Expression AST into RelationNode execution
 * plans.
 * 
 * ARCHITECTURE: Strong Type Separation with Class→Relation Transition
 * 
 * ┌──────────────────────────────────────────────────────────────────┐
 * │ CLASS PATH │
 * │ • Uses RelationalMapping for property→column resolution │
 * │ • Source: Class.all() → TableNode │
 * │ • Operations: filter (CLASS), project (CLASS) │
 * └─────────────────────────┬────────────────────────────────────────┘
 * │
 * ▼ project() = TRANSITION POINT
 * ┌──────────────────────────────────────────────────────────────────┐
 * │ RELATION PATH │
 * │ • Uses column names directly (no mapping) │
 * │ • Source: ProjectNode, TdsLiteral, RelationLiteral │
 * │ • Operations: filter, groupBy, sort, limit, join, extend, etc. │
 * └──────────────────────────────────────────────────────────────────┘
 * 
 * Design Principles:
 * 1. NO FALLBACKS - Explicit errors, never silent defaults
 * 2. TYPE-SAFE IR - Always use typed Expression nodes, never raw SQL
 * 3. EXHAUSTIVE PATTERNS - Handle each AST case explicitly
 * 4. STRONG TYPE SEPARATION - Class vs Relation/TDS with strict type checking
 */
public final class PureLegendCompiler {

    private final MappingRegistry mappingRegistry;
    private final ModelContext modelContext;
    private int aliasCounter = 0;

    // ========================================================================
    // CONSTRUCTOR
    // ========================================================================

    public PureLegendCompiler(MappingRegistry mappingRegistry) {
        this(mappingRegistry, null);
    }

    public PureLegendCompiler(MappingRegistry mappingRegistry, ModelContext modelContext) {
        this.mappingRegistry = Objects.requireNonNull(mappingRegistry, "Mapping registry cannot be null");
        this.modelContext = modelContext;
    }

    // ========================================================================
    // PUBLIC API
    // ========================================================================

    /**
     * Compiles a Pure query string to RelationNode.
     */
    public RelationNode compile(String pureQuery) {
        Expression ast = PureLegendParser.parse(pureQuery);
        return compileToRelation(ast, CompilationContext.empty());
    }

    /**
     * Compiles an Expression AST to RelationNode.
     */
    public RelationNode compile(Expression ast) {
        return compileToRelation(ast, CompilationContext.empty());
    }

    // ========================================================================
    // SOURCE TYPE CLASSIFICATION
    // ========================================================================

    /**
     * Source kinds for dual-path architecture.
     * CLASS uses mapping for property→column resolution.
     * TDS and RELATION use column names directly.
     */
    public enum SourceKind {
        CLASS, // Mapping-aware (e.g., Person.all())
        TDS, // Inline tabular data (#TDS...#)
        RELATION // Direct relation reference (#>{db.table}#)
    }

    /**
     * Classifies the source type of an expression chain.
     * This determines whether we use class-based (mapping) or relation-based
     * (column) resolution.
     */
    private SourceKind classifySource(Expression source) {
        return switch (source) {
            case Function f -> switch (f.function()) {
                case "all" -> SourceKind.CLASS;
                // project() transitions CLASS to RELATION
                case "project" -> {
                    if (f.parameters().isEmpty()) {
                        throw new CompileException("project() requires a source parameter");
                    }
                    // project() always transitions to RELATION, regardless of source
                    yield SourceKind.RELATION;
                }
                case "filter" -> {
                    if (f.parameters().isEmpty()) {
                        throw new CompileException("filter() requires a source parameter");
                    }
                    yield classifySource(f.parameters().get(0));
                }
                // All other relation operations maintain source kind
                case "groupBy", "extend", "select", "rename", "distinct", "sort", "sortBy",
                        "limit", "take", "drop", "slice", "first", "join", "concatenate",
                        "aggregate", "nth", "flatten", "pivot" -> {
                    if (f.parameters().isEmpty()) {
                        throw new CompileException(f.function() + "() requires a source parameter");
                    }
                    yield classifySource(f.parameters().get(0));
                }
                default -> throw new CompileException(
                        "Cannot classify source kind for function: " + f.function());
            };
            case TdsLiteral tds -> SourceKind.TDS;
            case RelationLiteral rel -> SourceKind.RELATION;
            case Variable v -> SourceKind.RELATION; // Variables are typically relation bindings
            default -> throw new CompileException(
                    "Cannot classify source kind for: " + describeExpression(source));
        };
    }

    // ========================================================================
    // RELATION COMPILATION - Main entry point for relation-returning expressions
    // ========================================================================

    /**
     * Compiles an Expression to a RelationNode.
     * This is the main dispatch for relation-level expressions.
     */
    private RelationNode compileToRelation(Expression expr, CompilationContext ctx) {
        return switch (expr) {
            case Function f -> compileFunction(f, ctx);
            case TdsLiteral tds -> compileTdsLiteral(tds);
            case RelationLiteral rel -> compileRelationLiteral(rel);
            case Variable v -> compileRelationVariable(v, ctx);
            case Lambda lambda -> compileConstantLambda(lambda, ctx);
            // These cannot be relations - explicit errors, no fallback
            case Property p -> throw new CompileException(
                    "Property access '" + p.property() + "' cannot be compiled as a relation. " +
                            "Property access returns a scalar value, not a relation.");
            case Literal l -> throw new CompileException(
                    "Literal value cannot be compiled as a relation. " +
                            "Use TDS literal #TDS...# for inline data.");
            case Collection c -> throw new CompileException(
                    "Collection cannot be compiled as a relation. " +
                            "Use TDS literal #TDS...# for inline data.");
        };
    }

    /**
     * Compiles a function call to a RelationNode.
     * Dispatches to specific handlers based on function name.
     */
    private RelationNode compileFunction(Function f, CompilationContext ctx) {
        String name = f.function();
        List<Expression> params = f.parameters();

        return switch (name) {
            // ===== PHASE 1: Core operations =====
            case "all" -> compileAll(params, ctx);
            case "project" -> compileProject(params, ctx);

            // ===== PHASE 2: Filtering =====
            case "filter" -> compileFilter(params, ctx);

            // ===== PHASE 3: Core Relational Operators =====
            case "groupBy" -> compileGroupBy(params, ctx);
            case "aggregate" -> compileAggregate(params, ctx);
            case "sort", "sortBy" -> compileSort(params, ctx);
            case "limit", "take" -> compileLimit(params, ctx);
            case "drop" -> compileDrop(params, ctx);
            case "slice" -> compileSlice(params, ctx);
            case "first" -> compileFirst(params, ctx);
            case "nth" -> compileNth(params, ctx);
            case "distinct" -> compileDistinct(params, ctx);
            case "rename" -> compileRename(params, ctx);
            case "concatenate" -> compileConcatenate(params, ctx);
            case "select" -> compileSelect(params, ctx);

            // ===== PHASE 4: Complex Logic =====
            case "join" -> compileJoin(params, ctx);
            case "asOfJoin" -> compileAsOfJoin(params, ctx);
            case "extend" -> compileExtend(params, ctx);
            case "flatten" -> compileFlatten(params, ctx);
            case "pivot" -> compilePivot(params, ctx);
            case "from" -> compileFrom(params, ctx);

            default -> throw new CompileException(
                    "Unknown relational function: '" + name + "'. " +
                            "If this is a scalar function, it should not be at the top level.");
        };
    }

    // ========================================================================
    // PHASE 1: Class.all() and project() - THE CORE
    // ========================================================================

    /**
     * Compiles Class.all() to a TableNode.
     * This is the entry point for class-based queries.
     */
    private RelationNode compileAll(List<Expression> params, CompilationContext ctx) {
        requireParams("all", params, 1);

        String className = extractClassName(params.get(0));
        RelationalMapping mapping = mappingRegistry.getByClassName(className);
        Table table = mapping.table();

        String alias = nextAlias();
        TableNode tableNode = new TableNode(table, alias);

        // Note: ctx is NOT updated here - we use mapping lookup during property
        // resolution
        return tableNode;
    }

    /**
     * Extracts class name from various AST representations.
     */
    private String extractClassName(Expression expr) {
        return switch (expr) {
            case Variable v -> v.name();
            case Function f when "class".equals(f.function()) -> {
                if (f.parameters().isEmpty()) {
                    throw new CompileException("class() function requires a class name argument");
                }
                yield extractString(f.parameters().get(0));
            }
            case Literal lit when lit.type() == org.finos.legend.pure.dsl.legend.Literal.Type.STRING ->
                (String) lit.value();
            default -> throw new CompileException(
                    "Cannot extract class name from: " + describeExpression(expr));
        };
    }

    /**
     * Compiles project() - THE TRANSITION POINT from Class to Relation.
     * After this, all subsequent operations use column names directly.
     */
    private RelationNode compileProject(List<Expression> params, CompilationContext ctx) {
        requireParams("project", params, 2);

        Expression source = params.get(0);
        SourceKind sourceKind = classifySource(source);

        return switch (sourceKind) {
            case CLASS -> compileClassProject(params, ctx);
            case TDS, RELATION -> compileRelationProject(params, ctx);
        };
    }

    /**
     * Compiles CLASS project: Person.all()->project({p | $p.firstName}, {p |
     * $p.lastName})
     * Uses mapping to resolve property names to column names.
     */
    private RelationNode compileClassProject(List<Expression> params, CompilationContext ctx) {
        Expression source = params.get(0);
        RelationNode sourceNode = compileToRelation(source, ctx);

        // Extract mapping from the source chain
        RelationalMapping mapping = extractMappingFromSource(source);
        String tableAlias = getTableAlias(sourceNode);

        // Compile projection lambdas
        List<Projection> projections = new ArrayList<>();

        // Check for LEGACY 2-array syntax: project([lambdas], ['aliases'])
        // params[0] = source, params[1] = [lambdas], params[2] = ['aliases']
        if (params.size() == 3
                && params.get(1) instanceof Collection lambdaColl
                && params.get(2) instanceof Collection aliasColl
                && !aliasColl.values().isEmpty()
                && aliasColl.values().get(0) instanceof Literal) {

            // Extract lambdas from first collection
            List<Lambda> lambdas = new ArrayList<>();
            for (Expression e : lambdaColl.values()) {
                if (e instanceof Lambda l) {
                    lambdas.add(l);
                } else {
                    throw new CompileException(
                            "project() lambda array must contain lambdas, got: " + describeExpression(e));
                }
            }

            // Extract aliases from second collection
            List<String> aliases = new ArrayList<>();
            for (Expression e : aliasColl.values()) {
                if (e instanceof Literal lit && lit.type() == Literal.Type.STRING) {
                    aliases.add((String) lit.value());
                } else {
                    throw new CompileException(
                            "project() alias array must contain strings, got: " + describeExpression(e));
                }
            }

            if (lambdas.size() != aliases.size()) {
                throw new CompileException(
                        "project() lambda count (" + lambdas.size() + ") must match alias count (" + aliases.size()
                                + ")");
            }

            // Compile each lambda with its corresponding alias
            for (int i = 0; i < lambdas.size(); i++) {
                String alias = aliases.get(i);
                projections.add(compileClassProjectionLambda(lambdas.get(i), mapping, tableAlias, alias, ctx));
            }
        } else {
            // Standard syntax: project(~[col1, col2]) or project({lambda1}, {lambda2})
            for (int i = 1; i < params.size(); i++) {
                Expression param = params.get(i);
                projections.addAll(compileClassProjectionParam(param, mapping, tableAlias, ctx));
            }
        }

        if (projections.isEmpty()) {
            throw new CompileException("project() requires at least one projection");
        }

        return new ProjectNode(sourceNode, projections);
    }

    /**
     * Compiles a single projection parameter for CLASS source.
     * Handles both lambda style {p | $p.name} and array style [lambdas], [aliases].
     */
    private List<Projection> compileClassProjectionParam(
            Expression param,
            RelationalMapping mapping,
            String tableAlias,
            CompilationContext ctx) {

        List<Projection> result = new ArrayList<>();

        if (param instanceof Lambda lambda) {
            // Single lambda: {p | $p.firstName}
            result.add(compileClassProjectionLambda(lambda, mapping, tableAlias, ctx));
        } else if (param instanceof Collection coll) {
            // Array of lambdas: [{p | $p.firstName}, {p | $p.lastName}]
            for (Expression e : coll.values()) {
                if (e instanceof Lambda l) {
                    result.add(compileClassProjectionLambda(l, mapping, tableAlias, ctx));
                } else {
                    throw new CompileException(
                            "project() array elements must be lambdas, got: " + describeExpression(e));
                }
            }
        } else {
            throw new CompileException(
                    "project() parameter must be a lambda or array of lambdas, got: " +
                            describeExpression(param));
        }

        return result;
    }

    private Projection compileClassProjectionLambda(
            Lambda lambda,
            RelationalMapping mapping,
            String tableAlias,
            CompilationContext ctx) {
        // Delegate - use property name as alias
        return compileClassProjectionLambda(lambda, mapping, tableAlias, null, ctx);
    }

    /**
     * Compiles a single projection lambda for CLASS source with explicit alias.
     * {p | $p.firstName} -> Projection(ColumnReference("t0", "FIRST_NAME"), alias)
     */
    private Projection compileClassProjectionLambda(
            Lambda lambda,
            RelationalMapping mapping,
            String tableAlias,
            String explicitAlias,
            CompilationContext ctx) {

        Expression body = lambda.body();

        // Extract property name from lambda body
        String propertyName = extractPropertyFromBody(body);

        // Look up column name via mapping
        String columnName = mapping.getColumnForProperty(propertyName)
                .orElseThrow(() -> new CompileException(
                        "Property '" + propertyName + "' not found in mapping for class " +
                                mapping.pureClass().name() + ". " +
                                "Available properties: " + mapping.propertyToColumnMap().keySet()));

        // Alias is explicit (from legacy syntax) or defaults to property name
        String alias = (explicitAlias != null) ? explicitAlias : propertyName;

        return new Projection(
                ColumnReference.of(tableAlias, columnName),
                alias);
    }

    /**
     * Extracts property name from a lambda body expression.
     */
    private String extractPropertyFromBody(Expression body) {
        return switch (body) {
            case Property p -> p.property();
            case Function f -> {
                // Handle chained property access or functions on properties
                // For now, just extract if the first param is a Variable
                if (!f.parameters().isEmpty() && f.parameters().get(0) instanceof Variable) {
                    throw new CompileException(
                            "Function calls in projection require explicit handling: " + f.function());
                }
                throw new CompileException(
                        "Cannot extract property from function: " + f.function());
            }
            default -> throw new CompileException(
                    "Lambda body must be a property access, got: " + describeExpression(body));
        };
    }

    /**
     * Compiles RELATION project: relation->project(...) using column names
     * directly.
     */
    private RelationNode compileRelationProject(List<Expression> params, CompilationContext ctx) {
        // For relation sources, columns are already named - just pass through
        RelationNode sourceNode = compileToRelation(params.get(0), ctx);
        String tableAlias = getTableAlias(sourceNode);

        List<Projection> projections = new ArrayList<>();

        for (int i = 1; i < params.size(); i++) {
            Expression param = params.get(i);
            projections.addAll(compileRelationProjectionParam(param, tableAlias, ctx));
        }

        if (projections.isEmpty()) {
            throw new CompileException("project() requires at least one projection");
        }

        return new ProjectNode(sourceNode, projections);
    }

    /**
     * Compiles a projection parameter for RELATION source.
     */
    private List<Projection> compileRelationProjectionParam(
            Expression param,
            String tableAlias,
            CompilationContext ctx) {

        List<Projection> result = new ArrayList<>();

        if (param instanceof Lambda lambda) {
            // Lambda referencing column: {r | $r.columnName}
            String columnName = extractPropertyFromBody(lambda.body());
            result.add(new Projection(ColumnReference.of(tableAlias, columnName), columnName));
        } else if (param instanceof Collection coll) {
            for (Expression e : coll.values()) {
                if (e instanceof Lambda l) {
                    String columnName = extractPropertyFromBody(l.body());
                    result.add(new Projection(ColumnReference.of(tableAlias, columnName), columnName));
                } else if (e instanceof Literal lit
                        && lit.type() == org.finos.legend.pure.dsl.legend.Literal.Type.STRING) {
                    String columnName = (String) lit.value();
                    result.add(new Projection(ColumnReference.of(tableAlias, columnName), columnName));
                } else {
                    throw new CompileException(
                            "project() array elements must be lambdas or column names, got: " +
                                    describeExpression(e));
                }
            }
        } else {
            throw new CompileException(
                    "project() parameter must be a lambda or array, got: " + describeExpression(param));
        }

        return result;
    }

    // ========================================================================
    // PHASE 2: Filter compilation (dual-path)
    // ========================================================================

    /**
     * Compiles filter() - dispatches to CLASS or RELATION path based on source
     * kind.
     */
    private RelationNode compileFilter(List<Expression> params, CompilationContext ctx) {
        requireParams("filter", params, 2);

        Expression source = params.get(0);
        SourceKind sourceKind = classifySource(source);

        return switch (sourceKind) {
            case CLASS -> compileClassFilter(params, ctx);
            case TDS, RELATION -> compileRelationFilter(params, ctx);
        };
    }

    /**
     * Compiles CLASS filter: Person.all()->filter({p | $p.lastName == 'Smith'})
     * Uses mapping for property→column resolution.
     */
    private RelationNode compileClassFilter(List<Expression> params, CompilationContext ctx) {
        Expression source = params.get(0);
        Lambda filterLambda = requireLambda(params.get(1), "filter");

        RelationNode sourceNode = compileToRelation(source, ctx);
        RelationalMapping mapping = extractMappingFromSource(source);
        String tableAlias = getTableAlias(sourceNode);

        // Bind lambda parameter to table alias for property resolution
        String lambdaParam = filterLambda.parameters().isEmpty() ? "x" : filterLambda.parameters().get(0);
        CompilationContext filterCtx = ctx
                .withRowBinding(lambdaParam, tableAlias)
                .withMapping(mapping);

        // Compile predicate using mapping-aware scalar compilation
        org.finos.legend.engine.plan.Expression predicate = compileScalar(filterLambda.body(), filterCtx);

        return new FilterNode(sourceNode, predicate);
    }

    /**
     * Compiles RELATION filter: relation->filter({r | $r.column == 'value'})
     * Uses column names directly, no mapping.
     */
    private RelationNode compileRelationFilter(List<Expression> params, CompilationContext ctx) {
        Expression source = params.get(0);
        Lambda filterLambda = requireLambda(params.get(1), "filter");

        RelationNode sourceNode = compileToRelation(source, ctx);
        String tableAlias = getTableAlias(sourceNode);

        // Bind lambda parameter to table alias for column resolution
        String lambdaParam = filterLambda.parameters().isEmpty() ? "x" : filterLambda.parameters().get(0);
        CompilationContext filterCtx = ctx.withRowBinding(lambdaParam, tableAlias);

        // Compile predicate using column-aware scalar compilation (no mapping)
        org.finos.legend.engine.plan.Expression predicate = compileScalar(filterLambda.body(), filterCtx);

        return new FilterNode(sourceNode, predicate);
    }

    // ========================================================================
    // SCALAR COMPILATION - For predicates and expressions
    // ========================================================================

    /**
     * Compiles a scalar expression (predicate, value, etc.) to an IR Expression.
     * This handles property access, literals, and function calls.
     */
    private org.finos.legend.engine.plan.Expression compileScalar(Expression expr, CompilationContext ctx) {
        return switch (expr) {
            case Literal lit -> compileLiteral(lit);
            case Variable v -> compileVariable(v, ctx);
            case Property p -> compileProperty(p, ctx);
            case Function f -> compileScalarFunction(f, ctx);
            case Lambda l -> throw new CompileException(
                    "Lambda cannot be used as a scalar value. " +
                            "Lambdas should be used with relational operations.");
            case Collection c -> throw new CompileException(
                    "Collection cannot be used as a scalar value.");
            case TdsLiteral t -> throw new CompileException(
                    "TDS literal cannot be used as a scalar value.");
            case RelationLiteral r -> throw new CompileException(
                    "Relation literal cannot be used as a scalar value.");
        };
    }

    /**
     * Compiles a Pure literal to an IR Literal.
     */
    private org.finos.legend.engine.plan.Expression compileLiteral(Literal lit) {
        return switch (lit.type()) {
            case STRING -> org.finos.legend.engine.plan.Literal.string((String) lit.value());
            case INTEGER -> org.finos.legend.engine.plan.Literal.integer(((Number) lit.value()).longValue());
            case DECIMAL -> new org.finos.legend.engine.plan.Literal(
                    ((Number) lit.value()).doubleValue(),
                    org.finos.legend.engine.plan.Literal.LiteralType.DOUBLE);
            case BOOLEAN -> org.finos.legend.engine.plan.Literal.bool((Boolean) lit.value());
            case NULL -> org.finos.legend.engine.plan.Literal.nullValue();
            case STRICT_DATE -> org.finos.legend.engine.plan.Literal.date((String) lit.value());
            case DATETIME -> org.finos.legend.engine.plan.Literal.date((String) lit.value());
            case STRICT_TIME -> org.finos.legend.engine.plan.Literal.time((String) lit.value());
        };
    }

    /**
     * Compiles a variable reference.
     */
    private org.finos.legend.engine.plan.Expression compileVariable(Variable v, CompilationContext ctx) {
        String name = v.name();

        // Check if it's a bound scalar
        if (ctx.hasScalarBinding(name)) {
            return ctx.getScalarBinding(name);
        }

        // Check if it's a row binding (lambda param) - this would need context to
        // resolve
        if (ctx.hasRowBinding(name)) {
            // Variable alone without property access is unusual
            throw new CompileException(
                    "Variable $" + name + " is a row reference and requires property access (e.g., $" +
                            name + ".propertyName)");
        }

        throw new CompileException(
                "Unknown variable: $" + name + ". " +
                        "Available bindings: " + ctx.describeBindings());
    }

    /**
     * Compiles property access. Uses mapping if in CLASS context, column names if
     * in RELATION context.
     */
    private org.finos.legend.engine.plan.Expression compileProperty(Property p, CompilationContext ctx) {
        String propertyName = p.property();

        // Get the source variable from Property.source() - it's an Expression
        // (typically a Variable)
        Expression sourceExpr = p.source();
        String sourceVar = null;
        if (sourceExpr instanceof Variable v) {
            sourceVar = v.name();
        }

        if (sourceVar == null || !ctx.hasRowBinding(sourceVar)) {
            throw new CompileException(
                    "Property access '" + propertyName + "' requires a bound source variable. " +
                            "Source: " + describeExpression(sourceExpr));
        }

        String tableAlias = ctx.getRowBinding(sourceVar);

        // If we have a mapping, use it to resolve property → column
        if (ctx.hasMapping()) {
            RelationalMapping mapping = ctx.getMapping();
            String columnName = mapping.getColumnForProperty(propertyName)
                    .orElseThrow(() -> new CompileException(
                            "Property '" + propertyName + "' not found in mapping. " +
                                    "Available: " + mapping.propertyToColumnMap().keySet()));
            return ColumnReference.of(tableAlias, columnName);
        }

        // No mapping - use property name as column name (RELATION path)
        return ColumnReference.of(tableAlias, propertyName);
    }

    /**
     * Compiles a scalar function call.
     */
    private org.finos.legend.engine.plan.Expression compileScalarFunction(Function f, CompilationContext ctx) {
        String name = f.function();
        List<Expression> params = f.parameters();

        return switch (name) {
            // ===== Comparison operators =====
            case "equal", "equals" -> compileComparison(params, ComparisonExpression.ComparisonOperator.EQUALS, ctx);
            case "notEqual", "notEquals" ->
                compileComparison(params, ComparisonExpression.ComparisonOperator.NOT_EQUALS, ctx);
            case "lessThan" -> compileComparison(params, ComparisonExpression.ComparisonOperator.LESS_THAN, ctx);
            case "lessThanEqual" ->
                compileComparison(params, ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS, ctx);
            case "greaterThan" -> compileComparison(params, ComparisonExpression.ComparisonOperator.GREATER_THAN, ctx);
            case "greaterThanEqual" ->
                compileComparison(params, ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, ctx);

            // ===== Logical operators =====
            case "and" -> compileLogical(params, LogicalExpression.LogicalOperator.AND, ctx);
            case "or" -> compileLogical(params, LogicalExpression.LogicalOperator.OR, ctx);
            case "not" -> {
                requireParams("not", params, 1);
                yield new LogicalExpression(
                        LogicalExpression.LogicalOperator.NOT,
                        List.of(compileScalar(params.get(0), ctx)));
            }

            // ===== Null checks =====
            case "isNull", "isEmpty" -> {
                requireParams(name, params, 1);
                yield new ComparisonExpression(
                        compileScalar(params.get(0), ctx),
                        ComparisonExpression.ComparisonOperator.IS_NULL,
                        null);
            }
            case "isNotNull", "isNotEmpty" -> {
                requireParams(name, params, 1);
                yield new ComparisonExpression(
                        compileScalar(params.get(0), ctx),
                        ComparisonExpression.ComparisonOperator.IS_NOT_NULL,
                        null);
            }

            // ===== Arithmetic =====
            case "plus" -> compileArithmetic(params, BinaryArithmeticExpr.Operator.ADD, ctx);
            case "minus" -> compileArithmetic(params, BinaryArithmeticExpr.Operator.SUBTRACT, ctx);
            case "times" -> compileArithmetic(params, BinaryArithmeticExpr.Operator.MULTIPLY, ctx);
            case "divide" -> compileArithmetic(params, BinaryArithmeticExpr.Operator.DIVIDE, ctx);

            default -> throw new CompileException(
                    "Unknown scalar function: '" + name + "'. " +
                            "Known functions: equal, notEqual, lessThan, greaterThan, and, or, not, " +
                            "plus, minus, times, divide, isNull, isNotNull");
        };
    }

    /**
     * Compiles a comparison expression.
     */
    private org.finos.legend.engine.plan.Expression compileComparison(
            List<Expression> params,
            ComparisonExpression.ComparisonOperator op,
            CompilationContext ctx) {
        requireParams("comparison", params, 2);
        return new ComparisonExpression(
                compileScalar(params.get(0), ctx),
                op,
                compileScalar(params.get(1), ctx));
    }

    /**
     * Compiles a logical expression.
     */
    private org.finos.legend.engine.plan.Expression compileLogical(
            List<Expression> params,
            LogicalExpression.LogicalOperator op,
            CompilationContext ctx) {
        List<org.finos.legend.engine.plan.Expression> operands = params.stream()
                .map(p -> compileScalar(p, ctx))
                .toList();
        return new LogicalExpression(op, operands);
    }

    /**
     * Compiles an arithmetic expression.
     */
    private org.finos.legend.engine.plan.Expression compileArithmetic(
            List<Expression> params,
            BinaryArithmeticExpr.Operator op,
            CompilationContext ctx) {
        requireParams("arithmetic", params, 2);
        return new ArithmeticExpression(
                compileScalar(params.get(0), ctx),
                op,
                compileScalar(params.get(1), ctx));
    }

    // ========================================================================
    // PHASE 3: Core Relational Operators (STUBS - To be implemented)
    // ========================================================================

    private RelationNode compileGroupBy(List<Expression> params, CompilationContext ctx) {
        // groupBy() only works on Relations (after project())
        // Legacy syntax: source->groupBy([groupByLambdas], [aggLambdas], [aliases]) - 4
        // params
        // Relation API: source->groupBy(~[cols], ~[agg:...]) - 2-3 params

        if (params.isEmpty()) {
            throw new CompileException("groupBy() requires at least source and grouping columns");
        }

        Expression sourceExpr = params.get(0);
        RelationNode source = compileToRelation(sourceExpr, ctx);

        if (params.size() == 4) {
            // Legacy 3-arg syntax (4 params including source):
            // ->groupBy([{r | $r.dept}], [{r | $r.sal}], ['totalSal'])
            return compileGroupByLegacy(source, params.get(1), params.get(2), params.get(3), ctx);
        } else if (params.size() == 2 || params.size() == 3) {
            // Relation API: groupBy(~[cols], ~[agg:...])
            return compileGroupByRelationApi(source, params.subList(1, params.size()), ctx);
        }

        throw new CompileException("groupBy() requires 2-4 parameters, got " + params.size());
    }

    /**
     * Compiles legacy groupBy syntax:
     * ->groupBy([{r | $r.dept}], [{r | $r.sal}], ['totalSal'])
     */
    private RelationNode compileGroupByLegacy(
            RelationNode source,
            Expression groupColLambdas,
            Expression aggLambdas,
            Expression aliasesExpr,
            CompilationContext ctx) {

        // Extract grouping column names from lambdas like [{r | $r.dept}]
        List<String> groupingColumns = new ArrayList<>();
        for (Expression col : extractListFromExpr(groupColLambdas)) {
            Lambda lambda = requireLambda(col, "groupBy column");
            groupingColumns.add(extractColumnFromLambda(lambda));
        }

        // Extract aliases
        List<String> aliases = new ArrayList<>();
        for (Expression a : extractListFromExpr(aliasesExpr)) {
            aliases.add(extractString(a));
        }

        // Extract aggregations from lambdas like [{r | $r.sal}] or [{r |
        // $r.sal->stdDev()}] or [{r | $r.col1->corr($r.col2)}]
        List<GroupByNode.AggregateProjection> aggregations = new ArrayList<>();
        List<Expression> aggExprs = extractListFromExpr(aggLambdas);

        for (int i = 0; i < aggExprs.size(); i++) {
            Lambda lambda = requireLambda(aggExprs.get(i), "aggregation");

            // Get alias - offset by grouping columns count
            int aliasIndex = groupingColumns.size() + i;
            String alias = (aliasIndex < aliases.size()) ? aliases.get(aliasIndex) : "agg" + i;

            // Parse the lambda body to determine column and aggregate function
            Expression body = lambda.body();
            String columnName;
            String secondColumnName = null;
            Double percentileValue = null;
            String separator = null;
            AggregateExpression.AggregateFunction aggFunc;

            if (body instanceof Function f) {
                // Pattern: $r.sal->stdDev() or $r.col1->corr($r.col2)
                if (f.parameters().isEmpty()) {
                    throw new CompileException("Aggregate function '" + f.function() + "' requires a source column");
                }

                columnName = extractPropertyOrColumn(f.parameters().get(0));
                aggFunc = mapAggregateFunctionName(f.function());

                // Handle bivariate functions (CORR, COVAR) - require second column
                if (aggFunc.isBivariate()) {
                    if (f.parameters().size() < 2) {
                        throw new CompileException(
                                aggFunc.name() + " requires a second column argument, e.g., $r.col1->corr($r.col2)");
                    }
                    Expression arg = f.parameters().get(1);
                    if (arg instanceof Property p) {
                        secondColumnName = p.property();
                    } else {
                        throw new CompileException(
                                aggFunc.name() + " second argument must be a column reference (e.g., $r.col2), got: "
                                        + describeExpression(arg));
                    }
                }

                // Handle percentile functions - require numeric percentile value
                if (isPercentileFunction(aggFunc)) {
                    if (f.parameters().size() < 2) {
                        throw new CompileException(aggFunc.name()
                                + " requires a percentile value argument, e.g., $r.col->percentileCont(0.5)");
                    }
                    Expression arg = f.parameters().get(1);
                    if (arg instanceof Literal lit && lit.value() instanceof Number n) {
                        percentileValue = n.doubleValue();
                        if (percentileValue < 0.0 || percentileValue > 1.0) {
                            throw new CompileException(aggFunc.name()
                                    + " percentile value must be between 0.0 and 1.0, got: " + percentileValue);
                        }
                    } else {
                        throw new CompileException(aggFunc.name()
                                + " requires a numeric literal argument (0.0 to 1.0), got: " + describeExpression(arg));
                    }
                }

                // Handle string aggregation - require string separator
                if (aggFunc == AggregateExpression.AggregateFunction.STRING_AGG) {
                    if (f.parameters().size() < 2) {
                        throw new CompileException(
                                "joinStrings requires a separator argument, e.g., $r.col->joinStrings(',')");
                    }
                    Expression arg = f.parameters().get(1);
                    if (arg instanceof Literal lit && lit.type() == Literal.Type.STRING) {
                        separator = (String) lit.value();
                    } else {
                        throw new CompileException(
                                "joinStrings separator must be a string literal, got: " + describeExpression(arg));
                    }
                }
            } else if (body instanceof Property p) {
                // Pattern: $r.sal - defaults to SUM
                columnName = p.property();
                aggFunc = AggregateExpression.AggregateFunction.SUM;
            } else {
                throw new CompileException("Invalid aggregation body: " + describeExpression(body));
            }

            // Use appropriate constructor based on agg type
            GroupByNode.AggregateProjection aggProj;
            if (secondColumnName != null) {
                // Bivariate
                aggProj = new GroupByNode.AggregateProjection(alias, columnName, secondColumnName, aggFunc);
            } else if (percentileValue != null) {
                // Percentile
                aggProj = new GroupByNode.AggregateProjection(alias, columnName, null, aggFunc, percentileValue);
            } else if (separator != null) {
                // String aggregation
                aggProj = new GroupByNode.AggregateProjection(alias, columnName, null, aggFunc, null, separator);
            } else {
                // Simple aggregate
                aggProj = new GroupByNode.AggregateProjection(alias, columnName, aggFunc);
            }
            aggregations.add(aggProj);
        }

        return new GroupByNode(source, groupingColumns, aggregations);
    }

    /**
     * Compiles Relation API groupBy syntax:
     * ->groupBy(~[str], ~newCol:x|$x.val:y|$y->plus())
     */
    private RelationNode compileGroupByRelationApi(
            RelationNode source,
            List<Expression> args,
            CompilationContext ctx) {

        List<String> groupingColumns = new ArrayList<>();
        List<GroupByNode.AggregateProjection> aggregations = new ArrayList<>();

        // First arg: grouping columns (column spec or array)
        if (!args.isEmpty()) {
            Expression groupArg = args.get(0);
            if (groupArg instanceof Function f && "column".equals(f.function())) {
                // Single column: ~col
                groupingColumns.add(extractString(f.parameters().get(0)));
            } else if (groupArg instanceof Collection coll) {
                // Multiple columns: ~[col1, col2]
                for (Expression col : coll.values()) {
                    if (col instanceof Function cf && "column".equals(cf.function())) {
                        groupingColumns.add(extractString(cf.parameters().get(0)));
                    }
                }
            }
        }

        // Second+ args: aggregation specs
        for (int i = 1; i < args.size(); i++) {
            Expression aggArg = args.get(i);
            if (aggArg instanceof Function f && "column".equals(f.function())) {
                // Single agg: ~alias:x|...:y|...
                aggregations.add(compileAggregationSpec(f, ctx));
            } else if (aggArg instanceof Collection coll) {
                // Multiple aggs: ~[...]
                for (Expression agg : coll.values()) {
                    if (agg instanceof Function af && "column".equals(af.function())) {
                        aggregations.add(compileAggregationSpec(af, ctx));
                    }
                }
            }
        }

        return new GroupByNode(source, groupingColumns, aggregations);
    }

    /**
     * Compiles an aggregation spec from ~alias:x|$x.col:y|$y->aggFunc()
     * or the single-lambda variant: ~alias:x|$x.col->aggFunc()
     */
    private GroupByNode.AggregateProjection compileAggregationSpec(Function f, CompilationContext ctx) {
        List<Expression> parts = f.parameters();
        if (parts.isEmpty()) {
            throw new CompileException("Aggregation spec requires at least an alias");
        }

        String alias = extractString(parts.get(0));
        String columnName = alias; // Default: column name = alias
        String secondColumn = null;
        Double percentileValue = null;
        String separator = null;
        AggregateExpression.AggregateFunction aggFunc = AggregateExpression.AggregateFunction.SUM;

        // If there's a map lambda, check if body is Property or Function (aggregate)
        if (parts.size() > 1 && parts.get(1) instanceof Lambda mapLambda) {
            Expression body = mapLambda.body();

            if (body instanceof Property p) {
                // Simple case: {x | $x.col} - just extracting column
                columnName = p.property();
            } else if (body instanceof Function aggCall) {
                // Single-lambda pattern: {x | $x.col->joinStrings(',')}
                // The body IS the aggregate function call
                aggFunc = mapAggregateFunctionName(aggCall.function());

                // First param to the function is the column being aggregated
                if (!aggCall.parameters().isEmpty()) {
                    columnName = extractPropertyOrColumn(aggCall.parameters().get(0));
                }

                // Handle bivariate functions
                if (aggFunc.isBivariate()) {
                    if (aggCall.parameters().size() < 2) {
                        throw new CompileException(aggFunc.name() + " requires a second column argument");
                    }
                    Expression arg = aggCall.parameters().get(1);
                    if (arg instanceof Property p) {
                        secondColumn = p.property();
                    } else {
                        throw new CompileException(aggFunc.name() + " second argument must be a column reference, got: "
                                + describeExpression(arg));
                    }
                }

                // Handle percentile functions
                if (isPercentileFunction(aggFunc)) {
                    if (aggCall.parameters().size() < 2) {
                        throw new CompileException(aggFunc.name() + " requires a percentile value argument");
                    }
                    Expression arg = aggCall.parameters().get(1);
                    if (arg instanceof Literal lit && lit.value() instanceof Number n) {
                        percentileValue = n.doubleValue();
                    } else {
                        throw new CompileException(aggFunc.name() + " requires a numeric literal argument, got: "
                                + describeExpression(arg));
                    }
                }

                // Handle string aggregation
                if (aggFunc == AggregateExpression.AggregateFunction.STRING_AGG) {
                    if (aggCall.parameters().size() < 2) {
                        throw new CompileException("joinStrings requires a separator argument");
                    }
                    Expression arg = aggCall.parameters().get(1);
                    if (arg instanceof Literal lit && lit.type() == Literal.Type.STRING) {
                        separator = (String) lit.value();
                    } else {
                        throw new CompileException(
                                "joinStrings separator must be a string literal, got: " + describeExpression(arg));
                    }
                }
            }
        }

        // If there's a separate agg lambda (two-lambda pattern), extract function from
        // it
        if (parts.size() > 2 && parts.get(2) instanceof Lambda aggLambda) {
            Expression body = aggLambda.body();
            if (body instanceof Function af) {
                aggFunc = mapAggregateFunctionName(af.function());
                // TODO: Also extract bivariate/percentile/separator from two-lambda pattern if
                // needed
            }
        }

        // Return appropriate projection
        if (secondColumn != null) {
            return new GroupByNode.AggregateProjection(alias, columnName, secondColumn, aggFunc);
        } else if (percentileValue != null) {
            return new GroupByNode.AggregateProjection(alias, columnName, null, aggFunc, percentileValue);
        } else if (separator != null) {
            return new GroupByNode.AggregateProjection(alias, columnName, null, aggFunc, null, separator);
        } else {
            return new GroupByNode.AggregateProjection(alias, columnName, aggFunc);
        }
    }

    /**
     * Extracts a column name from a lambda like {r | $r.col}
     */
    private String extractColumnFromLambda(Lambda lambda) {
        Expression body = lambda.body();
        if (body instanceof Property p) {
            return p.property();
        }
        if (body instanceof Function f) {
            // If body is a function, first param might be the column
            if ("column".equals(f.function())) {
                return extractString(f.parameters().get(0));
            }
            // For aggregate functions, first param is the column
            if (!f.parameters().isEmpty()) {
                return extractPropertyOrColumn(f.parameters().get(0));
            }
        }
        throw new CompileException("Cannot extract column from lambda body: " + describeExpression(body));
    }

    /**
     * Extracts property name or column name from expression.
     */
    private String extractPropertyOrColumn(Expression expr) {
        if (expr instanceof Property p) {
            return p.property();
        }
        if (expr instanceof Variable v) {
            return v.name();
        }
        if (expr instanceof Function f && "column".equals(f.function())) {
            return extractString(f.parameters().get(0));
        }
        throw new CompileException("Cannot extract column from: " + describeExpression(expr));
    }

    /**
     * Extracts a list of expressions from a Collection or single expression.
     */
    private List<Expression> extractListFromExpr(Expression expr) {
        if (expr instanceof Collection coll) {
            return coll.values();
        }
        return List.of(expr);
    }

    /**
     * Maps Pure aggregate function name to IR AggregateFunction.
     */
    private AggregateExpression.AggregateFunction mapAggregateFunctionName(String name) {
        return switch (name) {
            case "sum", "plus" -> AggregateExpression.AggregateFunction.SUM;
            case "count" -> AggregateExpression.AggregateFunction.COUNT;
            case "avg", "average" -> AggregateExpression.AggregateFunction.AVG;
            case "min" -> AggregateExpression.AggregateFunction.MIN;
            case "max" -> AggregateExpression.AggregateFunction.MAX;
            case "stdDev", "stdDevSample" -> AggregateExpression.AggregateFunction.STDDEV_SAMP;
            case "stdDevPopulation" -> AggregateExpression.AggregateFunction.STDDEV_POP;
            case "variance", "varianceSample" -> AggregateExpression.AggregateFunction.VAR_SAMP;
            case "variancePopulation" -> AggregateExpression.AggregateFunction.VAR_POP;
            case "median" -> AggregateExpression.AggregateFunction.MEDIAN;
            case "corr" -> AggregateExpression.AggregateFunction.CORR;
            case "covarSample" -> AggregateExpression.AggregateFunction.COVAR_SAMP;
            case "covarPopulation" -> AggregateExpression.AggregateFunction.COVAR_POP;
            case "percentileCont" -> AggregateExpression.AggregateFunction.PERCENTILE_CONT;
            case "percentileDisc" -> AggregateExpression.AggregateFunction.PERCENTILE_DISC;
            case "joinStrings" -> AggregateExpression.AggregateFunction.STRING_AGG;
            default -> throw new CompileException("Unknown aggregate function: '" + name + "'");
        };
    }

    /**
     * Returns true if the aggregate function is a percentile function.
     */
    private boolean isPercentileFunction(AggregateExpression.AggregateFunction func) {
        return func == AggregateExpression.AggregateFunction.PERCENTILE_CONT
                || func == AggregateExpression.AggregateFunction.PERCENTILE_DISC;
    }

    private RelationNode compileAggregate(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("aggregate() not yet implemented in clean compiler - Phase 3");
    }

    /**
     * Compiles sort(~col->ascending()) or sort(~[col1->ascending(),
     * col2->descending()])
     * Also handles legacy: sort('col', 'asc') or sortBy({e | $e.col}, 'desc')
     */
    private RelationNode compileSort(List<Expression> params, CompilationContext ctx) {
        if (params.isEmpty()) {
            throw new CompileException("sort() requires a source and column specification");
        }

        RelationNode source = compileToRelation(params.get(0), ctx);

        if (params.size() < 2) {
            throw new CompileException("sort() requires at least one column specification");
        }

        List<SortNode.SortColumn> sortColumns = new ArrayList<>();
        Expression sortSpec = params.get(1);

        // Check if there's a direction string argument (legacy pattern)
        SortNode.SortDirection defaultDirection = SortNode.SortDirection.ASC;
        if (params.size() > 2) {
            Expression dirArg = params.get(2);
            if (dirArg instanceof Literal lit && lit.type() == Literal.Type.STRING) {
                String dir = ((String) lit.value()).toLowerCase();
                defaultDirection = "desc".equals(dir) || "descending".equals(dir)
                        ? SortNode.SortDirection.DESC
                        : SortNode.SortDirection.ASC;
            }
        }

        // Handle single column or array of columns
        List<Expression> columns = (sortSpec instanceof Collection coll) ? coll.values() : List.of(sortSpec);

        for (Expression colExpr : columns) {
            SortNode.SortColumn sortCol = extractSortColumnWithDirection(colExpr, defaultDirection);
            sortColumns.add(sortCol);
        }

        if (sortColumns.isEmpty()) {
            throw new CompileException("sort() requires at least one sort column");
        }

        return new SortNode(source, sortColumns);
    }

    /**
     * Extracts a SortColumn from an expression.
     * Supports patterns:
     * - ~col->ascending() / ~col->descending() (Relation API)
     * - ~col (Relation API, defaults to ASC)
     * - 'col' (legacy string column name)
     * - {e | $e.col} (legacy lambda)
     */
    private SortNode.SortColumn extractSortColumn(Expression expr) {
        return extractSortColumnWithDirection(expr, SortNode.SortDirection.ASC);
    }

    /**
     * Extracts a SortColumn with an optional pending direction override.
     */
    private SortNode.SortColumn extractSortColumnWithDirection(Expression expr,
            SortNode.SortDirection pendingDirection) {
        String columnName;
        SortNode.SortDirection direction = pendingDirection;

        if (expr instanceof Function f) {
            if ("ascending".equals(f.function())) {
                // ~col->ascending()
                if (!f.parameters().isEmpty()) {
                    columnName = extractColumnSpecName(f.parameters().get(0));
                } else {
                    throw new CompileException("ascending() requires a column");
                }
                direction = SortNode.SortDirection.ASC;
            } else if ("descending".equals(f.function())) {
                // ~col->descending()
                if (!f.parameters().isEmpty()) {
                    columnName = extractColumnSpecName(f.parameters().get(0));
                } else {
                    throw new CompileException("descending() requires a column");
                }
                direction = SortNode.SortDirection.DESC;
            } else if ("column".equals(f.function())) {
                // ~col (no direction specified, uses pending)
                columnName = extractString(f.parameters().get(0));
            } else {
                throw new CompileException("Invalid sort column expression: " + f.function());
            }
        } else if (expr instanceof Literal lit && lit.type() == Literal.Type.STRING) {
            // Legacy: 'col' - string column name
            columnName = (String) lit.value();
        } else if (expr instanceof Lambda lambda) {
            // Legacy: {e | $e.col} - lambda extracting property
            columnName = extractPropertyFromLambda(lambda);
        } else {
            throw new CompileException("Invalid sort column: " + describeExpression(expr));
        }

        return new SortNode.SortColumn(columnName, direction);
    }

    /**
     * Extracts property name from a lambda like {e | $e.col}
     */
    private String extractPropertyFromLambda(Lambda lambda) {
        Expression body = lambda.body();
        if (body instanceof Property p) {
            return p.property();
        }
        throw new CompileException("Cannot extract property from lambda body: " + describeExpression(body));
    }

    /**
     * Extracts column name from a column spec like Function("column",
     * [Literal("name")])
     */
    private String extractColumnSpecName(Expression expr) {
        if (expr instanceof Function f && "column".equals(f.function())) {
            return extractString(f.parameters().get(0));
        }
        if (expr instanceof Literal lit && lit.type() == Literal.Type.STRING) {
            return (String) lit.value();
        }
        throw new CompileException("Cannot extract column name from: " + describeExpression(expr));
    }

    /**
     * Compiles limit(n) - LIMIT n
     */
    private RelationNode compileLimit(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("limit() requires source and count");
        }
        RelationNode source = compileToRelation(params.get(0), ctx);
        int limit = extractInteger(params.get(1));
        return LimitNode.limit(source, limit);
    }

    /**
     * Compiles drop(n) - OFFSET n
     */
    private RelationNode compileDrop(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("drop() requires source and count");
        }
        RelationNode source = compileToRelation(params.get(0), ctx);
        int offset = extractInteger(params.get(1));
        return LimitNode.offset(source, offset);
    }

    /**
     * Compiles slice(start, stop) - LIMIT (stop-start) OFFSET start
     */
    private RelationNode compileSlice(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 3) {
            throw new CompileException("slice() requires source, start, and stop");
        }
        RelationNode source = compileToRelation(params.get(0), ctx);
        int start = extractInteger(params.get(1));
        int stop = extractInteger(params.get(2));
        return LimitNode.slice(source, start, stop);
    }

    /**
     * Compiles first() - LIMIT 1
     */
    private RelationNode compileFirst(List<Expression> params, CompilationContext ctx) {
        if (params.isEmpty()) {
            throw new CompileException("first() requires a source");
        }
        RelationNode source = compileToRelation(params.get(0), ctx);
        return LimitNode.limit(source, 1);
    }

    /**
     * Compiles nth(n) - LIMIT 1 OFFSET n
     */
    private RelationNode compileNth(List<Expression> params, CompilationContext ctx) {
        if (params.size() < 2) {
            throw new CompileException("nth() requires source and index");
        }
        RelationNode source = compileToRelation(params.get(0), ctx);
        int n = extractInteger(params.get(1));
        return new LimitNode(source, 1, n);
    }

    /**
     * Compiles distinct() or distinct(~[col1, col2])
     */
    private RelationNode compileDistinct(List<Expression> params, CompilationContext ctx) {
        if (params.isEmpty()) {
            throw new CompileException("distinct() requires a source");
        }
        RelationNode source = compileToRelation(params.get(0), ctx);

        // distinct() with no columns = all columns
        if (params.size() == 1) {
            return DistinctNode.all(source);
        }

        // distinct(~[col1, col2]) = specific columns
        List<String> columns = new ArrayList<>();
        Expression colSpec = params.get(1);
        List<Expression> colExprs = (colSpec instanceof Collection coll) ? coll.values() : List.of(colSpec);

        for (Expression col : colExprs) {
            if (col instanceof Function f && "column".equals(f.function())) {
                columns.add(extractString(f.parameters().get(0)));
            } else {
                throw new CompileException("Invalid column in distinct(): " + describeExpression(col));
            }
        }

        return DistinctNode.columns(source, columns);
    }

    private RelationNode compileRename(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("rename() not yet implemented in clean compiler - Phase 3");
    }

    private RelationNode compileConcatenate(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("concatenate() not yet implemented in clean compiler - Phase 3");
    }

    private RelationNode compileSelect(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("select() not yet implemented in clean compiler - Phase 3");
    }

    // ========================================================================
    // PHASE 4: Complex Logic (STUBS - To be implemented)
    // ========================================================================

    private RelationNode compileJoin(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("join() not yet implemented in clean compiler - Phase 4");
    }

    private RelationNode compileAsOfJoin(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("asOfJoin() not yet implemented in clean compiler - Phase 4");
    }

    private RelationNode compileExtend(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("extend() not yet implemented in clean compiler - Phase 4");
    }

    private RelationNode compileFlatten(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("flatten() not yet implemented in clean compiler - Phase 4");
    }

    private RelationNode compilePivot(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("pivot() not yet implemented in clean compiler - Phase 4");
    }

    private RelationNode compileFrom(List<Expression> params, CompilationContext ctx) {
        throw new CompileException("from() not yet implemented in clean compiler - Phase 4");
    }

    // ========================================================================
    // LITERAL SOURCES
    // ========================================================================

    private RelationNode compileTdsLiteral(TdsLiteral tds) {
        // Convert AST columns to IR columns, preserving types
        List<TdsLiteralNode.TdsColumn> irColumns = tds.columns().stream()
                .map(c -> TdsLiteralNode.TdsColumn.of(c.name(), c.type()))
                .toList();
        return new TdsLiteralNode(irColumns, tds.rows());
    }

    private RelationNode compileRelationLiteral(RelationLiteral rel) {
        String tableName = rel.tableName();

        // First try to find table via modelContext
        if (modelContext != null) {
            Optional<Table> table = modelContext.findTable(tableName);
            if (table.isPresent()) {
                return new TableNode(table.get(), nextAlias());
            }
        }

        // Fallback: try via mappingRegistry if we can find a table by the name
        Optional<RelationalMapping> mapping = mappingRegistry.findByTableName(tableName);
        if (mapping.isPresent()) {
            return new TableNode(mapping.get().table(), nextAlias());
        }

        throw new CompileException(
                "Cannot resolve relation literal table: '" + tableName + "'. " +
                        "Database ref: " + rel.databaseRef());
    }

    private RelationNode compileRelationVariable(Variable v, CompilationContext ctx) {
        String name = v.name();

        if (ctx.hasRelationBinding(name)) {
            return ctx.getRelationBinding(name);
        }

        throw new CompileException(
                "Variable $" + name + " is not bound to a relation. " +
                        "Available relation bindings: " + ctx.describeRelationBindings());
    }

    private RelationNode compileConstantLambda(Lambda lambda, CompilationContext ctx) {
        Expression body = lambda.body();

        // If body is a relation expression, compile as relation
        if (isRelationExpression(body)) {
            return compileToRelation(body, ctx);
        }

        // Otherwise compile as scalar constant
        org.finos.legend.engine.plan.Expression scalarExpr = compileScalar(body, ctx);
        return new ConstantNode(scalarExpr);
    }

    private boolean isRelationExpression(Expression expr) {
        return switch (expr) {
            case Function f -> isRelationFunction(f.function());
            case TdsLiteral tds -> true;
            case RelationLiteral rel -> true;
            case Variable v -> false; // Variables need context to determine
            default -> false;
        };
    }

    private boolean isRelationFunction(String name) {
        return switch (name) {
            case "all", "filter", "project", "groupBy", "extend", "select",
                    "rename", "distinct", "concatenate", "sort", "sortBy",
                    "limit", "take", "drop", "slice", "first", "join",
                    "flatten", "pivot", "from", "aggregate", "nth" ->
                true;
            default -> false;
        };
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    private String nextAlias() {
        return "t" + (aliasCounter++);
    }

    private String getTableAlias(RelationNode node) {
        if (node instanceof TableNode tn) {
            return tn.alias();
        }
        return "t0";
    }

    private RelationalMapping extractMappingFromSource(Expression source) {
        return switch (source) {
            case Function f when "all".equals(f.function()) -> {
                String className = extractClassName(f.parameters().get(0));
                yield mappingRegistry.getByClassName(className);
            }
            case Function f -> {
                if (f.parameters().isEmpty()) {
                    throw new CompileException(
                            "Cannot extract mapping from function with no parameters: " + f.function());
                }
                yield extractMappingFromSource(f.parameters().get(0));
            }
            default -> throw new CompileException(
                    "Cannot extract mapping from: " + describeExpression(source));
        };
    }

    private void requireParams(String funcName, List<Expression> params, int min) {
        if (params.size() < min) {
            throw new CompileException(
                    funcName + "() requires at least " + min + " parameter(s), got " + params.size());
        }
    }

    private void requireParams(String funcName, List<Expression> params, int min, int max) {
        if (params.size() < min || params.size() > max) {
            throw new CompileException(
                    funcName + "() requires " + min + "-" + max + " parameter(s), got " + params.size());
        }
    }

    private Lambda requireLambda(Expression expr, String context) {
        if (expr instanceof Lambda lambda) {
            return lambda;
        }
        throw new CompileException(
                "Expected lambda for " + context + ", got: " + describeExpression(expr));
    }

    private String extractString(Expression expr) {
        if (expr instanceof Literal lit && lit.type() == org.finos.legend.pure.dsl.legend.Literal.Type.STRING) {
            return (String) lit.value();
        }
        throw new CompileException("Expected string literal, got: " + describeExpression(expr));
    }

    private int extractInteger(Expression expr) {
        if (expr instanceof Literal lit) {
            Object value = lit.value();
            if (value instanceof Number n) {
                return n.intValue();
            }
        }
        throw new CompileException("Expected integer literal, got: " + describeExpression(expr));
    }

    private String describeExpression(Expression expr) {
        return expr.getClass().getSimpleName() + "[" + expr + "]";
    }

    // ========================================================================
    // COMPILATION CONTEXT
    // ========================================================================

    /**
     * Immutable compilation context for tracking bindings during compilation.
     */
    public record CompilationContext(
            Map<String, String> rowBindings, // varName -> tableAlias
            Map<String, RelationNode> relationBindings, // varName -> RelationNode
            Map<String, org.finos.legend.engine.plan.Expression> scalarBindings, // varName -> Expression
            Set<String> lambdaParams, // Lambda parameter names
            RelationalMapping mapping, // Current mapping (for class-based sources)
            String className // Current class name
    ) {
        public static CompilationContext empty() {
            return new CompilationContext(
                    Map.of(), Map.of(), Map.of(), Set.of(), null, null);
        }

        public CompilationContext withRowBinding(String param, String alias) {
            var newBindings = new HashMap<>(rowBindings);
            newBindings.put(param, alias);
            return new CompilationContext(newBindings, relationBindings, scalarBindings, lambdaParams, mapping,
                    className);
        }

        public CompilationContext withRelationBinding(String param, RelationNode relation) {
            var newBindings = new HashMap<>(relationBindings);
            newBindings.put(param, relation);
            return new CompilationContext(rowBindings, newBindings, scalarBindings, lambdaParams, mapping, className);
        }

        public CompilationContext withScalarBinding(String param, org.finos.legend.engine.plan.Expression expr) {
            var newBindings = new HashMap<>(scalarBindings);
            newBindings.put(param, expr);
            return new CompilationContext(rowBindings, relationBindings, newBindings, lambdaParams, mapping, className);
        }

        public CompilationContext withLambdaParam(String param) {
            var newParams = new HashSet<>(lambdaParams);
            newParams.add(param);
            return new CompilationContext(rowBindings, relationBindings, scalarBindings, newParams, mapping, className);
        }

        public CompilationContext withMapping(RelationalMapping mapping) {
            return new CompilationContext(rowBindings, relationBindings, scalarBindings, lambdaParams, mapping,
                    className);
        }

        public CompilationContext withClassName(String className) {
            return new CompilationContext(rowBindings, relationBindings, scalarBindings, lambdaParams, mapping,
                    className);
        }

        public boolean hasRowBinding(String name) {
            return rowBindings.containsKey(name);
        }

        public String getRowBinding(String name) {
            return rowBindings.get(name);
        }

        public boolean hasRelationBinding(String name) {
            return relationBindings.containsKey(name);
        }

        public RelationNode getRelationBinding(String name) {
            return relationBindings.get(name);
        }

        public boolean hasScalarBinding(String name) {
            return scalarBindings.containsKey(name);
        }

        public org.finos.legend.engine.plan.Expression getScalarBinding(String name) {
            return scalarBindings.get(name);
        }

        public boolean isLambdaParam(String name) {
            return lambdaParams.contains(name);
        }

        public boolean hasMapping() {
            return mapping != null;
        }

        public RelationalMapping getMapping() {
            return mapping;
        }

        public String describeBindings() {
            List<String> parts = new ArrayList<>();
            if (!rowBindings.isEmpty())
                parts.add("rows=" + rowBindings.keySet());
            if (!relationBindings.isEmpty())
                parts.add("relations=" + relationBindings.keySet());
            if (!scalarBindings.isEmpty())
                parts.add("scalars=" + scalarBindings.keySet());
            if (!lambdaParams.isEmpty())
                parts.add("lambdaParams=" + lambdaParams);
            return parts.isEmpty() ? "(none)" : String.join(", ", parts);
        }

        public String describeRelationBindings() {
            return relationBindings.isEmpty() ? "(none)" : relationBindings.keySet().toString();
        }
    }

    // ========================================================================
    // EXCEPTION
    // ========================================================================

    public static class CompileException extends RuntimeException {
        public CompileException(String message) {
            super(message);
        }
    }
}
