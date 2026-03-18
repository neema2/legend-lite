package org.finos.legend.pure.dsl.ast;

import org.finos.legend.pure.dsl.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts new {@link ValueSpecification} AST → old {@link PureExpression} AST.
 *
 * <p>
 * This is a TEMPORARY bridge for Phase 2 of the parser isolation refactoring.
 * It allows the new clean parser ({@link CleanAstBuilder}) to produce generic
 * {@link AppliedFunction} nodes while the compiler still expects specialized
 * old types like {@link ClassFilterExpression}, {@link ProjectExpression}, etc.
 *
 * <p>
 * Strategy: Convert bottom-up. Children are converted first, so when we
 * convert an {@link AppliedFunction} like "filter", we can instanceof-check
 * the already-converted source to decide ClassFilterExpression vs
 * RelationFilterExpression.
 *
 * <p>
 * This adapter will be removed in Phase 3 when the compiler is migrated
 * to consume {@link ValueSpecification} directly.
 */
public final class AstAdapter {

    private AstAdapter() {
    }

    /**
     * Converts a new AST tree to the old PureExpression tree.
     */
    public static PureExpression toOldAst(ValueSpecification vs) {
        return switch (vs) {
            case AppliedFunction af -> convertAppliedFunction(af);
            case AppliedProperty ap -> convertAppliedProperty(ap);
            case PackageableElementPtr ptr -> new ClassReference(ptr.fullPath());
            case Variable v -> new VariableExpr(v.name());
            case LambdaFunction lf -> convertLambda(lf);
            case Collection c -> new ArrayLiteral(c.values().stream().map(AstAdapter::toOldAst).toList());
            case CInteger ci -> {
                if (ci.value() instanceof java.math.BigInteger bi) {
                    yield LiteralExpr.integer(bi);
                }
                yield LiteralExpr.integer(ci.value().longValue());
            }
            case CString cs -> LiteralExpr.string(cs.value());
            case CFloat cf -> LiteralExpr.floatValue(cf.value());
            case CDecimal cd -> LiteralExpr.decimalValue(cd.value());
            case CBoolean cb -> LiteralExpr.bool(cb.value());
            case CDateTime cdt -> LiteralExpr.date(cdt.value());
            case CStrictDate csd -> LiteralExpr.date(csd.value());
            case CStrictTime cst -> LiteralExpr.strictTime(cst.value());
            case CLatestDate ignored -> LiteralExpr.date("%latest");
            case CByteArray ignored -> LiteralExpr.string("<byte[]>");
            case GenericTypeInstance gti -> new TypeReference(gti.fullPath());
            case EnumValue ev -> new EnumValueReference(ev.fullPath(), ev.value());
            case ClassInstance ci -> convertClassInstance(ci);
            case UnitInstance ignored -> throw new UnsupportedOperationException("UnitInstance not yet supported");
        };
    }

    // ==========================================
    // LAMBDA
    // ==========================================

    private static PureExpression convertLambda(LambdaFunction lf) {
        List<String> paramNames = lf.parameters().stream()
                .map(Variable::name)
                .toList();
        // Old LambdaExpression requires at least one parameter — use "_" as default
        if (paramNames.isEmpty()) {
            paramNames = List.of("_");
        }

        // Extract type annotations from typed variables (e.g., i: Integer[1..4])
        List<LambdaExpression.TypeAnnotation> typeAnnotations = null;
        boolean hasTypes = lf.parameters().stream().anyMatch(v -> v.typeName() != null);
        if (hasTypes) {
            typeAnnotations = lf.parameters().stream()
                    .map(v -> v.typeName() != null
                            ? new LambdaExpression.TypeAnnotation(v.typeName(), v.multiplicity())
                            : null)
                    .toList();
        }

        PureExpression body = toOldAst(lf.body().get(0));
        return new LambdaExpression(paramNames, typeAnnotations, body);
    }

    // ==========================================
    // APPLIED PROPERTY
    // ==========================================

    private static PureExpression convertAppliedProperty(AppliedProperty ap) {
        if (ap.parameters().isEmpty()) {
            return new PropertyAccess(null, ap.property());
        }
        PureExpression source = toOldAst(ap.parameters().get(0));
        return new PropertyAccessExpression(source, ap.property());
    }

    // ==========================================
    // CLASS INSTANCE (DSL constructs)
    // ==========================================

    private static PureExpression convertClassInstance(ClassInstance ci) {
        return switch (ci.type()) {
            case "colSpec" -> {
                ColSpec cs = (ColSpec) ci.value();
                PureExpression fn1 = cs.function1() != null ? convertLambda(cs.function1()) : null;
                PureExpression fn2 = cs.function2() != null ? convertLambda(cs.function2()) : null;
                yield new ColumnSpec(cs.name(), fn1, fn2);
            }
            case "colSpecArray" -> {
                ColSpecArray csa = (ColSpecArray) ci.value();
                List<PureExpression> specs = csa.colSpecs().stream()
                        .map(cs -> {
                            PureExpression fn1 = cs.function1() != null ? convertLambda(cs.function1()) : null;
                            PureExpression fn2 = cs.function2() != null ? convertLambda(cs.function2()) : null;
                            return (PureExpression) new ColumnSpec(cs.name(), fn1, fn2);
                        })
                        .toList();
                yield new ColumnSpecArray(specs);
            }
            // rootGraphFetchTree is handled below with proper parsing
            case "relation" -> {
                // Raw content "store::DB.TABLE" — parse back to RelationLiteral
                String content = (String) ci.value();
                int dotIdx = content.lastIndexOf('.');
                if (dotIdx > 0) {
                    String dbRef = content.substring(0, dotIdx);
                    String table = content.substring(dotIdx + 1);
                    yield new RelationLiteral(dbRef, table);
                }
                yield new FunctionCall("relationLiteral", List.of(LiteralExpr.string(content)));
            }
            case "tdsLiteral" -> {
                // TDS raw content — the compiler parses this further
                // For now, pass it through via the old PureAstBuilder's TDS parsing
                String raw = (String) ci.value();
                yield parseTdsLiteral(raw);
            }
            case "instance" -> {
                CleanAstBuilder.InstanceData data = (CleanAstBuilder.InstanceData) ci.value();
                Map<String, Object> props = new LinkedHashMap<>();
                for (var entry : data.properties().entrySet()) {
                    PureExpression converted = toOldAst(entry.getValue());
                    props.put(entry.getKey(), extractValue(converted));
                }
                yield new InstanceExpression(data.className(), props);
            }
            case "rootGraphFetchTree" -> {
                // Structured GraphFetchTree from CleanAstBuilder sub-parse
                if (ci.value() instanceof GraphFetchTree gft) {
                    yield gft;
                }
                // Legacy: raw content string — parse into GraphFetchTree
                String content = (String) ci.value();
                yield parseGraphFetchContent(content);
            }
            default -> new FunctionCall("classInstance_" + ci.type(), List.of());
        };
    }

    // ==========================================
    // APPLIED FUNCTION (the big switch)
    // ==========================================

    // Registry of user-defined Pure functions for inlining (same as PureAstBuilder)
    private static final PureFunctionRegistry functionRegistry = PureFunctionRegistry.withBuiltins();

    private static PureExpression convertAppliedFunction(AppliedFunction af) {
        // Convert all parameters first (bottom-up)
        List<PureExpression> params = af.parameters().stream()
                .map(AstAdapter::toOldAst)
                .toList();

        String funcName = af.function();
        // Strip fully-qualified names to simple names (matching PureAstBuilder
        // behavior)
        // e.g., meta::pure::functions::math::abs -> abs
        String simpleName = funcName.contains("::")
                ? funcName.substring(funcName.lastIndexOf("::") + 2)
                : funcName;
        boolean hasReceiver = af.hasReceiver();

        // Check if this is a registered user function → create
        // UserFunctionCallExpression
        // (mirrors PureAstBuilder.createFunctionCall lines 285-289)
        if (functionRegistry.hasFunction(funcName) || functionRegistry.hasFunction(simpleName)) {
            if (hasReceiver && params.size() >= 1) {
                // Arrow call: source->func(args)
                PureExpression source = params.get(0);
                List<PureExpression> args = params.subList(1, params.size());
                return new UserFunctionCallExpression(
                        source, af.sourceText(), simpleName, args, af.argTexts());
            } else {
                // Standalone call: func(args)
                return new UserFunctionCallExpression(
                        null, null, simpleName, params, af.argTexts());
            }
        }

        return switch (simpleName) {
            // Class.all()
            case "getAll" -> {
                if (!params.isEmpty() && params.get(0) instanceof ClassReference classRef) {
                    yield new ClassAllExpression(classRef.className());
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "getAllVersions" -> {
                if (!params.isEmpty() && params.get(0) instanceof ClassReference classRef) {
                    yield new ClassAllVersionsExpression(classRef.className());
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Filter
            case "filter" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    LambdaExpression lambda = toLambda(params.get(1));
                    if (source instanceof ClassExpression classExpr) {
                        yield new ClassFilterExpression(classExpr, lambda);
                    }
                    if (source instanceof RelationExpression relExpr) {
                        yield new RelationFilterExpression(relExpr, lambda);
                    }
                    // Support CastExpression - unwrap and treat inner as source
                    if (source instanceof CastExpression cast
                            && cast.source() instanceof RelationExpression castSource) {
                        yield new RelationFilterExpression(castSource, lambda);
                    }
                    // VariableExpr from let bindings like $a->filter(...)
                    if (source instanceof VariableExpr) {
                        yield new RelationFilterExpression(source, lambda);
                    }
                    // Generic filter for collections, function results, etc.
                    yield new FilterExpression(source, lambda);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Project
            case "project" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    List<PureExpression> args = params.subList(1, params.size());
                    yield convertProjectCall(source, args);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // GroupBy
            case "groupBy" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    List<PureExpression> args = params.subList(1, params.size());
                    yield convertGroupByCall(source, args);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Extend
            case "extend" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    List<PureExpression> args = params.subList(1, params.size());
                    yield convertExtendCall(source, args);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Select
            case "select" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    List<String> columns = extractColumnsFromArgs(params.subList(1, params.size()));
                    yield new RelationSelectExpression(source, columns);
                } else if (params.size() == 1) {
                    // select() with no args — select all columns
                    yield new RelationSelectExpression(params.get(0), List.of());
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // From
            case "from" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    String mappingRef = extractStringValue(params.get(1));
                    yield new FromExpression(source, mappingRef);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Distinct
            case "distinct" -> {
                if (params.size() >= 1) {
                    PureExpression source = params.get(0);
                    List<PureExpression> args = params.size() > 1 ? params.subList(1, params.size()) : List.of();
                    if (args.isEmpty()) {
                        yield DistinctExpression.all(source);
                    }
                    List<String> columns = new ArrayList<>();
                    for (PureExpression arg : args) {
                        if (arg instanceof ColumnSpec cs)
                            columns.add(cs.name());
                        else if (arg instanceof ColumnSpecArray csa) {
                            for (PureExpression spec : csa.specs()) {
                                if (spec instanceof ColumnSpec cs2)
                                    columns.add(cs2.name());
                            }
                        }
                    }
                    yield columns.isEmpty() ? DistinctExpression.all(source)
                            : DistinctExpression.columns(source, columns);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Join
            case "join" -> {
                if (params.size() >= 4) {
                    PureExpression source = params.get(0);
                    PureExpression right = params.get(1);
                    JoinExpression.JoinType joinType = extractJoinTypeFromExpr(params.get(2));
                    LambdaExpression condition = toLambda(params.get(3));
                    yield new JoinExpression(source, right, joinType, condition);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Pivot
            case "pivot" -> {
                if (params.size() >= 3) {
                    PureExpression source = params.get(0);
                    List<String> pivotCols = new ArrayList<>();
                    PureExpression pivotArg = params.get(1);
                    if (pivotArg instanceof ColumnSpec cs)
                        pivotCols.add(cs.name());
                    else if (pivotArg instanceof ColumnSpecArray csa) {
                        for (PureExpression s : csa.specs()) {
                            if (s instanceof ColumnSpec cs)
                                pivotCols.add(cs.name());
                        }
                    }
                    List<PivotExpression.AggregateSpec> aggregates = new ArrayList<>();
                    PureExpression aggArg = params.get(2);
                    if (aggArg instanceof ColumnSpec cs) {
                        aggregates.add(extractPivotAggSpec(cs));
                    } else if (aggArg instanceof ColumnSpecArray csa) {
                        for (PureExpression s : csa.specs()) {
                            if (s instanceof ColumnSpec cs)
                                aggregates.add(extractPivotAggSpec(cs));
                        }
                    }
                    if (!pivotCols.isEmpty() && !aggregates.isEmpty()) {
                        yield PivotExpression.dynamic(source, pivotCols, aggregates);
                    }
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // AsOfJoin
            case "asOfJoin" -> {
                if (params.size() >= 3) {
                    PureExpression left = params.get(0);
                    PureExpression right = params.get(1);
                    LambdaExpression matchCondition = toLambda(params.get(2));
                    LambdaExpression keyCondition = params.size() >= 4 ? toLambda(params.get(3)) : null;
                    yield new AsOfJoinExpression(left, right, matchCondition, keyCondition);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Aggregate (standalone — no group-by columns)
            case "aggregate" -> {
                if (params.size() >= 2 && params.get(0) instanceof RelationExpression source) {
                    List<LambdaExpression> mapFunctions = new ArrayList<>();
                    List<LambdaExpression> aggFunctions = new ArrayList<>();
                    List<String> aliases = new ArrayList<>();

                    for (int i = 1; i < params.size(); i++) {
                        PureExpression aggArg = params.get(i);
                        if (aggArg instanceof ColumnSpec cs) {
                            aliases.add(cs.name());
                            LambdaExpression mapLambda = cs.lambda() instanceof LambdaExpression le ? le : null;
                            LambdaExpression aggLambda = cs.extraFunction() instanceof LambdaExpression le ? le : null;
                            mapFunctions.add(
                                    mapLambda != null ? mapLambda : new LambdaExpression("x", new VariableExpr("x")));
                            aggFunctions.add(
                                    aggLambda != null ? aggLambda : new LambdaExpression("y", new VariableExpr("y")));
                        } else if (aggArg instanceof ColumnSpecArray csa) {
                            for (PureExpression spec : csa.specs()) {
                                if (spec instanceof ColumnSpec cs) {
                                    aliases.add(cs.name());
                                    LambdaExpression mapLambda = cs.lambda() instanceof LambdaExpression le ? le : null;
                                    LambdaExpression aggLambda = cs.extraFunction() instanceof LambdaExpression le ? le
                                            : null;
                                    mapFunctions.add(mapLambda != null ? mapLambda
                                            : new LambdaExpression("x", new VariableExpr("x")));
                                    aggFunctions.add(aggLambda != null ? aggLambda
                                            : new LambdaExpression("y", new VariableExpr("y")));
                                }
                            }
                        }
                    }

                    if (!mapFunctions.isEmpty()) {
                        yield new AggregateExpression(source, mapFunctions, aggFunctions, aliases);
                    }
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Rename
            case "rename" -> {
                if (params.size() >= 3) {
                    PureExpression source = params.get(0);
                    String oldName = extractColName(params.get(1));
                    String newName = extractColName(params.get(2));
                    yield new RenameExpression(source, oldName, newName);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Concatenate
            case "concatenate" -> {
                if (params.size() >= 2)
                    yield new ConcatenateExpression(params.get(0), params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Drop
            case "drop" -> {
                if (params.size() >= 2)
                    yield new DropExpression(params.get(0), extractInt(params.get(1)));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Write
            case "write" -> {
                if (params.size() >= 2)
                    yield new RelationWriteExpression(params.get(0), params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Save/update/delete
            case "save" -> {
                if (params.size() >= 1)
                    yield new SaveExpression(params.get(0));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "update" -> {
                if (params.size() >= 2)
                    yield new UpdateExpression(params.get(0), toLambda(params.get(1)));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "delete" -> {
                if (params.size() >= 1)
                    yield new DeleteExpression(params.get(0));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // First
            case "first" -> {
                if (params.size() >= 1)
                    yield new FirstExpression(params.get(0));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Slice
            case "slice" -> {
                if (params.size() >= 3) {
                    yield new SliceExpression(params.get(0), extractInt(params.get(1)), extractInt(params.get(2)));
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Block expression (multi-statement code)
            case "block" -> {
                List<LetExpression> lets = new ArrayList<>();
                PureExpression result = null;
                for (int i = 0; i < params.size(); i++) {
                    PureExpression p = params.get(i);
                    if (p instanceof LetExpression le) {
                        lets.add(le);
                    } else if (i == params.size() - 1) {
                        result = p;
                    }
                }
                if (result == null) {
                    if (!lets.isEmpty()) {
                        LetExpression lastLet = lets.get(lets.size() - 1);
                        result = new VariableExpr(lastLet.variableName());
                    } else {
                        result = LiteralExpr.string("");
                    }
                }
                if (lets.isEmpty()) {
                    yield result;
                }
                yield new BlockExpression(lets, result);
            }

            // Limit / take
            case "limit", "take" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    int count = extractInt(params.get(1));
                    if (source instanceof ClassExpression classExpr) {
                        yield ClassLimitExpression.limit(classExpr, count);
                    }
                    if (source instanceof RelationExpression relExpr) {
                        yield RelationLimitExpression.limit(relExpr, count);
                    }
                    yield new LimitExpression(source, count);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Sort - handle ColumnSpec-based sort with ascending/descending
            case "sort" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    List<PureExpression> sortArgs = params.subList(1, params.size());

                    // Check for ColumnSpec-based sort: sort(~[~col->desc(), ~col->asc()])
                    List<PureExpression> flatSortSpecs = new ArrayList<>();
                    for (PureExpression arg : sortArgs) {
                        if (arg instanceof ArrayLiteral arr) {
                            flatSortSpecs.addAll(arr.elements());
                        } else {
                            flatSortSpecs.add(arg);
                        }
                    }

                    // ColumnSpec-based sort: create SortExpression with flattened specs
                    // Just like PureAstBuilder.parseSortCall — one SortExpression with all specs
                    yield new SortExpression(source, flatSortSpecs);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "sortBy" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    LambdaExpression lambda = toLambda(params.get(1));
                    boolean asc = params.size() <= 2 || !"desc".equals(extractStringValue(params.get(2)));
                    if (source instanceof ClassExpression classExpr) {
                        yield new ClassSortByExpression(classExpr, lambda, asc);
                    }
                    yield new SortExpression(source, List.of(lambda));
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Comparison operators
            case "equal" -> {
                if (params.size() == 2)
                    yield new ComparisonExpr(params.get(0), ComparisonExpr.Operator.EQUALS, params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "notEqual" -> {
                if (params.size() == 2)
                    yield new ComparisonExpr(params.get(0), ComparisonExpr.Operator.NOT_EQUALS, params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "lessThan" -> {
                if (params.size() == 2)
                    yield new BinaryExpression(params.get(0), "<", params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "lessThanEqual" -> {
                if (params.size() == 2)
                    yield new BinaryExpression(params.get(0), "<=", params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "greaterThan" -> {
                if (params.size() == 2)
                    yield new BinaryExpression(params.get(0), ">", params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "greaterThanEqual" -> {
                if (params.size() == 2)
                    yield new BinaryExpression(params.get(0), ">=", params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Boolean operators
            case "and" -> {
                if (params.size() == 2)
                    yield LogicalExpr.and(params.get(0), params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "or" -> {
                if (params.size() == 2)
                    yield LogicalExpr.or(params.get(0), params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "not" -> {
                if (params.size() == 1)
                    yield new UnaryExpression("!", params.get(0));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Arithmetic
            case "plus" -> {
                if (params.size() == 2)
                    yield new BinaryExpression(params.get(0), "+", params.get(1));
                if (params.size() == 1)
                    yield params.get(0); // unary +
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "minus" -> {
                if (params.size() == 2)
                    yield new BinaryExpression(params.get(0), "-", params.get(1));
                if (params.size() == 1)
                    yield new UnaryExpression("-", params.get(0));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "times" -> {
                if (params.size() == 2)
                    yield new BinaryExpression(params.get(0), "*", params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            case "divide" -> {
                if (params.size() == 2)
                    yield new BinaryExpression(params.get(0), "/", params.get(1));
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Let binding
            case "letFunction" -> {
                if (params.size() == 2) {
                    yield new LetExpression(extractStringValue(params.get(0)), params.get(1));
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // At (array access) — keep as FunctionCall, matching old parser behavior
            case "at" -> {
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }
            // GraphFetch
            case "graphFetch" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    PureExpression treeArg = params.get(1);
                    if (source instanceof RelationExpression) {
                        throw new PureParseException(
                                "graphFetch() can only be called on class expressions like Person.all(), " +
                                        "not on relation expressions like #>{db.table}#");
                    }
                    if (treeArg instanceof GraphFetchTree tree && source instanceof ClassExpression classExpr) {
                        yield new GraphFetchExpression(classExpr, tree);
                    }
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Serialize
            case "serialize" -> {
                if (params.size() >= 1) {
                    PureExpression source = params.get(0);
                    if (!(source instanceof GraphFetchExpression)) {
                        throw new PureParseException(
                                "serialize() must be called on a graphFetch() expression. " +
                                        "Use: Class.all()->graphFetch(#{...}#)->serialize(#{...}#)");
                    }
                    GraphFetchExpression graphFetch = (GraphFetchExpression) source;
                    GraphFetchTree serializeTree = null;
                    if (params.size() >= 2 && params.get(1) instanceof GraphFetchTree tree) {
                        serializeTree = tree;
                    }
                    yield new SerializeExpression(graphFetch, serializeTree);
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Cast
            case "cast" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    PureExpression typeArg = params.get(1);
                    if (typeArg instanceof TypeReference typeRef) {
                        yield new CastExpression(source, typeRef.typeName());
                    }
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Flatten
            case "flatten" -> {
                if (params.size() >= 2) {
                    PureExpression source = params.get(0);
                    PureExpression colArg = params.get(1);
                    if (colArg instanceof ColumnSpec colSpec) {
                        yield new FlattenExpression(source, colSpec.name());
                    }
                }
                yield fallbackFunctionCall(simpleName, params, hasReceiver);
            }

            // Default: generic FunctionCall
            default -> fallbackFunctionCall(simpleName, params, hasReceiver);
        };
    }

    // ==========================================
    // HELPERS
    // ==========================================

    private static PureExpression fallbackFunctionCall(String funcName, List<PureExpression> params,
            boolean hasReceiver) {
        if (params.isEmpty()) {
            return new FunctionCall(funcName, List.of());
        }
        if (hasReceiver) {
            // Arrow call: params[0] is the receiver expression
            PureExpression source = params.get(0);
            List<PureExpression> args = params.size() > 1 ? params.subList(1, params.size()) : List.of();
            return new FunctionCall(funcName, source, args);
        }
        // Standalone call: all params are arguments, no source
        return new FunctionCall(funcName, params);
    }

    private static JoinExpression.JoinType extractJoinTypeFromExpr(PureExpression expr) {
        String text = extractStringValue(expr).toUpperCase();
        if (text.contains("INNER"))
            return JoinExpression.JoinType.INNER;
        if (text.contains("LEFT"))
            return JoinExpression.JoinType.LEFT_OUTER;
        if (text.contains("RIGHT"))
            return JoinExpression.JoinType.RIGHT_OUTER;
        if (text.contains("FULL"))
            return JoinExpression.JoinType.FULL_OUTER;
        return JoinExpression.JoinType.INNER;
    }

    private static LambdaExpression toLambda(PureExpression expr) {
        if (expr instanceof LambdaExpression le)
            return le;
        // Wrap non-lambda in a lambda for compatibility
        return new LambdaExpression("_", expr);
    }

    private static List<LambdaExpression> toLambdaList(PureExpression expr) {
        if (expr instanceof ArrayLiteral arr) {
            return arr.elements().stream()
                    .map(AstAdapter::toLambda)
                    .toList();
        }
        return List.of(toLambda(expr));
    }

    private static List<String> toStringList(PureExpression expr) {
        if (expr instanceof ArrayLiteral arr) {
            return arr.elements().stream()
                    .map(AstAdapter::extractStringValue)
                    .toList();
        }
        return List.of(extractStringValue(expr));
    }

    private static String extractStringValue(PureExpression expr) {
        if (expr instanceof StringLiteral sl)
            return sl.value();
        if (expr instanceof LiteralExpr le && le.value() instanceof String s)
            return s;
        if (expr instanceof ClassReference cr)
            return cr.className();
        if (expr instanceof VariableExpr ve)
            return ve.name();
        return expr.toString();
    }

    private static int extractInt(PureExpression expr) {
        if (expr instanceof IntegerLiteral il)
            return il.value().intValue();
        if (expr instanceof LiteralExpr le && le.value() instanceof Number n)
            return n.intValue();
        return 0;
    }

    // ==========================================
    // RELATION API CONVERTERS
    // ==========================================

    private static PureExpression convertProjectCall(PureExpression source, List<PureExpression> args) {
        List<LambdaExpression> columns = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for (PureExpression arg : args) {
            if (arg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    extractColumnProjection(spec, columns, aliases);
                }
            } else if (arg instanceof ColumnSpec cs) {
                extractColumnProjection(cs, columns, aliases);
            } else if (arg instanceof LambdaExpression lambda) {
                columns.add(lambda);
            } else if (arg instanceof ArrayLiteral array) {
                for (PureExpression element : array.elements()) {
                    if (element instanceof LambdaExpression lambda) {
                        columns.add(lambda);
                    } else if (element instanceof LiteralExpr literal) {
                        aliases.add(literal.value().toString());
                    }
                }
            }
        }

        if (source instanceof ClassExpression classExpr) {
            return new ProjectExpression(classExpr, columns, aliases);
        }
        return new RelationProjectExpression(source, columns, aliases);
    }

    private static void extractColumnProjection(PureExpression spec, List<LambdaExpression> columns,
            List<String> aliases) {
        if (spec instanceof ColumnSpec cs) {
            if (cs.lambda() != null) {
                columns.add(toLambda(cs.lambda()));
            } else {
                columns.add(new LambdaExpression("x", new PropertyAccessExpression(new VariableExpr("x"), cs.name())));
            }
            aliases.add(cs.name());
        }
    }

    private static PureExpression convertGroupByCall(PureExpression source, List<PureExpression> args) {
        // Validate: groupBy cannot be called directly on Class.all() - must use
        // project() first
        if (source instanceof ClassExpression || source instanceof ClassAllExpression) {
            throw new PureParseException(
                    "groupBy() cannot be called directly on a Class. " +
                            "Use project() first to create a relation: Class.all()->project(...)->groupBy(...)");
        }

        // Relation API syntax: groupBy(~groupCol, ~aggSpec) - 2+ args with ColumnSpec
        if (!args.isEmpty() && isColumnSpecArg(args.get(0))) {
            List<String> groupByColumnNames = new ArrayList<>();
            List<LambdaExpression> aggregations = new ArrayList<>();
            List<String> aliases = new ArrayList<>();

            // First arg: group-by column(s)
            PureExpression groupByArg = args.get(0);
            if (groupByArg instanceof ColumnSpec cs) {
                groupByColumnNames.add(cs.name());
            } else if (groupByArg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    if (spec instanceof ColumnSpec cs)
                        groupByColumnNames.add(cs.name());
                }
            }

            // Second+ args: aggregation column specs
            for (int i = 1; i < args.size(); i++) {
                PureExpression aggArg = args.get(i);
                if (aggArg instanceof ColumnSpec cs) {
                    aliases.add(cs.name());
                    aggregations.add(buildAggregationLambda(cs));
                } else if (aggArg instanceof ColumnSpecArray csa) {
                    for (PureExpression spec : csa.specs()) {
                        if (spec instanceof ColumnSpec cs) {
                            aliases.add(cs.name());
                            aggregations.add(buildAggregationLambda(cs));
                        }
                    }
                }
            }

            // Convert group-by column names to property access lambdas
            List<LambdaExpression> groupByLambdas = new ArrayList<>();
            for (String colName : groupByColumnNames) {
                PropertyAccessExpression propAccess = new PropertyAccessExpression(
                        new VariableExpr("r"), colName);
                groupByLambdas.add(new LambdaExpression("r", propAccess));
            }

            List<String> fullAliases = new ArrayList<>(groupByColumnNames);
            fullAliases.addAll(aliases);

            return new GroupByExpression(source, groupByLambdas, aggregations, fullAliases);
        }

        // Legacy 3-arg syntax: [groupByCols], [aggCols], [aliases]
        if (args.size() >= 3) {
            List<LambdaExpression> groupByColumns = toLambdaList(args.get(0));
            List<LambdaExpression> aggregations = toLambdaList(args.get(1));
            List<String> aliases = toStringList(args.get(2));
            if (source instanceof RelationExpression) {
                return new GroupByExpression(source, groupByColumns, aggregations, aliases);
            }
        }

        return fallbackFunctionCall("groupBy", prepend(source, args), true);
    }

    private static LambdaExpression buildAggregationLambda(ColumnSpec cs) {
        LambdaExpression mapLambda = cs.lambda() instanceof LambdaExpression le ? le : null;
        LambdaExpression aggLambda = cs.extraFunction() instanceof LambdaExpression le ? le : null;

        if (mapLambda != null && aggLambda != null) {
            PureExpression columnAccess = mapLambda.body();
            if (aggLambda.body() instanceof FunctionCall aggCall && aggCall.source() != null) {
                PureExpression mergedBody = new FunctionCall(aggCall.functionName(), columnAccess,
                        aggCall.arguments());
                return new LambdaExpression(mapLambda.parameter(), mergedBody);
            }
            // Handle when adapter stripped plus()/minus() to bare VariableExpr
            // e.g., $y->plus() became just $y — re-wrap with plus()
            if (aggLambda.body() instanceof VariableExpr) {
                PureExpression mergedBody = new FunctionCall("plus", columnAccess, List.of());
                return new LambdaExpression(mapLambda.parameter(), mergedBody);
            }
            return mapLambda;
        } else if (mapLambda != null) {
            return mapLambda;
        } else if (aggLambda != null) {
            return aggLambda;
        }
        return new LambdaExpression("x", new VariableExpr("x"));
    }

    private static PureExpression convertExtendCall(PureExpression source, List<PureExpression> args) {
        // Window function pattern: extend(over(...), ~col:{p,w,r|...})
        if (!args.isEmpty() && isOverCall(args.get(0))) {
            return convertTypedWindowExtend(source, args);
        }

        // Simple extend: extend(~col: x | expr)
        if (args.size() == 1 && args.get(0) instanceof ColumnSpec cs
                && cs.lambda() instanceof LambdaExpression lambda
                && cs.extraFunction() == null) {
            return new RelationExtendExpression(source, cs.name(), lambda);
        }

        // Multiple extends: extend(~[col1:{x|...}, col2:{x|...}])
        if (args.size() == 1 && args.get(0) instanceof ColumnSpecArray csa) {
            PureExpression result = source;
            for (PureExpression spec : csa.specs()) {
                if (spec instanceof ColumnSpec cs) {
                    if (cs.lambda() != null && cs.extraFunction() != null) {
                        result = convertAggregateExtendNoOver(result, cs);
                    } else if (cs.lambda() instanceof LambdaExpression lambda) {
                        result = new RelationExtendExpression(result, cs.name(), lambda);
                    }
                }
            }
            return result;
        }

        // Window function pattern 2: extend(~col : func()->over())
        if (args.size() == 1 && args.get(0) instanceof ColumnSpec cs && isOverCall(cs.extraFunction())) {
            return convertWindowExtendFromChain(source, cs);
        }

        // Aggregate extend: extend(~col:c|$c.id:y|$y->plus())
        if (args.size() == 1 && args.get(0) instanceof ColumnSpec cs
                && cs.lambda() != null && cs.extraFunction() != null) {
            return convertAggregateExtendNoOver(source, cs);
        }

        // Fallback
        return fallbackFunctionCall("extend", prepend(source, args), true);
    }

    // ---- Window extend helpers ----

    private static boolean isOverCall(PureExpression expr) {
        return expr instanceof FunctionCall fc && "over".equals(fc.functionName());
    }

    private static PureExpression convertTypedWindowExtend(PureExpression source, List<PureExpression> args) {
        PureExpression overExpr = args.get(0);
        WindowContext ctx = parseWindowContext(overExpr);

        if (args.size() < 2) {
            return fallbackFunctionCall("extend", prepend(source, args), true);
        }

        PureExpression colArg = args.get(1);

        if (colArg instanceof ColumnSpec cs) {
            WindowFunctionSpec funcSpec = parseWindowFunctionFromColumnSpec(cs);
            RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                    funcSpec, ctx.partitionCols, ctx.orderSpecs, ctx.frame);
            return RelationExtendExpression.window(source, cs.name(), typedSpec);
        }

        if (colArg instanceof ColumnSpecArray csa) {
            PureExpression result = source;
            for (PureExpression spec : csa.specs()) {
                if (spec instanceof ColumnSpec cs) {
                    WindowFunctionSpec funcSpec = parseWindowFunctionFromColumnSpec(cs);
                    RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                            funcSpec, ctx.partitionCols, ctx.orderSpecs, ctx.frame);
                    result = RelationExtendExpression.window(result, cs.name(), typedSpec);
                }
            }
            return result;
        }

        return fallbackFunctionCall("extend", prepend(source, args), true);
    }

    private static PureExpression convertWindowExtendFromChain(PureExpression source, ColumnSpec cs) {
        WindowContext ctx = parseWindowContext(cs.extraFunction());
        WindowFunctionSpec funcSpec = parseWindowFunctionFromColumnSpec(cs);
        RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                funcSpec, ctx.partitionCols, ctx.orderSpecs, ctx.frame);
        return RelationExtendExpression.window(source, cs.name(), typedSpec);
    }

    private static PureExpression convertAggregateExtendNoOver(PureExpression source, ColumnSpec cs) {
        // Aggregate extend without window: ~col:c|$c.id:y|$y->plus()
        WindowFunctionSpec funcSpec = parseWindowFunctionFromColumnSpec(cs);
        RelationExtendExpression.TypedWindowSpec typedSpec = RelationExtendExpression.TypedWindowSpec.of(
                funcSpec, List.of(), List.of(), null);
        return RelationExtendExpression.window(source, cs.name(), typedSpec);
    }

    private record WindowContext(
            List<String> partitionCols,
            List<RelationExtendExpression.SortSpec> orderSpecs,
            RelationExtendExpression.FrameSpec frame) {
    }

    private static WindowContext parseWindowContext(PureExpression overExpr) {
        List<PureExpression> overArgs = (overExpr instanceof FunctionCall fc)
                ? (fc.source() != null ? concatWithSource(fc.source(), fc.arguments()) : fc.arguments())
                : List.of();

        List<String> partitionCols = new ArrayList<>();
        List<RelationExtendExpression.SortSpec> orderSpecs = new ArrayList<>();
        RelationExtendExpression.FrameSpec frame = null;

        for (PureExpression arg : overArgs) {
            if (arg instanceof ColumnSpec cs) {
                partitionCols.add(cs.name());
            } else if (arg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    if (spec instanceof ColumnSpec c)
                        partitionCols.add(c.name());
                }
            } else if (arg instanceof ArrayLiteral arr) {
                // Sort spec arrays like [~o->ascending(), ~i->ascending()] arrive as
                // ArrayLiteral
                for (PureExpression elem : arr.elements()) {
                    if (elem instanceof FunctionCall sortFc) {
                        String sortMethod = sortFc.functionName();
                        if (sortMethod.contains("::"))
                            sortMethod = sortMethod.substring(sortMethod.lastIndexOf("::") + 2);
                        if (sortFc.source() != null && ("descending".equals(sortMethod) || "desc".equals(sortMethod))) {
                            orderSpecs.add(new RelationExtendExpression.SortSpec(
                                    extractColName(sortFc.source()), RelationExtendExpression.SortDirection.DESC));
                        } else if (sortFc.source() != null
                                && ("ascending".equals(sortMethod) || "asc".equals(sortMethod))) {
                            orderSpecs.add(new RelationExtendExpression.SortSpec(
                                    extractColName(sortFc.source()), RelationExtendExpression.SortDirection.ASC));
                        }
                    }
                }
            } else if (arg instanceof FunctionCall fc) {
                String method = fc.functionName();
                // Strip qualified name prefix (e.g., meta::pure::functions::relation::ascending
                // -> ascending)
                if (method.contains("::")) {
                    method = method.substring(method.lastIndexOf("::") + 2);
                }
                if (fc.source() != null && ("descending".equals(method) || "desc".equals(method))) {
                    orderSpecs.add(new RelationExtendExpression.SortSpec(
                            extractColName(fc.source()), RelationExtendExpression.SortDirection.DESC));
                } else if (fc.source() != null && ("ascending".equals(method) || "asc".equals(method))) {
                    orderSpecs.add(new RelationExtendExpression.SortSpec(
                            extractColName(fc.source()), RelationExtendExpression.SortDirection.ASC));
                } else if ("rows".equals(method)) {
                    frame = parseFrame(fc, RelationExtendExpression.FrameType.ROWS);
                } else if ("range".equals(method) || "_range".equals(method)) {
                    frame = parseFrame(fc, RelationExtendExpression.FrameType.RANGE);
                }
            }
        }

        return new WindowContext(partitionCols, orderSpecs, frame);
    }

    private static WindowFunctionSpec parseWindowFunctionFromColumnSpec(ColumnSpec cs) {
        // Aggregate pattern: extraFunction is the agg lambda
        if (cs.extraFunction() instanceof LambdaExpression aggLambda && !isOverCall(cs.extraFunction())) {
            String columnName = extractColumnFromMapLambda(cs.lambda());
            String secondColumnName = extractSecondColumnFromRowMapper(cs.lambda());
            PureExpression aggBody = aggLambda.body();
            if (aggBody instanceof FunctionCall mc && mc.source() != null) {
                AggregateFunctionSpec.AggregateFunction aggFunc = switch (mc.functionName().toLowerCase()) {
                    case "plus", "sum" -> AggregateFunctionSpec.AggregateFunction.SUM;
                    case "avg", "average" -> AggregateFunctionSpec.AggregateFunction.AVG;
                    case "count", "size" -> AggregateFunctionSpec.AggregateFunction.COUNT;
                    case "min" -> AggregateFunctionSpec.AggregateFunction.MIN;
                    case "max" -> AggregateFunctionSpec.AggregateFunction.MAX;
                    case "stddev" -> AggregateFunctionSpec.AggregateFunction.STDDEV;
                    case "stddevsample" -> AggregateFunctionSpec.AggregateFunction.STDDEV_SAMP;
                    case "stddevpopulation" -> AggregateFunctionSpec.AggregateFunction.STDDEV_POP;
                    case "variance" -> AggregateFunctionSpec.AggregateFunction.VARIANCE;
                    case "variancesample" -> AggregateFunctionSpec.AggregateFunction.VAR_SAMP;
                    case "variancepopulation" -> AggregateFunctionSpec.AggregateFunction.VAR_POP;
                    case "median" -> AggregateFunctionSpec.AggregateFunction.MEDIAN;
                    case "mode" -> AggregateFunctionSpec.AggregateFunction.MODE;
                    case "corr" -> AggregateFunctionSpec.AggregateFunction.CORR;
                    case "covarsample" -> AggregateFunctionSpec.AggregateFunction.COVAR_SAMP;
                    case "covarpopulation" -> AggregateFunctionSpec.AggregateFunction.COVAR_POP;
                    case "percentile", "percentilecont" -> AggregateFunctionSpec.AggregateFunction.PERCENTILE_CONT;
                    case "percentiledisc" -> AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC;
                    case "joinstrings" -> AggregateFunctionSpec.AggregateFunction.STRING_AGG;
                    case "wavg" -> AggregateFunctionSpec.AggregateFunction.WAVG;
                    case "maxby" -> AggregateFunctionSpec.AggregateFunction.ARG_MAX;
                    case "minby" -> AggregateFunctionSpec.AggregateFunction.ARG_MIN;
                    default ->
                        throw new PureParseException("Unknown aggregate function in window: " + mc.functionName());
                };
                // Percentile: extract value and ascending/continuous flags
                if ((aggFunc == AggregateFunctionSpec.AggregateFunction.PERCENTILE_CONT
                        || aggFunc == AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC)
                        && !mc.arguments().isEmpty()) {
                    double pVal = extractDouble(mc.arguments().get(0));
                    if (mc.arguments().size() >= 3) {
                        Object contVal = extractLiteralValue(mc.arguments().get(2));
                        if (Boolean.FALSE.equals(contVal)) {
                            aggFunc = AggregateFunctionSpec.AggregateFunction.PERCENTILE_DISC;
                        }
                    }
                    if (mc.arguments().size() >= 2) {
                        Object ascVal = extractLiteralValue(mc.arguments().get(1));
                        if (Boolean.FALSE.equals(ascVal)) {
                            pVal = 1.0 - pVal;
                        }
                    }
                    return AggregateFunctionSpec.percentile(aggFunc, columnName, pVal, List.of(), List.of());
                }
                // Bivariate: corr, covarSample, covarPopulation need two columns
                if (secondColumnName != null) {
                    return AggregateFunctionSpec.bivariate(aggFunc, columnName, secondColumnName, List.of(), List.of());
                }
                return AggregateFunctionSpec.of(aggFunc, columnName, List.of(), List.of());
            }
            return AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.SUM,
                    columnName, List.of(), List.of());
        }

        if (!(cs.lambda() instanceof LambdaExpression lambda)) {
            throw new PureParseException("Expected lambda for window function column spec: " + cs.name() + ", got: "
                    + cs.lambda().getClass().getSimpleName());
        }

        PureExpression body = lambda.body();

        // Ranking/value/aggregate functions: {p,w,r|$p->rowNumber($r)}
        if (body instanceof FunctionCall mc && mc.source() != null) {
            // Check for chain: window_func()->scalar_func() like
            // cumulativeDistribution()->round(2)
            if (mc.source() instanceof FunctionCall innerMc && innerMc.source() != null) {
                WindowFunctionSpec innerSpec = parseWindowFunctionFromFunctionCall(innerMc);
                // Wrap with post-processor (round, abs, etc.) — extract args as Objects
                List<Object> postProcessorArgs = mc.arguments().stream()
                        .map(AstAdapter::extractLiteralValue)
                        .toList();
                return new PostProcessedWindowFunctionSpec(innerSpec, mc.functionName(), postProcessorArgs);
            }
            return parseWindowFunctionFromFunctionCall(mc);
        }

        // PropertyAccess patterns: {p,w,r|$p->lead($r).salary}
        if (body instanceof PropertyAccessExpression pae && pae.source() instanceof FunctionCall mc
                && mc.source() != null) {
            WindowFunctionSpec spec = parseWindowFunctionFromFunctionCall(mc);
            if (spec instanceof ValueFunctionSpec vfs) {
                return new ValueFunctionSpec(vfs.function(), pae.propertyName(),
                        vfs.offset(), vfs.partitionBy(), vfs.orderBy(), vfs.frame());
            }
            if (spec instanceof AggregateFunctionSpec afs) {
                return AggregateFunctionSpec.of(afs.function(), pae.propertyName(),
                        afs.partitionBy(), afs.orderBy());
            }
            return spec;
        }

        // FirstExpression pattern: {p,w,r|$p->first($w,$r).salary}
        // The adapter converts first() to FirstExpression before this is reached
        if (body instanceof PropertyAccessExpression pae && pae.source() instanceof FirstExpression) {
            return ValueFunctionSpec.firstLast(ValueFunctionSpec.ValueFunction.FIRST_VALUE,
                    pae.propertyName(), List.of(), List.of(), null);
        }

        // Bare FirstExpression without property: {p,w,r|$p->first($w,$r)}
        if (body instanceof FirstExpression) {
            return ValueFunctionSpec.firstLast(ValueFunctionSpec.ValueFunction.FIRST_VALUE,
                    null, List.of(), List.of(), null);
        }

        throw new PureParseException("Unrecognized window function pattern in lambda body: "
                + lambda.body().getClass().getSimpleName() + " = " + lambda.body());
    }

    private static WindowFunctionSpec parseWindowFunctionFromFunctionCall(FunctionCall mc) {
        return switch (mc.functionName()) {
            case "rowNumber" ->
                RankingFunctionSpec.of(RankingFunctionSpec.RankingFunction.ROW_NUMBER, List.of(), List.of());
            case "rank" -> RankingFunctionSpec.of(RankingFunctionSpec.RankingFunction.RANK, List.of(), List.of());
            case "denseRank" ->
                RankingFunctionSpec.of(RankingFunctionSpec.RankingFunction.DENSE_RANK, List.of(), List.of());
            case "percentRank" ->
                RankingFunctionSpec.of(RankingFunctionSpec.RankingFunction.PERCENT_RANK, List.of(), List.of());
            case "cumulativeDistribution" ->
                RankingFunctionSpec.of(RankingFunctionSpec.RankingFunction.CUME_DIST, List.of(), List.of());
            case "ntile" -> {
                int n = 1;
                if (mc.arguments().size() >= 2) {
                    var bucketArg = mc.arguments().get(1);
                    if (bucketArg instanceof LiteralExpr lit && lit.value() instanceof Number num)
                        n = num.intValue();
                    else if (bucketArg instanceof IntegerLiteral lit)
                        n = lit.value().intValue();
                }
                yield RankingFunctionSpec.ntile(n, List.of(), List.of());
            }
            case "lag" -> ValueFunctionSpec.lagLead(ValueFunctionSpec.ValueFunction.LAG, null, 1, List.of(), List.of());
            case "lead" ->
                ValueFunctionSpec.lagLead(ValueFunctionSpec.ValueFunction.LEAD, null, 1, List.of(), List.of());
            case "first", "firstValue" -> ValueFunctionSpec.firstLast(ValueFunctionSpec.ValueFunction.FIRST_VALUE, null,
                    List.of(), List.of(), null);
            case "last", "lastValue" -> ValueFunctionSpec.firstLast(ValueFunctionSpec.ValueFunction.LAST_VALUE, null,
                    List.of(), List.of(), null);
            case "nth", "nthValue" -> {
                int nOffset = 1;
                if (mc.arguments().size() >= 3) {
                    var nArg = mc.arguments().get(2);
                    if (nArg instanceof LiteralExpr lit && lit.value() instanceof Number num)
                        nOffset = num.intValue();
                    else if (nArg instanceof IntegerLiteral lit)
                        nOffset = lit.value().intValue();
                }
                yield ValueFunctionSpec.lagLead(ValueFunctionSpec.ValueFunction.NTH_VALUE, null, nOffset, List.of(),
                        List.of());
            }
            case "sum", "plus" ->
                AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.SUM, "value", List.of(), List.of());
            case "avg", "average" ->
                AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.AVG, "value", List.of(), List.of());
            case "count" ->
                AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.COUNT, "*", List.of(), List.of());
            case "min" ->
                AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.MIN, "value", List.of(), List.of());
            case "max" ->
                AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.MAX, "value", List.of(), List.of());
            case "stdDev" ->
                AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.STDDEV, "value", List.of(), List.of());
            case "variance" -> AggregateFunctionSpec.of(AggregateFunctionSpec.AggregateFunction.VARIANCE, "value",
                    List.of(), List.of());
            default -> throw new PureParseException("Unknown window function: " + mc.functionName());
        };
    }

    private static RelationExtendExpression.FrameSpec parseFrame(FunctionCall fc,
            RelationExtendExpression.FrameType type) {
        List<PureExpression> frameArgs = fc.source() != null ? concatWithSource(fc.source(), fc.arguments())
                : fc.arguments();
        if (frameArgs.size() < 2)
            return null;
        return new RelationExtendExpression.FrameSpec(type,
                parseFrameBound(frameArgs.get(0)),
                parseFrameBound(frameArgs.get(1)));
    }

    private static RelationExtendExpression.FrameBound parseFrameBound(PureExpression expr) {
        if (expr instanceof FunctionCall fc) {
            String name = fc.functionName();
            if (name.contains("::"))
                name = name.substring(name.lastIndexOf("::") + 2);
            if ("unbounded".equals(name))
                return RelationExtendExpression.FrameBound.unbounded();
        }
        if (expr instanceof IntegerLiteral lit)
            return RelationExtendExpression.FrameBound.fromDouble(lit.value().doubleValue());
        if (expr instanceof LiteralExpr le && le.value() instanceof Number n)
            return RelationExtendExpression.FrameBound.fromDouble(n.doubleValue());
        if (expr instanceof UnaryExpression unary && "-".equals(unary.operator())) {
            if (unary.operand() instanceof LiteralExpr le && le.value() instanceof Number n)
                return RelationExtendExpression.FrameBound.fromDouble(-n.doubleValue());
            if (unary.operand() instanceof IntegerLiteral lit)
                return RelationExtendExpression.FrameBound.fromDouble(-lit.value().doubleValue());
        }
        return RelationExtendExpression.FrameBound.currentRow();
    }

    private static String extractColName(PureExpression expr) {
        if (expr instanceof ColumnSpec cs)
            return cs.name();
        if (expr instanceof PropertyAccess pa)
            return pa.propertyName();
        return "unknown";
    }

    private static String extractColumnFromMapLambda(PureExpression expr) {
        if (expr instanceof LambdaExpression lambda) {
            PureExpression body = lambda.body();
            if (body instanceof PropertyAccessExpression pae)
                return pae.propertyName();
            if (body instanceof PropertyAccess pa)
                return pa.propertyName();
            // Handle chained calls like $r.id->cast(@Integer) — extract the PropertyAccess
            // from source
            if (body instanceof FunctionCall fc) {
                return extractColumnFromFunctionCallSource(fc);
            }
            // Handle CastExpression: $r.val->cast(@Integer) → extract 'val'
            if (body instanceof CastExpression cast) {
                if (cast.source() instanceof PropertyAccessExpression pae)
                    return pae.propertyName();
                if (cast.source() instanceof PropertyAccess pa)
                    return pa.propertyName();
            }
        }
        return "value";
    }

    private static String extractColumnFromFunctionCallSource(FunctionCall fc) {
        // Handle rowMapper patterns for bivariate functions (corr, covar)
        if (fc.functionName().endsWith("rowMapper")) {
            // Unqualified: source is $r.col1, args[0] is $r.col2
            if (fc.source() instanceof PropertyAccessExpression pae)
                return pae.propertyName();
            if (fc.source() instanceof PropertyAccess pa)
                return pa.propertyName();
            // Qualified: no source, args are [$r.col1, $r.col2]
            if (!fc.arguments().isEmpty()) {
                String col = extractColumnFromExpression(fc.arguments().get(0));
                if (col != null)
                    return col;
            }
        }
        if (fc.source() instanceof PropertyAccessExpression pae)
            return pae.propertyName();
        if (fc.source() instanceof PropertyAccess pa)
            return pa.propertyName();
        if (fc.source() instanceof FunctionCall innerFc)
            return extractColumnFromFunctionCallSource(innerFc);
        if (fc.source() instanceof VariableExpr) {
            // Pattern: $p->rowNumber($r) — no column, use function name
            return "value";
        }
        return "value";
    }

    /**
     * Extract second column from rowMapper pattern: {p,w,r|rowMapper($r.col1,
     * $r.col2)}
     */
    private static String extractSecondColumnFromRowMapper(PureExpression expr) {
        if (expr instanceof LambdaExpression lambda) {
            return extractSecondColumnFromRowMapper(lambda.body());
        }
        // Unqualified: $r.col1->rowMapper($r.col2)
        if (expr instanceof FunctionCall fc && fc.source() != null && "rowMapper".equals(fc.functionName())) {
            if (!fc.arguments().isEmpty()) {
                return extractColumnFromExpression(fc.arguments().get(0));
            }
        }
        // Qualified: meta::pure::functions::math::mathUtility::rowMapper($r.col1,
        // $r.col2)
        if (expr instanceof FunctionCall fc && fc.functionName().endsWith("rowMapper")) {
            if (fc.arguments().size() >= 2) {
                return extractColumnFromExpression(fc.arguments().get(1));
            }
        }
        return null;
    }

    private static String extractColumnFromExpression(PureExpression expr) {
        if (expr instanceof PropertyAccessExpression pae)
            return pae.propertyName();
        if (expr instanceof PropertyAccess pa)
            return pa.propertyName();
        return null;
    }

    private static double extractDouble(PureExpression expr) {
        if (expr instanceof LiteralExpr lit && lit.value() instanceof Number num)
            return num.doubleValue();
        return 0.0;
    }

    private static Object extractLiteralValue(PureExpression expr) {
        if (expr instanceof LiteralExpr lit)
            return lit.value();
        return null;
    }

    private static List<PureExpression> concatWithSource(PureExpression source, List<PureExpression> args) {
        List<PureExpression> result = new ArrayList<>();
        result.add(source);
        result.addAll(args);
        return result;
    }

    private static PivotExpression.AggregateSpec extractPivotAggSpec(ColumnSpec cs) {
        String aggFunction = "sum"; // default
        if (cs.extraFunction() instanceof LambdaExpression aggLambda) {
            PureExpression aggBody = aggLambda.body();
            if (aggBody instanceof FunctionCall mc && mc.source() != null) {
                aggFunction = mapAggFunctionName(mc.functionName());
            }
        }
        // Check value lambda body type
        if (cs.lambda() instanceof LambdaExpression valueLambda) {
            PureExpression valueBody = valueLambda.body();
            // Simple column access: x|$x.treePlanted
            if (valueBody instanceof PropertyAccessExpression pae) {
                return PivotExpression.AggregateSpec.column(cs.name(), pae.propertyName(), aggFunction);
            }
            if (valueBody instanceof PropertyAccess pa) {
                return PivotExpression.AggregateSpec.column(cs.name(), pa.propertyName(), aggFunction);
            }
            // Literal: x|1
            if (valueBody instanceof LiteralExpr lit) {
                return PivotExpression.AggregateSpec.expression(cs.name(), String.valueOf(lit.value()), aggFunction);
            }
            // Computed expression: x|$x.treePlanted * 2
            // TODO: long-term, change AggregateSpec.valueExpression from String to
            // PureExpression
            String exprStr = expressionToSqlString(valueBody);
            if (exprStr != null) {
                return PivotExpression.AggregateSpec.expression(cs.name(), exprStr, aggFunction);
            }
        }
        String colName = extractColumnFromMapLambda(cs.lambda());
        return PivotExpression.AggregateSpec.column(cs.name(), colName, aggFunction);
    }

    private static String mapAggFunctionName(String pureName) {
        return switch (pureName) {
            case "plus" -> "sum";
            case "count" -> "count";
            case "average", "avg", "mean" -> "avg";
            default -> pureName.toLowerCase();
        };
    }

    /**
     * Minimal expression-to-SQL-string for pivot value expressions.
     * Handles: column refs, literals, binary ops, toOne unwrap.
     * TODO: long-term, store PureExpression instead of String in AggregateSpec.
     */
    private static String expressionToSqlString(PureExpression expr) {
        if (expr instanceof LiteralExpr lit) {
            Object value = lit.value();
            if (value instanceof String s)
                return "'" + s.replace("'", "''") + "'";
            return String.valueOf(value);
        }
        if (expr instanceof PropertyAccessExpression pae)
            return "\"" + pae.propertyName() + "\"";
        if (expr instanceof PropertyAccess pa)
            return "\"" + pa.propertyName() + "\"";
        if (expr instanceof BinaryExpression bin) {
            String left = expressionToSqlString(bin.left());
            String right = expressionToSqlString(bin.right());
            if (left != null && right != null)
                return "(" + left + " " + bin.operator() + " " + right + ")";
        }
        if (expr instanceof FunctionCall fc && fc.source() != null && "toOne".equals(fc.functionName())) {
            return expressionToSqlString(fc.source());
        }
        return null;
    }

    private static List<String> extractColumnsFromArgs(List<PureExpression> args) {
        List<String> columns = new ArrayList<>();
        for (PureExpression arg : args) {
            if (arg instanceof ColumnSpec cs) {
                columns.add(cs.name());
            } else if (arg instanceof ColumnSpecArray csa) {
                for (PureExpression spec : csa.specs()) {
                    if (spec instanceof ColumnSpec cs2) {
                        columns.add(cs2.name());
                    }
                }
            }
        }
        return columns;
    }

    private static boolean isColumnSpecArg(PureExpression arg) {
        return arg instanceof ColumnSpec || arg instanceof ColumnSpecArray;
    }

    private static List<PureExpression> prepend(PureExpression first, List<PureExpression> rest) {
        List<PureExpression> result = new ArrayList<>();
        result.add(first);
        result.addAll(rest);
        return result;
    }

    // ==========================================
    // DSL PARSERS
    // ==========================================

    private static PureExpression parseTdsLiteral(String raw) {
        // TDS raw format: #TDS\ncol1,col2\nval1,val2\n#
        // The old parser handles this → for now, delegate to legacy
        // by creating a minimal TdsLiteral with just column names
        try {
            String content = raw;
            if (content.startsWith("#TDS")) {
                content = content.substring(4);
            }
            if (content.endsWith("#")) {
                content = content.substring(0, content.length() - 1);
            }
            content = content.trim();

            String[] lines = content.split("\n");
            if (lines.length == 0) {
                return new FunctionCall("tdsLiteral", List.of(LiteralExpr.string(raw)));
            }

            // Parse header using quote-aware CSV parser
            List<TdsLiteral.TdsColumn> columns = new ArrayList<>();
            for (String col : parseTdsCsvLine(lines[0])) {
                col = col.trim();
                int colonIdx = col.indexOf(':');
                if (colonIdx > 0) {
                    columns.add(TdsLiteral.TdsColumn.of(col.substring(0, colonIdx).trim(),
                            col.substring(colonIdx + 1).trim()));
                } else {
                    columns.add(TdsLiteral.TdsColumn.of(col));
                }
            }

            // Parse rows using quote-aware CSV parser
            List<List<Object>> rows = new ArrayList<>();
            for (int i = 1; i < lines.length; i++) {
                String line = lines[i].trim();
                if (line.isEmpty())
                    continue;
                List<String> values = parseTdsCsvLine(line);
                List<Object> row = new ArrayList<>();
                for (String val : values) {
                    row.add(parseTdsValue(val.trim()));
                }
                // Pad short rows with null to match column count
                while (row.size() < columns.size()) {
                    row.add(null);
                }
                rows.add(row);
            }

            return new TdsLiteral(columns, rows);
        } catch (Exception e) {
            return new FunctionCall("tdsLiteral", List.of(LiteralExpr.string(raw)));
        }
    }

    /**
     * Parse a CSV-style line respecting quoted strings. Commas inside quotes are
     * not delimiters.
     */
    private static List<String> parseTdsCsvLine(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                result.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        result.add(current.toString().trim());
        return result;
    }

    /** Parse a TDS cell value to appropriate type. */
    private static Object parseTdsValue(String value) {
        if (value.isEmpty() || "null".equalsIgnoreCase(value)) {
            return null;
        }
        // Decimal suffix (e.g., 21d, 41.0D)
        if ((value.endsWith("d") || value.endsWith("D")) && value.length() > 1) {
            try {
                return Double.parseDouble(value.substring(0, value.length() - 1));
            } catch (NumberFormatException e) {
                /* fall through */ }
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            /* not an integer */ }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            /* not a number */ }
        // Strip surrounding quotes (e.g., "[1,2,3]" → [1,2,3])
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static GraphFetchTree parseGraphFetchContent(String content) {
        // Parse "Person { firstName, lastName }" format
        content = content.trim();
        int braceIdx = content.indexOf('{');
        if (braceIdx < 0) {
            return GraphFetchTree.of(content.trim());
        }
        String rootClass = content.substring(0, braceIdx).trim();
        String propsContent = content.substring(braceIdx + 1);
        if (propsContent.endsWith("}")) {
            propsContent = propsContent.substring(0, propsContent.length() - 1);
        }
        String[] props = propsContent.split(",");
        List<String> propNames = new ArrayList<>();
        for (String p : props) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                propNames.add(trimmed);
            }
        }
        return GraphFetchTree.of(rootClass, propNames.toArray(new String[0]));
    }

    private static Object extractValue(PureExpression expr) {
        if (expr instanceof LiteralExpr le)
            return le.value();
        if (expr instanceof StringLiteral sl)
            return sl.value();
        if (expr instanceof IntegerLiteral il)
            return il.value();
        return expr;
    }
}
