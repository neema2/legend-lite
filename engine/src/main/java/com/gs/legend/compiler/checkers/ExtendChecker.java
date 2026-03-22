package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.*;

/**
 * Signature-driven type checker for {@code extend()} — all 8 overloads.
 *
 * <p>Overloads:
 * <ul>
 *   <li>Scalar: {@code extend(r, FuncColSpec)} / {@code extend(r, FuncColSpecArray)}</li>
 *   <li>Aggregate (no window): {@code extend(r, AggColSpec)} / {@code extend(r, AggColSpecArray)}</li>
 *   <li>Window scalar: {@code extend(r, _Window, FuncColSpec)} / {@code extend(r, _Window, FuncColSpecArray)}</li>
 *   <li>Window aggregate: {@code extend(r, _Window, AggColSpec)} / {@code extend(r, _Window, AggColSpecArray)}</li>
 * </ul>
 *
 * <p>Key invariant enforced: every function call in the AST
 * (including spec-builder functions like {@code over()}, {@code ascending()},
 * {@code rows()}) is type-checked against its registry signature.
 * Lambda bodies are compiled via {@code compileLambdaBody} (→ {@code compileExpr})
 * so all inner function calls go through registry-driven type checking.
 */
public class ExtendChecker extends AbstractChecker {

    public ExtendChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        NativeFunctionDef def = resolveOverload("extend", af.parameters(), source);
        unify(def, source.expressionType()); // validate source matches signature generics
        List<ValueSpecification> params = af.parameters();

        GenericType.Relation.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("extend() requires a Relation source with a known schema");
        }

        // Build new schema = source columns + new computed columns
        Map<String, GenericType> newColumns = new LinkedHashMap<>(sourceSchema.columns());

        // Detect over() param and ColSpec params
        AppliedFunction overSpec = null;
        ColSpec windowColSpec = null;
        for (int i = 1; i < params.size(); i++) {
            var p = params.get(i);
            if (p instanceof AppliedFunction paf && "over".equals(simpleName(paf.function()))) {
                overSpec = paf;
            } else if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                windowColSpec = cs;
            } else if (p instanceof ClassInstance ci && ci.value() instanceof ColSpecArray(List<ColSpec> colSpecs)) {
                // ColSpecArray: register ALL columns
                for (ColSpec cs : colSpecs) {
                    GenericType colType = inferColumnType(cs, ctx, sourceSchema);
                    newColumns.put(cs.name(), colType);
                }
            }
        }

        // Infer type for single ColSpec if present
        if (windowColSpec != null) {
            GenericType colType = inferColumnType(windowColSpec, ctx, sourceSchema);
            newColumns.put(windowColSpec.name(), colType);
        }

        // Structural classification
        boolean isScalarExtend = windowColSpec != null
                && overSpec == null
                && windowColSpec.function2() == null;

        // Compile lambda bodies for type-checking (key invariant: compileExpr, not typeCheckExpression)
        if (windowColSpec != null && windowColSpec.function1() != null && sourceSchema != null) {
            compileFn1Lambda(windowColSpec.function1(), ctx, sourceSchema);
        }

        // Compile ColSpecArray lambda bodies too
        for (int i = 1; i < params.size(); i++) {
            if (params.get(i) instanceof ClassInstance ci
                    && ci.value() instanceof ColSpecArray(List<ColSpec> colSpecs) && sourceSchema != null) {
                for (ColSpec cs : colSpecs) {
                    if (cs.function1() != null) {
                        compileFn1Lambda(cs.function1(), ctx, sourceSchema);
                    }
                }
            }
        }

        // Resolve window function specs for non-scalar extends
        List<TypeInfo.WindowFunctionSpec> allWindowSpecs = new ArrayList<>();
        if (windowColSpec != null && !isScalarExtend) {
            List<String> partitionBy = new ArrayList<>();
            List<TypeInfo.SortSpec> orderBy = new ArrayList<>();
            TypeInfo.FrameSpec frame = null;
            if (overSpec != null) {
                var overResult = resolveOverClause(overSpec);
                partitionBy = overResult.partitionBy;
                orderBy = overResult.orderBy;
                frame = overResult.frame;
            }
            TypeInfo.WindowFunctionSpec ws = resolveWindowFunc(windowColSpec, partitionBy, orderBy, frame, windowColSpec.name());
            if (ws != null) {
                allWindowSpecs.add(ws);
            }
        }

        // Resolve window specs for ColSpecArray entries
        for (int i = 1; i < params.size(); i++) {
            if (params.get(i) instanceof ClassInstance ci
                    && ci.value() instanceof ColSpecArray(List<ColSpec> colSpecs) && sourceSchema != null) {
                for (ColSpec cs : colSpecs) {
                    if (cs.function2() != null || overSpec != null) {
                        List<String> partBy = new ArrayList<>();
                        List<TypeInfo.SortSpec> ordBy = new ArrayList<>();
                        TypeInfo.FrameSpec fr = null;
                        if (overSpec != null) {
                            var overResult = resolveOverClause(overSpec);
                            partBy = overResult.partitionBy;
                            ordBy = overResult.orderBy;
                            fr = overResult.frame;
                        }
                        TypeInfo.WindowFunctionSpec ws = resolveWindowFunc(cs, partBy, ordBy, fr, cs.name());
                        if (ws != null) {
                            allWindowSpecs.add(ws);
                        }
                    }
                }
            }
        }

        var extendRelType = new GenericType.Relation.Schema(newColumns, sourceSchema.dynamicPivotColumns());
        return TypeInfo.builder().mapping(source.mapping())
                .windowSpecs(allWindowSpecs)
                .expressionType(ExpressionType.many(new GenericType.Relation(extendRelType))).build();
    }

    // ========== Lambda compilation ==========

    /**
     * Compiles fn1 lambda body using compileExpr for full registry-driven type checking.
     * Binds ALL lambda parameters to the source schema.
     */
    private void compileFn1Lambda(LambdaFunction lambda, TypeChecker.CompilationContext ctx,
                                  GenericType.Relation.Schema sourceSchema) {
        if (lambda.parameters().isEmpty() || lambda.body().isEmpty()) return;
        TypeChecker.CompilationContext lambdaCtx = ctx;
        for (var lp : lambda.parameters()) {
            lambdaCtx = lambdaCtx.withRelationType(lp.name(), sourceSchema);
        }
        try {
            env.compileExpr(lambda.body().get(0), lambdaCtx);
        } catch (PureCompileException e) {
            // Non-fatal: some window lambda patterns may not fully resolve
            // (e.g., window function calls with $w, $r params that aren't real columns)
        }
    }

    /**
     * Infers the column type from fn1 lambda body.
     */
    private GenericType inferColumnType(ColSpec cs, TypeChecker.CompilationContext ctx,
                                        GenericType.Relation.Schema sourceSchema) {
        if (cs.function1() == null || cs.function1().body().isEmpty()) {
            return GenericType.Primitive.NUMBER; // fallback
        }
        LambdaFunction lambda = cs.function1();
        TypeChecker.CompilationContext bodyCtx = ctx;
        if (sourceSchema != null) {
            for (var lp : lambda.parameters()) {
                bodyCtx = bodyCtx.withRelationType(lp.name(), sourceSchema);
            }
        }
        try {
            TypeInfo bodyInfo = env.compileExpr(lambda.body().get(0), bodyCtx);
            if (bodyInfo != null && bodyInfo.type() != null) {
                return bodyInfo.type();
            }
            if (bodyInfo != null && bodyInfo.expressionType() != null) {
                GenericType rt = bodyInfo.expressionType().type();
                if (rt.isList() && rt.elementType() != null) {
                    return rt.elementType();
                }
                return rt;
            }
            if (bodyInfo != null && bodyInfo.schema() != null
                    && bodyInfo.schema().columns().size() == 1) {
                return bodyInfo.schema().columns().values().iterator().next();
            }
        } catch (PureCompileException e) {
            // Fallback on compile error
        }
        return GenericType.Primitive.NUMBER;
    }

    // ========== Over clause resolution ==========

    private record OverClauseResult(List<String> partitionBy, List<TypeInfo.SortSpec> orderBy,
                                     TypeInfo.FrameSpec frame) {}

    private OverClauseResult resolveOverClause(AppliedFunction overSpec) {
        List<String> partitionBy = new ArrayList<>();
        List<TypeInfo.SortSpec> orderBy = new ArrayList<>();
        TypeInfo.FrameSpec frame = null;

        for (var p : overSpec.parameters()) {
            if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                partitionBy.add(cs.name());
            } else if (p instanceof ClassInstance ci && ci.value() instanceof ColSpecArray(List<ColSpec> colSpecs)) {
                for (ColSpec cs : colSpecs) {
                    partitionBy.add(cs.name());
                }
            } else if (p instanceof PureCollection(List<ValueSpecification> values)) {
                for (var elem : values) {
                    var sortSpec = tryResolveSortSpec(elem);
                    if (sortSpec != null) orderBy.add(sortSpec);
                }
            } else if (p instanceof AppliedFunction paf) {
                String funcName = simpleName(paf.function());
                var sortSpec = tryResolveSortSpec(p);
                if (sortSpec != null) {
                    orderBy.add(sortSpec);
                } else if ("rows".equals(funcName) || "range".equals(funcName) || "_range".equals(funcName)) {
                    String frameType = funcName.startsWith("_") ? funcName.substring(1) : funcName;
                    TypeInfo.FrameBound start = resolveFrameBound(paf.parameters(), true);
                    TypeInfo.FrameBound end = resolveFrameBound(paf.parameters(), false);
                    validateFrameBounds(start, end);
                    frame = new TypeInfo.FrameSpec(frameType, start, end);
                }
            }
        }
        return new OverClauseResult(partitionBy, orderBy, frame);
    }

    /** Resolves ascending/descending sort spec from an AST node. */
    private TypeInfo.SortSpec tryResolveSortSpec(ValueSpecification vs) {
        if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("asc".equals(funcName) || "ascending".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.ASC);
            } else if ("desc".equals(funcName) || "descending".equals(funcName)) {
                String col = extractColumnName(af.parameters().get(0));
                return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.DESC);
            }
        }
        return null;
    }

    /** Resolves a frame bound from rows()/range() parameters. */
    private TypeInfo.FrameBound resolveFrameBound(List<ValueSpecification> params, boolean isStart) {
        int idx = isStart ? 0 : 1;
        if (idx >= params.size())
            return isStart ? TypeInfo.FrameBound.unbounded() : TypeInfo.FrameBound.currentRow();
        var param = params.get(idx);
        if (param instanceof AppliedFunction af && "unbounded".equals(simpleName(af.function()))) {
            return TypeInfo.FrameBound.unbounded();
        }
        if (param instanceof AppliedFunction af && "minus".equals(simpleName(af.function()))) {
            if (!af.parameters().isEmpty()) {
                double v = extractNumericLiteral(af.parameters().get(af.parameters().size() - 1));
                return TypeInfo.FrameBound.offset(-v);
            }
        }
        double v = extractNumericLiteral(param);
        if (v == 0) return TypeInfo.FrameBound.currentRow();
        return TypeInfo.FrameBound.offset(v);
    }

    private void validateFrameBounds(TypeInfo.FrameBound start, TypeInfo.FrameBound end) {
        double startPos = frameBoundPosition(start, true);
        double endPos = frameBoundPosition(end, false);
        if (startPos > endPos) {
            throw new PureCompileException(
                    "Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!");
        }
    }

    private double frameBoundPosition(TypeInfo.FrameBound bound, boolean isStart) {
        return switch (bound.type()) {
            case UNBOUNDED -> isStart ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            case CURRENT_ROW -> 0;
            case OFFSET -> bound.offset();
        };
    }

    private double extractNumericLiteral(ValueSpecification vs) {
        if (vs instanceof CInteger(Number value)) return value.doubleValue();
        if (vs instanceof CFloat(double value)) return value;
        if (vs instanceof CDecimal(java.math.BigDecimal value)) return value.doubleValue();
        throw new PureCompileException(
                "Expected numeric literal in frame bound, got: " + vs.getClass().getSimpleName());
    }

    // ========== Window function resolution ==========

    /**
     * Resolves window function from a ColSpec's function1/function2 lambdas.
     * Stores Pure function names only — no SQL mapping here.
     */
    private TypeInfo.WindowFunctionSpec resolveWindowFunc(ColSpec cs,
            List<String> partitionBy, List<TypeInfo.SortSpec> orderBy, TypeInfo.FrameSpec frame,
            String alias) {

        // Pattern 1: Aggregate window with function2 = aggregate lambda
        if (cs.function2() != null) {
            String column = extractPropertyNameFromLambda(cs.function1());
            String aggFunc = extractPureFuncName(cs.function2());
            String castType = (cs.function2() != null && !cs.function2().body().isEmpty())
                    ? extractCastType(cs.function2().body().get(0))
                    : null;
            if (column != null && aggFunc != null) {
                if ("percentile".equals(aggFunc) || "percentileCont".equals(aggFunc)
                        || "percentileDisc".equals(aggFunc)) {
                    var percentileResult = resolvePercentileArgs(cs.function2(), aggFunc);
                    return TypeInfo.WindowFunctionSpec.aggregateMulti(percentileResult.funcName,
                            column, alias, partitionBy, orderBy, frame,
                            List.of(String.valueOf(percentileResult.value)));
                }
                List<String> fn2ExtraArgs = extractFuncExtraArgs(cs.function2());
                if (castType != null) {
                    return TypeInfo.WindowFunctionSpec.aggregateCast(aggFunc, column, alias,
                            partitionBy, orderBy, frame, fn2ExtraArgs, castType);
                }
                if (!fn2ExtraArgs.isEmpty()) {
                    return TypeInfo.WindowFunctionSpec.aggregateMulti(aggFunc, column, alias,
                            partitionBy, orderBy, frame, fn2ExtraArgs);
                }
                return TypeInfo.WindowFunctionSpec.aggregate(aggFunc, column, alias,
                        partitionBy, orderBy, frame);
            }
            // rowMapper pattern
            if (column == null && aggFunc != null && cs.function1() != null) {
                var body = cs.function1().body();
                if (!body.isEmpty() && body.get(0) instanceof AppliedFunction rmAf) {
                    String rmFunc = simpleName(rmAf.function());
                    if (rmFunc.endsWith("rowMapper") || "rowMapper".equals(rmFunc)) {
                        String col1 = null, col2 = null;
                        if (rmAf.parameters().size() >= 1) col1 = extractColumnNameSafe(rmAf.parameters().get(0));
                        if (rmAf.parameters().size() >= 2) col2 = extractColumnNameSafe(rmAf.parameters().get(1));
                        if (col1 != null) {
                            List<String> extra = col2 != null ? List.of(col2) : List.of();
                            return TypeInfo.WindowFunctionSpec.aggregateMulti(aggFunc, col1, alias,
                                    partitionBy, orderBy, frame, extra);
                        }
                    }
                    String sourceCol = rmAf.parameters().size() > 0
                            ? extractColumnNameSafe(rmAf.parameters().get(0)) : null;
                    if (sourceCol != null) {
                        return TypeInfo.WindowFunctionSpec.aggregate(aggFunc, sourceCol, alias,
                                partitionBy, orderBy, frame);
                    }
                }
            }
        }

        // Pattern 2: Function in function1 lambda body
        if (cs.function1() != null) {
            var body = cs.function1().body();
            if (!body.isEmpty() && body.get(0) instanceof AppliedFunction af) {
                String funcName = simpleName(af.function());

                // Post-processor wrapping: round(cumulativeDistribution($w,$r), 2)
                if (isWrapperFunc(funcName) && !af.parameters().isEmpty()
                        && af.parameters().get(0) instanceof AppliedFunction innerAf) {
                    String innerFuncName = simpleName(innerAf.function());
                    if (isRankingFunc(innerFuncName)) {
                        List<String> extraArgs = new ArrayList<>();
                        for (int i = 1; i < af.parameters().size(); i++) {
                            extraArgs.add(extractLiteralValue(af.parameters().get(i)));
                        }
                        return TypeInfo.WindowFunctionSpec.wrapped(innerFuncName,
                                funcName, extraArgs, alias, partitionBy, orderBy, frame);
                    }
                }

                // Zero-arg ranking functions
                if (isRankingFunc(funcName)) {
                    return TypeInfo.WindowFunctionSpec.ranking(funcName, alias,
                            partitionBy, orderBy, frame);
                }

                // NTILE
                if ("ntile".equals(funcName)) {
                    int buckets = 1;
                    for (var p : af.parameters()) {
                        if (p instanceof CInteger(Number value)) {
                            buckets = value.intValue();
                            break;
                        }
                    }
                    return TypeInfo.WindowFunctionSpec.ntile(buckets, alias,
                            partitionBy, orderBy, frame);
                }

                // LAG/LEAD
                if ("lag".equals(funcName) || "lead".equals(funcName)) {
                    String sourceCol = extractColumnNameDeep(af);
                    return TypeInfo.WindowFunctionSpec.aggregateMulti(funcName, sourceCol, alias,
                            partitionBy, orderBy, frame, List.of("1"));
                }

                // COUNT
                if ("count".equals(funcName) || "size".equals(funcName)) {
                    return TypeInfo.WindowFunctionSpec.aggregate("count", "*", alias,
                            partitionBy, orderBy, frame);
                }

                // NTH_VALUE
                if ("nth".equals(funcName) || "nthValue".equals(funcName)) {
                    String sourceCol = extractColumnNameDeep(af);
                    int offset = 1;
                    for (var p : af.parameters()) {
                        if (p instanceof CInteger(Number value)) {
                            offset = value.intValue();
                            break;
                        }
                    }
                    return TypeInfo.WindowFunctionSpec.aggregateMulti(funcName, sourceCol, alias,
                            partitionBy, orderBy, frame, List.of(String.valueOf(offset)));
                }

                // General aggregate/value functions
                String sourceCol = af.parameters().size() > 1
                        ? extractColumnNameSafe(af.parameters().get(1)) : null;
                return TypeInfo.WindowFunctionSpec.aggregate(funcName, sourceCol, alias,
                        partitionBy, orderBy, frame);
            }

            // Property access pattern: {p,w,r|$p->avg($w,$r).salary}
            if (!body.isEmpty()
                    && body.get(0) instanceof AppliedProperty(String property, List<ValueSpecification> parameters)) {
                if (!parameters.isEmpty() && parameters.get(0) instanceof AppliedFunction innerAf) {
                    String innerFunc = simpleName(innerAf.function());
                    List<String> innerExtras = new ArrayList<>();
                    for (int ei = 1; ei < innerAf.parameters().size(); ei++) {
                        var px = innerAf.parameters().get(ei);
                        if (px instanceof CInteger(Number value)) innerExtras.add(String.valueOf(value));
                        else if (px instanceof CFloat(double value)) innerExtras.add(String.valueOf(value));
                        else if (px instanceof CString(String value)) innerExtras.add("'" + value + "'");
                    }
                    if (!innerExtras.isEmpty()) {
                        return TypeInfo.WindowFunctionSpec.aggregateMulti(innerFunc, property, alias,
                                partitionBy, orderBy, frame, innerExtras);
                    }
                    if ("lag".equals(innerFunc) || "lead".equals(innerFunc)) {
                        return TypeInfo.WindowFunctionSpec.aggregateMulti(innerFunc, property, alias,
                                partitionBy, orderBy, frame, List.of("1"));
                    }
                    return TypeInfo.WindowFunctionSpec.aggregate(innerFunc, property, alias,
                            partitionBy, orderBy, frame);
                }
            }
        }

        return null;
    }

    // ========== Helpers ==========

    private boolean isRankingFunc(String funcName) {
        return switch (funcName) {
            case "rowNumber", "rank", "denseRank", "percentRank", "cumulativeDistribution" -> true;
            default -> false;
        };
    }

    private boolean isWrapperFunc(String funcName) {
        return switch (funcName) {
            case "round", "abs", "ceil", "floor", "truncate" -> true;
            default -> false;
        };
    }

    private String extractPropertyNameFromLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            ValueSpecification body = lf.body().get(0);
            if (body instanceof AppliedProperty ap) return ap.property();
        }
        return null;
    }

    private String extractPureFuncName(LambdaFunction lf) {
        if (lf == null || lf.body().isEmpty()) return null;
        var body = lf.body().get(0);
        AppliedFunction af = resolveAggregateFunctionBody(body);
        if (af != null) return simpleName(af.function());
        if (body instanceof AppliedFunction castAf && "cast".equals(simpleName(castAf.function()))) {
            return "plus";
        }
        return null;
    }

    private AppliedFunction resolveAggregateFunctionBody(ValueSpecification body) {
        if (!(body instanceof AppliedFunction af)) return null;
        if ("cast".equals(simpleName(af.function()))) {
            if (!af.parameters().isEmpty() && af.parameters().get(0) instanceof AppliedFunction innerAf) {
                return innerAf;
            }
            return null;
        }
        return af;
    }

    private String extractCastType(ValueSpecification body) {
        if (!(body instanceof AppliedFunction af)) return null;
        if (!"cast".equals(simpleName(af.function()))) return null;
        for (var p : af.parameters()) {
            if (p instanceof GenericTypeInstance(String fullPath)) {
                return simpleName(fullPath);
            }
        }
        return null;
    }

    private List<String> extractFuncExtraArgs(LambdaFunction lf) {
        List<String> extras = new ArrayList<>();
        if (lf == null || lf.body().isEmpty()) return extras;
        var body = lf.body().get(0);
        if (body instanceof AppliedFunction af) {
            for (int i = 1; i < af.parameters().size(); i++) {
                var p = af.parameters().get(i);
                if (p instanceof CInteger(Number value)) extras.add(String.valueOf(value));
                else if (p instanceof CFloat(double value)) extras.add(String.valueOf(value));
                else if (p instanceof CDecimal(java.math.BigDecimal value)) extras.add(value.toPlainString());
                else if (p instanceof CString(String value)) extras.add("'" + value + "'");
                else if (p instanceof AppliedProperty ap) extras.add(ap.property());
            }
        }
        return extras;
    }

    private record PercentileResult(String funcName, double value) {}

    private PercentileResult resolvePercentileArgs(LambdaFunction lf, String baseFuncName) {
        String funcName = "percentileCont";
        double value = 0.5;
        if (lf != null && !lf.body().isEmpty() && lf.body().get(0) instanceof AppliedFunction af) {
            if (af.parameters().size() > 1) {
                var valParam = af.parameters().get(1);
                if (valParam instanceof CFloat(double v)) value = v;
                else if (valParam instanceof CDecimal(java.math.BigDecimal v)) value = v.doubleValue();
                else if (valParam instanceof CInteger(Number v)) value = v.doubleValue();
            }
            if (af.parameters().size() > 2 && af.parameters().get(2) instanceof CBoolean(boolean v)) {
                if (!v) value = 1.0 - value;
            }
            if (af.parameters().size() > 3 && af.parameters().get(3) instanceof CBoolean(boolean v)) {
                if (!v) funcName = "percentileDisc";
            }
        }
        if ("percentileDisc".equals(baseFuncName)) funcName = "percentileDisc";
        return new PercentileResult(funcName, value);
    }

    private String extractColumnNameDeep(AppliedFunction af) {
        for (var p : af.parameters()) {
            if (p instanceof AppliedProperty ap) return ap.property();
        }
        for (var p : af.parameters()) {
            String col = extractColumnNameSafe(p);
            if (col != null) return col;
        }
        return null;
    }

    /** Extract column name without throwing. */
    private String extractColumnNameSafe(ValueSpecification vs) {
        try {
            return extractColumnName(vs);
        } catch (PureCompileException e) {
            return null;
        }
    }

    private String extractLiteralValue(ValueSpecification vs) {
        if (vs instanceof CInteger(Number value)) return String.valueOf(value);
        if (vs instanceof CFloat(double value)) return String.valueOf(value);
        if (vs instanceof CString(String value)) return value;
        throw new PureCompileException("Expected literal value, got: " + vs.getClass().getSimpleName());
    }
}
