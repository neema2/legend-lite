package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.*;

/**
 * Signature-driven type checker for {@code extend()} — all 8 overloads.
 *
 * <p>Follows GroupByChecker's "AST + small hint" pattern:
 * <ul>
 *   <li>compileLambdaBody type-checks ALL functions inside fn1/fn2 bodies
 *       (rowNumber, rank, lag, sum, etc.) via compileExpr → ScalarChecker → resolveOverload</li>
 *   <li>over()/ascending()/descending()/rows()/_range() type-checked via resolveOverload</li>
 *   <li>Produces minimal WindowSpec (5 fields): resolvedFunc + OverSpec + alias + types</li>
 *   <li>PlanGenerator reads fn1/fn2 structure from AST (not from sidecar)</li>
 * </ul>
 */
public class ExtendChecker extends AbstractChecker {

    public ExtendChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        NativeFunctionDef def = resolveOverload("extend", af.parameters(), source);
        unify(def, source.expressionType());
        List<ValueSpecification> params = af.parameters();

        GenericType.Relation.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("extend() requires a Relation source with a known schema");
        }

        Map<String, GenericType> newColumns = new LinkedHashMap<>(sourceSchema.columns());
        List<TypeInfo.WindowSpec> windowSpecs = new ArrayList<>();

        // --- Compile over() clause ---
        // compileOverClause validates each sub-component (ascending, rows, unbounded, etc.)
        // individually via resolveOverload — no top-level over() resolution needed.
        TypeInfo.OverSpec overSpec = null;
        for (int i = 1; i < params.size(); i++) {
            if (params.get(i) instanceof AppliedFunction paf
                    && "over".equals(simpleName(paf.function()))) {
                overSpec = compileOverClause(paf, sourceSchema);
                break;
            }
        }

        // --- Compile each ColSpec/ColSpecArray ---
        for (int i = 1; i < params.size(); i++) {
            var p = params.get(i);
            if (p instanceof AppliedFunction) continue; // over() — already handled
            if (p instanceof ClassInstance ci) {
                List<ColSpec> colSpecs = extractColSpecs(ci);
                for (ColSpec cs : colSpecs) {
                    var result = compileColSpec(cs, sourceSchema, source, ctx, overSpec, def);
                    if (newColumns.containsKey(result.alias)) {
                        throw new PureCompileException(
                                "extend(): column '" + result.alias
                                + "' already exists — use rename() first or choose a different name");
                    }
                    newColumns.put(result.alias, result.returnType);
                    if (result.windowSpec != null) {
                        windowSpecs.add(result.windowSpec);
                    }
                }
            }
        }

        var schema = new GenericType.Relation.Schema(newColumns, sourceSchema.dynamicPivotColumns());
        return TypeInfo.builder()
                .mapping(source.mapping())
                .windowSpecs(windowSpecs)
                .expressionType(ExpressionType.many(new GenericType.Relation(schema)))
                .build();
    }

    // ========== Public helpers for PlanGenerator ==========

    /**
     * Extracts all ColSpecs from extend() params (skipping over() and source).
     * Used by PlanGenerator to zip WindowSpec sidecar against AST ColSpec.
     */
    public static List<ColSpec> extractAllColSpecs(List<ValueSpecification> params) {
        List<ColSpec> result = new ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            var p = params.get(i);
            if (p instanceof AppliedFunction) continue; // over()
            if (p instanceof ClassInstance ci) {
                if (ci.value() instanceof ColSpecArray(List<ColSpec> specs)) result.addAll(specs);
                else if (ci.value() instanceof ColSpec cs) result.add(cs);
            }
        }
        return result;
    }

    // ========== ColSpec compilation ==========

    private record ColSpecResult(String alias, GenericType returnType,
                                  TypeInfo.WindowSpec windowSpec) {}

    private ColSpecResult compileColSpec(ColSpec cs,
                                          GenericType.Relation.Schema sourceSchema,
                                          TypeInfo source,
                                          TypeChecker.CompilationContext ctx,
                                          TypeInfo.OverSpec overSpec,
                                          NativeFunctionDef def) {
        String alias = cs.name();

        // --- fn1: compile via compileLambdaBody (ALL inner functions type-checked) ---
        if (cs.function1() == null || cs.function1().body().isEmpty()) {
            throw new PureCompileException(
                    "extend(): spec '" + alias + "' is missing expression lambda (fn1)");
        }
        LambdaFunction fn1 = cs.function1();

        // --- Signature-driven target typing ---
        // Extract lambda param types from the resolved extend() signature's FuncColSpec.
        // Uses T→Tuple(schema) bindings so _Window<T> resolves correctly.
        PType.FunctionType sigFnType = findColSpecFunctionType(def, fn1.parameters().size());
        var bindings = new Bindings();
        bindings.put("T", new GenericType.Tuple(sourceSchema));

        TypeChecker.CompilationContext fn1Ctx = ctx;
        for (int pi = 0; pi < fn1.parameters().size() && pi < sigFnType.paramTypes().size(); pi++) {
            String paramName = fn1.parameters().get(pi).name();
            PType sigType = sigFnType.paramTypes().get(pi).type();
            GenericType resolvedType = resolve(sigType, bindings,
                    "extend() lambda param " + pi);
            System.out.println("[DEBUG ExtendChecker] param " + pi + " '" + paramName
                    + "' sigType=" + sigType + " resolvedType=" + resolvedType
                    + " (" + resolvedType.getClass().getSimpleName() + ")");
            fn1Ctx = bindLambdaParam(fn1Ctx, paramName, resolvedType, source);
        }
        // compileLambdaBody → compileExpr → ScalarChecker → resolveOverload
        // for EVERY function call inside fn1 body (rowNumber, rank, lag, sum, etc.)
        TypeInfo fn1Result = compileLambdaBody(fn1, fn1Ctx);

        if (cs.function2() != null) {
            // === Aggregate extend (fn1 + fn2) — same as GroupByChecker ===
            return compileAggregateExtend(cs, fn1Result, source, ctx, overSpec);
        }

        // === Scalar/ranking/window fn1-only ===
        GenericType returnType = fn1Result.type() != null
                ? fn1Result.type() : GenericType.Primitive.NUMBER;

        // For window extends, look up the function resolved by ScalarChecker during
        // compileLambdaBody — no re-resolution needed.
        NativeFunctionDef resolvedFunc = lookupResolvedFunc(fn1);
        var ws = overSpec != null && resolvedFunc != null
                ? new TypeInfo.WindowSpec(resolvedFunc, overSpec, alias, returnType, null)
                : null;
        return new ColSpecResult(alias, returnType, ws);
    }

    /**
     * Finds the FunctionType from the resolved extend() def's ColSpec parameter
     * that matches the given lambda arity. Throws if not found.
     */
    private PType.FunctionType findColSpecFunctionType(NativeFunctionDef def, int lambdaArity) {
        for (var param : def.params()) {
            if (param.type() instanceof PType.Parameterized fp
                    && !fp.typeArgs().isEmpty()
                    && fp.typeArgs().get(0) instanceof PType.FunctionType ft) {
                String raw = fp.rawType();
                if ("FuncColSpec".equals(raw) || "AggColSpec".equals(raw)
                        || "FuncColSpecArray".equals(raw) || "AggColSpecArray".equals(raw)) {
                    if (ft.paramTypes().size() == lambdaArity) {
                        return ft;
                    }
                }
            }
        }
        throw new PureCompileException(
                "extend(): no ColSpec param in signature matches lambda arity " + lambdaArity);
    }

    /**
     * Aggregate extend: exact GroupByChecker pattern.
     * fn1 extracts value, fn2 applies aggregate.
     */
    private ColSpecResult compileAggregateExtend(ColSpec cs, TypeInfo fn1Result,
                                                  TypeInfo source,
                                                  TypeChecker.CompilationContext ctx,
                                                  TypeInfo.OverSpec overSpec) {
        String alias = cs.name();
        LambdaFunction fn2 = cs.function2();
        String fn2Param = fn2.parameters().isEmpty() ? null : fn2.parameters().get(0).name();
        TypeChecker.CompilationContext fn2Ctx = ctx.withLambdaParam(fn2Param, fn1Result.type());
        TypeInfo fn2Result = compileLambdaBody(fn2, fn2Ctx);

        // Look up aggregate function from fn2 body (stamped by ScalarChecker)
        var fn2Body = fn2.body().get(0);
        AppliedFunction innerAf = unwrapCast(fn2Body, "extend()");
        TypeInfo innerInfo = env.lookupCompiled(innerAf);
        if (innerInfo == null || innerInfo.resolvedFunc() == null) {
            throw new PureCompileException(
                    "extend(): aggregate function in '" + alias
                    + "' did not resolve — fn2 body must be a registered function");
        }
        NativeFunctionDef resolved = innerInfo.resolvedFunc();

        // Return type refinement
        GenericType returnType = fn2Result.type();
        if (returnType == GenericType.Primitive.NUMBER && fn1Result.type() != null
                && fn1Result.type().isNumeric()) {
            returnType = fn1Result.type();
        }
        GenericType castType = extractCastGenericType(fn2Body);

        // When there's no over() clause, aggregates need FUNC() OVER() (whole-relation).
        // Create an empty OverSpec so PlanGenerator generates the OVER() clause.
        var effectiveOver = overSpec != null ? overSpec
                : new TypeInfo.OverSpec(List.of(), List.of(), null);
        var ws = new TypeInfo.WindowSpec(resolved, effectiveOver, alias, returnType, castType);
        GenericType colType = castType != null ? castType : returnType;
        return new ColSpecResult(alias, colType, ws);
    }

    // ========== Over clause compilation ==========

    /**
     * Compiles over() clause. All sub-functions type-checked via resolveOverload.
     */
    private TypeInfo.OverSpec compileOverClause(AppliedFunction overAf,
                                                GenericType.Relation.Schema sourceSchema) {
        List<String> partitionBy = new ArrayList<>();
        List<TypeInfo.SortSpec> orderBy = new ArrayList<>();
        TypeInfo.FrameSpec frame = null;

        for (var p : overAf.parameters()) {
            if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                sourceSchema.requireColumn(cs.name());
                partitionBy.add(cs.name());
            } else if (p instanceof ClassInstance ci
                    && ci.value() instanceof ColSpecArray(List<ColSpec> colSpecs)) {
                for (ColSpec cs : colSpecs) {
                    sourceSchema.requireColumn(cs.name());
                    partitionBy.add(cs.name());
                }
            } else if (p instanceof PureCollection(List<ValueSpecification> values)) {
                for (var elem : values) {
                    var sortSpec = compileSortSpec(elem, sourceSchema);
                    if (sortSpec != null) orderBy.add(sortSpec);
                }
            } else if (p instanceof AppliedFunction paf) {
                String fn = simpleName(paf.function());
                var sortSpec = compileSortSpec(p, sourceSchema);
                if (sortSpec != null) {
                    orderBy.add(sortSpec);
                } else if ("rows".equals(fn)) {
                    resolveOverload("rows", paf.parameters(), null);
                    frame = compileFrameSpec("rows", paf.parameters());
                } else if ("range".equals(fn) || "_range".equals(fn)) {
                    resolveOverload("_range", paf.parameters(), null);
                    frame = compileFrameSpec("range", paf.parameters());
                }
            }
        }
        return new TypeInfo.OverSpec(partitionBy, orderBy, frame);
    }

    private TypeInfo.SortSpec compileSortSpec(ValueSpecification vs,
                                              GenericType.Relation.Schema sourceSchema) {
        if (vs instanceof AppliedFunction af) {
            String fn = simpleName(af.function());
            if ("asc".equals(fn) || "ascending".equals(fn)) {
                resolveOverload(fn, af.parameters(), null);
                String col = extractColumnName(af.parameters().get(0));
                sourceSchema.requireColumn(col);
                return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.ASC);
            } else if ("desc".equals(fn) || "descending".equals(fn)) {
                resolveOverload(fn, af.parameters(), null);
                String col = extractColumnName(af.parameters().get(0));
                sourceSchema.requireColumn(col);
                return new TypeInfo.SortSpec(col, TypeInfo.SortDirection.DESC);
            }
        }
        return null;
    }

    // ========== Function resolution helpers ==========

    /**
     * Looks up the resolved function from the types map for the inner function in fn1.
     * compileLambdaBody already compiled all inner functions via ScalarChecker,
     * which stamped resolvedFunc. We just look it up by AST node identity.
     *
     * <p>Handles 3 AST shapes produced by the parser:
     * <ul>
     *   <li>{@code $p->func($w,$r).property} → AppliedProperty wrapping AppliedFunction</li>
     *   <li>{@code wrapper(innerFunc($w,$r), N)} → nested AppliedFunction (e.g., round(cumDist(), 2))</li>
     *   <li>{@code func($r)} → direct AppliedFunction</li>
     * </ul>
     */
    private NativeFunctionDef lookupResolvedFunc(LambdaFunction fn1) {
        if (fn1.body().isEmpty()) return null;
        var body = fn1.body().get(0);

        // $p->func($w,$r).property → extract func from inside AppliedProperty
        if (body instanceof AppliedProperty ap && !ap.parameters().isEmpty()
                && ap.parameters().get(0) instanceof AppliedFunction af) {
            TypeInfo info = env.lookupCompiled(af);
            return info != null ? info.resolvedFunc() : null;
        }
        // Direct or wrapper function call
        if (body instanceof AppliedFunction af) {
            // wrapper(innerFunc($w,$r), N) → look up inner first
            if (!af.parameters().isEmpty()
                    && af.parameters().get(0) instanceof AppliedFunction inner) {
                var defs = BuiltinFunctionRegistry.instance().resolve(simpleName(inner.function()));
                if (!defs.isEmpty()) {
                    TypeInfo info = env.lookupCompiled(inner);
                    return info != null ? info.resolvedFunc() : null;
                }
            }
            TypeInfo info = env.lookupCompiled(af);
            return info != null ? info.resolvedFunc() : null;
        }
        return null;
    }

    // ========== Frame spec compilation ==========

    private TypeInfo.FrameSpec compileFrameSpec(String frameType, List<ValueSpecification> params) {
        TypeInfo.FrameBound start = resolveFrameBound(params, 0);
        TypeInfo.FrameBound end = resolveFrameBound(params, 1);
        validateFrameBounds(start, end);
        return new TypeInfo.FrameSpec(frameType, start, end);
    }

    private TypeInfo.FrameBound resolveFrameBound(List<ValueSpecification> params, int idx) {
        if (idx >= params.size()) {
            return idx == 0 ? TypeInfo.FrameBound.unbounded() : TypeInfo.FrameBound.currentRow();
        }
        var param = params.get(idx);
        if (param instanceof AppliedFunction af && "unbounded".equals(simpleName(af.function()))) {
            resolveOverload("unbounded", af.parameters(), null);
            return TypeInfo.FrameBound.unbounded();
        }
        if (param instanceof AppliedFunction af && "minus".equals(simpleName(af.function()))) {
            if (!af.parameters().isEmpty()) {
                double v = extractNumericValue(af.parameters().get(af.parameters().size() - 1));
                return TypeInfo.FrameBound.offset(-v);
            }
        }
        double v = extractNumericValue(param);
        if (v == 0) return TypeInfo.FrameBound.currentRow();
        return TypeInfo.FrameBound.offset(v);
    }

    private void validateFrameBounds(TypeInfo.FrameBound start, TypeInfo.FrameBound end) {
        double startPos = boundPosition(start, true);
        double endPos = boundPosition(end, false);
        if (startPos > endPos) {
            throw new PureCompileException(
                    "Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!");
        }
    }

    private double boundPosition(TypeInfo.FrameBound bound, boolean isStart) {
        return switch (bound.type()) {
            case UNBOUNDED -> isStart ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            case CURRENT_ROW -> 0;
            case OFFSET -> bound.offset();
        };
    }

    // ========== Shared utilities ==========

    private List<ColSpec> extractColSpecs(ClassInstance ci) {
        if (ci.value() instanceof ColSpecArray(List<ColSpec> specs)) return specs;
        if (ci.value() instanceof ColSpec cs) return List.of(cs);
        throw new PureCompileException(
                "extend() parameter must be ~col:... or ~[...], got: "
                        + ci.value().getClass().getSimpleName());
    }


    private double extractNumericValue(ValueSpecification vs) {
        if (vs instanceof CInteger(Number value)) return value.doubleValue();
        if (vs instanceof CFloat(double value)) return value;
        if (vs instanceof CDecimal(java.math.BigDecimal value)) return value.doubleValue();
        throw new PureCompileException(
                "Expected numeric literal, got: " + vs.getClass().getSimpleName());
    }
}
