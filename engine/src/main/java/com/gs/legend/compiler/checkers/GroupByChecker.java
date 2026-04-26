package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedAggCall;
import com.gs.legend.compiler.typed.TypedCast;
import com.gs.legend.compiler.typed.TypedColumnGroupKey;
import com.gs.legend.compiler.typed.TypedExpressionGroupKey;
import com.gs.legend.compiler.typed.TypedGroupBy;
import com.gs.legend.compiler.typed.TypedGroupKey;
import com.gs.legend.compiler.typed.TypedLambda;
import com.gs.legend.compiler.typed.TypedNativeCall;
import com.gs.legend.compiler.typed.TypedParam;
import com.gs.legend.compiler.typed.TypedPropertyAccess;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

import java.util.*;

/**
 * Type checker for {@code groupBy()} — Relation API and class-source.
 *
 * <p>Supports the 4 Relation overloads from legend-engine plus a class-source overload:
 * <pre>
 * groupBy(r:Relation<T>, cols:ColSpec<Z⊆T>,       agg:AggColSpec<...>)      → Relation<Z+R>
 * groupBy(r:Relation<T>, cols:ColSpecArray<Z⊆T>,  agg:AggColSpec<...>)      → Relation<Z+R>
 * groupBy(r:Relation<T>, cols:ColSpec<Z⊆T>,       agg:AggColSpecArray<...>) → Relation<Z+R>
 * groupBy(r:Relation<T>, cols:ColSpecArray<Z⊆T>,  agg:AggColSpecArray<...>) → Relation<Z+R>
 * groupBy(cl:C[*],       keys:FuncColSpecArray,   aggs:AggColSpecArray)     → Relation<Z+R>
 * </pre>
 *
 * <p>The class-source overload follows the same pattern as
 * {@link ProjectChecker}: FuncColSpec keys carry extraction lambdas,
 * compiled against the ClassType. Produces projections in TypeInfo for PlanGenerator.
 *
 * @see AggregateChecker
 */
public class GroupByChecker extends AbstractChecker {

    public GroupByChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedGroupBy check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("groupBy", params, source);

        // Legacy TDS desugar: rewrite arity-4 form and recompile through the
        // modern path. Branches on source type: ClassType → FuncColSpec keys
        // (extraction lambdas); Relation → bare-name ColSpec keys.
        if (def.arity() == 4) {
            boolean classSource = source.type() instanceof Type.ClassType;
            AppliedFunction rewritten = rewriteLegacyGroupBy(af,
                    (PureCollection) params.get(1), (PureCollection) params.get(2),
                    (PureCollection) params.get(3), classSource);
            // Recompile lands back in this checker's modern arity-3 branch.
            return (TypedGroupBy) env.compileExpr(rewritten, ctx);
        }

        unify(def, source.expressionType());

        if (source.type() instanceof Type.ClassType) {
            return checkClassSource(af, def, source, ctx);
        }

        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException(
                    "groupBy() requires a Relation source with a known schema");
        }

        Map<String, Type> resultColumns = new LinkedHashMap<>();
        List<TypedGroupKey> keys = new ArrayList<>();
        List<TypedAggCall> aggs = new ArrayList<>();

        // Group columns: bare ~col / ~[col1,col2] — validated against source.
        for (String col : extractColumnNames(params.get(1))) {
            if (!sourceSchema.columns().containsKey(col)) {
                throw new PureCompileException(
                        "groupBy(): group column '" + col + "' not found in source. Available: "
                                + sourceSchema.columns().keySet());
            }
            resultColumns.put(col, sourceSchema.columns().get(col));
            keys.add(new TypedColumnGroupKey(col, col));
        }

        // Aggregate columns: ~alias : fn1 : fn2.
        Type fn1ParamType = new Type.Relation(sourceSchema);
        for (ColSpec cs : extractAggColSpecs(params.get(2))) {
            TypedAggCall agg = compileTypedAggCall(cs, fn1ParamType, source, ctx);
            resultColumns.put(agg.alias(),
                    agg.castType().orElse(agg.returnType()));
            aggs.add(agg);
        }

        if (resultColumns.isEmpty()) {
            throw new PureCompileException("groupBy() produced no output columns");
        }

        var schema = Type.Schema.withoutPivot(resultColumns);
        return new TypedGroupBy(source, keys, aggs,
                ExpressionType.one(new Type.Relation(schema)));
    }

    // ========== Class-source compilation ==========

    /**
     * Handles the class-source overload: {@code groupBy(cl:C[*], keys:FuncColSpecArray, aggs:AggColSpecArray)}.
     *
     * <p>Follows the same pattern as {@link ProjectChecker}: FuncColSpec keys carry
     * extraction lambdas compiled against the ClassType. Produces {@link TypeInfo#projections()}
     * so PlanGenerator can resolve key columns via StoreResolution.
     */
    private TypedGroupBy checkClassSource(AppliedFunction af, NativeFunctionDef def,
                                       TypedSpec source,
                                       TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        var bindings = unify(def, source.expressionType());

        Type.FunctionType ft = extractFunctionType(def.params().get(1));
        Type resolvedParamType = resolve(ft.params().get(0).type(), bindings,
                "groupBy() key lambda param");

        Map<String, Type> resultColumns = new LinkedHashMap<>();
        List<TypedGroupKey> keys = new ArrayList<>();

        // Key columns: FuncColSpec with extraction lambdas compiled against
        // the ClassType. Association paths are read off the typed body
        // (was previously a sidecar lookup).
        for (ColSpec cs : extractColSpecs(params.get(1))) {
            String alias = cs.name();
            LambdaFunction lambda = cs.function1();
            if (lambda == null) {
                // Bare ~prop — synthesize identity lambda {x | $x.prop}.
                lambda = new LambdaFunction(
                        List.of(new Variable("x")),
                        List.of(new AppliedProperty(alias, List.of(new Variable("x")))));
            }
            String paramName = lambda.parameters().isEmpty() ? "x"
                    : lambda.parameters().get(0).name();
            TypeChecker.CompilationContext lambdaCtx = bindLambdaParam(
                    ctx, paramName, resolvedParamType, source);
            TypedSpec body = compileLambdaBody(lambda, lambdaCtx);

            List<String> associationPath = null;
            if (body instanceof TypedPropertyAccess tpa && tpa.associationPath().isPresent()) {
                associationPath = tpa.associationPath().get();
            }
            TypedLambda keyFn = buildTypedLambda(lambda, resolvedParamType, body);
            if (associationPath != null) {
                keys.add(new com.gs.legend.compiler.typed.TypedAssociationGroupKey(
                        associationPath, alias));
            } else {
                keys.add(new TypedExpressionGroupKey(keyFn, alias));
            }
            resultColumns.put(alias, body.type());
        }

        // Aggregate columns: compile fn1 against the ClassType.
        for (ColSpec cs : extractAggColSpecs(params.get(2))) {
            TypedAggCall agg = compileTypedAggCall(cs, resolvedParamType, source, ctx);
            resultColumns.put(agg.alias(),
                    agg.castType().orElse(agg.returnType()));
            // Class-source aggs get added below.
        }
        // Re-collect aggs (keep ordering consistent with resultColumns).
        List<TypedAggCall> aggs = new ArrayList<>();
        for (ColSpec cs : extractAggColSpecs(params.get(2))) {
            aggs.add(compileTypedAggCall(cs, resolvedParamType, source, ctx));
        }

        if (resultColumns.isEmpty()) {
            throw new PureCompileException("groupBy() produced no output columns");
        }

        var schema = Type.Schema.withoutPivot(resultColumns);
        return new TypedGroupBy(source, keys, aggs,
                ExpressionType.one(new Type.Relation(schema)));
    }

    // ========== Aggregate column compilation ==========

    /**
     * Compiles a single aggregate ColSpec into an AggColumnSpec.
     *
     * <p>PCT pattern: {@code ~alias : x | $x.sourceCol : y | $y->aggFunc(args)}
     * <br>Parser produces: {@code ColSpec("alias", fn1={x→$x.sourceCol}, fn2={y→$y->aggFunc(args)})}
     *
     * <p>fn1 body can be:
     * <ul>
     *   <li>Simple property access: {@code $x.col}</li>
     *   <li>Expression: {@code $x.price * $x.quantity}</li>
     *   <li>rowMapper: {@code rowMapper($x.col1, $x.col2)} (for wavg, corr, etc.)</li>
     * </ul>
     */
    /**
     * Compiles a single aggregate ColSpec into a {@link TypedAggCall}. This is
     * the shared helper used by both {@link GroupByChecker} and
     * {@link AggregateChecker}.
     *
     * <p>Resolves the aggregate function from the fn2 body: if fn2's body
     * compiles to a {@link TypedNativeCall}, its {@code func} IS the aggregate.
     * No sidecar lookup; the typed tree carries the resolution.
     */
    TypedAggCall compileTypedAggCall(ColSpec cs, Type fn1ParamType,
                                     TypedSpec source,
                                     TypeChecker.CompilationContext ctx) {
        String alias = cs.name();

        if (cs.function1() == null || cs.function1().body().isEmpty()) {
            throw new PureCompileException(
                    "groupBy(): aggregate spec '" + alias
                            + "' is missing value extraction lambda (fn1)");
        }
        LambdaFunction fn1 = cs.function1();
        String fn1Param = fn1.parameters().isEmpty() ? null
                : fn1.parameters().get(0).name();
        TypeChecker.CompilationContext fn1Ctx = bindLambdaParam(
                ctx, fn1Param, fn1ParamType, source);
        TypedSpec fn1Body = compileLambdaBody(fn1, fn1Ctx);
        TypedLambda fn1Typed = buildTypedLambda(fn1, fn1ParamType, fn1Body);

        if (cs.function2() == null || cs.function2().body().isEmpty()) {
            throw new PureCompileException(
                    "groupBy(): aggregate spec '" + alias
                            + "' is missing aggregate lambda (fn2)");
        }
        LambdaFunction fn2 = cs.function2();
        String fn2Param = fn2.parameters().isEmpty() ? null
                : fn2.parameters().get(0).name();
        TypeChecker.CompilationContext fn2Ctx = ctx.withLambdaParam(
                fn2Param, fn1Body.type());
        TypedSpec fn2Body = compileLambdaBody(fn2, fn2Ctx);
        TypedLambda fn2Typed = buildTypedLambda(fn2, fn1Body.type(), fn2Body);

        // The aggregate function lives in the fn2 body — if it compiled to a
        // {@link TypedNativeCall}, its {@code func} is the resolved def.
        // Unwrap any outer cast() wrapper first. Cast may compile to either
        // {@link TypedCast} (the dedicated coercion node) or to a
        // {@link TypedNativeCall} for {@code cast} — handle both shapes.
        TypedSpec inner = fn2Body;
        Type castType = null;
        if (fn2.body().get(0) instanceof AppliedFunction outerAf
                && "cast".equals(simpleName(outerAf.function()))) {
            castType = extractCastGenericType(fn2.body().get(0));
            if (inner instanceof TypedCast tc) {
                inner = tc.expr();
            } else if (inner instanceof TypedNativeCall tnc && !tnc.args().isEmpty()) {
                inner = tnc.args().get(0);
            }
        }
        NativeFunctionDef resolved = inner instanceof TypedNativeCall tnc
                ? tnc.func() : null;
        if (resolved == null) {
            throw new PureCompileException(
                    "groupBy(): aggregate function in '" + alias
                            + "' did not resolve — fn2 body must be a registered function");
        }

        // Extra reducer operands: multi-operand aggregates carry their
        // additional args inside fn2's body (e.g., joinStrings(values, sep)
        // — the separator is at args[1]; corr(x, y), percentile(values, p)).
        // The first arg is the fn2 lambda parameter ({@code $y}) — that's
        // what fn1 produces. Args 1..N are the typed extra operands; pass
        // them through to lowering so each variant can be constructed with
        // every operand it needs (see SqlAggregate multi-operand records).
        List<TypedSpec> extraArgs = inner instanceof TypedNativeCall tnc && tnc.args().size() > 1
                ? List.copyOf(tnc.args().subList(1, tnc.args().size()))
                : List.of();

        // rowMapper unpacking: aggregates like corr/covar/maxBy/minBy/wavg
        // have a Pure overload that takes a single {@code rowMapper(value, key)}
        // bundling two columns. We rewrite this here into the canonical
        // 2-operand form: fn1's body becomes just the value (rowMapper.args[0])
        // and the key (rowMapper.args[1]) is added to extraArgs. Bindings then
        // see args[0] = value, args[1] = key uniformly with the 2-arg numeric
        // overloads (corr(x, y), covarSample(x, y), maxBy(values, keys)).
        // The resolved NativeFunctionDef stays the rowMapper one; AggregateBindings
        // binds both overloads to the same constructor.
        if (fn1Body instanceof TypedNativeCall fn1Tnc
                && fn1Tnc.func() == com.gs.legend.compiler.Pure.ROW_MAPPER__T_0_1__U_0_1
                && fn1Tnc.args().size() == 2) {
            TypedSpec value = fn1Tnc.args().get(0);
            TypedSpec key   = fn1Tnc.args().get(1);
            fn1Typed = new TypedLambda(fn1Typed.parameters(),
                    java.util.List.of(value), value.info());
            extraArgs = java.util.List.of(key);
        }

        // Return type refinement: generic {@code Number} aggregates preserve
        // the fn1 input numeric type. See matching comment in
        // {@code ExtendChecker.compileAggregateExtend}.
        Type returnType = fn2Body.type();
        if (returnType == Primitive.NUMBER && fn1Body.type() != null
                && fn1Body.type().isNumeric()) {
            returnType = fn1Body.type();
        }

        return new TypedAggCall(alias, resolved, fn1Typed, fn2Typed, extraArgs,
                returnType, Optional.ofNullable(castType));
    }

    /**
     * Builds a {@link TypedLambda} wrapper from a raw AST lambda plus its
     * compiled body. Used when the shared {@code compileLambdaArg} pipeline
     * doesn't apply (e.g., ColSpec-scoped lambdas with custom param types).
     */
    static TypedLambda buildTypedLambda(LambdaFunction lambda, Type paramType,
                                         TypedSpec body) {
        List<TypedParam> params = new ArrayList<>(lambda.parameters().size());
        for (var v : lambda.parameters()) {
            params.add(new TypedParam(v.name(), paramType, Multiplicity.ONE));
        }
        return new TypedLambda(params, List.of(body), body.expressionType());
    }

    // ========== Legacy TDS Desugaring ==========

    /**
     * Rewrites legacy TDS arity-4 groupBy to Relation DSL arity-3.
     *
     * <pre>
     * groupBy(source, [{r|$r.dept}], [agg({r|$r.sal}, {y|$y->sum()})], ['dept', 'totalSal'])
     *   → groupBy(source, ~[dept], ~[totalSal : {r|$r.sal} : {y|$y->sum()}])
     * </pre>
     *
     * <p>Aliases are split: first N for key columns, remaining for agg columns.
     */
    private static AppliedFunction rewriteLegacyGroupBy(
            AppliedFunction af, PureCollection keyFns, PureCollection aggs, PureCollection aliases,
            boolean funcColSpecKeys) {
        List<ValueSpecification> keyList = keyFns.values();
        List<ValueSpecification> aggList = aggs.values();
        List<ValueSpecification> aliasList = aliases.values();

        int expectedAliases = keyList.size() + aggList.size();
        if (aliasList.size() != expectedAliases) {
            throw new PureCompileException(
                    "groupBy() legacy syntax: expected " + expectedAliases + " aliases ("
                            + keyList.size() + " keys + " + aggList.size() + " aggs), got " + aliasList.size());
        }

        // Key columns: FuncColSpec (with lambda) for class source, bare ColSpec for Relation
        List<ColSpec> keyCols = new ArrayList<>();
        for (int i = 0; i < keyList.size(); i++) {
            if (!(aliasList.get(i) instanceof CString cs))
                throw new PureCompileException(
                        "groupBy() legacy syntax: key alias[" + i + "] must be a String literal");
            if (funcColSpecKeys && keyList.get(i) instanceof LambdaFunction lambda) {
                keyCols.add(new ColSpec(cs.value(), lambda));
            } else {
                keyCols.add(new ColSpec(cs.value()));
            }
        }

        // Agg columns: ColSpec(alias, mapFn, aggFn) — extracted from agg(mapFn, aggFn) calls
        List<ColSpec> aggCols = new ArrayList<>();
        for (int i = 0; i < aggList.size(); i++) {
            int aliasIdx = keyList.size() + i;
            if (!(aliasList.get(aliasIdx) instanceof CString cs))
                throw new PureCompileException(
                        "groupBy() legacy syntax: agg alias[" + i + "] must be a String literal");
            if (!(aggList.get(i) instanceof AppliedFunction aggCall) || !"agg".equals(aggCall.function()))
                throw new PureCompileException(
                        "groupBy() legacy syntax: agg[" + i + "] must be agg(mapFn, aggFn)");
            List<ValueSpecification> aggParams = aggCall.parameters();
            if (aggParams.size() != 2
                    || !(aggParams.get(0) instanceof LambdaFunction mapFn)
                    || !(aggParams.get(1) instanceof LambdaFunction aggFn))
                throw new PureCompileException(
                        "groupBy() legacy syntax: agg[" + i + "] must have exactly 2 lambda params");
            aggCols.add(new ColSpec(cs.value(), mapFn, aggFn));
        }

        // Build rewritten arity-3: groupBy(source, ColSpecArray(keyCols), ColSpecArray(aggCols))
        var keyArray = new ColSpecArray(keyCols);
        var aggArray = new ColSpecArray(aggCols);

        return new AppliedFunction(
                af.function(),
                List.of(af.parameters().get(0), keyArray, aggArray),
                af.hasReceiver(),
                af.sourceText(),
                af.argTexts());
    }

    // ========== Helpers ==========

    /** Extracts ColSpec list from a FuncColSpecArray parameter (keys with lambdas). */
    private static List<ColSpec> extractColSpecs(ValueSpecification param) {
        if (param instanceof ColSpecArray(List<ColSpec> specs)) {
            return specs;
        }
        throw new PureCompileException(
                "groupBy() class-source keys must be a FuncColSpecArray (~[...]), got "
                        + param.getClass().getSimpleName());
    }

    /**
     * Extracts ColSpec list from AggColSpec or AggColSpecArray parameter.
     */
    public static List<ColSpec> extractAggColSpecs(ValueSpecification vs) {
        if (vs instanceof com.gs.legend.ast.ColumnInstance ci) {
            if (ci instanceof ColSpecArray(List<ColSpec> specs)) {
                return specs;
            }
            if (ci instanceof ColSpec cs) {
                return List.of(cs);
            }
        }
        throw new PureCompileException(
                "groupBy() aggregate parameter must be ~col:... or ~[...], got: "
                        + vs.getClass().getSimpleName());
    }
}
