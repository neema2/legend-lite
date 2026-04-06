package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

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
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        NativeFunctionDef def = resolveOverload("groupBy", params, source);

        // Legacy TDS desugar: groupBy([keyFns], [agg(mapFn, aggFn)], ['aliases'])
        //                   → groupBy(~[keyCols], ~[aggAlias:fn1:fn2])
        // Branches on source type:
        //   ClassType → FuncColSpec keys (with extraction lambdas) → matches class-source overload
        //   Relation  → bare-name ColSpec keys → matches existing Relation overloads
        if (def.arity() == 4) {
            boolean classSource = source.type() instanceof GenericType.ClassType;
            AppliedFunction rewritten = rewriteLegacyGroupBy(af,
                    (PureCollection) params.get(1), (PureCollection) params.get(2),
                    (PureCollection) params.get(3), classSource);
            TypeInfo result = env.compileExpr(rewritten, ctx);
            return TypeInfo.from(result).inlinedBody(rewritten).build();
        }

        unify(def, source.expressionType()); // validate source matches signature generics

        // Class-source overload: groupBy(cl:C[*], keys:FuncColSpecArray, aggs:AggColSpecArray)
        // Keys have extraction lambdas (like project) — produces projections for PlanGenerator
        if (source.type() instanceof GenericType.ClassType) {
            return checkClassSource(af, def, source, ctx);
        }

        // Relation-source overloads: bare-name keys validated against source schema
        GenericType.Relation.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException(
                    "groupBy() requires a Relation source with a known schema");
        }

        Map<String, GenericType> resultColumns = new LinkedHashMap<>();
        List<TypeInfo.ColumnSpec> groupCols = new ArrayList<>();
        List<TypeInfo.AggColumnSpec> aggCols = new ArrayList<>();

        // --- Group columns (param 1): ~col or ~[col1, col2] ---
        List<String> groupColNames = extractColumnNames(params.get(1));
        for (String col : groupColNames) {
            if (!sourceSchema.columns().containsKey(col)) {
                throw new PureCompileException(
                        "groupBy(): group column '" + col + "' not found in source. Available: "
                                + sourceSchema.columns().keySet());
            }
            resultColumns.put(col, sourceSchema.columns().get(col));
            groupCols.add(TypeInfo.ColumnSpec.col(col));
        }

        // --- Aggregate columns (param 2): AggColSpec or AggColSpecArray ---
        GenericType fn1ParamType = new GenericType.Relation(sourceSchema);
        List<ColSpec> aggSpecs = extractAggColSpecs(params.get(2));
        for (ColSpec cs : aggSpecs) {
            TypeInfo.AggColumnSpec acs = compileAggColSpec(cs, fn1ParamType, source, ctx);
            resultColumns.put(acs.alias(), acs.castType() != null ? acs.castType() : acs.returnType());
            aggCols.add(acs);
        }

        if (resultColumns.isEmpty()) {
            throw new PureCompileException("groupBy() produced no output columns");
        }

        var schema = GenericType.Relation.Schema.withoutPivot(resultColumns);
        return TypeInfo.builder()
                .columnSpecs(groupCols)
                .aggColumnSpecs(aggCols)
                .expressionType(ExpressionType.one(new GenericType.Relation(schema)))
                .build();
    }

    // ========== Class-source compilation ==========

    /**
     * Handles the class-source overload: {@code groupBy(cl:C[*], keys:FuncColSpecArray, aggs:AggColSpecArray)}.
     *
     * <p>Follows the same pattern as {@link ProjectChecker}: FuncColSpec keys carry
     * extraction lambdas compiled against the ClassType. Produces {@link TypeInfo#projections()}
     * so PlanGenerator can resolve key columns via StoreResolution.
     */
    private TypeInfo checkClassSource(AppliedFunction af, NativeFunctionDef def,
                                      TypeInfo source, TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        var bindings = unify(def, source.expressionType());

        // Resolve lambda param type from signature (C[1])
        PType.FunctionType ft = extractFunctionType(def.params().get(1));
        GenericType resolvedParamType = resolve(ft.paramTypes().get(0).type(), bindings,
                "groupBy() key lambda param");

        Map<String, GenericType> resultColumns = new LinkedHashMap<>();
        List<TypeInfo.ColumnSpec> groupCols = new ArrayList<>();
        List<TypeInfo.ProjectionSpec> projectionSpecs = new ArrayList<>();

        // --- Key columns: FuncColSpec with extraction lambdas ---
        List<ColSpec> keyColSpecs = extractColSpecs(params.get(1));
        for (ColSpec cs : keyColSpecs) {
            String alias = cs.name();
            LambdaFunction lambda = cs.function1();
            if (lambda == null) {
                // Simple column reference: ~prop → synthesize identity lambda
                lambda = new LambdaFunction(
                        List.of(new Variable("x")),
                        List.of(new AppliedProperty(alias, List.of(new Variable("x")))));
            }
            String paramName = lambda.parameters().isEmpty() ? "x"
                    : lambda.parameters().get(0).name();
            TypeChecker.CompilationContext lambdaCtx = bindLambdaParam(
                    ctx, paramName, resolvedParamType, source);
            TypeInfo bodyType = compileLambdaBody(lambda, lambdaCtx);
            TypeInfo bodyInfo = env.lookupCompiled(lambda.body().get(0));
            List<String> associationPath = bodyInfo != null ? bodyInfo.associationPath() : null;

            resultColumns.put(alias, bodyType.type());
            groupCols.add(TypeInfo.ColumnSpec.col(alias));
            projectionSpecs.add(new TypeInfo.ProjectionSpec(associationPath, alias));
        }

        // --- Aggregate columns: compile fn1 against ClassType ---
        List<ColSpec> aggSpecs = extractAggColSpecs(params.get(2));
        List<TypeInfo.AggColumnSpec> aggCols = new ArrayList<>();
        for (ColSpec cs : aggSpecs) {
            TypeInfo.AggColumnSpec acs = compileAggColSpec(cs, resolvedParamType, source, ctx);
            resultColumns.put(acs.alias(), acs.castType() != null ? acs.castType() : acs.returnType());
            aggCols.add(acs);
        }

        if (resultColumns.isEmpty()) {
            throw new PureCompileException("groupBy() produced no output columns");
        }

        var schema = GenericType.Relation.Schema.withoutPivot(resultColumns);
        return TypeInfo.builder()
                .projections(projectionSpecs)
                .columnSpecs(groupCols)
                .aggColumnSpecs(aggCols)
                .expressionType(ExpressionType.one(new GenericType.Relation(schema)))
                .build();
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
    TypeInfo.AggColumnSpec compileAggColSpec(ColSpec cs,
                                             GenericType fn1ParamType,
                                             TypeInfo source,
                                             TypeChecker.CompilationContext ctx) {
        String alias = cs.name();

        // --- fn1: compile map lambda ---
        if (cs.function1() == null || cs.function1().body().isEmpty()) {
            throw new PureCompileException(
                    "groupBy(): aggregate spec '" + alias + "' is missing value extraction lambda (fn1)");
        }
        LambdaFunction fn1 = cs.function1();
        String fn1Param = fn1.parameters().isEmpty() ? null : fn1.parameters().get(0).name();
        TypeChecker.CompilationContext fn1Ctx = bindLambdaParam(ctx, fn1Param, fn1ParamType, source);
        TypeInfo fn1Result = compileLambdaBody(fn1, fn1Ctx);

        // --- fn2: compile reduce lambda ---
        if (cs.function2() == null || cs.function2().body().isEmpty()) {
            throw new PureCompileException(
                    "groupBy(): aggregate spec '" + alias + "' is missing aggregate lambda (fn2)");
        }
        LambdaFunction fn2 = cs.function2();
        String fn2Param = fn2.parameters().isEmpty() ? null : fn2.parameters().get(0).name();
        // fn2 param has type K[*] — the collection of mapped values
        TypeChecker.CompilationContext fn2Ctx = ctx.withLambdaParam(fn2Param, fn1Result.type());
        TypeInfo fn2Result = compileLambdaBody(fn2, fn2Ctx);

        // --- Resolve aggregate function from fn2 body (stamped by ScalarChecker) ---
        var fn2Body = fn2.body().get(0);
        AppliedFunction innerAf = unwrapCast(fn2Body, "groupBy()");
        TypeInfo innerInfo = env.lookupCompiled(innerAf);
        if (innerInfo == null || innerInfo.resolvedFunc() == null) {
            throw new PureCompileException(
                    "groupBy(): aggregate function in '" + alias
                    + "' did not resolve — fn2 body must be a registered function");
        }
        NativeFunctionDef resolved = innerInfo.resolvedFunc();

        // --- Extract cast type (if fn2 is wrapped in cast()) ---
        GenericType castType = extractCastGenericType(fn2Body);

        // --- Return type: use fn2 result, refined by cast if present ---
        GenericType returnType = fn2Result.type();
        // For type-variable aggregates (min, max, sum, etc.), refine to source column type
        if (returnType == GenericType.Primitive.NUMBER && fn1Result.type() != null
                && fn1Result.type().isNumeric()) {
            returnType = fn1Result.type();
        }

        return new TypeInfo.AggColumnSpec(alias, resolved, returnType, castType);
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
        var keyArray = new ClassInstance("colSpecArray", new ColSpecArray(keyCols));
        var aggArray = new ClassInstance("colSpecArray", new ColSpecArray(aggCols));

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
        if (param instanceof ClassInstance ci && ci.value() instanceof ColSpecArray(List<ColSpec> specs)) {
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
        if (vs instanceof ClassInstance ci) {
            if (ci.value() instanceof ColSpecArray(List<ColSpec> specs)) {
                return specs;
            }
            if (ci.value() instanceof ColSpec cs) {
                return List.of(cs);
            }
        }
        throw new PureCompileException(
                "groupBy() aggregate parameter must be ~col:... or ~[...], got: "
                        + vs.getClass().getSimpleName());
    }
}
