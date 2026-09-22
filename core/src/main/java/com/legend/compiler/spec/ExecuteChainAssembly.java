// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedEval;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The {@code execute(f, mapping, runtime, ext)} CHAIN ASSEMBLY — the
 * compiler half of frame building (Invariant 7: minting typed nodes is
 * compiler work). {@link #prepare} peels the query argument to its
 * lambda and validates the mapping reference; {@link #chain} inlines the
 * body and attaches the execution context as a {@link TypedFrom}. The
 * executor interleaves its execution-bound steps between the two calls
 * (runtime-argument effects, table-replace recording) and owns the
 * eager run — WHEN a frame's value exists is the executor's; WHAT the
 * chain means is the compiler's.
 */
public final class ExecuteChainAssembly {

    private ExecuteChainAssembly() {
    }

    /** The peeled, validated query half: the zero-arg query lambda and
     * the explicit mapping reference (null under the empty-mapping
     * sentinel {@code ^Mapping(name='')} — every branch then carries
     * its own {@code ->from()}). */
    public record Prepared(TypedLambda lam,
            @com.legend.base.Nullable TypedPackageableRef mref) {
    }

    /** The assembled chain and whether its ROOT is relation-shaped (the
     * engine's {@code Result.values} for a TDS query holds ONE TDS; for
     * a class or scalar root, values IS the collection). */
    public record Chain(TypedSpec chain, boolean relationRooted) {
    }


    /** Peel the query argument to its zero-arg lambda (β-inline a
     * lambda-building user call; read through preval/withFeatureFlags
     * plan-time wrappers; fold concatenateTemporalTdsQueries BY
     * EMISSION) and validate the mapping argument. */
    public static Prepared prepare(TypedNativeCall ec,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        TypedSpec q = Lets.bound(ec.args().get(0), letPrefix);
        // a LAMBDA-BUILDING user call in query position (corpus
        // buildQuery(value) returning FunctionDefinition<{->Person[*]}>):
        // β-inline it — the body's single expression IS the lambda literal
        if (q instanceof TypedUserCall) {
            q = new UserCallInliner(specs).inlineBody(List.of(q)).get(0);
        }
        // preval(query, extensions) / withFeatureFlags(query, flags):
        // plan-time wrappers, IDENTITY for row semantics — read through
        // to the wrapped query lambda. RECEIPTS: withFeatureFlags's body
        // IS `$object` (executionPlanFeature.pure:27); preval is a
        // semantics-preserving partial evaluation of the function
        // (preeval.pure:63-92 — constants folded, the same function
        // returned in a PrevalWrapper), so the rows are the wrapped
        // query's rows.
        while (q instanceof TypedNativeCall pv
                && (com.legend.builtin.NativeFn.Handle.of(pv.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.Handle.PREVAL
                    || com.legend.builtin.NativeFn.PlanWrapper.of(pv.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.PlanWrapper.WITH_FEATURE_FLAGS)) {
            q = Lets.bound(pv.args().get(0), letPrefix);
        }
        // if(<literal>, |{|q1}, |{|q2}): a query lambda SELECTED by a
        // compile-time constant (the corpus's checked/unchecked helper —
        // testSubTypeGraphFetch Impl(checked, expected), the flag a literal
        // after inlining): the taken branch IS the query. Orchestration
        // picks the query; no value is computed. A non-literal condition
        // stays on the loud wall below.
        q = peelSelections(q, letPrefix);
        // concatenateTemporalTdsQueries(lfs): the real body folds the
        // queries into concatenate SFEs (reflection metamodel) — the SAME
        // semantics BY EMISSION: fold the lambdas' result expressions
        // into a TypedConcatenate chain under one zero-arg lambda.
        if (q instanceof TypedNativeCall cq
                && com.legend.builtin.NativeFn.PlanWrapper.of(cq.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.PlanWrapper.CONCATENATE_TEMPORAL_TDS_QUERIES) {
            q = concatenateFold(cq, letPrefix, specs);
        }
        if (!(q instanceof TypedLambda lam) || !lam.parameters().isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "execute() whose query argument is not a lambda");
        }
        TypedSpec mArg = Lets.bound(ec.args().get(1), letPrefix);
        // the EMPTY-MAPPING SENTINEL ^Mapping(name='') (testFrom.pure:30):
        // every branch carries its own ->from() — no explicit mapping to
        // attach; the chain's from() walls stay the honest failure
        boolean sentinelMapping = mArg instanceof TypedNewInstance sni
                && "meta::pure::mapping::Mapping".equals(sni.classFqn());
        TypedPackageableRef mref = null;
        if (!sentinelMapping) {
            if (!(mArg instanceof TypedPackageableRef mr)) {
                throw new com.legend.error.NotImplementedException(
                        "execute() mapping argument must be a mapping reference");
            }
            mref = mr;
        }
        return new Prepared(lam, mref);
    }

    /**
     * The {@code meta::legend::executeLegendQuery(f, vars, [exeCtx,] ext)}
     * half of {@link #prepare} — the ROUTER'S STRING ENTRY (real engine
     * devUtils.pure:30/:35 &rarr; {@code meta::legend::execute}). The
     * query lambda's PARAMETERS bind from the vars pair list as LEADING
     * LETS, each coerced by the parameter's DECLARED type exactly as the
     * engine's execute entry coerces its JSON-borne variable values: an
     * enum-typed parameter takes the string as the enum VALUE name, a
     * date-typed parameter parses the string as a date literal,
     * everything else passes through. Lets rather than &beta;-substitution:
     * the variables keep their source spelling, which the graph-tree
     * serialize keys ({@code customer($processingDate, $businessDate)})
     * spell — engine parity, the same let channel {@link #chain} already
     * threads for ordinary queries. The zero-arg lambda that results
     * rides the ordinary chain; the mapping reference is null (the
     * string entry has none — every branch carries its own
     * {@code ->from()}).
     */
    public static Prepared prepareLegendQuery(TypedNativeCall ec,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        TypedSpec q = Lets.bound(ec.args().get(0), letPrefix);
        if (q instanceof TypedUserCall) {
            q = new UserCallInliner(specs).inlineBody(List.of(q)).get(0);
        }
        q = peelSelections(q, letPrefix);
        if (!(q instanceof TypedLambda lam)) {
            throw new com.legend.error.NotImplementedException(
                    "executeLegendQuery whose query argument is not a lambda ("
                            + q.getClass().getSimpleName() + ")");
        }
        // a lambda's info is the bare FunctionType, or the element
        // compiler's WRAPPED Function<{…}> / FunctionDefinition<{…}>
        // form — accept both (EvalChecker's rule)
        Type lt = lam.info().type();
        if (lt instanceof Type.GenericType g && g.arguments().size() == 1
                && g.arguments().get(0) instanceof Type.FunctionType inner) {
            lt = inner;
        }
        if (!(lt instanceof Type.FunctionType ft)
                || ft.params().size() != lam.parameters().size()) {
            throw new com.legend.error.NotImplementedException(
                    "executeLegendQuery: the query lambda's parameter types"
                            + " are not declared (" + lt.typeName() + ")");
        }
        Map<String, TypedSpec> vars = varPairs(
                Lets.bound(ec.args().get(1), letPrefix), letPrefix);
        // an α-RENAMED lambda (the inliner's fresh binders inside an inlined
        // helper body — `_i<n>`, the source name gone) binds by POSITION:
        // the vars list is spelled in parameter order (the engine binds by
        // name; position is the one fact a renamed binder still carries).
        // Only when NO parameter matches by name and the counts agree.
        boolean positional = !lam.parameters().isEmpty()
                && lam.parameters().size() == vars.size()
                && lam.parameters().stream().noneMatch(vars::containsKey)
                && lam.parameters().stream().allMatch(p -> p.startsWith("_i"));
        List<TypedSpec> positionalValues = new ArrayList<>(vars.values());
        List<TypedSpec> body = new ArrayList<>();
        for (int i = 0; i < lam.parameters().size(); i++) {
            String name = lam.parameters().get(i);
            TypedSpec value = positional ? positionalValues.get(i) : vars.get(name);
            if (value == null) {
                throw new com.legend.error.NotImplementedException(
                        "executeLegendQuery: no vars pair binds the query"
                                + " parameter '$" + name + "' (parameters "
                                + lam.parameters() + ", vars " + vars.keySet() + ")");
            }
            TypedSpec coerced = coerceVar(value, ft.params().get(i));
            body.add(new TypedLet(name, coerced, coerced.info()));
        }
        body.addAll(lam.body());
        TypedLambda zeroArg = new TypedLambda(List.of(), body,
                ExprType.one(new Type.FunctionType(List.of(), ft.result())));
        return new Prepared(zeroArg, null);
    }

    /** STRUCTURAL query selection — pure data selection over literal
     * shapes, no value computed (foldPairProjection's rule): a query
     * lambda chosen by {@code if(<literal>, |{|q1}, |{|q2})} (the corpus's
     * checked/unchecked helper, the flag a literal after inlining), the
     * {@code ->cast(@FunctionDefinition<{...}>)} over a lambda value, and
     * {@code ->at(k)} over a literal collection of lambdas (the folded
     * {@code compileLegendGrammar(...)} carrier: {@code ->at(0)->cast(...)}
     * selects the grammar's function). Each step resolves through the
     * let prefix. A non-literal condition or index stays on the caller's
     * loud wall. */
    private static TypedSpec peelSelections(TypedSpec q0,
            List<TypedSpec> letPrefix) {
        TypedSpec q = q0;
        while (true) {
            if (q instanceof com.legend.compiler.spec.typed.TypedIf ti
                    && Lets.bound(ti.condition(), letPrefix)
                            instanceof com.legend.compiler.spec.typed.TypedCBoolean flag) {
                TypedSpec branch = flag.value() ? ti.thenBranch()
                        : ti.elseBranch().orElseThrow(() ->
                                new com.legend.error.NotImplementedException(
                                        "execute() whose query is an if() without"
                                                + " an else branch"));
                q = Lets.bound(branch, letPrefix);
                continue;
            }
            if (q instanceof com.legend.compiler.spec.typed.TypedCast c
                    && (c.target() instanceof Type.FunctionType
                        || c.target() instanceof Type.GenericType g
                            && g.arguments().size() == 1
                            && g.arguments().get(0) instanceof Type.FunctionType)) {
                q = Lets.bound(c.source(), letPrefix);
                continue;
            }
            if (q instanceof TypedNativeCall at
                    && ResultEnvelopeSplice.AT_FQN.equals(at.callee().qualifiedName())
                    && at.args().size() == 2
                    && Lets.bound(at.args().get(0), letPrefix)
                            instanceof TypedCollection coll
                    && at.args().get(1)
                            instanceof com.legend.compiler.spec.typed.TypedCInteger k
                    && k.value().longValue() >= 0
                    && k.value().longValue() < coll.elements().size()) {
                q = Lets.bound(coll.elements().get(k.value().intValue()), letPrefix);
                continue;
            }
            return q;
        }
    }

    /** {@code [pair('n', v), ...]}, one bare pair, {@code ^Pair(first=,
     * second=)} or {@code []} as name &rarr; value. */
    private static Map<String, TypedSpec> varPairs(TypedSpec varsArg,
            List<TypedSpec> letPrefix) {
        List<TypedSpec> entries = varsArg instanceof TypedCollection c
                ? c.elements() : List.of(varsArg);
        Map<String, TypedSpec> out = new java.util.LinkedHashMap<>();
        for (TypedSpec e0 : entries) {
            TypedSpec e = Lets.bound(e0, letPrefix);
            if (e instanceof TypedNativeCall pc
                    && "meta::pure::functions::collection::pair"
                            .equals(pc.callee().qualifiedName())
                    && pc.args().size() == 2
                    && Lets.bound(pc.args().get(0), letPrefix)
                            instanceof com.legend.compiler.spec.typed.TypedCString k) {
                out.put(k.value(), Lets.bound(pc.args().get(1), letPrefix));
                continue;
            }
            if (e instanceof TypedNewInstance ni
                    && "meta::pure::functions::collection::Pair".equals(ni.classFqn())
                    && ni.properties().get("first")
                            instanceof com.legend.compiler.spec.typed.TypedCString k2
                    && ni.properties().get("second") != null) {
                out.put(k2.value(), ni.properties().get("second"));
                continue;
            }
            throw new com.legend.error.NotImplementedException(
                    "executeLegendQuery vars: expected pair(name, value)"
                            + " entries, got " + e.getClass().getSimpleName());
        }
        return out;
    }

    /** The engine's variable coercion by DECLARED parameter type (the
     * execute entry reads JSON-borne strings): enum name &rarr; enum value,
     * date string &rarr; date literal, everything else as written. */
    private static TypedSpec coerceVar(TypedSpec value, Type.Param p) {
        if (!(value instanceof com.legend.compiler.spec.typed.TypedCString s)) {
            return value;
        }
        ExprType info = new ExprType(p.type(), p.multiplicity());
        if (p.type() instanceof Type.EnumType et) {
            return new com.legend.compiler.spec.typed.TypedEnumValue(
                    et.fqn(), s.value(), info);
        }
        if (p.type() == Type.Primitive.DATE
                || p.type() == Type.Primitive.STRICT_DATE
                || p.type() == Type.Primitive.DATE_TIME) {
            return new com.legend.compiler.spec.typed.TypedCDate(
                    com.legend.values.PureDateLiteral.parse(s.value()), info);
        }
        return value;
    }

    /** The RESULT JSON of the string entry (engine: {@code meta::legend::
     * execute} &rarr; the result serializer), BY EMISSION over the chain:
     * a graph {@code serialize} root is the json-builder envelope around
     * the serialized value; a primitive scalar root is the bare JSON
     * scalar (the platform-operations witnesses assert {@code 'false'}).
     * TDS and class roots (the tdsBuilder / classBuilder envelopes) and
     * String scalars (JSON-quoted) are the next leg — each a NAMED wall. */
    public static TypedSpec legendQueryEnvelope(TypedSpec chain,
            com.legend.compiler.element.ModelContext model,
            @com.legend.base.Nullable String activitySql) {
        TypedSpec root = chain;
        while (root instanceof TypedFrom f) {
            root = f.source();
        }
        ExprType str = ExprType.one(Type.Primitive.STRING);
        if (root instanceof com.legend.compiler.spec.typed.TypedSerialize) {
            TypedSpec parts = new TypedCollection(List.of(
                    new com.legend.compiler.spec.typed.TypedCString(
                            "{\"builder\":{\"_type\":\"json\"},\"values\":", str),
                    chain,
                    new com.legend.compiler.spec.typed.TypedCString("}", str)),
                    new ExprType(Type.Primitive.STRING,
                            Multiplicity.Bounded.ZERO_MANY), false);
            return call(model, "meta::pure::functions::string::joinStrings",
                    List.of(parts,
                            new com.legend.compiler.spec.typed.TypedCString("", str)),
                    str);
        }
        Type t = chain.info().type();
        if (t instanceof Type.Primitive p
                && chain.info().multiplicity().equals(Multiplicity.Bounded.ONE)
                && (p == Type.Primitive.BOOLEAN || p == Type.Primitive.INTEGER
                        || p == Type.Primitive.FLOAT || p == Type.Primitive.DECIMAL
                        || p == Type.Primitive.NUMBER)) {
            return call(model, "meta::pure::functions::string::toString",
                    List.of(chain), str);
        }
        if (Type.isRelation(t)) {
            return new com.legend.compiler.spec.typed.TypedJsonResult(chain,
                    com.legend.compiler.spec.typed.TypedJsonResult.Kind.TDS,
                    activitySql, str);
        }
        if (t instanceof Type.ClassType && chain.info().multiplicity().isMany()) {
            return new com.legend.compiler.spec.typed.TypedJsonResult(chain,
                    com.legend.compiler.spec.typed.TypedJsonResult.Kind.CLASS,
                    activitySql, str);
        }
        throw new com.legend.error.NotImplementedException(
                "executeLegendQuery over a " + t.typeName()
                        + " result: the result JSON envelope is not emitted yet");
    }

    private static TypedSpec call(com.legend.compiler.element.ModelContext model,
            String fqn, List<TypedSpec> args, ExprType out) {
        var callee = model.findFunction(fqn).stream()
                .filter(f -> f.parameters().size() == args.size())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no registered " + fqn + "/" + args.size()));
        return Typer.emitCall(callee, args, out);
    }

    private static TypedSpec concatenateFold(TypedNativeCall cq,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        TypedSpec lfsArg = Lets.bound(cq.args().get(0), letPrefix);
        // evaluateAndDeactivate may wrap the WHOLE collection
        // ([...]->evaluateAndDeactivate()) — identity, peel first
        while (lfsArg instanceof TypedNativeCall ow
                && ow.args().size() == 1
                && "meta::pure::functions::meta::evaluateAndDeactivate"
                        .equals(ow.callee().qualifiedName())) {
            lfsArg = Lets.bound(ow.args().get(0), letPrefix);
        }
        // MAP-BUILT collections ($bds->map(bd|{|...}->eAD())): β-expand
        // the map over the literal elements — one TypedEval per element,
        // reduced by the inliner (the full β-substitution engine)
        if (lfsArg instanceof TypedMap mapC
                && Lets.bound(mapC.mapper(), letPrefix)
                        instanceof TypedLambda mapLam
                && mapLam.parameters().size() == 1
                && Lets.bound(mapC.source(), letPrefix)
                        instanceof TypedCollection dc) {
            List<TypedSpec> expanded = new ArrayList<>(dc.elements().size());
            for (TypedSpec d : dc.elements()) {
                expanded.add(new UserCallInliner(specs)
                        .inlineBody(List.of(new TypedEval(
                                mapLam, List.of(d),
                                mapLam.body().get(mapLam.body().size() - 1)
                                        .info())))
                        .get(0));
            }
            lfsArg = new TypedCollection(expanded, lfsArg.info());
        }
        List<TypedSpec> els = lfsArg instanceof TypedCollection tc
                ? tc.elements() : List.of(lfsArg);
        List<TypedSpec> queries = new ArrayList<>();
        for (TypedSpec e : els) {
            TypedSpec le = Lets.bound(e, letPrefix);
            while (le instanceof TypedNativeCall w
                    && w.args().size() == 1
                    && "meta::pure::functions::meta::evaluateAndDeactivate"
                            .equals(w.callee().qualifiedName())) {
                le = Lets.bound(w.args().get(0), letPrefix);
            }
            if (!(le instanceof TypedLambda ql) || !ql.parameters().isEmpty()) {
                throw new com.legend.error.NotImplementedException(
                        "concatenateTemporalTdsQueries over a non-literal"
                        + " lambda collection is not supported yet"
                        + " (element " + le.getClass().getSimpleName()
                        + ", carrier " + lfsArg.getClass().getSimpleName()
                        + ")");
            }
            queries.add(ql.body().get(ql.body().size() - 1));
        }
        TypedSpec folded = queries.get(0);
        for (int qi = 1; qi < queries.size(); qi++) {
            folded = new TypedConcatenate(folded, queries.get(qi),
                    folded.info());
        }
        return new TypedLambda(List.of(), List.of(folded),
                new ExprType(new Type.FunctionType(List.of(),
                        new Type.Param(folded.info().type(),
                                folded.info().multiplicity())),
                        Multiplicity.Bounded.ONE));
    }

    /**
     * Inline the prepared lambda's body against the caller's let prefix
     * and attach the execution context: a chain with no {@code ->from()}
     * inside gains one from the EXPLICIT mapping argument (plus the
     * ambient runtime and the runtime argument's ModelChainConnection
     * mappings / JSON sources — the XStore rule, same as FromChecker's
     * instance-runtime arm). Inliner-consumed query lets accumulate into
     * {@code queryLetsSink} (the resolver's let env resolves surviving
     * reads — engine inScopeVars).
     */
    public static Chain chain(Prepared p, TypedNativeCall ec,
            List<TypedSpec> letPrefix, SpecCompiler specs,
            @com.legend.base.Nullable String runtimeFqn,
            Map<String, TypedSpec> queryLetsSink) {
        List<TypedSpec> qb = new ArrayList<>(letPrefix);
        qb.addAll(p.lam().body());
        var inliner = new UserCallInliner(specs);
        TypedSpec chain = inliner.inlineBody(qb).get(0);
        queryLetsSink.putAll(inliner.queryLets());
        if (!containsTypedFrom(chain)) {
            if (p.mref() == null) {
                throw new com.legend.error.NotImplementedException(
                        "execute() with the empty-mapping sentinel requires"
                        + " ->from() context inside the query");
            }
            Optional<TypedPackageableRef> runtime = runtimeFqn == null
                    ? Optional.empty()
                    : Optional.of(new TypedPackageableRef(runtimeFqn,
                            p.mref().info()));
            // the execute() RUNTIME ARGUMENT's connection content is
            // harness-ambient EXCEPT ModelChainConnection mappings — the
            // XStore chain: an M2M mapping's ~src classes resolve THROUGH
            // them (same rule as FromChecker's instance-runtime arm)
            // the runtime argument brought to its VALUE (lets chased, helper
            // calls inlined) and read ONCE
            TypedSpec rtValue = ec.args().size() >= 3
                    ? new UserCallInliner(specs).inlineBody(List.of(
                            Lets.bound(ec.args().get(2), letPrefix))).get(0)
                    : null;
            TypedSpec ctxArg = executionContextArg(ec);
            java.util.function.UnaryOperator<TypedSpec> bind = v -> Lets.bound(v, letPrefix);
            com.legend.compiler.spec.typed.ExecutionContext bound =
                    com.legend.compiler.spec.typed.ExecutionContext.reader()
                            .bind(bind)
                            .read(Optional.of(p.mref()), rtValue)
                            .withRuntime(runtime)
                            .withOptions(ctxArg, bind);
            // the engine's importDataFlow option: the union's key threads
            // become result columns — derived HERE, where the query's root
            // class and the mapping are both in view; appended where the
            // frame executes (ImportDataFlowAppend, beside DriverPkAppend)
            if (com.legend.compiler.spec.typed.ExecutionContext
                    .importDataFlowRequested(ctxArg, bind)) {
                bound = bound.withImportDataFlowColumns(ImportDataFlow.columns(
                        p.mref().fullPath(), chain, specs.ctx()));
            }
            chain = new TypedFrom(chain, bound, chain.info());
        }
        // a TDS-typed root (tableToTDS, a TabularDataSet-declared value)
        // is ONE relation like a schema-typed one: its values envelope
        // holds one TDS
        return new Chain(chain,
                Type.isRelation(chain.info().type())
                        || com.legend.compiler.element.type.PlatformTypes
                                .isTdsType(chain.info().type()));
    }

    /** Whether the chain (transitively) carries a {@code ->from()}. */
    /** The ExecutionContext argument of an execute call on the engine's
     * exeCtx overload (f, mapping, runtime, exeCtx, extensions); null on
     * the others. Identified by the overload's SIGNATURE, never by shape. */
    public static @com.legend.base.Nullable TypedSpec executionContextArg(TypedNativeCall ec) {
        return com.legend.builtin.Pure.ROUTER_EXECUTE__FN_1__MAPPING_1__RUNTIME_1__EXECUTION_CONTEXT_1__EXTENSION_MANY
                .signatureKey().equals(ec.callee().signatureKey()) && ec.args().size() == 5
                ? ec.args().get(3) : null;
    }

    public static boolean containsTypedFrom(TypedSpec n) {
        if (n instanceof TypedFrom) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsTypedFrom(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * WORLD_MAP §4 — a verdict side's stamp is what the COMPILER knows about
     * its value, not the declared type of the function that produced it: a
     * let-bound program call ({@code let w = wrapH2Boolean($op, [])},
     * declared {@code RelationalOperationElement[1]}) whose inlined value is
     * a {@code DynaFunction} (a literal, or a shape-CASE residual over two
     * DynaFunctions) is judged as a DynaFunction — its key tree, not an
     * identity the abstract declaration cannot carry. Only a STRICT
     * narrowing to a subclass re-stamps; the multiplicity stays declared.
     * The inline runs under the same let prefix the side is evaluated
     * under, and walls the same way (loud, never caught here).
     */
    public static List<TypedSpec> narrowSideStamps(List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs) {
        List<TypedSpec> out = new java.util.ArrayList<>(args.size());
        for (TypedSpec a : args) {
            out.add(narrowSideStamp(a, letPrefix, specs));
        }
        return out;
    }

    private static TypedSpec narrowSideStamp(TypedSpec a, List<TypedSpec> letPrefix,
            SpecCompiler specs) {
        if (!(a instanceof com.legend.compiler.spec.typed.TypedVariable v)
                || !(a.info().type() instanceof Type.ClassType declared)) {
            return a;
        }
        TypedSpec bound = Lets.bound(a, letPrefix);
        if (!(bound instanceof com.legend.compiler.spec.typed.TypedUserCall)) {
            return a;
        }
        List<TypedSpec> single = new java.util.ArrayList<>(letPrefix);
        single.add(bound);
        List<TypedSpec> inlined = new UserCallInliner(specs).inlineBody(single);
        TypedSpec value = inlined.get(inlined.size() - 1);
        if (value.info().type() instanceof Type.ClassType actual
                && !actual.fqn().equals(declared.fqn())
                && specs.ctx().isSubtype(actual.fqn(), declared.fqn())) {
            return new com.legend.compiler.spec.typed.TypedVariable(v.name(),
                    new ExprType(actual, a.info().multiplicity()));
        }
        return a;
    }
}
