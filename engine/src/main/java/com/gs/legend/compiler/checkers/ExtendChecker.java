package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.*;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Primitive;
import com.gs.legend.model.m3.Type;

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
    public TypedExtend check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        NativeFunctionDef def = resolveOverload("extend", af.parameters(), source);
        unify(def, source.expressionType());
        List<ValueSpecification> params = af.parameters();

        // Class-source overload: extend(C[*], FuncColSpec/FuncColSpecArray) -> C[*]
        // Stays in object space — compiles lambdas against ClassType for type safety,
        // returns ClassType[*] unchanged. Downstream project/graphFetch handles SQL.
        if (source.type() instanceof Type.ClassType) {
            return checkClassSource(af, def, source, ctx);
        }

        Type.Schema sourceSchema = source.schema();
        if (sourceSchema == null) {
            throw new PureCompileException("extend() requires a Relation source with a known schema");
        }

        // --- Pass 1: collect over() and top-level traverse() clauses ---
        // Each traverse clause becomes one {@link TraversalSpec}; hops within
        // a spec chain (hop[i] parents hop[i-1]), specs themselves all root at
        // the source alias — mirrors the legacy plangen's per-spec prevAlias
        // reset.
        TypedOver overSpec = null;
        List<TraversalSpec> traversalSpecs = new ArrayList<>();
        List<Type.Schema> terminalSchemas = null;
        Type.Schema colSpecSchema = sourceSchema;
        for (int i = 1; i < params.size(); i++) {
            if (params.get(i) instanceof AppliedFunction paf) {
                String fn = simpleName(paf.function());
                if ("over".equals(fn)) {
                    overSpec = compileOverClause(paf, sourceSchema);
                } else if ("traverse".equals(fn)) {
                    var result = compileTraverseClause(paf, sourceSchema, ctx);
                    traversalSpecs.add(new TraversalSpec(result.hops));
                    colSpecSchema = result.terminalSchema;
                }
            } else if (params.get(i) instanceof PureCollection pc) {
                // Multi-traverse: PureCollection of traverse() calls — each exposes
                // its own terminal schema, bound to a distinct lambda param.
                // Each element is a separate spec; chains are independent (e.g.
                // {@code summary = concat(@OrdCust|.NAME, ' ', @OrdProd|.NAME)}).
                terminalSchemas = new ArrayList<>();
                for (var elem : pc.values()) {
                    if (elem instanceof AppliedFunction taf
                            && "traverse".equals(simpleName(taf.function()))) {
                        var result = compileTraverseClause(taf, sourceSchema, ctx);
                        traversalSpecs.add(new TraversalSpec(result.hops));
                        terminalSchemas.add(result.terminalSchema);
                    }
                }
            }
        }

        // --- Pass 2: compile each ColSpec/ColSpecArray into a TypedExtendCol ---
        Map<String, Type> newColumns = new LinkedHashMap<>(sourceSchema.columns());
        List<TypedExtendCol> extensions = new ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            var p = params.get(i);
            if (p instanceof AppliedFunction) continue;       // over()/traverse() — handled above
            if (p instanceof PureCollection) continue;        // multi-traverse — handled above
            if (!(p instanceof ColumnInstance ci)) continue;

            for (ColSpec cs : extractColSpecs(ci)) {
                // Association extend: fn1 is a 0-param lambda wrapping traverse().
                // Compile the traverse to type-check the condition and emit a
                // first-class TypedAssociationExtendCol carrying the hops — no
                // raw-AST pattern matching downstream. Also propagate the joined
                // terminal schema so any subsequent scalar cols in the same
                // extend() can reference the joined columns.
                if (isAssociationExtend(cs)) {
                    AppliedFunction innerTraverse =
                            (AppliedFunction) cs.function1().body().get(0);
                    var result = compileTraverseClause(innerTraverse, sourceSchema, ctx);
                    traversalSpecs.add(new TraversalSpec(result.hops));
                    colSpecSchema = result.terminalSchema;
                    // Target class's compiled mapping function is resolved by a
                    // pass-2 fan-out over associationNavigations on TypeChecker,
                    // exposed via CompiledDependencies.mappingFunctions. We
                    // deliberately do not thread the owner class through the
                    // compilation context to avoid mutable side-channel state.
                    extensions.add(new TypedAssociationExtendCol(
                            cs.name(), result.hops));
                    continue;
                }
                // Embedded extend: fn1 is 0-param lambda wrapping ColSpecArray.
                // Emit a first-class TypedEmbeddedExtendCol so MappingResolver can
                // lower it structurally without re-parsing the raw AST.
                if (isEmbeddedExtend(cs)) {
                    extensions.add(buildEmbeddedExtendCol(cs));
                    continue;
                }
                TypedExtendCol col;
                if (terminalSchemas != null && cs.function1() != null
                        && cs.function1().parameters().size() > 2) {
                    col = compileMultiTraverseColSpec(cs, sourceSchema,
                            terminalSchemas, source, ctx);
                } else {
                    col = compileColSpec(cs, colSpecSchema, sourceSchema, source, ctx,
                            overSpec, def);
                }
                if (newColumns.containsKey(col.alias())) {
                    throw new PureCompileException(
                            "extend(): column '" + col.alias()
                            + "' already exists — use rename() first or choose a different name");
                }
                newColumns.put(col.alias(), extendColType(col));
                extensions.add(col);
            }
        }

        var schema = new Type.Schema(newColumns, sourceSchema.dynamicPivotColumns());
        return new TypedExtend(source, traversalSpecs, extensions, def,
                ExpressionType.one(new Type.Relation(schema)));
    }

    /** Pulls the output column type from a {@link TypedExtendCol} variant. */
    private static Type extendColType(TypedExtendCol col) {
        return switch (col) {
            case TypedScalarExtendCol s -> s.returnType();
            case TypedWindowExtendCol w -> w.castType().orElse(w.returnType());
            case TypedTraverseExtendCol t -> t.expression().expressionType().type();
            // Association and embedded extends do not contribute a column to the
            // source schema — they are join markers / property-grouping markers
            // and are skipped before this method is called via `continue` in the
            // main extend() loop. Reaching here is a compiler invariant violation.
            case TypedAssociationExtendCol a -> throw new IllegalStateException(
                    "extendColType: TypedAssociationExtendCol '" + a.alias()
                            + "' does not project a column — should be skipped before schema assembly");
            case TypedEmbeddedExtendCol e -> throw new IllegalStateException(
                    "extendColType: TypedEmbeddedExtendCol '" + e.alias()
                            + "' does not project a column — should be skipped before schema assembly");
        };
    }

    // ========== Class-source overload ==========

    /**
     * Handles class-source extend: {@code extend(cl:C[*], FuncColSpec/FuncColSpecArray) -> C[*]}.
     *
     * <p>Compiles lambda bodies against ClassType for type safety. Returns ClassType[*]
     * (stays in object space). Tracks projections so downstream project/graphFetch
     * and PlanGenerator can resolve the extend columns via StoreResolution.
     */
    private TypedExtend checkClassSource(AppliedFunction af, NativeFunctionDef def,
                                       TypedSpec source, TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        var bindings = unify(def, source.expressionType());

        // Resolve lambda param type from signature (C[1])
        Type.FunctionType ft = extractFunctionType(def.params().get(1));
        Type resolvedParamType = resolve(ft.params().get(0).type(), bindings,
                "extend() class-source lambda param");

        if (!(params.get(1) instanceof ColumnInstance ci)) {
            throw new PureCompileException(
                    "extend() class-source: param 2 must be FuncColSpec/FuncColSpecArray, got "
                            + params.get(1).getClass().getSimpleName());
        }

        List<TypedExtendCol> extensions = new ArrayList<>();
        for (ColSpec cs : extractColSpecs(ci)) {
            String alias = cs.name();
            LambdaFunction lambda = cs.function1();
            if (lambda == null) {
                // Bare ~prop — synthesize {x | $x.prop}.
                lambda = new LambdaFunction(
                        List.of(new Variable("x")),
                        List.of(new AppliedProperty(alias, List.of(new Variable("x")))));
            }
            String paramName = lambda.parameters().isEmpty() ? "x"
                    : lambda.parameters().get(0).name();
            TypeChecker.CompilationContext lambdaCtx = bindLambdaParam(
                    ctx, paramName, resolvedParamType, source);
            TypedSpec body = compileLambdaBody(lambda, lambdaCtx);
            TypedLambda expr = buildLambda(lambda, resolvedParamType, body);
            extensions.add(new TypedScalarExtendCol(alias, expr, body.type()));
            validateMappingPropertyBinding(ctx, alias, body);
        }

        // Class-source extend stays in object space — ClassType[*] unchanged.
        return new TypedExtend(source, List.of(), extensions, def, source.expressionType());
    }

    /**
     * Validates a class-source extend ColSpec against the materialized class's
     * declared property — when (and only when) we are inside the body of a
     * synthetic mapping function. Catches the M2M / Pure-class mapping case
     * where a property bound to a {@code [*]}-multiplicity source expression
     * narrows to a {@code [0..1]} or {@code [1]} target property: produces a
     * compile-time error instead of a runtime SQL fan-out.
     *
     * <p>Layering: this is a no-op outside synth mapping function compilation
     * (the {@code ctx.mappingTarget()} hint is only stamped by
     * {@code TypeChecker.compileFunction} when entering a synth body). Generic
     * {@code extend} on a ClassType in user code is unaffected.
     *
     * <p>Validates <strong>multiplicity only</strong>. Type compatibility is
     * intentionally NOT checked here: M2M mappings cross class types by design
     * (e.g. {@code address: $src.rawAddresses} where source is {@code RawAddress}
     * and the property is {@code Address}). The cross-type bridge is the
     * downstream mapping function for the value class — which would itself
     * be invoked with a {@code RawAddress} and produce an {@code Address}.
     * Reasoning about that bridge here would re-implement M2M chain
     * resolution, wrong layer. Multiplicity is dimension-free and always
     * comparable.
     *
     * <p>Properties not declared on the target class are tolerated — synthetic
     * mappings sometimes carry helper extends that don't correspond to a Pure
     * property. If you want to enforce "every ColSpec must bind a property",
     * tighten this here.
     */
    private void validateMappingPropertyBinding(
            TypeChecker.CompilationContext ctx,
            String alias,
            TypedSpec body) {
        var targetOpt = ctx.mappingTarget();
        if (targetOpt.isEmpty()) return;
        String classFqn = targetOpt.get().classFqn();
        var classOpt = findClass(classFqn);
        if (classOpt.isEmpty()) return;
        var propOpt = classOpt.get().findProperty(alias, env.modelContext());
        if (propOpt.isEmpty()) return; // ColSpec doesn't bind a declared property
        var prop = propOpt.get();
        Multiplicity declaredMult = prop.multiplicity();
        Multiplicity bodyMult = body.multiplicity();
        // Multiplicity narrowing — the M2M [*] → [1] regression this validation
        // is here to catch.
        if (bodyMult != null && declaredMult != null
                && !Multiplicity.fits(bodyMult, declaredMult)) {
            throw new PureCompileException(
                    "Mapping for class '" + classFqn + "' property '" + alias
                            + "': source expression has multiplicity " + bodyMult
                            + " but property is declared " + declaredMult
                            + ". Narrow the source (e.g. ->first(), ->take(1)) or"
                            + " widen the property's multiplicity.");
        }
    }

    // ========== Association / Embedded extend detection ==========

    /**
     * Checks if a ColSpec is an association extend marker.
     * Association extends have fn1 as a 0-param lambda whose body is a traverse() call.
     * These are synthesized by MappingNormalizer and resolved by MappingResolver — the
     * compiler skips them (but the inner traverse IS compiled for TypeInfo stamping).
     */
    public static boolean isAssociationExtend(ColSpec cs) {
        if (cs.function1() == null) return false;
        var fn1 = cs.function1();
        if (!fn1.parameters().isEmpty()) return false; // must be 0-param
        if (fn1.body().isEmpty()) return false;
        return fn1.body().get(0) instanceof AppliedFunction af
                && "traverse".equals(SymbolTable.extractSimpleName(af.function()));
    }

    /**
     * Builds a {@link TypedEmbeddedExtendCol} from an embedded-extend ColSpec produced
     * by {@code MappingNormalizer}. The inner {@link ColSpecArray} carries one ColSpec
     * per sub-property, each with a 1-param lambda whose body is {@code $row.<COL>}.
     * No type-checking is required — embedded extends are pure shape.
     */
    private static TypedEmbeddedExtendCol buildEmbeddedExtendCol(ColSpec cs) {
        ColSpecArray subArray = (ColSpecArray) cs.function1().body().get(0);
        List<TypedEmbeddedExtendCol.Sub> subs = new ArrayList<>();
        for (ColSpec sub : subArray.colSpecs()) {
            if (sub.function1() == null || sub.function1().body().isEmpty()) {
                throw new PureCompileException(
                        "embedded extend sub-property '" + sub.name()
                                + "' has no expression lambda");
            }
            var body = sub.function1().body().get(0);
            if (!(body instanceof AppliedProperty ap)) {
                throw new PureCompileException(
                        "embedded extend sub-property '" + sub.name()
                                + "' body must be a simple property access, got "
                                + body.getClass().getSimpleName());
            }
            subs.add(new TypedEmbeddedExtendCol.Sub(sub.name(), ap.property()));
        }
        return new TypedEmbeddedExtendCol(cs.name(), subs);
    }

    /**
     * Checks if a ColSpec is an embedded property extend marker.
     * Embedded extends have fn1 as a 0-param lambda whose body is a
     * {@link ColSpecArray} containing sub-property ColSpecs.
     * These are synthesized by MappingNormalizer and resolved by MappingResolver.
     */
    public static boolean isEmbeddedExtend(ColSpec cs) {
        if (cs.function1() == null) return false;
        var fn1 = cs.function1();
        if (!fn1.parameters().isEmpty()) return false; // must be 0-param
        if (fn1.body().isEmpty()) return false;
        return fn1.body().get(0) instanceof ColSpecArray;
    }

    // ========== ColSpec compilation ==========

    private TypedExtendCol compileColSpec(ColSpec cs,
                                          Type.Schema colSpecSchema,
                                          Type.Schema originalSourceSchema,
                                          TypedSpec source,
                                          TypeChecker.CompilationContext ctx,
                                          TypedOver overSpec,
                                          NativeFunctionDef def) {
        String alias = cs.name();
        if (cs.function1() == null || cs.function1().body().isEmpty()) {
            throw new PureCompileException(
                    "extend(): spec '" + alias + "' is missing expression lambda (fn1)");
        }
        LambdaFunction fn1 = cs.function1();

        // Extract lambda param types from the resolved extend() signature's FuncColSpec.
        // For traverse extends: S = original source schema, T = terminal (joined) table schema.
        // For non-traverse extends: S = T = source schema.
        Type.FunctionType sigFnType = findColSpecFunctionType(def, fn1.parameters().size());
        var bindings = new Bindings();
        bindings.put("S", new Type.Tuple(originalSourceSchema));
        bindings.put("T", new Type.Tuple(colSpecSchema));

        TypeChecker.CompilationContext fn1Ctx = ctx;
        List<TypedParam> fn1Params = new ArrayList<>();
        for (int pi = 0; pi < fn1.parameters().size() && pi < sigFnType.params().size(); pi++) {
            String paramName = fn1.parameters().get(pi).name();
            Type sigType = sigFnType.params().get(pi).type();
            Type resolvedType = resolve(sigType, bindings, "extend() lambda param " + pi);
            fn1Ctx = bindLambdaParam(fn1Ctx, paramName, resolvedType, source);
            fn1Params.add(new TypedParam(paramName, resolvedType, Multiplicity.ONE));
        }
        TypedSpec fn1Body = compileLambdaBody(fn1, fn1Ctx);
        TypedLambda fn1Lambda = new TypedLambda(fn1Params, List.of(fn1Body), fn1Body.expressionType());

        // Aggregate extend (fn1 + fn2): mirrors GroupByChecker pattern.
        if (cs.function2() != null) {
            return compileAggregateExtend(cs, fn1Lambda, fn1Body, source, ctx, overSpec);
        }

        if (fn1Body.type() == null) {
            throw new PureCompileException(
                    "extend(): lambda for column '" + alias
                            + "' has no inferred type — fix TypeChecker");
        }
        Type returnType = fn1Body.type();

        // Window extend (over() present + fn1 resolves to a native window function).
        NativeFunctionDef resolvedFunc = resolvedFuncFrom(fn1Body);
        if (overSpec != null && resolvedFunc != null) {
            return buildWindowCol(alias, fn1Lambda, fn1Body, resolvedFunc,
                    /*reducer=*/ Optional.empty(), overSpec, returnType,
                    /*castType=*/ null);
        }
        // Plain scalar per-row extend.
        return new TypedScalarExtendCol(alias, fn1Lambda, returnType);
    }

    /**
     * Compiles a multi-traverse ColSpec by procedurally binding lambda params to
     * the source schema and each terminal schema. Bypasses signature matching since
     * the lambda arity (N+1) doesn't match any fixed extend() signature.
     */
    private TypedExtendCol compileMultiTraverseColSpec(
            ColSpec cs,
            Type.Schema sourceSchema,
            List<Type.Schema> terminalSchemas,
            TypedSpec source,
            TypeChecker.CompilationContext ctx) {
        String alias = cs.name();
        LambdaFunction fn1 = cs.function1();
        var lambdaParams = fn1.parameters();

        TypeChecker.CompilationContext fn1Ctx = ctx;
        List<TypedParam> typedParams = new ArrayList<>();

        // param 0 = src (source schema)
        fn1Ctx = bindLambdaParam(fn1Ctx, lambdaParams.get(0).name(),
                new Type.Tuple(sourceSchema), source);
        typedParams.add(new TypedParam(lambdaParams.get(0).name(),
                new Type.Tuple(sourceSchema), Multiplicity.ONE));

        // params 1..N = t1, t2, ... (each terminal schema)
        for (int ti = 0; ti < terminalSchemas.size() && (ti + 1) < lambdaParams.size(); ti++) {
            String name = lambdaParams.get(ti + 1).name();
            Type.Tuple t = new Type.Tuple(terminalSchemas.get(ti));
            fn1Ctx = bindLambdaParam(fn1Ctx, name, t, source);
            typedParams.add(new TypedParam(name, t, Multiplicity.ONE));
        }

        TypedSpec body = compileLambdaBody(fn1, fn1Ctx);
        if (body.type() == null) {
            throw new PureCompileException(
                    "extend(): lambda for column '" + alias
                            + "' has no inferred type — fix TypeChecker");
        }
        TypedLambda lambda = new TypedLambda(typedParams, List.of(body), body.expressionType());
        // Multi-traverse column is expressed as a scalar extend over the joined
        // rowset — the join wiring is captured by the enclosing TypedExtend's
        // {@code traversalHops}.
        return new TypedScalarExtendCol(alias, lambda, body.type());
    }

    /**
     * Finds the FunctionType from the resolved extend() def's ColSpec parameter
     * that matches the given lambda arity. Throws if not found.
     */
    private Type.FunctionType findColSpecFunctionType(NativeFunctionDef def, int lambdaArity) {
        for (var param : def.params()) {
            if (param.type() instanceof Type.GenericType fp
                    && !fp.typeArgs().isEmpty()
                    && fp.typeArgs().get(0) instanceof Type.FunctionType ft) {
                com.gs.legend.model.m3.LClass raw = fp.rawType() instanceof com.gs.legend.model.m3.LClass lc ? lc : null;
                if (raw == com.gs.legend.model.m3.LClass.FUNC_COL_SPEC
                        || raw == com.gs.legend.model.m3.LClass.AGG_COL_SPEC
                        || raw == com.gs.legend.model.m3.LClass.FUNC_COL_SPEC_ARRAY
                        || raw == com.gs.legend.model.m3.LClass.AGG_COL_SPEC_ARRAY) {
                    if (ft.params().size() == lambdaArity) {
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
     * fn1 extracts value, fn2 applies the aggregate function.
     */
    private TypedExtendCol compileAggregateExtend(ColSpec cs,
                                                  TypedLambda fn1Lambda,
                                                  TypedSpec fn1Body,
                                                  TypedSpec source,
                                                  TypeChecker.CompilationContext ctx,
                                                  TypedOver overSpec) {
        String alias = cs.name();
        LambdaFunction fn2 = cs.function2();
        String fn2ParamName = fn2.parameters().isEmpty() ? null : fn2.parameters().get(0).name();
        TypeChecker.CompilationContext fn2Ctx = ctx.withLambdaParam(fn2ParamName, fn1Body.type());
        TypedSpec fn2Body = compileLambdaBody(fn2, fn2Ctx);

        // Locate the aggregate native function inside fn2 body.
        ValueSpecification fn2AstBody = fn2.body().get(0);
        NativeFunctionDef resolved = resolvedFuncFrom(fn2Body);
        if (resolved == null) {
            throw new PureCompileException(
                    "extend(): aggregate function in '" + alias
                    + "' did not resolve — fn2 body must be a registered function");
        }

        // Return-type refinement — retrofit for under-specified aggregate signatures.
        //
        // Aggregates like {@code sum(Number[*]):Number[1]} use generic Number in their native
        // signatures because the signature grammar doesn't express type-dependent returns
        // cleanly. In practice sum/min/max/etc. preserve their input numeric type (sum of
        // Integer is Integer, not Number). This branch compensates by refining the declared
        // Number return to the input column's concrete numeric type.
        //
        // TODO phase-2.5d: rewrite affected native signatures with generic T bounded by Number
        //   (e.g., {@code sum<T>(col:T[*] where T extends Number):T[1]}) and delete this
        //   refinement. Any aggregate that legitimately returns Number (not T) must be
        //   audited — today this code silently downgrades them.
        Type returnType = fn2Body.type();
        if (returnType == Primitive.NUMBER && fn1Body.type() != null
                && fn1Body.type().isNumeric()) {
            returnType = fn1Body.type();
        }
        Type castType = extractCastGenericType(fn2AstBody);

        // Build fn2 as a TypedLambda: single param typed with fn1's output type.
        TypedLambda fn2Lambda = new TypedLambda(
                List.of(new TypedParam(
                        fn2ParamName == null ? "x" : fn2ParamName,
                        fn1Body.type(), Multiplicity.ONE)),
                List.of(fn2Body), fn2Body.expressionType());

        // When no over() clause is supplied, aggregates still need FUNC() OVER()
        // (whole-relation window) — emit an empty TypedOver.
        TypedOver effectiveOver = overSpec != null ? overSpec
                : new TypedOver(List.of(), List.of(), Optional.empty());

        // Aggregate window: funcArgs is the value expression (fn1Body) plus
        // any non-variable trailing args from the reducer body (e.g. the
        // separator literal in {@code $y->joinStrings('')} becomes the second
        // arg to STRING_AGG). The reducer identifies which aggregate
        // (sum/avg/joinStrings/...) via its body's terminal call. The
        // row-param name is the fn1 lambda's last parameter (matches the
        // legacy convention for both 1-param aggregates {r|...} and 3-param
        // windowed-aggregates {p,w,r|...}).
        String rowParamName = lastParamName(fn1Lambda);
        // funcArgs is the typed reducer-arg list: fn1's body (which may itself
        // be a structured call like {@code rowMapper(a, b)} for the
        // bivariate-aggregate sugar) plus any non-variable trailing args from
        // the reducer body (e.g. the separator literal in joinStrings). The
        // checker stays structurally honest — no per-function unpacking;
        // bindings pattern-match on their own dispatch key in
        // {@link com.gs.legend.plan.lowering.natives.WindowBindings} /
        // {@link com.gs.legend.plan.lowering.natives.AggregateBindings}.
        List<TypedSpec> funcArgs = new ArrayList<>();
        funcArgs.add(fn1Body);
        funcArgs.addAll(extractReducerExtraArgs(fn2Body));
        return new TypedWindowExtendCol(alias, resolved,
                rowParamName,
                funcArgs,
                Optional.of(fn2Lambda),
                /*outerWrapper=*/ Optional.empty(),
                effectiveOver, returnType,
                Optional.ofNullable(castType));
    }

    /**
     * Extracts literal / constant args from a reducer body beyond the
     * reducer's own parameter. For {@code $y -> joinStrings(y, '')} the
     * reducer call has two args: the variable {@code $y} (already represented
     * by {@code fn1Body}) and the literal {@code ''}. The latter must flow
     * into the aggregate's SQL call as the separator.
     *
     * <p>Skips {@link TypedVariable}s (they stand in for the reducer param and
     * are structurally redundant with {@code fn1Body}).
     */
    private static List<TypedSpec> extractReducerExtraArgs(TypedSpec fn2Body) {
        if (!(fn2Body instanceof TypedNativeCall call)) return List.of();
        List<TypedSpec> out = new ArrayList<>();
        for (TypedSpec a : call.args()) {
            if (a instanceof TypedVariable) continue;
            out.add(a);
        }
        return out;
    }

    // ========== Window HIR construction ==========

    /**
     * Builds the new-shape {@link TypedWindowExtendCol} from a resolved window
     * function and fn1 body, classifying one of the legacy pattern cases:
     *
     * <ol>
     *   <li><b>Direct call</b> ({@code $p->rowNumber($r)}, {@code $p->ntile($r, 4)}):
     *       fn1Body is a {@link TypedNativeCall}; funcArgs are the args with
     *       variable/context references filtered out.</li>
     *   <li><b>Property chain</b> ({@code $p->lag($r).salary}): fn1Body is a
     *       {@link TypedPropertyAccess} whose source is the window call; the
     *       property name becomes the leading funcArg (as a synthetic
     *       property access on the row variable), followed by any non-variable
     *       args of the inner call (offsets).</li>
     *   <li><b>Wrapper</b> ({@code round(cumulativeDistribution($w,$r), 2)}):
     *       fn1Body is a {@link TypedNativeCall} whose first arg is itself a
     *       registered native call; the inner is the window func and the
     *       outer becomes a {@link TypedWindowExtendCol.OuterWrapper} whose
     *       expression substitutes a gensymmed {@link TypedVariable} for the
     *       inner call. The lowering rule binds the wrapper's holeName to the
     *       built window-call expression at substitution time.</li>
     * </ol>
     *
     * Cases not yet classified fall back to a bare call (no funcArgs), which
     * PlanGen will emit as {@code FUNC() OVER (...)} — a legal default that
     * surfaces failures at execution time rather than silently producing bad
     * SQL.
     */
    private TypedExtendCol buildWindowCol(String alias, TypedLambda fn1Lambda,
                                           TypedSpec fn1Body,
                                           NativeFunctionDef resolvedFunc,
                                           Optional<TypedLambda> reducer,
                                           TypedOver overSpec,
                                           Type returnType,
                                           Type castType) {
        String rowParamName = lastParamName(fn1Lambda);
        WindowShape shape = classifyWindowBody(fn1Body, rowParamName);
        return new TypedWindowExtendCol(alias, resolvedFunc,
                rowParamName,
                shape.funcArgs,
                reducer,
                shape.outerWrapper,
                overSpec, returnType,
                Optional.ofNullable(castType));
    }

    /**
     * Classified window body: funcArgs plus, when the body wraps the window
     * call in a surrounding scalar, an {@link TypedWindowExtendCol.OuterWrapper}
     * holder bundling the rewritten scalar expression with the gensymmed name
     * of the {@link TypedVariable} it references.
     */
    private record WindowShape(
            List<TypedSpec> funcArgs,
            Optional<TypedWindowExtendCol.OuterWrapper> outerWrapper) {}

    private WindowShape classifyWindowBody(TypedSpec body, String rowParamName) {
        // Pattern 3 — wrapper: outerFunc(innerWindowCall, ...rest).
        // Allocate a fresh symbol via the TypeCheckEnv gensym and substitute a
        // TypedVariable for the inner call inside the rebuilt outer expression.
        // At lowering time ExtendLowering binds that name to the built
        // WindowCall in the LoweringContext, so the TypedVariable resolves via
        // the standard variable pathway (no sentinel node required).
        if (body instanceof TypedNativeCall outer
                && !outer.args().isEmpty()
                && outer.args().get(0) instanceof TypedNativeCall inner
                && !BuiltinRegistry.instance().resolve(inner.func().name()).isEmpty()) {
            List<TypedSpec> funcArgs = filterContextArgs(inner.args());
            String holeName = env.freshSymbol("$$wh");
            TypedSpec hole = new TypedVariable(holeName, Role.LET_BINDING, inner.expressionType());
            List<TypedSpec> rewrittenOuterArgs = new ArrayList<>(outer.args().size());
            rewrittenOuterArgs.add(hole);
            for (int i = 1; i < outer.args().size(); i++) {
                rewrittenOuterArgs.add(outer.args().get(i));
            }
            TypedSpec wrapperExpr = new TypedNativeCall(outer.func(), rewrittenOuterArgs, outer.info());
            return new WindowShape(funcArgs,
                    Optional.of(new TypedWindowExtendCol.OuterWrapper(wrapperExpr, holeName)));
        }
        // Pattern 2 — property chain: $p->lag($r).salary
        if (body instanceof TypedPropertyAccess tpa
                && tpa.source() instanceof TypedNativeCall inner) {
            List<TypedSpec> funcArgs = new ArrayList<>();
            // The property becomes the value arg: model it as a property
            // access on the row variable so lowering resolves it to a column
            // ref on the source alias.
            TypedSpec rowVar = new TypedVariable(rowParamName, Role.LAMBDA_PARAM, tpa.info());
            funcArgs.add(new TypedPropertyAccess(rowVar, tpa.property(), Optional.empty(), tpa.info()));
            // Plus any non-variable inner args (e.g., lag's offset).
            funcArgs.addAll(filterContextArgs(inner.args()));
            return new WindowShape(funcArgs, Optional.empty());
        }
        // Pattern 4 — direct call: $p->rowNumber($r), $p->ntile($r, 4)
        if (body instanceof TypedNativeCall tnc) {
            return new WindowShape(filterContextArgs(tnc.args()), Optional.empty());
        }
        // Fallback — unknown shape; emit zero args and let PlanGen surface the issue.
        return new WindowShape(List.of(), Optional.empty());
    }

    /**
     * Filters out {@link TypedVariable} args (row / window / partition context
     * references) that the window function receives structurally but that
     * should not flow into the SQL call's arg list.
     */
    private static List<TypedSpec> filterContextArgs(List<TypedSpec> args) {
        List<TypedSpec> out = new ArrayList<>(args.size());
        for (TypedSpec a : args) {
            if (!(a instanceof TypedVariable)) out.add(a);
        }
        return out;
    }

    /** Last lambda-parameter name, or {@code "r"} when the lambda has none. */
    private static String lastParamName(TypedLambda lam) {
        if (lam.parameters().isEmpty()) return "r";
        return lam.parameters().get(lam.parameters().size() - 1).name();
    }

    // ========== Over clause compilation ==========

    /** Compiles over() clause. Validates each sub-function via resolveOverload. */
    private TypedOver compileOverClause(AppliedFunction overAf,
                                        Type.Schema sourceSchema) {
        List<String> partitionBy = new ArrayList<>();
        List<TypedSortKey> orderBy = new ArrayList<>();
        TypedFrame frame = null;

        for (var p : overAf.parameters()) {
            if (p instanceof ColSpec cs) {
                sourceSchema.requireColumn(cs.name());
                partitionBy.add(cs.name());
            } else if (p instanceof ColSpecArray(List<ColSpec> colSpecs)) {
                for (ColSpec cs : colSpecs) {
                    sourceSchema.requireColumn(cs.name());
                    partitionBy.add(cs.name());
                }
            } else if (p instanceof PureCollection(List<ValueSpecification> values)) {
                for (var elem : values) {
                    TypedSortKey sk = compileSortSpec(elem, sourceSchema);
                    if (sk != null) orderBy.add(sk);
                }
            } else if (p instanceof AppliedFunction paf) {
                String fn = simpleName(paf.function());
                TypedSortKey sk = compileSortSpec(p, sourceSchema);
                if (sk != null) {
                    orderBy.add(sk);
                } else if ("rows".equals(fn)) {
                    resolveOverload("rows", paf.parameters(), null);
                    frame = compileFrameSpec(FrameType.ROWS, paf.parameters());
                } else if ("range".equals(fn) || "_range".equals(fn)) {
                    resolveOverload("_range", paf.parameters(), null);
                    frame = compileFrameSpec(FrameType.RANGE, paf.parameters());
                }
            }
        }
        return new TypedOver(partitionBy, orderBy, Optional.ofNullable(frame));
    }

    private TypedSortKey compileSortSpec(ValueSpecification vs, Type.Schema sourceSchema) {
        if (vs instanceof AppliedFunction af) {
            String fn = simpleName(af.function());
            if ("asc".equals(fn) || "ascending".equals(fn)) {
                resolveOverload(fn, af.parameters(), null);
                String col = extractColumnName(af.parameters().get(0));
                sourceSchema.requireColumn(col);
                return new TypedColumnSortKey(col, SortDirection.ASC);
            } else if ("desc".equals(fn) || "descending".equals(fn)) {
                resolveOverload(fn, af.parameters(), null);
                String col = extractColumnName(af.parameters().get(0));
                sourceSchema.requireColumn(col);
                return new TypedColumnSortKey(col, SortDirection.DESC);
            }
        }
        return null;
    }

    // ========== Function resolution helpers ==========

    /**
     * Extracts the resolved {@link NativeFunctionDef} from a typed lambda body.
     * Handles the three AST shapes produced by the parser:
     * <ul>
     *   <li>{@code $p->func($w,$r).property} — a {@link TypedPropertyAccess} whose
     *       source is a {@link TypedNativeCall}.</li>
     *   <li>{@code wrapper(innerFunc($w,$r), N)} — a {@link TypedNativeCall}
     *       whose first arg is itself a native call (e.g., {@code round(cumDist(), 2)}).</li>
     *   <li>{@code func($r)} — a direct {@link TypedNativeCall}.</li>
     * </ul>
     */
    private static NativeFunctionDef resolvedFuncFrom(TypedSpec body) {
        TypedSpec inner = body;
        // Unwrap an outer cast() — `cast($x->plus(), @Integer)` puts the
        // aggregate inside a TypedCast (or a cast TypedNativeCall when the
        // checker compiled it as a native).
        if (inner instanceof com.gs.legend.compiler.typed.TypedCast tc) {
            inner = tc.expr();
        }
        if (inner instanceof TypedPropertyAccess tpa) {
            inner = tpa.source();
        }
        if (inner instanceof TypedNativeCall tnc) {
            if (!tnc.args().isEmpty() && tnc.args().get(0) instanceof TypedNativeCall wrapped
                    && !BuiltinRegistry.instance().resolve(wrapped.func().name()).isEmpty()) {
                return wrapped.func();
            }
            return tnc.func();
        }
        return null;
    }

    // ========== Frame spec compilation ==========

    private TypedFrame compileFrameSpec(FrameType frameType, List<ValueSpecification> params) {
        TypedFrameBound start = resolveFrameBound(params, 0);
        TypedFrameBound end = resolveFrameBound(params, 1);
        validateFrameBounds(start, end);
        return new TypedFrame(frameType, start, end);
    }

    private TypedFrameBound resolveFrameBound(List<ValueSpecification> params, int idx) {
        if (idx >= params.size()) {
            return idx == 0 ? new Unbounded() : new CurrentRow();
        }
        var param = params.get(idx);
        if (param instanceof AppliedFunction af && "unbounded".equals(simpleName(af.function()))) {
            resolveOverload("unbounded", af.parameters(), null);
            return new Unbounded();
        }
        if (param instanceof AppliedFunction af && "minus".equals(simpleName(af.function()))
                && !af.parameters().isEmpty()) {
            double v = extractNumericValue(af.parameters().get(af.parameters().size() - 1));
            return new Offset(-v);
        }
        double v = extractNumericValue(param);
        if (v == 0) return new CurrentRow();
        return new Offset(v);
    }

    private void validateFrameBounds(TypedFrameBound start, TypedFrameBound end) {
        double startPos = boundPosition(start, true);
        double endPos = boundPosition(end, false);
        if (startPos > endPos) {
            throw new PureCompileException(
                    "Invalid window frame boundary - lower bound of window frame cannot be greater than the upper bound!");
        }
    }

    private double boundPosition(TypedFrameBound bound, boolean isStart) {
        return switch (bound) {
            case Unbounded u -> isStart ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
            case CurrentRow c -> 0;
            case Offset o -> o.value();
        };
    }

    // ========== Traverse clause compilation ==========

    private record TraverseResult(List<TraversalHop> hops, Type.Schema terminalSchema) {}

    private record HopParts(ValueSpecification target, LambdaFunction condition) {}

    /**
     * Compiles a traverse() clause: flattens the chain, resolves each hop's target table,
     * type-checks each condition lambda, and returns the typed hops plus the terminal schema.
     */
    private TraverseResult compileTraverseClause(AppliedFunction traverseAf,
                                                  Type.Schema sourceSchema,
                                                  TypeChecker.CompilationContext ctx) {
        List<HopParts> hopParts = new ArrayList<>();
        flattenTraverseChain(traverseAf, hopParts);

        List<TraversalHop> hops = new ArrayList<>();
        Type.Schema prevSchema = sourceSchema;

        for (HopParts parts : hopParts) {
            TypedSpec targetTyped = env.compileExpr(parts.target, ctx);
            Type.Schema targetSchema = targetTyped.schema();
            if (targetSchema == null) {
                throw new PureCompileException(
                        "traverse() target must be a Relation with a known schema");
            }
            if (!(targetTyped instanceof TypedTableReference ttr)) {
                throw new PureCompileException(
                        "traverse() target must be a tableReference() with a resolved table name");
            }
            String tableName = ttr.tableName();

            LambdaFunction condLambda = parts.condition;
            if (condLambda.parameters().size() != 2) {
                throw new PureCompileException(
                        "traverse() condition lambda must have exactly 2 parameters (prev, hop), got "
                                + condLambda.parameters().size());
            }

            String prevParam = condLambda.parameters().get(0).name();
            String hopParam = condLambda.parameters().get(1).name();
            Type.Tuple prevT = new Type.Tuple(prevSchema);
            Type.Tuple hopT = new Type.Tuple(targetSchema);

            TypeChecker.CompilationContext lambdaCtx = ctx
                    .withLambdaParam(prevParam, prevT)
                    .withLambdaParam(hopParam, hopT);

            TypedSpec condBody = compileLambdaBody(condLambda, lambdaCtx);
            TypedLambda condTyped = new TypedLambda(
                    List.of(
                            new TypedParam(prevParam, prevT, Multiplicity.ONE),
                            new TypedParam(hopParam, hopT, Multiplicity.ONE)),
                    List.of(condBody), condBody.expressionType());

            hops.add(new TraversalHop(tableName, condTyped));
            prevSchema = targetSchema;
        }

        return new TraverseResult(hops, prevSchema);
    }

    /**
     * Recursively flattens a traverse chain into ordered hop parts.
     * Standalone: traverse(target, cond) → 1 hop.
     * Chained: traverse(traverse(...), target, cond) → N hops (recursive).
     */
    private void flattenTraverseChain(AppliedFunction traverseAf, List<HopParts> out) {
        var params = traverseAf.parameters();
        if (params.size() == 2) {
            // Standalone: traverse(target, cond)
            if (!(params.get(1) instanceof LambdaFunction)) {
                throw new PureCompileException(
                        "traverse(): second parameter must be a condition lambda");
            }
            out.add(new HopParts(params.get(0), (LambdaFunction) params.get(1)));
        } else if (params.size() == 3) {
            // Chained: traverse(prevTraverse, target, cond)
            if (!(params.get(0) instanceof AppliedFunction inner
                    && "traverse".equals(simpleName(inner.function())))) {
                throw new PureCompileException(
                        "traverse(): chained form requires a traverse() as first parameter");
            }
            flattenTraverseChain(inner, out);
            if (!(params.get(2) instanceof LambdaFunction)) {
                throw new PureCompileException(
                        "traverse(): third parameter must be a condition lambda");
            }
            out.add(new HopParts(params.get(1), (LambdaFunction) params.get(2)));
        } else {
            throw new PureCompileException(
                    "traverse() requires 2 (standalone) or 3 (chained) parameters, got "
                            + params.size());
        }
    }

    // ========== Shared utilities ==========

    private List<ColSpec> extractColSpecs(com.gs.legend.ast.ColumnInstance ci) {
        if (ci instanceof ColSpecArray(List<ColSpec> specs)) return specs;
        if (ci instanceof ColSpec cs) return List.of(cs);
        throw new PureCompileException(
                "extend() parameter must be ~col:... or ~[...], got: "
                        + ci.getClass().getSimpleName());
    }


    private double extractNumericValue(ValueSpecification vs) {
        if (vs instanceof CInteger(Number value)) return value.doubleValue();
        if (vs instanceof CFloat(double value)) return value;
        if (vs instanceof CDecimal(java.math.BigDecimal value)) return value.doubleValue();
        throw new PureCompileException(
                "Expected numeric literal, got: " + vs.getClass().getSimpleName());
    }

    /**
     * Builds a single-param {@link TypedLambda} whose param name comes from
     * the AST lambda, typed with {@code paramType} and arity {@code [1]}.
     * Shared by the class-source and relation-source paths.
     */
    private static TypedLambda buildLambda(LambdaFunction lambda, Type paramType, TypedSpec body) {
        String paramName = lambda.parameters().isEmpty() ? "x"
                : lambda.parameters().get(0).name();
        return new TypedLambda(
                List.of(new TypedParam(paramName, paramType, Multiplicity.ONE)),
                List.of(body), body.expressionType());
    }
}
