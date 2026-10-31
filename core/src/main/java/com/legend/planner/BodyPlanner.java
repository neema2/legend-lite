// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE BODY PLANNER (the planner-driver seam program's S2, docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md §3.3), grown
 * kind by kind as a strangler: what the body driver asks to have a statement planned, over the body's planning half
 * ({@link PlanEnv}) and never a session. S2g's first move (2026-10-31): the statement resolver every site constructs
 * ({@link #resolver}: the query's lets and the handles' rows ride every resolution), the Lowerer a statement lowers
 * with and the plan-preparation ladder ({@link #lowererFor}, {@link #lowerAndPrepare}), the run's feature flags, an
 * execute call's bound context, and a value's road to its executable body (the lets spliced, the user calls inlined,
 * the natives staged, the stores resolved and checked: {@link #sideBody}). The driver keeps what runs.
 */
public final class BodyPlanner {

    private BodyPlanner() {
    }

    /** THE statement resolver: the query lets and the registered handle
     * rows ride every resolution (one construction, every site). */
    public static com.legend.resolver.StoreResolver resolver(
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env) {
        return new com.legend.resolver.StoreResolver(env.ctx(), specs)
                .withLetBindings(env.queryLets()).withPlanRows(env.planRows())
                .withHandleRegistrar(pn -> {
                    // an INLINE handle met by the resolver: build its rows
                    // now (the let-time path for let-bound handles)
                    PlanAllocations.registerHandleRows("", pn, java.util.List.of(), specs, env);
                    return env.planRows().get(com.legend.plan.PlanRows.scopeId(pn));
                });
    }

    /** The execution context an executionPlan / execute / toSQLString call
     * binds through its RUNTIME argument: the argument is brought to its
     * VALUE (helper calls inlined) and read ONCE. */
    /** The feature flags an execution runs under: the executing frame's own
     *  (its execute call's context argument) plus the runner's defaults
     *  (ExecuteOptions.features) — the two sources, merged ONCE here. */
    /** PUSH_DOWN_ENUM_TRANSFORM on this plan's context (the plan printer's
     *  enum-tuple form follows it). */
    public static boolean pushDownEnums(PlanEnv env, java.util.List<TypedSpec> body) {
        return featuresOf(env, body).contains(
                com.legend.platform.Feature.PUSH_DOWN_ENUM_TRANSFORM);
    }

    public static java.util.Set<com.legend.platform.Feature> featuresOf(PlanEnv env,
            java.util.List<TypedSpec> body) {
        java.util.Set<com.legend.platform.Feature> all =
                java.util.EnumSet.noneOf(com.legend.platform.Feature.class);
        if (env.frame() != null) {
            all.addAll(env.frame().features());
        }
        all.addAll(env.features());
        all.addAll(com.legend.compiler.spec.typed.ExecutionContext.treeFeatures(body));
        return all.isEmpty() ? java.util.Set.of() : java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(all));
    }

    public static com.legend.compiler.spec.typed.ExecutionContext boundContext(
            TypedSpec runtimeArg, SpecCompiler specs) {
        TypedSpec value = new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(java.util.List.of(runtimeArg)).get(0);
        return com.legend.compiler.spec.typed.ExecutionContext.reader()
                .read(java.util.Optional.empty(), value);
    }

    /** The same read under a statement: the argument and the values it
     * names (a let-bound runtime, a let-bound hook operand) chase the
     * statement's preceding lets. */
    public static com.legend.compiler.spec.typed.ExecutionContext boundContext(
            TypedSpec runtimeArg, java.util.List<TypedSpec> letPrefix, SpecCompiler specs) {
        TypedSpec value = new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(java.util.List.of(com.legend.compiler.spec.typed.Lets.bound(runtimeArg, letPrefix))).get(0);
        return com.legend.compiler.spec.typed.ExecutionContext.reader()
                .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                .read(java.util.Optional.empty(), value);
    }

    public static void collectStores(TypedSpec n, java.util.Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedTableReference tr) {
            out.add(tr.store());
        }
        for (TypedSpec c : n.children()) {
            collectStores(c, out);
        }
    }

    /** ONE VALUE expression through the ordinary back half (G½ inline →
     * H resolve → lower/execute) — the assert-verdict arm's side
     * evaluator. The same sequence as the generic statement tail. */
    /** LOWER → post-process → two-phase dynamic pivot → deferred
     * relation-toString resolution: the whole plan-preparation ladder
     * (split from executeTyped at the 250-line shape guard). */
    public static com.legend.sql.SqlQuery lowerAndPrepare(
            java.util.List<TypedSpec> body, PlanEnv env, ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, boolean identity) {
        return lowerAndPrepare(body, env, ctx, dialect, identity, false);
    }

    /** {@link #lowerAndPrepare(java.util.List, PlanEnv, ModelContext, com.legend.sql.dialect.SqlDialect,
     *  boolean)}; {@code named}: the query's columns named in its select, in its relation's
     *  order (Lowerer.namedColumns: a staged table takes them by position). The two schema reads here (a static
     *  pivot's keys, a deferred relation's columns) go through the session seam (track B's B1a, 2026-10-29). */
    public static com.legend.sql.SqlQuery lowerAndPrepare(
            java.util.List<TypedSpec> body, PlanEnv env, ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, boolean identity, boolean named) {
        com.legend.lowering.Lowerer lowerer = lowererFor(body, env, ctx, identity);
        com.legend.sql.SqlQuery plan =
                lowerer.lower(com.legend.lowering.SeedableLets
                        .withSeedableLetPrefix(body, env.queryLets(), ctx));
        // post-process; a dynamic pivot stays dynamic: its plan is keyed where it is planned (JudgePlans, PlanMaker.keyed:
        // its keys read when the plan runs), never staticized by a read at planning (the seam program's S3a, 2026-10-31)
        plan = com.legend.lowering.SqlPostProcessors.applyRecorded(plan,
                env.tableReplace(),
                env.postProcessors().extractCtes(),
                env.postProcessors().nonExecutable());
        // A DYNAMIC PIVOT's TEXT whose columns nothing names (its templates unresolved): its columns exist only once the
        // database is asked, and planning asks the database nothing (the seam program's S3a, 2026-10-30: the LIMIT-0
        // column probe deleted; the six channel B bodies that reached it named their columns by a cast, F10). Refused
        // by name: the pivot's keyed form writes no text yet; no body in the corpus or channel B reaches it (step H's
        // H1). A raw grid's text never comes here: toCSV is its report's texts, toString refused (Render, S3c).
        if (!lowerer.deferredTds().isEmpty()) {
            throw new com.legend.error.NotImplementedException("a dynamic pivot's text (toString/toCSV) over columns"
                    + " nothing names: its columns exist only once the database is asked, planning asks the database"
                    + " nothing, and the pivot's keyed form writes no text");
        }
        return named ? lowerer.namedColumns(plan) : plan;
    }

    /** The Lowerer a statement's body lowers with (lowerAndPrepare's and the store write's): the model's layouts,
     *  keys, time zone and the body's features. */
    public static com.legend.lowering.Lowerer lowererFor(java.util.List<TypedSpec> body, PlanEnv env,
            ModelContext ctx, boolean identity) {
        // F13: the verdict-side lane's layouts carry the canon rider too;
        // the value lane's are the object layout (docs/OBJECT_IDENTITY_2026_10_20.md
        // §3.1): every object its __id and __type, so equal() compares a
        // keyless class's objects by identity, never as structs.
        com.legend.lowering.Lowerer lowerer = new com.legend.lowering.Lowerer(
                t -> identity ? com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t, true)
                        : com.legend.compiler.element.ClassLayouts.objectLayoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent(), ctx.implementations()).withEngineExistsJoinForm()
                .withDbTimeZone(env.timeZone());
        // D91: the <<equality.Key>> resolver rides EVERY lane — equal()
        // over keyed instances is the key relation on the execute path
        // too (the structural fallback erased class identity and read
        // non-key fields). Identity minting stays verdict-lane-only.
        lowerer = lowerer.withInstanceKeys(t2 -> com.legend.compiler
                .element.EqualityKeys.resolve(ctx, t2));
        lowerer = identity ? lowerer.withInstanceIds(env.instanceIds()::idOf)
                : lowerer.withObjectIdentity(env.instanceIds()::idOf).withDeclaredEgress();
        // the query's feature flags: a LOWERING concern (Lowerer.withFeatures)
        lowerer = lowerer.withFeatures(featuresOf(env, body));
        return lowerer;
    }

    /** A VALUE as the executor's body: the let prefix spliced, user calls
     * inlined (with the caller's envelope hook), natives staged, stores
     * resolved — the ONE road both the driver's evalValue and
     * planValue take (leg 3.1b: the audit's one real duplication). */
    public static java.util.List<TypedSpec> sideBody(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        return stagedSide(inlinedSide(value, letPrefix, specs, env, hook), specs, env);
    }

    /** The inlined side: the let prefix spliced and user calls inlined — the
     * form a literal-only side can be FOLDED from (a helper-built golden text is
     * literal only after its call is inlined). */
    public static java.util.List<TypedSpec> inlinedSide(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        java.util.List<TypedSpec> single = new java.util.ArrayList<>(letPrefix);
        single.add(value);
        var inliner = hook == null
                ? new com.legend.compiler.spec.UserCallInliner(specs)
                : new com.legend.compiler.spec.UserCallInliner(specs, hook);
        java.util.List<TypedSpec> body = new java.util.ArrayList<>(
                inliner.inlineBody(single));
        env.queryLets().putAll(inliner.queryLets());
        return body;
    }

    /** The inlined side staged and resolved for execution. */
    public static java.util.List<TypedSpec> stagedSide(java.util.List<TypedSpec> body,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env) {
        final java.util.List<TypedSpec> stageEnv = body;
        body.replaceAll(b -> com.legend.compiler.spec.NativeDispatch
                .stage(b, stageEnv, EnginePlans.nativeRoutines(specs, env)));
        return resolvedToExecute(resolver(specs, env), body, env);
    }

    /** Phase H for a body that EXECUTES on the env's session: resolved, then every store it touches checked
     *  against the runtime's connections (CrossStoreGuard — one session per query; a store the runtime does
     *  not bind refused). Every execution path resolves through here (C3b: four of the five ran unchecked). */
    public static java.util.List<TypedSpec> resolvedToExecute(com.legend.resolver.StoreResolver resolver,
            java.util.List<TypedSpec> body, PlanEnv env) {
        java.util.List<TypedSpec> resolved = resolver.resolve(body, env.runtimeFqn());
        // C2.2: stores bound to DIFFERENT connections cannot share
        // the one session connection — wall, never wrong-database rows
        CrossStoreGuard.check(resolved, env.ctx(), env.runtimeFqn());
        return resolved;
    }

    /** A statement's body PREPARED over {@code env} (already routed: {@link SystemRoutes}): framed by the execute call
     *  it runs under and ready to plan, or the kind of answer it is given with no plan ({@link Prepared}; the shared
     *  preparation, 2026-10-31: the body driver's prelude decided both while it ran the answers). */
    public static Prepared prepare(java.util.List<TypedSpec> body, PlanEnv env) {
        ModelContext ctx = env.ctx();
        // a late-bound grid is never pinned here: its columns are the ones its result reports, read when the plan
        // runs (the seam program's S3c, 2026-10-30; until then RawGridSchema.stamp took GridProbe's LIMIT-0 roster)
        // the from being executed IS the frame: its bound context carries the
        // post-processors, the connection's time zone and the options this
        // execution runs under (batch 120 — no static slot). The OUTERMOST
        // from in tree order (an assert side re-plans a spliced chain under a
        // property access; nested froms are the resolver's own wrappers).
        java.util.List<com.legend.compiler.spec.typed.TypedFrom> froms =
                com.legend.compiler.spec.typed.ExecutionContext.froms(
                        body.get(body.size() - 1));
        if (!froms.isEmpty()) {
            env = env.withFrame(froms.get(0).context());
            // the engine's addDriverTablePkForProject option (#45) is PER
            // EXECUTE CALL — the from's own bound option (batch 121):
            // projections gain the driver table's PK columns
            if (froms.get(0).context().driverTablePk()) {
                body = com.legend.resolver.DriverPkAppend.apply(body, ctx);
            }
            // the engine's importDataFlow option (batch 146): the union-row
            // projection gains the union's key threads, derived at chain
            // assembly and carried by the frame's bound context
            if (!froms.get(0).context().importDataFlowColumns().isEmpty()) {
                body = com.legend.resolver.ImportDataFlowAppend.apply(body,
                        froms.get(0).context().importDataFlowColumns(), ctx);
            }
        }
        TypedSpec root = body.get(body.size() - 1);
        // from() is context-only, but its info is the PRE-RESOLUTION
        // declared type — kept: a primitive-many declared root whose
        // resolved source became relation-shaped (scalar ->map lowers to
        // a one-column project) still executes as a VALUE COLLECTION, so
        // the Executor's null-drop applies (pure collections hold no
        // empties — the no-match parent contributes nothing, task #78).
        com.legend.compiler.element.type.ExprType declaredInfo = null;
        while (root instanceof com.legend.compiler.spec.typed.TypedFrom fr) {
            if (declaredInfo == null) {
                declaredInfo = fr.info();
            }
            root = fr.source();
        }
        // CATALOG DISPATCH (EFFECT rows, ladder census §10m): the
        // effectful K-natives run their registered arm when evaluation
        // reaches the call — one lookup, no name literals.
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && com.legend.builtin.NativeFn.Effect.of(nc.callee().id()).isPresent()) {
            return new Prepared.Effect(body, nc, env);
        }
        // A WRITE INTO A STORE TABLE (docs/WRITES_DESIGN_2026_10_18.md §4, L1: write, delete, update, merge): the
        // database runs the statement, and its update count is the write's value. It runs in the query's transaction
        // (BodyDriver.execute: Executor.inTransaction), never through the effect arms (an effect reseeds).
        if (com.legend.compiler.spec.StatementEffects.isStoreWrite(root)) {
            return new Prepared.StoreWrite(body, root, env);
        }
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall st
                && com.legend.builtin.Pure.Lite.STAGE.equals(st.callee().qualifiedName())) {
            throw new com.legend.error.NotImplementedException("stage(rows) holds rows for the statements after it:"
                    + " it is a let's value (let s = rows->stage()), not a statement's");
        }
        // DDL STRING generators (toDDL deprecated forms): evaluated HERE —
        // the engine walks its Database metamodel, we render from the
        // compiled store model (the lowerer has no model access)
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall ds
                && com.legend.builtin.NativeFn.DdlStatement.of(ds.callee().id()).isPresent()) {
            return new Prepared.DdlText(EnginePlans.ddlText(ds, env), ds.info().type());
        }
        com.legend.compiler.element.type.@com.legend.base.Nullable Type handle = handleType(root, env);
        if (handle != null) {
            return new Prepared.Handle(handle);
        }
        // a COLLECTION whose elements include DDL string generators (the
        // aggregationAware setup shape: [dropSchemaStatement(..), ...]
        // ->map(s|executeInDb(..))): every element evaluates here
        if (root instanceof com.legend.compiler.spec.typed.TypedCollection ddlColl
                && ddlColl.elements().stream().anyMatch(e ->
                        e instanceof com.legend.compiler.spec.typed.TypedNativeCall enc
                        && com.legend.builtin.NativeFn.DdlStatement.of(enc.callee().id()).isPresent())) {
            java.util.List<Object> strs = new java.util.ArrayList<>();
            for (TypedSpec e : ddlColl.elements()) {
                if (e instanceof com.legend.compiler.spec.typed.TypedNativeCall enc
                        && com.legend.builtin.NativeFn.DdlStatement.of(enc.callee().id()).isPresent()) {
                    strs.add(EnginePlans.ddlText(enc, env));
                } else if (e instanceof com.legend.compiler.spec.typed.TypedCString cs2) {
                    strs.add(cs2.value());
                } else {
                    throw new IllegalStateException("DDL statement collection"
                            + " carries a non-DDL, non-literal element: "
                            + e.getClass().getSimpleName());
                }
            }
            return new Prepared.DdlTexts(strs);
        }
        // map over an EFFECTFUL lambda ($sqls->map(sql|executeInDb(...))):
        // the source collection evaluates through the pipeline; each
        // element executes the lambda body with the parameter bound (the
        // one statement-orchestration shape the corpus's setup bodies use).
        if (root instanceof com.legend.compiler.spec.typed.TypedMap tm
                && containsEffectfulNode(java.util.List.of(tm.mapper()))) {
            return new Prepared.EffectfulMap(body, tm, env);
        }
        return new Prepared.Planned(body, root, declaredInfo, env);
    }

    /** The type of an ORCHESTRATION HANDLE (a connection, a runtime, a connection store): a value the program
     *  passes around and never lowers; null when {@code root} is not one. */
    private static com.legend.compiler.element.type.@com.legend.base.Nullable Type handleType(
            TypedSpec root, PlanEnv env) {
        if (root instanceof com.legend.compiler.spec.typed.TypedCast castC
                && castC.source()
                        instanceof com.legend.compiler.spec.typed.TypedNativeCall cbe2
                && com.legend.builtin.NativeFn.Effect.CONNECTION_BY_ELEMENT.fqn()
                        .equals(cbe2.callee().qualifiedName())) {
            return castC.info().type();
        }
        if (root instanceof com.legend.compiler.spec.typed.TypedNewInstance rni
                && ("meta::core::runtime::Runtime".equals(rni.classFqn())
                        || "meta::core::runtime::ConnectionStore"
                                .equals(rni.classFqn()))) {
            if (containsEffectfulNode(new java.util.ArrayList<>(
                    rni.properties().values()))) {
                throw new IllegalStateException("^" + rni.classFqn()
                        + "(...) constructor argument carries an"
                        + " executeInDb-family effect; the orchestration-"
                        + "handle arm never evaluates arguments");
            }
            return rni.info().type();
        }
        // TYPE-driven handle rule (XStore slice 2b): a CONNECTION/RUNTIME-
        // typed VALUE is an orchestration handle regardless of expression
        // shape — the corpus's connection-picking idiom
        // (testRuntime().connectionStores->filter(c|...)->toOne()) must
        // never lower to SQL (it list_filter'd a struct literal). Same
        // effect guard as the ctor arm: nested effects never drop silently.
        if (root.info().type()
                instanceof com.legend.compiler.element.type.Type.ClassType hct
                // Nil (the []-born BOTTOM type) subtypes EVERYTHING —
                // including Connection — but a Nil-typed root is an empty
                // VALUE ([]->tail() is an empty collection), never an
                // orchestration handle: the handle arm returned Scalar(null)
                // where the caller expects an empty Collection
                && !com.legend.compiler.element.type.PlatformTypes.isNil(hct)
                && ("meta::core::runtime::Runtime".equals(hct.fqn())
                        || "meta::core::runtime::ConnectionStore".equals(hct.fqn())
                        || env.ctx().isSubtype(hct.fqn(),
                                "meta::core::runtime::Connection"))) {
            if (containsEffectfulNode(java.util.List.of(root))) {
                throw new IllegalStateException("a connection-typed value"
                        + " expression carries an executeInDb-family effect;"
                        + " the orchestration-handle arm never evaluates it");
            }
            return root.info().type();
        }
        return null;
    }

    /** Does any node in these (post-inline) trees carry a REAL K effect?
     * An executeInDb whose SQL is a literal SELECT is provably read-only
     * (the corpus prints such probes) and does not count. */
    public static boolean containsEffectfulNode(java.util.List<TypedSpec> nodes) {
        for (TypedSpec n : nodes) {
            if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                    && com.legend.builtin.NativeFn.Effect.isDbEffect(nc.callee().id())
                    && !isLiteralSelect(nc)) {
                return true;
            }
            if (containsEffectfulNode(n.children())) {
                return true;
            }
        }
        return false;
    }

    public static boolean isLiteralSelect(
            com.legend.compiler.spec.typed.TypedNativeCall nc) {
        String fqn = nc.callee().qualifiedName();
        boolean sqlCarrier = com.legend.builtin.NativeFn.Effect.EXECUTE_IN_DB.fqn().equals(fqn);
        return sqlCarrier && !nc.args().isEmpty()
                && nc.args().get(0)
                        instanceof com.legend.compiler.spec.typed.TypedCString cs
                && cs.value().strip().toLowerCase(java.util.Locale.ROOT)
                        .startsWith("select");
    }

    /** A value's plan with no canon wrap, planned with no session: the plan and its root. */
    public record Bare(com.legend.sql.SqlQuery plan, TypedSpec root) {
    }

    /** {@code value}'s plan as a side reads it (the lets spliced, the user calls inlined, the natives staged, the stores
     *  resolved, the system route's tables named, the body prepared and lowered on the identity lane), or null when
     *  the value is answered with no plan ({@link Prepared}). The driver's {@code planBare} with no rider, no splice
     *  hook and no session: nothing opened, nothing run (the frame's planned CTE reads it). */
    public static @com.legend.base.Nullable Bare bare(TypedSpec value, java.util.List<TypedSpec> letPrefix,
            SpecCompiler specs, PlanEnv env) {
        java.util.List<TypedSpec> body = sideBody(value, letPrefix, specs, env, null);
        SystemRoutes.Route route = SystemRoutes.of(body, env);
        PlanEnv routed = route == null ? env : SystemRoutes.routed(env, SystemRoutes.renames(route));
        if (!(prepare(body, routed) instanceof Prepared.Planned p)) {
            return null;
        }
        return new Bare(lowerAndPrepare(p.body(), p.env(), p.env().ctx(), p.env().dialect(), true), p.root());
    }

    /** The test-data generator's VIEW fetch SQL: the view's lifted function (E.5, reached through the include closure
     *  like a table) as the compiler's relation -- the same node a user's accessor inlines to -- planned like any
     *  query with no session (the resolver materializes its joins), the fetched base tables renamed to their
     *  temporary tables (the replaceTables pass, the engine's {@code fixTables}), rendered in the lane's own dialect
     *  (the text runs on this connection). The generator's package never reaches the lowering: the driver hands it
     *  this (the seam program's S2j, 2026-10-31: planned here; the body driver's {@code planBare} until then). */
    public static String viewSql(String db, String view, java.util.Map<String, String> tableToTemp,
            SpecCompiler specs, PlanEnv env) {
        com.legend.compiler.element.TypedFunction fn = env.ctx().findViewFunction(db, view)
                .orElseThrow(() -> new com.legend.error.NotImplementedException(
                        "testDataGen: view '" + view + "' is not a view of '" + db + "'"));
        Bare b = bare(specs.viewRelation(fn), java.util.List.of(), specs, env);
        if (b == null) {
            throw new com.legend.error.NotImplementedException(
                    "testDataGen: view '" + view + "' planned to a host answer, not a query");
        }
        com.legend.sql.SqlQuery plan = com.legend.lowering.SqlPostProcessors.apply(
                b.plan(), t -> tableToTemp.getOrDefault(t, t));
        return env.dialect().render(plan);
    }
}
