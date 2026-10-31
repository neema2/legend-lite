// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * legend-engine's PLAN TEXT, written at planning (the planner-driver seam program's S2g, 2026-10-31; the body driver's
 * until then): a query's SQL in the engine's own spelling ({@link #engineSql}: lowered with the engine-text options
 * and rendered by an {@link com.legend.sql.dialect.EngineTextDialect}), the plan a {@code planToString} or
 * {@code toSQLString} call prints (D30: plan-time values, the routines {@link #nativeRoutines} hands the native
 * stager), and a plan's node model for the {@code $plan} handle's rows ({@link #planModel}). Every one is a function
 * of the model and the query over a {@link PlanEnv}: no session, nothing run.
 */
public final class EnginePlans {

    private EnginePlans() {
    }

    /**
     * The engine's SQL-text surface: lower the query lambda through the
     * platform's own G½->H->I against the MAPPING ARGUMENT and render with
     * the engine-style dialect. H2 only — other DatabaseTypes throw until
     * their renderers exist. Never lowers, never touches the connection.
     */
    private static @com.legend.base.Nullable String toSqlString(
            com.legend.compiler.spec.NativeDispatch.RoutineCall call,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env) {
        // the inputs across the overloads (SqlTextInputs): the receiver
        // form toSQL(...).toSQLString(dbType, …) reads its lambda and
        // mapping off the toSQL handle (batch 75)
        SqlTextInputs in = SqlTextInputs.of(call, letPrefix);
        if (in == null) {
            throw new com.legend.error.NotImplementedException(
                    "toSQLString over an SQLResult that is not a toSQL(...) handle");
        }
        // the dialect: an enum literal names it; toSQLStringPretty's
        // RUNTIME overload carries the connection in arg 2 — resolve its
        // DatabaseType structurally; the receiver form reads the
        // connection the engine reads ($connection.type off the handle's
        // runtime), through the lets and the inlined user calls
        TypedSpec dbArg = in.dialect();
        String db;
        if (dbArg instanceof com.legend.compiler.spec.typed.TypedEnumValue) {
            db = typedEnumTail(dbArg);
        } else if (in.receiverForm()) {
            TypedSpec rt = new com.legend.compiler.spec.UserCallInliner(specs)
                    .inlineBody(java.util.List.of(
                            com.legend.compiler.spec.typed.Lets.bound(in.runtime(), letPrefix)))
                    .get(0);
            String boundDb = com.legend.compiler.spec.typed.ExecutionContext.reader()
                    .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                    .read(java.util.Optional.empty(), rt).databaseType();
            if (boundDb == null) {
                throw new com.legend.error.NotImplementedException(
                        "toSQLString over a toSQL handle whose runtime carries"
                                + " no statically readable connection");
            }
            db = boundDb;
        } else {
            String dbBound = com.legend.compiler.spec.typed.ExecutionContext.reader()
                    .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                    .read(java.util.Optional.empty(), dbArg).databaseType();
            // C4 (handed off, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md): an unread type is the
            // reader's gap, read as the engine test connection's database until the rebuild reads it
            db = dbBound == null ? EngineText.ENGINE_TEST_DATABASE.name() : dbBound;
        }
        com.legend.sql.dialect.EngineTextDialect renderer = EngineText.engineText(com.legend.database.Databases.named(db));
        if (!(in.query()
                instanceof com.legend.compiler.spec.typed.TypedLambda lam)) {
            throw new com.legend.error.NotImplementedException(
                    "toSQLString whose query argument is not a lambda literal");
        }
        if (!(in.mapping()
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr)) {
            throw new com.legend.error.NotImplementedException(
                    "toSQLString mapping argument must be a mapping reference");
        }
        EngineSql es = engineSql(lam, pr.fullPath(), specs, env, renderer);
        com.legend.sql.SqlQuery post = com.legend.lowering.SqlPostProcessors
                .apply(es.plan(), env.tableReplace());
        // toNonExecutableSQLString: the engine's nonExecutable post-processor
        // (every SELECT takes `and 1 = 2`) — the IR pass, then the render
        if (com.legend.builtin.NativeFn.JavaRoutine.TO_NON_EXECUTABLE_SQL_STRING.fqn().equals(call.fqn())) {
            post = com.legend.lowering.SqlPostProcessors.nonExecutable(post);
        }
        return post == es.plan() ? es.sql()
                        : renderer.render(post);
    }

    public record EngineSql(com.legend.sql.SqlQuery plan, String sql,
            java.util.List<TypedSpec> body) {
    }

    public static EngineSql engineSql(
            com.legend.compiler.spec.typed.TypedLambda lam,
            @com.legend.base.Nullable String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            PlanEnv env,
            com.legend.sql.dialect.EngineTextDialect renderer) {
        return engineSql(lam.body(), mappingFqn, specs, env, renderer,
                java.util.Map.of(),
                java.util.function.UnaryOperator.identity());
    }

    /** {@code raw}'s SQL in the engine's text for its test connection's database (H2): no plan parameters, no
     *  renames -- an activity's SQL, as the engine records the SQL it generated (the frame planner's string entry). */
    public static String engineTestSql(java.util.List<TypedSpec> raw, @com.legend.base.Nullable String mappingFqn,
            SpecCompiler specs, PlanEnv env) {
        return engineSql(raw, mappingFqn, specs, env, EngineText.ENGINE_TEXT, java.util.Map.of(),
                java.util.function.UnaryOperator.identity()).sql();
    }

    /** The body form, with plan-TEMPLATE parameters: each named free
     * variable lowers to the engine's {@code ${name}} placeholder
     * (value = string-typed, driving the freemarker quote template). */
    public static EngineSql engineSql(java.util.List<TypedSpec> raw,
            @com.legend.base.Nullable String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            PlanEnv env,
            com.legend.sql.dialect.EngineTextDialect renderer,
            java.util.Map<String, com.legend.sql.SqlExpr.PlanParam>
                    planParams,
            java.util.function.UnaryOperator<String> tableRenames) {
        return engineSql(raw, mappingFqn, specs, env, renderer, planParams,
                tableRenames, java.util.List.of());
    }

    /** {@code chainMappings}: ModelChainConnection mappings from the plan
     * surface's runtime argument (M2M2R — ~src classes resolve through
     * them). */
    public static EngineSql engineSql(java.util.List<TypedSpec> raw,
            @com.legend.base.Nullable String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            PlanEnv env,
            com.legend.sql.dialect.EngineTextDialect renderer,
            java.util.Map<String, com.legend.sql.SqlExpr.PlanParam>
                    planParams,
            java.util.function.UnaryOperator<String> tableRenames,
            java.util.List<String> chainMappings) {
        java.util.List<TypedSpec> body =
                new com.legend.compiler.spec.UserCallInliner(specs)
                        .inlineBody(raw);
        boolean temporalRoot = com.legend.compiler.element.Temporal
                .anyTemporalGetAll(body, env.ctx());
        // the same resolver the frame runs with: the caller's lets (a
        // let-bound milestoning date) and the registered rows ride the
        // render too
        body = BodyPlanner.resolver(specs, env)
                .resolve(body, env.runtimeFqn(), mappingFqn, chainMappings);
        body = com.legend.resolver.RelationalRootForm.apply(
                body, env.ctx(), mappingFqn);
        // the query's feature flags: a LOWERING concern (Lowerer.withFeatures)
        com.legend.lowering.Lowerer lw = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(env.ctx(), t),
                f -> env.ctx().findClass(f).isPresent(), env.ctx().implementations())
                .withFeatures(BodyPlanner.featuresOf(env, body));
        if (!temporalRoot) {
            lw = lw.withEngineExistsJoinForm();
        }
        planParams.values().forEach(lw::bindPlanParam);
        // ENGINE-TEXT lowering: wire coercions read bare (a Lowerer option
        // since batch 136; a thread-local scope before)
        com.legend.sql.SqlQuery plan = lw.withEngineText().lower(body);
        // engine plans keep enum columns RAW (host-side decode) — the
        // plan-text form of enum-mapped columns/parameters — UNLESS the
        // context carries PUSH_DOWN_ENUM_TRANSFORM (pureToSQLQuery
        // pushDownEnumTransformations): then the decode stays in the SQL
        if (!BodyPlanner.featuresOf(env, body).contains(
                        com.legend.platform.Feature.PUSH_DOWN_ENUM_TRANSFORM)
                && plan instanceof com.legend.sql.SqlSelect sel
                && com.legend.compiler.element.type.Type.relationSchema(
                        body.get(body.size() - 1).info().type())
                        instanceof com.legend.compiler.element.type.Type
                                .RelationType rt) {
            plan = com.legend.plan.PlanEnumForm.apply(sel, rt);
        }
        // the runtime's relationalMapperPostProcessor renames (extracted
        // structurally from the plan call's runtime argument)
        if (plan instanceof com.legend.sql.SqlQuery p2) {
            plan = com.legend.lowering.SqlPostProcessors.apply(p2,
                    tableRenames);
        }
        // TEXT-channel rendering (synthetic scalar-map aliases drop — the
        // engine-style renderer IS the text channel)
        String text = renderer.render(plan);
        return new EngineSql(plan, text, body);
    }

    /** {@code planToString(executionPlan(func, MAPPING, runtime, ...),
     * ext)}: the SINGLE-RELATIONAL literal plan text (#47 pilot —
     * com.legend.plan.PlanText owns the format). */
    /** The executor's routine table for catalog-dispatched natives
     * (NativeDispatch): exactly the catalog's JAVA_ROUTINE rows. The
     * rules and literal minting live in the COMPILER (NativeDispatch,
     * Invariant 7); this supplies plan compilation only. */
    public static java.util.Map<String, com.legend.compiler.spec
            .NativeDispatch.Routine> nativeRoutines(
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env) {
        com.legend.compiler.spec.NativeDispatch.Routine text =
                (call, letPrefix) -> {
                    String r = planToString(call, letPrefix,
                            specs, env);
                    return java.util.Objects.requireNonNull(r, "plan text");
                };
        com.legend.compiler.spec.NativeDispatch.Routine sqlText =
                (call, letPrefix) -> {
                    String r = toSqlString(call, letPrefix, specs, env);
                    return java.util.Objects.requireNonNull(r, "sql text");
                };
        return java.util.Map.of(
                com.legend.builtin.NativeFn.JavaRoutine.PLAN_TO_STRING.fqn(), text,
                com.legend.builtin.NativeFn.JavaRoutine.PLAN_TO_STRING_WITHOUT_FORMATTING.fqn(),
                (call, letPrefix) -> text.value(call, letPrefix)
                        .replace("\n", "").replace(" ", ""),
                com.legend.builtin.NativeFn.JavaRoutine.TO_SQL_STRING.fqn(), sqlText,
                com.legend.builtin.NativeFn.JavaRoutine.TO_SQL_STRING_PRETTY.fqn(), sqlText,
                com.legend.builtin.NativeFn.JavaRoutine.TO_NON_EXECUTABLE_SQL_STRING.fqn(), sqlText);
    }

    private static @com.legend.base.Nullable String planToString(
            com.legend.compiler.spec.NativeDispatch.RoutineCall call,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env) {
        // the plan value chases through the LET PREFIX (getAll-76 lane:
        // `let plan = executionPlan(...)` then `$plan->planToString(...)`
        // inside an assert — the handle is a symbolic binding)
        TypedSpec a0 = com.legend.compiler.spec.typed.Lets.bound(call.args().get(0), letPrefix);
        if (a0 instanceof com.legend.compiler.spec.typed.TypedTestDataGen tg) {
            // the generator's plan names the engine's test connection, as every engine test of it does (its
            // runtime argument is not read)
            return env.generatorPlanText().apply(tg);
        }
        if (!(a0
                instanceof com.legend.compiler.spec.typed.TypedNativeCall ep)
                || !com.legend.compiler.element.type.PlatformTypes
                        .EXECUTION_PLAN.equals(ep.callee().qualifiedName())) {
            throw new com.legend.error.NotImplementedException(
                    "planToString over a non-executionPlan value");
        }
        // preval(lambda, ext) pre-folds constants at plan time — the
        // wrapped lambda IS the query (our lowering folds literals
        // anyway, so the unwrap is semantically inert here)
        com.legend.compiler.spec.typed.TypedSpec q = ep.args().get(0);
        if (q instanceof com.legend.compiler.spec.typed.TypedNativeCall pv
                && com.legend.compiler.element.type.PlatformTypes.PREVAL
                        .equals(pv.callee().qualifiedName())) {
            q = pv.args().get(0);
        }
        if (!(q instanceof com.legend.compiler.spec.typed.TypedLambda lam)) {
            throw new com.legend.error.NotImplementedException(
                    "executionPlan whose query argument is not a lambda");
        }
        // the 2-arg overload executionPlan(func, extensions) carries its
        // context IN the query — ->from(mapping, runtime) on the terminal
        String mappingFqn;
        boolean hasRuntimeArg;
        java.util.List<String> queryChain = java.util.List.of();
        if (ep.args().get(1) instanceof
                com.legend.compiler.spec.typed.TypedPackageableRef pr) {
            mappingFqn = pr.fullPath();
            hasRuntimeArg = ep.args().size() > 2;
        } else {
            // a DUMMY ^Mapping(name='') argument (or the 2-arg overload)
            // defers to the query's own ->from calls — cross-mapping
            // queries carry one per branch; the FIRST one names the plan
            mappingFqn = firstFromMapping(
                    lam.body().get(lam.body().size() - 1));
            hasRuntimeArg = false;
            if (mappingFqn == null) {
                // QUERY-SIDE CHAIN dispatch (withChainedMappings->from(rt)
                // — a runtime-only from carrying chainMappings): the chain
                // mapping that BINDS the root class names the plan
                java.util.List<String> qChain = firstFromChainMappings(
                        lam.body().get(lam.body().size() - 1));
                String rc0 = com.legend.plan.PlanText.rootGetAllClass(lam.body());
                if (!qChain.isEmpty() && rc0 != null) {
                    var srcs = new com.legend.resolver.ClassSources(
                            env.ctx(), specs);
                    java.util.List<String> binders = qChain.stream()
                            .distinct()
                            .filter(m2 -> srcs.binds(m2, rc0)).toList();
                    if (binders.size() == 1) {
                        mappingFqn = binders.get(0);
                        queryChain = qChain;
                    }
                }
            }
            if (mappingFqn == null) {
                throw new com.legend.error.NotImplementedException(
                        "executionPlan mapping argument must be a reference"
                        + " (or the query must carry ->from), got "
                        + ep.args().get(1).getClass().getSimpleName());
            }
        }
        // connection FLAGS ride instance properties; a testRuntime(true)
        // HELPER call carries them inside its body — inline once so the
        // property walkers see the constructed connection (the
        // quoteIdentifiers-flag goldens' testRuntime(quote) idiom)
        com.legend.compiler.spec.typed.ExecutionContext pc = hasRuntimeArg
                ? BodyPlanner.boundContext(ep.args().get(2), specs) : null;
        // the query's feature flags: the exeCtx overload's context argument
        // (an ExecutionOptionContext) and withFeatureFlags calls in the query
        // itself — read into the frame's options, the ONE ambient channel the
        // lowering entries consult
        java.util.Set<com.legend.platform.Feature> flags =
                java.util.EnumSet.noneOf(com.legend.platform.Feature.class);
        flags.addAll(com.legend.compiler.spec.typed.ExecutionContext.treeFeatures(lam.body()));
        // the MAPPING-LESS plan form (the query carries ->from): the engine's
        // executionPlan(f, context, extensions) adds PUSH_DOWN_ENUM_TRANSFORM
        // before routing (executionPlan_generation.pure contextWithEnumPushDown)
        // — enum decodes stay in the SQL, the TDS tuple carries no mapping id
        if (!(ep.args().get(1) instanceof com.legend.compiler.spec.typed.TypedPackageableRef)) {
            flags.add(com.legend.platform.Feature.PUSH_DOWN_ENUM_TRANSFORM);
        }
        if (com.legend.model.FunctionId.of(com.legend.builtin.Pure.EXECUTION_PLAN__FUNCTION_DEFINITION_1__MAPPING_1__RUNTIME_1__EXECUTION_CONTEXT_1__EXTENSION_MANY).equals(ep.callee().id()) && ep.args().size() == 5) {
            flags.addAll((pc == null ? com.legend.compiler.spec.typed.ExecutionContext.NONE : pc)
                    .withOptions(ep.args().get(3), v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                    .features());
        }
        if (!flags.isEmpty()) {
            flags.addAll(env.features());
            env = env.withFeatures(java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(flags)));
        }
        boolean quote = pc != null && pc.quoteIdentifiers();
        String tz = pc != null ? pc.timeZone() : null;
        com.legend.compiler.spec.typed.ExecutionContext.PlanConnection fromConn = pc == null
                ? firstFromConnectionName(
                        lam.body().get(lam.body().size() - 1))
                : null;
        var conn = pc != null && pc.connectionName() != null ? pc.connectionName() : fromConn;
        String connName = conn != null ? conn.planText()
                // no connection instance read at all: the engine's test runtime's (the reader gap W2.1)
                : "TestDatabaseConnection(type = \"" + EngineText.ENGINE_TEST_DATABASE.name() + "\")";
        com.legend.model.ConnectionDefinition.DatabaseType dbType = planDatabase(pc);
        if (!lam.parameters().isEmpty() || lam.body().size() > 1
                // a lone LET is a sequence too (E2E §4.4 cluster 1):
                // the engine prints Allocation, never bare Relational
                || (lam.body().size() == 1 && lam.body().get(0)
                        instanceof com.legend.compiler.spec.typed.TypedLet)) {
            return sequencePlan(lam, mappingFqn, specs, env, quote, tz,
                    connName, dbType);
        }
        String rootClass = com.legend.plan.PlanText.rootGetAllClass(lam.body());
        if (rootClass == null) {
            // a RELATION root (table accessor / tableToTDS): one node whose
            // TDS tuples resolve through the root table's database
            var tr = com.legend.plan.PlanText.rootTableReference(lam.body());
            if (tr == null) {
                throw new com.legend.error.NotImplementedException("planToString: no getAll or table root (multi-node plans pending)");
            }
            EngineSql tes = engineSql(lam.body(), mappingFqn, specs, env, EngineText.enginePlanText(dbType, quote, tz),
                    java.util.Map.of(), java.util.function.UnaryOperator.identity(), java.util.List.of());
            return com.legend.plan.PlanText.singleRelationRoot(env.ctx(), tr.store(),
                    tr.accessor(), tes.plan(), tes.sql(), lam.body(), connName);
        }
        java.util.List<String> chainMaps = new java.util.ArrayList<>(
                pc != null ? pc.chainMappings() : java.util.List.of());
        queryChain.stream().filter(m2 -> !chainMaps.contains(m2))
                .forEach(chainMaps::add);
        String crossDb = crossDbTdsPlan(lam, mappingFqn, specs,
                env, quote, tz, connName, dbType, rootClass, chainMaps);
        if (crossDb != null) {
            return crossDb;
        }
        EngineSql es = engineSql(lam.body(), mappingFqn, specs, env,
                EngineText.enginePlanText(dbType, quote, tz), java.util.Map.of(),
                java.util.function.UnaryOperator.identity(), chainMaps);
        return com.legend.plan.PlanText.single(env.ctx(), rootClass,
                        mappingFqn, es.plan(), es.sql(),
                        // PRE-resolution body: the TDS-vs-Class shape and
                        // the documentation channel live in the G output
                        // (post-H everything is a relation)
                        lam.body(), connName, chainMaps, es.plan(), BodyPlanner.pushDownEnums(env, lam.body()),
                        EngineText.enginePlanText(dbType, quote, tz)::selectList);
    }

    /** The engine's CROSS-STORE plan split (executionPlan tdsJoinTwoDB*
     * goldens): a TDS join whose sides live in DIFFERENT databases
     * becomes Sequence(Allocation(name=tdsVar, value=(Relational(left))),
     * Relational(join sql with the left side spliced as
     * {@code (${tdsVar})} — the engine VarSetPlaceHolder)). Null when the
     * terminal has no cross-store TDS join (the single-node path
     * continues). Type and resultColumns for the terminal resolve over
     * the ORIGINAL (pre-splice) plan — physical typing needs the real
     * from tree; only the SQL TEXT renders the placeholder form. */
    private static @com.legend.base.Nullable String crossDbTdsPlan(
            com.legend.compiler.spec.typed.TypedLambda lam,
            String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            PlanEnv env, boolean quote, @com.legend.base.Nullable String tz,
            @com.legend.base.Nullable String connName,
            com.legend.model.ConnectionDefinition.DatabaseType dbType, String rootClass,
            java.util.List<String> chainMaps) {
        TypedSpec term = lam.body().get(lam.body().size() - 1);
        com.legend.compiler.spec.typed.TypedJoin xj = null;
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>();
        work.add(term);
        while (!work.isEmpty()) {
            TypedSpec t = work.poll();
            if (t instanceof com.legend.compiler.spec.typed.TypedJoin j
                    && j.prefix().isEmpty()) {
                String a = streamStoreOf(j.left(), env.ctx(), mappingFqn);
                String b = streamStoreOf(j.right(), env.ctx(), mappingFqn);
                if (!a.isEmpty() && !b.isEmpty() && !a.equals(b)) {
                    xj = j;
                    break;
                }
            }
            work.addAll(t.children());
        }
        if (xj == null || !com.legend.compiler.element.type.Type
                .isRelation(xj.left().info().type())) {
            return null;
        }
        // the LEFT SPINE of nested cross-store joins, outermost first:
        // each spine join's LEFT becomes one Allocation (tdsVar,
        // tdsVar_0, ...) whose SQL splices the PREVIOUS var; the
        // outermost join is the terminal (tdsTwoJoinThreeDB: two flat
        // Allocations, never nested Sequences)
        java.util.List<com.legend.compiler.spec.typed.TypedJoin> spine =
                new java.util.ArrayList<>();
        com.legend.compiler.spec.typed.TypedJoin cur = xj;
        while (true) {
            spine.add(cur);
            com.legend.compiler.spec.typed.TypedJoin inner = null;
            java.util.ArrayDeque<TypedSpec> lw = new java.util.ArrayDeque<>();
            lw.add(cur.left());
            while (!lw.isEmpty()) {
                TypedSpec t = lw.poll();
                if (t instanceof com.legend.compiler.spec.typed.TypedJoin j2
                        && j2.prefix().isEmpty()) {
                    String a = streamStoreOf(j2.left(), env.ctx(), mappingFqn);
                    String b = streamStoreOf(j2.right(), env.ctx(), mappingFqn);
                    if (!a.isEmpty() && !b.isEmpty() && !a.equals(b)) {
                        inner = j2;
                        break;
                    }
                }
                lw.addAll(t.children());
            }
            if (inner == null) {
                break;
            }
            cur = inner;
        }
        java.util.List<String> allocs = new java.util.ArrayList<>();
        String prevVar = null;
        var planText = EngineText.enginePlanText(dbType, quote, tz);
        for (int k = spine.size() - 1; k >= 0; k--) {
            TypedSpec at = k == spine.size() - 1
                    ? spine.get(k).left() : spine.get(k + 1);
            // engine naming: the OUTERMOST spine allocation is tdsVar, the
            // inner ones tdsVar_0, tdsVar_1 … innermost-first in the
            // Sequence (tdsTwoJoinThreeDB: tdsVar_0 = person, tdsVar = the
            // person⋈firm join splicing ${tdsVar_0}; batch 112)
            String var = k == 0 ? "tdsVar" : "tdsVar_" + (k - 1);
            String aRoot = com.legend.plan.PlanText.rootGetAllClass(java.util.List.of(at));
            if (aRoot == null) {
                return null;
            }
            EngineSql aEs = engineSql(java.util.List.of(at), mappingFqn,
                    specs, env, planText,
                    java.util.Map.of(),
                    java.util.function.UnaryOperator.identity(), chainMaps);
            String aSql = prevVar == null ? aEs.sql()
                    : com.legend.plan.PlanText.spliceLeftVar(aEs.plan(), prevVar, planText::render);
            if (aSql == null) { return null; }
            String[] aImpl = com.legend.lineage.ScanRelations.rootImpl(
                    env.ctx(), mappingFqn, aRoot, chainMaps);
            allocs.add(com.legend.plan.PlanText.allocation(var,
                    com.legend.plan.PlanText.typeBlock(env.ctx(), aRoot,
                            aImpl, aEs.plan(), java.util.List.of(at),
                            mappingFqn, BodyPlanner.pushDownEnums(env, java.util.List.of(at))),
                    com.legend.plan.PlanText.single(env.ctx(), aRoot,
                            mappingFqn, aEs.plan(), aSql,
                            java.util.List.of(at), connName, chainMaps,
                            // a SPLICED allocation's resultColumns type
                            // through the placeholder (every var-sourced
                            // column INT), like the terminal's
                            prevVar == null ? aEs.plan()
                                    : com.legend.plan.PlanText.colsPlanFor(
                                            aEs.plan(), prevVar),
                            BodyPlanner.pushDownEnums(env, java.util.List.of(at)), planText::selectList)));
            prevVar = var;
        }
        EngineSql fullEs = engineSql(lam.body(), mappingFqn, specs, env,
                planText, java.util.Map.of(),
                java.util.function.UnaryOperator.identity(), chainMaps);
        String splicedSql = com.legend.plan.PlanText.spliceLeftVar(
                fullEs.plan(), java.util.Objects.requireNonNull(prevVar), planText::render);
        if (splicedSql == null) { return null; }
        String terminal = com.legend.plan.PlanText.single(env.ctx(),
                rootClass, mappingFqn, fullEs.plan(), splicedSql,
                lam.body(), connName, chainMaps,
                com.legend.plan.PlanText.colsPlanFor(
                        fullEs.plan(), prevVar), BodyPlanner.pushDownEnums(env, lam.body()), planText::selectList);
        String[] impl = com.legend.lineage.ScanRelations.rootImpl(
                env.ctx(), mappingFqn, rootClass, chainMaps);
        java.util.List<String> children = new java.util.ArrayList<>(allocs);
        children.add(terminal);
        return com.legend.plan.PlanText.sequence(
                        com.legend.plan.PlanText.typeBlock(env.ctx(),
                                rootClass, impl, fullEs.plan(),
                                lam.body(), mappingFqn, BodyPlanner.pushDownEnums(env, lam.body())),
                        children);
    }

    /** Pre-order search for the first {@code TypedFrom} carrying a
     * connection-name hint (instance-runtime from()). */
    private static com.legend.compiler.spec.typed.ExecutionContext.@com.legend.base.Nullable PlanConnection firstFromConnectionName(
            com.legend.compiler.spec.typed.TypedSpec t) {
        return com.legend.compiler.spec.typed.ExecutionContext.froms(t).stream()
                .map(com.legend.compiler.spec.typed.TypedFrom::connectionName)
                .filter(java.util.Objects::nonNull).findFirst().orElse(null);
    }

    /** Pre-order search for the first {@code TypedFrom} carrying
     * chainMappings (the query-side withChainedMappings channel). */
    private static java.util.List<String> firstFromChainMappings(
            com.legend.compiler.spec.typed.TypedSpec t) {
        return com.legend.compiler.spec.typed.ExecutionContext.froms(t).stream()
                .map(com.legend.compiler.spec.typed.TypedFrom::chainMappings)
                .filter(c -> !c.isEmpty()).findFirst().orElse(java.util.List.of());
    }

    /** Pre-order search for the first {@code ->from(mapping, …)} in the
     * query tree — the branch-level context of cross-mapping queries. */
    private static @com.legend.base.Nullable String firstFromMapping(
            com.legend.compiler.spec.typed.TypedSpec t) {
        if (t instanceof com.legend.compiler.spec.typed.TypedFrom fr
                && fr.mapping().isPresent()) {
            return fr.mapping().get().fullPath();
        }
        for (com.legend.compiler.spec.typed.TypedSpec c : t.children()) {
            String m = firstFromMapping(c);
            if (m != null) {
                return m;
            }
        }
        return null;
    }

    /** The SEQUENCE envelope: parameterized lambdas open with a
     * FunctionParametersValidationNode, each let becomes an Allocation
     * (literal values = Constant nodes), and the terminal Relational
     * lowers with every open variable as a {@code ${name}} plan-template
     * parameter. */
    private static @com.legend.base.Nullable String sequencePlan(
            com.legend.compiler.spec.typed.TypedLambda lam,
            String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            PlanEnv env, boolean quote, @com.legend.base.Nullable String timeZone,
            @com.legend.base.Nullable String connName, com.legend.model.ConnectionDefinition.DatabaseType dbType) {
        java.util.LinkedHashMap<String, com.legend.sql.SqlExpr.PlanParam>
                params = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, String> paramSpells =
                new java.util.LinkedHashMap<>();
        java.util.List<String> children = new java.util.ArrayList<>();
        java.util.List<QueryParameters.Declared> declared = QueryParameters.of(lam);
        if (!declared.isEmpty()) {
            java.util.List<String> ps = new java.util.ArrayList<>(declared.size());
            for (QueryParameters.Declared p : declared) {
                ps.add(p.name() + ":" + p.signature());
                paramSpells.put(p.name(), p.signature());
                String emFn = p.type() instanceof com.legend.compiler
                        .element.type.Type.EnumType et
                        ? com.legend.plan.PlanText.enumMapFnOf(env.ctx(),
                                mappingFqn, et.fqn())
                        : null;
                params.put(p.name(), p.planParam(emFn));
            }
            children.add(com.legend.plan.PlanText
                    .functionParametersNode(String.join(", ", ps)));
        }
        for (int i = 0; i < lam.body().size() - 1; i++) {
            if (!(lam.body().get(i)
                    instanceof com.legend.compiler.spec.typed.TypedLet let)) {
                throw new com.legend.error.NotImplementedException(
                        "plan: non-let intermediate statement");
            }
            children.add(PlanAllocations.node(let, mappingFqn, specs, env,
                    params, paramSpells, quote, timeZone, dbType, connName));
            params.put(let.name(), new com.legend.sql.SqlExpr.PlanParam(
                    let.name(), com.legend.lowering.PlanParams.kindOf(
                            let.info().type())));
        }
        TypedSpec term = lam.body().get(lam.body().size() - 1);
        if (term instanceof com.legend.compiler.spec.typed.TypedLet tlet) {
            // TRAILING let = Allocation node (ledger cluster 36; engine
            // processes a let cluster into AllocationExecutionNode and
            // emits Sequence only when clusters != 1) — a lone let IS
            // the plan, no Sequence envelope.
            children.add(java.util.Objects.requireNonNull(PlanAllocations
                    .node(tlet, mappingFqn, specs, env, params, paramSpells,
                            quote, timeZone, dbType, connName)));
            if (children.size() != 1) {
                throw new com.legend.error.NotImplementedException(
                        "plan: trailing let in a multi-node sequence"
                        + " (envelope type block from the let pending)");
            }
            return children.get(0);
        }
        String rootClass = com.legend.plan.PlanText.rootGetAllClass(java.util.List.of(term));
        if (rootClass == null) {
            throw new com.legend.error.NotImplementedException(
                    "plan: sequence terminal without a getAll root");
        }
        EngineSql es = engineSql(java.util.List.of(term), mappingFqn, specs,
                env, EngineText.enginePlanText(dbType, quote, timeZone), params,
                java.util.function.UnaryOperator.identity());
        String[] impl = com.legend.lineage.ScanRelations.rootImpl(
                env.ctx(), mappingFqn, rootClass);
        // temp-table IN protocol + envelope assembly (extracted at the
        // file guardrail — PlanEnvelope owns the block-vs-sequence rule)
        return PlanEnvelope.emit(es, children, env, rootClass, impl,
                mappingFqn, term, connName, dbType,
                EngineText.enginePlanText(dbType, quote, timeZone),
                !lam.parameters().isEmpty());
    }

    /** The plan connection handle (cluster 60): the engine's generated
     * plan carries the runtime connection on SQLExecutionNode, with
     * processRuntimeTestConnections' DDL expansion on testDataSetupSqls
     * — a TestDatabaseConnection with a PRESENT csv ('' is a present
     * [0..1] value in pure) and no declared sqls gets setUpDataSQLs
     * text; a LocalH2DatasourceSpecification concatenates the expansion
     * onto its declared sqls. */
    private static com.legend.plan.@com.legend.base.Nullable PlanConn planConnOf(
            com.legend.compiler.spec.typed.@com.legend.base.Nullable ExecutionContext pc, PlanEnv env) {
        if (pc == null) {
            return new com.legend.plan.PlanConn(
                    // C4 (handed off): no readable connection — the engine test connection's database
                    "TestDatabaseConnection", EngineText.ENGINE_TEST_DATABASE.name(), null,
                    java.util.List.of(), null);
        }
        var ni = pc.connectionInstance();
        String storeFqn = pc.storeFqn();
        com.legend.model.DatabaseDefinition db = storeFqn == null ? null
                : env.ctx().findDatabase(storeFqn).orElse(null);
        java.util.function.Function<String, java.util.Optional<
                com.legend.model.DatabaseDefinition>> lookup =
                f -> env.ctx().findDatabase(f);
        if (ni == null) {
            return new com.legend.plan.PlanConn(
                    // C4 (handed off): no readable connection — the engine test connection's database
                    "TestDatabaseConnection", EngineText.ENGINE_TEST_DATABASE.name(), null,
                    java.util.List.of(), null);
        }
        String kind = com.legend.compiler.element.type.PlatformTypes
                .relationalConnectionSimpleName(ni.classFqn());
        if (kind == null) {
            throw new IllegalStateException(
                    "plan connection: unmatched connection class " + ni.classFqn());
        }
        // the database the context read declares (a literal, a let-bound or function-answered one); C4 (handed off,
        // as planDatabase): one it cannot read is planned for the engine test connection's
        String type = pc.databaseType() != null ? pc.databaseType() : EngineText.ENGINE_TEST_DATABASE.name();
        String csv = ni.properties().get("testDataSetupCsv")
                instanceof com.legend.compiler.spec.typed.TypedCString c
                ? c.value() : null;
        java.util.List<String> sqls = java.util.List.of();
        if (csv != null && db != null) {
            sqls = com.legend.setup.Ddl.setUpDataSqlsText(csv, db, lookup, EngineText.ENGINE_TEXT);
        }
        com.legend.plan.PlanConn.DsSpec spec = null;
        if (ni.properties().get("datasourceSpecification")
                instanceof com.legend.compiler.spec.typed
                        .TypedNewInstance ds
                && com.legend.model.ConnectionSpecification.TEST_DATA_SPECIFICATION_CLASS.equals(ds.classFqn())) {
            String specCsv = ds.properties().get("testDataSetupCsv")
                    instanceof com.legend.compiler.spec.typed
                            .TypedCString sc2
                    ? sc2.value() : null;
            java.util.List<String> specSqls = specCsv != null && db != null
                    ? com.legend.setup.Ddl.setUpDataSqlsText(specCsv, db,
                            lookup, EngineText.ENGINE_TEXT)
                    : java.util.List.of();
            spec = new com.legend.plan.PlanConn.DsSpec(
                    "LocalH2DatasourceSpecification", specCsv, specSqls);
        }
        return new com.legend.plan.PlanConn(kind, type, csv, sqls, spec);
    }

    /** The database a plan's connection declares. C4 (handed off,
     * docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md): a plan whose connection the reader cannot
     * read is the reader's gap, planned for the engine test connection's database until the rebuild reads it. */
    private static com.legend.model.ConnectionDefinition.DatabaseType planDatabase(
            com.legend.compiler.spec.typed.@com.legend.base.Nullable ExecutionContext pc) {
        String declared = pc == null ? null : pc.databaseType();
        return declared != null ? com.legend.database.Databases.named(declared) : EngineText.ENGINE_TEST_DATABASE;
    }

    /** The PLAN NODE MODEL for an executionPlan call — same shapes the
     * text printer spells (Sequence / FunctionParametersValidation /
     * RelationalInstantiation / SQLExecution). */
    public static com.legend.plan.PlanNode planModel(
            com.legend.compiler.spec.typed.TypedNativeCall ep,
            com.legend.compiler.spec.SpecCompiler specs, PlanEnv env) {
        if (!(ep.args().get(0)
                instanceof com.legend.compiler.spec.typed.TypedLambda lam)) {
            throw new com.legend.error.NotImplementedException(
                    "plan walk: executionPlan argument shapes pending");
        }
        // mapping arg: the generalized reader (a bare ref, a dummy
        // ^Mapping, or the 2-arg overload with in-query from) — the
        // SAME rules the text printer already applies at planText
        // (E2E §4.4 cluster 8: this path demanded a bare ref only)
        String pmFqn = ep.args().size() > 1 && ep.args().get(1)
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr
                ? pr.fullPath()
                : firstFromMapping(lam.body().get(lam.body().size() - 1));
        if (pmFqn == null) {
            throw new com.legend.error.NotImplementedException(
                    "plan walk: no mapping (arg or in-query from) named");
        }
        // same helper-call inlining as planToString: testRuntime(true)
        // carries the connection flags inside its body
        com.legend.compiler.spec.typed.ExecutionContext pc2 = ep.args().size() > 2
                ? BodyPlanner.boundContext(ep.args().get(2), specs) : null;
        boolean quote = pc2 != null && pc2.quoteIdentifiers();
        String tz = pc2 != null ? pc2.timeZone() : null;
        java.util.LinkedHashMap<String, com.legend.sql.SqlExpr.PlanParam>
                params = new java.util.LinkedHashMap<>();
        java.util.List<com.legend.plan.PlanNode.Param> fps =
                new java.util.ArrayList<>();
        for (QueryParameters.Declared p : QueryParameters.of(lam)) {
            params.put(p.name(), p.planParam(null));
            // supportsStream (engine storeContract:221 + executionPlan_
            // generation findParamsSupportedForStreamInput): the param
            // streams iff it is read under an in(...) call at least once
            // and NEVER in any other call position
            fps.add(new com.legend.plan.PlanNode.Param(
                    p.name(),
                    supportsStream(lam, p.name(),
                            src -> streamStoreOf(src, env.ctx(), pmFqn))));
        }
        TypedSpec term = lam.body().get(lam.body().size() - 1);
        // the runtime argument may carry relationalMapperPostProcessor
        // renames — extracted structurally, applied over the lowered IR
        java.util.function.UnaryOperator<String> mapperRenames =
                ep.args().size() > 2
                ? com.legend.plan.RelationalMapperRenames.extract(
                        ep.args().get(2), specs, env.queryLets(), env.ctx())
                : java.util.function.UnaryOperator.identity();
        // the runtime's ModelChainConnection mappings (and an in-query
        // from's chain) dispatch the M2M source layers — the same
        // context planToString resolves under (testModelConnection*)
        java.util.List<String> chainMaps = new java.util.ArrayList<>(
                pc2 != null ? pc2.chainMappings() : java.util.List.of());
        firstFromChainMappings(term).stream()
                .filter(m2 -> !chainMaps.contains(m2)).forEach(chainMaps::add);
        EngineSql es = engineSql(java.util.List.of(term), pmFqn,
                specs, env, EngineText.enginePlanText(planDatabase(pc2), quote, tz),
                params, mapperRenames, chainMaps);
        com.legend.plan.PlanNode sqlNode = new com.legend.plan.PlanNode(
                "SQLExecutionNode", java.util.List.of(), es.sql(),
                java.util.List.of(),
                com.legend.plan.PlanNode.EXEC_TRACE_COMMENT,
                planConnOf(pc2, env));
        com.legend.plan.PlanNode rel = new com.legend.plan.PlanNode(
                "RelationalInstantiationExecutionNode",
                java.util.List.of(sqlNode), null, java.util.List.of());
        if (lam.parameters().isEmpty() && lam.body().size() == 1) {
            return rel;
        }
        com.legend.plan.PlanNode fpvn = new com.legend.plan.PlanNode(
                "FunctionParametersValidationNode", java.util.List.of(),
                null, fps);
        return new com.legend.plan.PlanNode("SequenceExecutionNode",
                java.util.List.of(fpvn, rel), null, java.util.List.of());
    }

    /** The member name of a typed enum-shaped read (DatabaseType.H2). */
    /** Engine stream-input rule (storeContract.pure:221 supportsStream +
     * executionPlan_generation.pure findParamsSupportedForStreamInput): a
     * parameter SUPPORTS STREAM iff at least one read sits under an
     * {@code in(...)} call (directly or through a collection literal) and
     * NO read sits anywhere else. The supported flag re-evaluates at
     * every call boundary (a read under {@code isEmpty}/{@code map}/...
     * is an unsupported occurrence, even inside an in's argument tree). */
    private static boolean supportsStream(
            com.legend.compiler.spec.typed.TypedLambda lam, String param,
            java.util.function.Function<TypedSpec, String> storeOf) {
        boolean[] flags = new boolean[2];   // {under-in, elsewhere}
        java.util.Set<String> inStores = new java.util.LinkedHashSet<>();
        for (TypedSpec b : lam.body()) {
            streamScan(b, param, false, "", flags, inStores, storeOf);
        }
        // engine findParamsSupportedForStreamInput: supported usages
        // spanning MORE THAN ONE STORE also disqualify (the TwoDB pin)
        return flags[0] && !flags[1] && inStores.size() <= 1;
    }

    private static void streamScan(TypedSpec n, String param,
            boolean underIn, String store, boolean[] flags,
            java.util.Set<String> inStores,
            java.util.function.Function<TypedSpec, String> storeOf) {
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable v
                && v.name().equals(param)) {
            if (underIn) {
                flags[0] = true;
                inStores.add(store);
            } else {
                flags[1] = true;
            }
            return;
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedLambda il
                && il.parameters().contains(param)) {
            return;   // shadowed
        }
        // a FILTER's predicate reads belong to ITS chain's store
        // (engine elementPathForCluster — each cluster carries its store)
        if (n instanceof com.legend.compiler.spec.typed.TypedFilter tf) {
            streamScan(tf.source(), param, false, store, flags, inStores,
                    storeOf);
            streamScan(tf.predicate(), param, false,
                    storeOf.apply(tf.source()), flags, inStores, storeOf);
            return;
        }
        boolean isIn = n instanceof
                com.legend.compiler.spec.typed.TypedNativeCall nc
                && "meta::pure::functions::collection::in"
                        .equals(nc.callee().qualifiedName());
        boolean carry = n instanceof
                com.legend.compiler.spec.typed.TypedCollection && underIn;
        for (TypedSpec c : n.children()) {
            streamScan(c, param, isIn || carry, store, flags, inStores,
                    storeOf);
        }
    }

    /** The DATABASE behind a chain's first class extent — the stream
     * scan's store key (falls back to the class FQN when unmapped). */
    private static String streamStoreOf(TypedSpec source,
            com.legend.compiler.element.ModelContext ctx, String mappingFqn) {
        TypedSpec cur = source;
        java.util.ArrayDeque<TypedSpec> q = new java.util.ArrayDeque<>();
        q.add(cur);
        while (!q.isEmpty()) {
            TypedSpec x = q.poll();
            if (x instanceof com.legend.compiler.spec.typed.TypedGetAll ga) {
                try {
                    return com.legend.lineage.ScanRelations.rootImpl(ctx,
                            mappingFqn, ga.classFqn())[2];
                } catch (com.legend.error.NotImplementedException e) {
                    return ga.classFqn();
                }
            }
            q.addAll(x.children());
        }
        return "";
    }

    private static String typedEnumTail(TypedSpec v) {
        if (v instanceof com.legend.compiler.spec.typed.TypedEnumValue ev) {
            return ev.value();
        }
        if (v instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa) {
            return pa.property();
        }
        return String.valueOf(v);
    }
}
