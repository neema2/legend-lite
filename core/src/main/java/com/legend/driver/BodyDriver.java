// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.ExecuteOptions;
import com.legend.Execution;
import com.legend.TypedQuery;
import com.legend.planner.CrossStoreGuard;
import com.legend.planner.MetamodelSeeds;
import com.legend.planner.PlanMaker;
import com.legend.planner.QueryParameters;

import com.legend.compiler.element.ModelContext;
import com.legend.judge.TypedSide;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE BODY DRIVER (named for what it is 2026-10-29, the result boundary program's track D; the K-phase
 * {@code StatementExecutor} since audit 17): the orchestration of a TEST BODY -- statement sequencing over effectful
 * bodies, call frames, effect analysis, the judges and their arms, and the K-native dispatch arms (executeInDb,
 * dropAndCreateTableInDb/Schema, print). {@link Execution#runProgram} is the one entry and delegates here; nothing in
 * this class decides pipeline ORDER -- it runs already-typed statements, each through its plan.
 *
 * <p>The execution environment is ONE ambient session ({@link com.legend.exec.Sessions.Session}: the driver holds
 * no JDBC connection since track B's B1b, 2026-10-29, and reaches the database only through the
 * session's statements), one dialect, one optional raw-SQL failure sink ({@link ExecEnv}) — connection-valued
 * expressions are never evaluated.
 *
 * <p>Public since step C (D73, 2026-10-30), for ONE caller: the execution door (Execution.runProgram) -- the class was package-private by design while it shared the root package with the door; ArchitectureTest.theDriverHasOneCaller keeps the design across the package line.
 */
public final class BodyDriver {

    private BodyDriver() {
    }

    /** The G½→H→I→J→K back half over a query TYPED ONCE ({@link TypedQuery}: its statements typed as a query body,
     * by the spec compiler the driver then compiles every helper through), on {@code session} -- the one entry for a
     * program hands it here ({@link Execution#runProgram}; track B's B2, 2026-10-29: until then the driver typed the
     * resolved AST itself for each of three entries). {@code assertListener} observes every statement-root assert
     * verdict (the runner's scoring seam — judging stays here, in one place); {@code replayReferee} is the registered
     * SQL-replay referee (SQLTEXT charter §2 — same registration seam, same nullable carriage; production passes
     * neither); the execute OPTIONS ride the environment and the result — no static slot. {@code answer}, when
     * given, takes the body's last statement's answer as a stream ({@link Execution.Answer}: the doors, 2026-10-29;
     * until then the last side came back materialized); the driver returns nothing. */
    public static void execute(TypedQuery typed,
            @com.legend.base.Nullable String runtimeFqn,
            com.legend.sql.dialect.SqlDialect dialect,
            com.legend.exec.Sessions.Session session,
            com.legend.judge.@com.legend.base.Nullable AssertListener assertListener,
            com.legend.judge.@com.legend.base.Nullable SqlReplayReferee replayReferee,
            ExecuteOptions options, Execution.@com.legend.base.Nullable Answer answer) {
        ModelContext ctx = typed.context();
        SpecCompiler specs = typed.specs();
        java.util.List<TypedSpec> typedBody = new java.util.ArrayList<>(typed.body());
        com.legend.protocol.spec.ValueSpecification resolved = typed.resolved();
        // the execution OPTIONS: the per-call ones ride each execute call's
        // own from (the engine's exeCtx overload binds them onto its bound
        // context); the caller's ride the environment
        ExecEnv env0 = ExecEnv.of(ctx, runtimeFqn, dialect, session, options);
        ExecEnv env = assertListener == null && replayReferee == null ? env0
                : env0.withListeners(assertListener, replayReferee);
        if (resolved instanceof com.legend.protocol.spec.LambdaFunction rlf
                && rlf.parameters().isEmpty()) {
            env = env.withProtocolBody(rlf.body());
        }
        // a program that writes a store table runs in ONE transaction (docs/WRITES_DESIGN_2026_10_18.md §4): its
        // writes all land or none do; a caller's open transaction is joined, not committed. So does one that stages
        // rows: its staged tables are the transaction's (SqlDml.CreateTempTableAs.staged)
        java.util.Map<com.legend.model.FunctionId, Boolean> memo = new java.util.HashMap<>();
        if (typedBody.stream().anyMatch(s -> com.legend.resolver.StagedRows.rowsOf(s) != null
                || com.legend.compiler.spec.StatementEffects.writesStore(s, specs, memo))) {
            com.legend.resolver.StagedRows.refuseLetsTheWritesChange(typedBody, specs);
            ExecEnv tx = env;
            tx.session().inTransaction(() -> BodyCompiler.execute(typedBody, new java.util.ArrayList<>(), specs,
                    tx, answer));
            return;
        }
        BodyCompiler.execute(typedBody, new java.util.ArrayList<>(), specs, env, answer);
    }

    /** The raw-SQL ledger append (Phase 2b): the caller's recorder, when it
     * handed one through the options; the executor never keeps one. */

    /** The engine's golden DDL TEXT renderer (root layer only — invariant 4d): the text of the database
     *  legend-engine's test connections declare. */
    static final com.legend.sql.dialect.EngineTextDialect ENGINE_TEXT = com.legend.planner.EngineText.ENGINE_TEXT;

    private static void record(ExecEnv env, String sql, boolean query) {
        var r = env.options().recorder();
        if (r != null) {
            r.recordExecuted(sql, query);
        }
    }

    /** The K-phase execution environment: the body's PLANNING half ({@link com.legend.planner.PlanEnv}: the model,
     * the runtime, the ONE dialect -- audit 17: recomputing it per arm invited a future mixed-dialect bug -- the
     * query's lets, the renames, the frame, the features, the site ids) and what the body RUNS on: ONE ambient
     * session, the trace, the verdict batch, the effect sink, the listeners and the caller's execute options. Split
     * in two by the seam program's S2g (2026-10-31) so planning helpers can leave the driver; until then one record. */
    record ExecEnv(com.legend.planner.PlanEnv plan,
            com.legend.exec.Sessions.Session session,
            com.legend.judge.@com.legend.base.Nullable AssertListener assertListener,
            com.legend.judge.@com.legend.base.Nullable SqlReplayReferee replayReferee,
            ExecuteOptions options,
            com.legend.exec.ExecutionTrace trace,
            com.legend.judge.@com.legend.base.Nullable VerdictBatch verdictBatch,
            com.legend.exec.@com.legend.base.Nullable EffectSink effectSink) {

        /** A body's environment at its start: its planning half fresh ({@link com.legend.planner.PlanEnv#of}, the
         *  options' features), no listeners, a fresh trace, no batch, no sink. */
        static ExecEnv of(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
                com.legend.sql.dialect.SqlDialect dialect, com.legend.exec.Sessions.Session session,
                ExecuteOptions options) {
            return new ExecEnv(com.legend.planner.PlanEnv.of(ctx, runtimeFqn, dialect, options.features(),
                    // the generator's plan names the engine's test connection, as every engine test of it does (its
                    // runtime argument is not read)
                    tg -> com.legend.testdatagen.TestDataGenerationNatives.planTextResult(tg, ctx, ENGINE_TEXT,
                            "TestDatabaseConnection(type = \"" + com.legend.planner.EngineText.ENGINE_TEST_DATABASE.name()
                                    + "\")")), session,
                    null, null, options, new com.legend.exec.ExecutionTrace(), null, null);
        }

        ModelContext ctx() {
            return plan.ctx();
        }
        @com.legend.base.Nullable String runtimeFqn() {
            return plan.runtimeFqn();
        }
        com.legend.sql.dialect.SqlDialect dialect() {
            return plan.writtenIn();
        }
        java.util.Map<String, TypedSpec> queryLets() {
            return plan.queryLets();
        }
        java.util.Map<String, String> tableReplace() {
            return plan.tableReplace();
        }
        java.util.Map<String, java.util.Map<String, java.util.List<java.util.List<String>>>> planRows() {
            return plan.planRows();
        }
        java.util.List<com.legend.protocol.spec.ValueSpecification> protocolBody() {
            return plan.protocolBody();
        }
        com.legend.compiler.spec.typed.@com.legend.base.Nullable ExecutionContext frame() {
            return plan.frame();
        }
        com.legend.compiler.spec.typed.ExecutionContext.PostProcessors postProcessors() {
            return plan.postProcessors();
        }
        /** The frame connection's time zone (every DateTime literal spells in it). */
        @com.legend.base.Nullable String timeZone() {
            return plan.timeZone();
        }

        /** The same environment over another planning half. */
        ExecEnv withPlan(com.legend.planner.PlanEnv p) {
            return p == plan ? this
                    : new ExecEnv(p, session, assertListener, replayReferee, options, trace, verdictBatch, effectSink);
        }
        /** Leg 3.4: the body's deferred verdicts (database mode). */
        ExecEnv withVerdictBatch(com.legend.judge.VerdictBatch b) {
            return new ExecEnv(plan, session, assertListener, replayReferee, options, trace, b, effectSink);
        }
        /** Block-compiler stage 3: the effect natives' sends land in this sink (an
         * effect segment under construction) instead of the connection. */
        ExecEnv withEffectSink(com.legend.exec.@com.legend.base.Nullable EffectSink sink) {
            return new ExecEnv(plan, session, assertListener, replayReferee, options, trace, verdictBatch, sink);
        }
        /** The caller's execute options (the PCT wire render); their features are the planning half's. */
        ExecEnv withOptions(ExecuteOptions o) {
            return new ExecEnv(plan.withFeatures(o.features()), session, assertListener, replayReferee, o, trace,
                    verdictBatch, effectSink);
        }
        /** The executing frame's bound context (post-processors, time zone,
         * options) — set where an execute frame is entered. */
        ExecEnv withFrame(com.legend.compiler.spec.typed.ExecutionContext f) {
            return withPlan(plan.withFrame(f));
        }
        ExecEnv withTableReplace(java.util.Map<String, String> tr) {
            return withPlan(plan.withTableReplace(tr));
        }
        ExecEnv withListeners(com.legend.judge.@com.legend.base.Nullable AssertListener l,
                com.legend.judge.@com.legend.base.Nullable SqlReplayReferee o) {
            return new ExecEnv(plan, session, l, o, options, trace, verdictBatch, effectSink);
        }
        /** The same environment over another session — the system
         * database's session for a body that reads the metamodel (the same
         * session object on every route, so two sides' sessions compare by
         * identity). */
        ExecEnv withSession(com.legend.exec.Sessions.Session other) {
            return other == session ? this
                    : new ExecEnv(plan, other, assertListener, replayReferee, options, trace, verdictBatch, effectSink);
        }

        /** The plan runner's source for this environment's session: the body's session, each query's
         *  execution-trace stamp published to this environment's trace ({@link com.legend.exec.PlanSessions#of}) --
         *  the one way the body driver reaches the database (the doors, 2026-10-29: the schema probes and the seeds
         *  are plan statements the runner sends, the transaction bracket the session's own; track B's B1a and B1b had
         *  them on the session's statements, and before that on a JDBC connection). */
        com.legend.exec.PlanSessions.Source planSessions() {
            return com.legend.exec.PlanSessions.of(session, trace);
        }

        /** Where a statement of this environment runs: the body's target ({@link JudgePlans#target}). */
        com.legend.executionplan.ExecutionPlan.Target target() {
            return JudgePlans.target(this);
        }

        /** The query's PROTOCOL statements (the source-shaped lets a
         * handle's rows are built from — the lineage scan walks the raw
         * query lambda). */
        ExecEnv withProtocolBody(
                java.util.List<com.legend.protocol.spec.ValueSpecification> body) {
            return withPlan(plan.withProtocolBody(body));
        }
    }

    /**
     * A BODY — lets and one result expression, or a sequence with effects (corpus
     * setup functions: {@code executeInDb} statements) — compiles to ONE artifact
     * ({@link BodyCompiler}): frames, verdict rows, scripts, prepared values, the
     * fragment map. A statement-position call to an effectful function expands as a
     * statement sequence in a FRESH call frame (parameters bound as lets; closed
     * bodies make frames capture-proof — no &alpha;-renaming needed). Value
     * evaluation still ALWAYS lowers to SQL; only the sequencing lives host-side.
     */
    static @com.legend.base.Nullable TypedSide executeStatements(
            java.util.List<TypedSpec> stmts, java.util.List<TypedSpec> letPrefix,
            SpecCompiler specs, ExecEnv env0) {
        // THE BLOCK COMPILER (stages 1–4, 2026-09-22): every body is walked ONCE by the
        // segment walk — verdict segments (deferred rows under the database judge; the
        // host arm judges at once, it has no batch), effect segments as scripts, values
        // prepared at compile and run at the segment's close. The statement-by-statement
        // loop that preceded it is deleted (stage 4); a body no arm claims walls loudly.
        return BodyCompiler.execute(stmts, letPrefix, specs, env0);
    }

    /** The body's verdict batch: the fusion (flat WITH + UNION ALL), the judge, and the runner that sends each
     * statement as a plan (JudgePlans, step 6). */
    static com.legend.judge.VerdictBatch newVerdictBatch(ExecEnv env) {
        return new com.legend.judge.VerdictBatch(com.legend.lowering.VerdictSql::batch, DatabaseJudge::verdictOf,
                JudgePlans.runner(env), env.trace());
    }

    /** A statement at VALUE position, PREPARED — the compile phases only (helper
     * inlining, native staging, the inlined root's re-classification, store
     * resolution, the cross-store wall) with nothing run (block-compiler stage 2,
     * 2026-09-21: the loop and the compiler share this ONE preparation; the loop
     * runs it at once, the compiler at the artifact's run). Exactly one of
     * {@code verdict} (an inlined assert root, already adjudicated into the
     * batch), {@code contextOwner} (assertError's arm), {@code forcedFrame}
     * (execute at value position: the eager frame run IS the value) or the
     * resolved {@code body} decides the run. */
    record PreparedValue(java.util.List<TypedSpec> body, ExecEnv env,
            java.util.List<TypedSpec> lets,
            @com.legend.base.Nullable TypedSide verdict,
            com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedNativeCall contextOwner,
            com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedNativeCall forcedFrame,
            /** the session's temporary table a staged let's rows are computed into (Pure.Lite.STAGE), null for
             *  any other value */
            @com.legend.base.Nullable String stagedInto) {
        PreparedValue(java.util.List<TypedSpec> body, ExecEnv env, java.util.List<TypedSpec> lets,
                @com.legend.base.Nullable TypedSide verdict,
                com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedNativeCall contextOwner,
                com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedNativeCall forcedFrame) {
            this(body, env, lets, verdict, contextOwner, forcedFrame, null);
        }

        /** This relation's rows, run into the session's temporary table {@code table}. */
        PreparedValue stagedInto(String table) {
            if (verdict != null || contextOwner != null || forcedFrame != null) {
                throw new com.legend.error.NotImplementedException("rows staged (stage) from a statement that is not a"
                        + " query of the database");
            }
            return new PreparedValue(body, env, lets, null, null, null, table);
        }
    }

    static PreparedValue prepareValue(TypedSpec stmt, TypedSpec bare,
            java.util.List<TypedSpec> letPrefix, java.util.Map<String, ExecFrame> execFrames,
            SpecCompiler specs, ExecEnv env) {
        java.util.List<TypedSpec> lets = new java.util.ArrayList<>(letPrefix);
        java.util.List<TypedSpec> single = new java.util.ArrayList<>(letPrefix);
        single.add(stmt);
        var stmtInliner = new com.legend.compiler.spec.UserCallInliner(specs,
                spliceHook(execFrames, letPrefix, specs, env));
        java.util.List<TypedSpec> body = new java.util.ArrayList<>(
                stmtInliner.inlineBody(single));                      // Phase G½
        env.queryLets().putAll(stmtInliner.queryLets());
        final java.util.List<TypedSpec> stageEnv = body;
        body.replaceAll(b -> com.legend.compiler.spec.NativeDispatch
                .stage(b, stageEnv, com.legend.planner.EnginePlans.nativeRoutines(specs, env.plan())));

        TypedSpec preRoot = com.legend.compiler.spec.typed.Lets.bare(body.get(body.size() - 1));
        while (preRoot instanceof com.legend.compiler.spec.typed.TypedFrom pf) {
            preRoot = pf.source();
        }
        preRoot = foldPairProjection(preRoot);
        // a helper call that β-reduced to an ASSERT-family root (the
        // corpus's runLegendTest(f, vars, expected) wrappers: lets +
        // one assert): the verdict is World 1's exactly as for a
        // statement-root assert — adjudicate the inlined root
        // — scoped to the STRING ENTRY's reads (a TypedJsonResult /
        // TypedJsonAccess in the inlined statement): every other
        // helper-wrapped assert keeps its existing route (the SQL-text
        // arms adjudicate those downstream; adjudicating them here as
        // plain equality regressed ~200 text-golden flips, 2026-09-03)
        // — and to an assert over CLASS VALUES (toPostgresModel's
        // assertConversion(expected:Node, input): assertEquals of two
        // constructed instances — the struct verdict of batch 53; no
        // SQL-text arm can judge a struct, so nothing downstream is
        // displaced)
        if (bare instanceof com.legend.compiler.spec.typed.TypedUserCall
                && preRoot instanceof com.legend.compiler.spec.typed.TypedNativeCall
                && (com.legend.compiler.spec.VerdictRoutes.readsStringEntry(preRoot)
                        || com.legend.compiler.spec.VerdictRoutes.assertsClassValue(preRoot))) {
            TypedSide inlinedVerdict = AssertVerdicts.tryAdjudicate(
                    preRoot, letPrefix, specs, frameReplaceEnv(stmt, execFrames, env, letPrefix, specs),
                    spliceHook(execFrames, letPrefix, specs, env));
            if (inlinedVerdict != null) {
                return new PreparedValue(body, env, lets, inlinedVerdict, null, null);
            }
        }
        // $plan.processingTemplateFunctions — the ExecutionPlan class
        // property (executionPlan.pure:67): every relational node
        // carries relationalPlanSupportFunctions(connection), deduped
        // plan-wide (executionPlan_generation.pure:215)
        // CATALOG DISPATCH at the statement's value position (§4AG
        // — ladder migration #22, zero function-name if-checks):
        // CONTEXT_OWNER rows run their registered ARM (assertError:
        // f's body runs in the database under the arm's catch);
        // HANDLE rows run their registered FORCE (execute: the eager
        // frame run IS the value); everything the plan reader can
        // answer ($plan navigation — the engine's own plan API,
        // evaluated over the PLAN NODE MODEL) returns its value.
        if (preRoot instanceof com.legend.compiler.spec.typed
                        .TypedNativeCall cat) {
            if (com.legend.builtin.NativeFn.ContextOwner.of(cat.callee().id()).isPresent()) {
                return new PreparedValue(body, env, lets, null, cat, null);
            }
            if (com.legend.builtin.NativeFn.Handle.forcesAtValuePosition(cat.callee().id())) {
                return new PreparedValue(body, env, lets, null, null, cat);
            }
        }
        com.legend.resolver.StoreResolver resolver =
                com.legend.planner.BodyPlanner.resolver(specs, env.plan());
        body = com.legend.planner.BodyPlanner.resolvedToExecute(resolver, body, env.plan());              // Phase H
        // the statement's env is widened LAST, after resolution — the loop's own order
        // (computing it before resolution changed a lowering: an order dependence
        // in the shared state the inliner touches, measured 2026-09-21, owed)
        return new PreparedValue(body, frameReplaceEnv(stmt, execFrames, env, letPrefix, specs),
                lets, null, null, null);
    }

    /** The body's LAST value statement answered to the caller's sink (the doors, 2026-10-29): a planned statement
     *  streams through the plan runner as the service's answer would ({@link JudgePlans#streamResult}); a side the
     *  driver computes (a staged let's rows, an adjudicated verdict, assertError's, a frame's) is written as host
     *  rows ({@link JudgePlans#host}). Nothing materialized is handed back. */
    static void answerValue(PreparedValue pv, SpecCompiler specs, Execution.Answer answer) {
        if (pv.stagedInto() != null || pv.verdict() != null || pv.contextOwner() != null
                || pv.forcedFrame() != null) {
            TypedSide side = runValue(pv, specs, new java.util.ArrayDeque<>());
            if (side != null) {
                JudgePlans.host(side, answer);
            }
            return;
        }
        try (var __o = com.legend.exec.StatementOrigin.enterIfUnmarked(
                com.legend.exec.StatementOrigin.STATEMENT)) {
            executeTyped(pv.body(), pv.env(), false, answer);
        }
    }

    /** Run a prepared value statement: the run phase only. */
    static @com.legend.base.Nullable TypedSide runValue(PreparedValue pv, SpecCompiler specs,
            java.util.Deque<String> frames) {
        if (pv.stagedInto() != null) {
            return stageRows(pv.body(), java.util.Objects.requireNonNull(pv.stagedInto()), pv.env());
        }
        TypedSide verdict = pv.verdict();
        if (verdict != null) {
            return verdict;
        }
        if (pv.contextOwner() != null) {
            return AssertErrorNative.run(pv.contextOwner(), pv.lets(), specs, pv.env(), frames);
        }
        if (pv.forcedFrame() != null) {
            return buildFrame(pv.forcedFrame(), pv.lets(), true, true, specs, pv.env()).result();
        }
        try (var __o = com.legend.exec.StatementOrigin.enterIfUnmarked(
                com.legend.exec.StatementOrigin.STATEMENT)) {
            return executeTyped(pv.body(), pv.env());
        }
    }

    /** The statement's env widened with the tableReplace maps of every
     * exec frame the statement REFERENCES (union; conflicting renames
     * throw — never a silent pick). The re-plan of a spliced chain is
     * the architecture; the renames must ride with it (ledger cluster
     * 59). */
    static ExecEnv frameReplaceEnv(TypedSpec stmt,
            java.util.Map<String, ExecFrame> execFrames, ExecEnv env,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs) {
        java.util.Map<String, String> union = null;
        // execute() calls the statement REACHES inline or through ordinary
        // lets (batch 80: `let result = execute(...).values` is no frame)
        java.util.Map<String, String> reached = com.legend.lowering.SqlPostProcessors
                .reachableRenames(stmt, v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix), v -> v instanceof
                        com.legend.compiler.spec.typed.TypedUserCall
                        ? new com.legend.compiler.spec.UserCallInliner(specs)
                                .inlineBody(java.util.List.of(v)).get(0) : v);
        if (!reached.isEmpty()) {
            union = new java.util.LinkedHashMap<>(env.tableReplace());
            union.putAll(reached);
        }
        for (var e : execFrames.entrySet()) {
            if (e.getValue().tableReplace().isEmpty()
                    || !com.legend.compiler.spec.UserCallInliner
                            .referencesVar(stmt, e.getKey())) {
                continue;
            }
            if (union == null) {
                union = new java.util.LinkedHashMap<>(env.tableReplace());
            }
            for (var r : e.getValue().tableReplace().entrySet()) {
                String prev = union.putIfAbsent(r.getKey(), r.getValue());
                if (prev != null && !prev.equals(r.getValue())) {
                    throw new IllegalStateException("conflicting table"
                            + " renames for '" + r.getKey() + "': '" + prev
                            + "' vs '" + r.getValue() + "'");
                }
            }
        }
        return union == null ? env : env.withTableReplace(union);
    }

    // ===== the plan-handle WALK vocabulary (plan node model) =========

    // =====================================================================
    // The RESULT FRAME (audit 19d B2): let-bound execute() runs EAGERLY and
    // becomes a frame; downstream reads over the frame splice into typed
    // queries — Result is a typing surface plus an orchestration handle,
    // NEVER a host object graph (tenet #1: Java orchestrates, the database
    // executes). The splice rules moved VERBATIM from the harness.
    // =====================================================================

    /** One executed {@code execute()} binding: the from-wrapped typed query
     * chain (unresolved — downstream reads compose over it and resolve as a
     * whole), whether the query ROOT is relation-shaped (the engine's
     * {@code Result.values} for a TDS query holds ONE TDS; for a class or
     * scalar root, values IS the collection), and the eager run's result. */
    record ExecFrame(TypedSpec chain, boolean relationRooted,
            @com.legend.base.Nullable TypedSide result,
            java.util.Map<String, String> tableReplace,
            @com.legend.base.Nullable com.legend.compiler.spec.typed
                    .TypedNativeCall sourceExec,
            com.legend.sql.@com.legend.base.Nullable SqlQuery plan,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType plannedInfo,
            com.legend.sql.@com.legend.base.Nullable SqlSelect extentRows) {
        /** Pre-activity-model constructor (alias frames keep it). */
        ExecFrame(TypedSpec chain, boolean relationRooted,
                @com.legend.base.Nullable TypedSide result,
                java.util.Map<String, String> tableReplace) {
            this(chain, relationRooted, result, tableReplace, null, null, null, null);
        }

        ExecFrame(TypedSpec chain, boolean relationRooted,
                @com.legend.base.Nullable TypedSide result,
                java.util.Map<String, String> tableReplace,
                @com.legend.base.Nullable com.legend.compiler.spec.typed
                        .TypedNativeCall sourceExec) {
            this(chain, relationRooted, result, tableReplace, sourceExec, null, null, null);
        }
    }

    /**
     * Build the frame for one {@code execute(f, mapping, runtime, ext)}
     * call: fold the query lambda's (and the caller's) lets, attach the
     * EXPLICIT mapping argument as the chain's execution context, and — for
     * a let binding — run it eagerly through the pipeline.
     */

    static ExecFrame buildFrame(
            com.legend.compiler.spec.typed.TypedNativeCall ec,
            java.util.List<TypedSpec> letPrefix, boolean eager,
            SpecCompiler specs, ExecEnv env) {
        return buildFrame(ec, letPrefix, eager, false, specs, env);
    }

    /** The activity's execution-trace comment: the run's when the frame ran
     * at its let; under a verdict batch, the comment RESERVED for the body's
     * fused statement — the statement that will run the frame. */
    private static @com.legend.base.Nullable String activityComment(
            @com.legend.base.Nullable TypedSide run, ExecEnv env) {
        if (run != null) {
            return env.trace().lastComment();
        }
        return env.verdictBatch() != null ? env.trace().reserve() : null;
    }

    /** {@code resultNeeded}: the frame's own result is the value asked for
     * (a value-position execute) — it runs even under a verdict batch. */
    private static ExecFrame buildFrame(
            com.legend.compiler.spec.typed.TypedNativeCall ec0,
            java.util.List<TypedSpec> letPrefix, boolean eager, boolean resultNeeded,
            SpecCompiler specs, ExecEnv env0) {
        // the frame's chain, planned (the seam program's S2g, 2026-10-31): the plan-execute normalization, the string
        // entry's envelope and activity SQL, the runtime's table renames and the chain's assembly -- the planner's
        com.legend.planner.FramePlanner.FrameChain fc =
                com.legend.planner.FramePlanner.chain(ec0, letPrefix, specs, env0.plan());
        com.legend.compiler.spec.typed.TypedNativeCall ec = fc.source();
        ExecEnv env = env0.withPlan(fc.env());
        TypedSide run = null;
        // ONE STATEMENT PER BODY (lean ladder, 2026-09-20): under a verdict
        // batch a let's frame is never RUN at the let — its readers derive
        // from it inside the body's one statement (a CTE for a planned
        // relation frame; the chain pasted otherwise) and a broken pipeline
        // surfaces at the flush. A frame whose RESULT is the value asked for
        // (a value-position execute) still runs.
        if (eager && (env.verdictBatch() == null || resultNeeded)) {
            // the inliner consumed the query's lets; graph-tree date args
            // still spell the variables (serialize-key source form) — the
            // resolver's let env resolves them (engine inScopeVars)
            com.legend.resolver.StoreResolver chainResolver =
                    com.legend.planner.BodyPlanner.resolver(specs, env.plan());
            java.util.List<TypedSpec> body = com.legend.planner.BodyPlanner.resolvedToExecute(chainResolver,
                    java.util.List.of(fc.runChain()), env.plan());
            try (var __o = com.legend.exec.StatementOrigin.enter(resultNeeded
                    ? com.legend.exec.StatementOrigin.VALUE : com.legend.exec.StatementOrigin.LET)) {
            run = executeTyped(body, env);
            }
        }
        com.legend.planner.PlanAllocations.registerActivityRows(ec, fc.chain(), letPrefix, specs,
                activityComment(run, env), env.plan());
        if (fc.legendQuery()) {
            // the string entry's value is one string: no planned CTE, no read kind
            return new ExecFrame(fc.chain(), false, run, env.tableReplace(), ec);
        }
        // under the database judge a frame's readers reference its planned CTE where it has one (the planner's)
        com.legend.planner.FramePlanner.Cte cte = eager && env.verdictBatch() != null
                ? com.legend.planner.FramePlanner.cte(fc, letPrefix, specs) : com.legend.planner.FramePlanner.Cte.NONE;
        com.legend.sql.SqlQuery plan = cte.plan();
        com.legend.compiler.element.type.ExprType plannedInfo = cte.plannedInfo();
        com.legend.sql.SqlSelect extentRows = cte.extentRows();
        if (env.verdictBatch() != null) {
            com.legend.judge.VerdictBatch.frame(extentRows != null ? com.legend.judge.VerdictBatch.FrameRead.CLASS_CTE
                    : plan != null ? com.legend.judge.VerdictBatch.FrameRead.CTE
                    : fc.relationRooted() ? com.legend.judge.VerdictBatch.FrameRead.PASTED
                    : com.legend.judge.VerdictBatch.FrameRead.CLASS);
        }
        return new ExecFrame(fc.chain(),
                fc.relationRooted(), run, env.tableReplace(), ec, plan, plannedInfo,
                extentRows);
    }

    /** A callee body with a NON-LET statement before its last (a
     * statement sequence, not one expression). */

    /** pair(a, b).first/.second folds STRUCTURALLY (the datetime
     * helpers thread plan + plan-text through a pair) — pure data
     * selection, no evaluation order. */
    private static TypedSpec foldPairProjection(TypedSpec n) {
        while (n instanceof com.legend.compiler.spec.typed
                        .TypedPropertyAccess pp2
                && ("first".equals(pp2.property())
                        || "second".equals(pp2.property()))
                && pp2.source() instanceof com.legend.compiler.spec
                        .typed.TypedNativeCall pc2
                && "meta::pure::functions::collection::pair"
                        .equals(pc2.callee().qualifiedName())
                && pc2.args().size() == 2) {
            n = pc2.args().get("first".equals(pp2.property()) ? 0 : 1);
        }
        return n;
    }

    /**
     * The TYPED splice — the REWRITE RULES live in the compiler
     * ({@link com.legend.compiler.spec.ResultEnvelopeSplice}, Invariant
     * 7: minting typed nodes is compiler work); this adapter supplies
     * the execution-bound half only — frame lookup, inline-execute
     * frame builds (JDBC), and the aggregationAware rewrittenQuery
     * print.
     */
    static java.util.function.BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> spliceHook(
            java.util.Map<String, ExecFrame> allFrames,
            java.util.List<TypedSpec> letPrefix, SpecCompiler specs, ExecEnv env) {
        return com.legend.compiler.spec.ResultEnvelopeSplice.hook(
                new com.legend.compiler.spec.ResultEnvelopeSplice.Frames() {
            @Override
            public com.legend.compiler.spec.ResultEnvelopeSplice
                    .@com.legend.base.Nullable View frame(String name) {
                ExecFrame f = allFrames.get(name);
                if (f == null) {
                    return null;
                }
                // leg 3.4 step 2: a planned frame is read by REFERENCE
                // (its CTE, named after the let, DEFINED on the body's batch
                // — attached wherever its SQL is made); an unplanned one pastes
                var batch = env.verdictBatch();
                if (f.plan() == null || batch == null
                        || (!f.relationRooted() && f.extentRows() == null)) {
                    return new com.legend.compiler.spec.ResultEnvelopeSplice
                            .View(f.chain(), f.relationRooted(), f.sourceExec());
                }
                // a class frame's CTE is its EXTENT ROWS; a relation frame's, its plan
                batch.defineFrame("frame_" + name,
                        f.extentRows() != null ? f.extentRows() : f.plan());
                return new com.legend.compiler.spec.ResultEnvelopeSplice
                        .View(f.chain(), f.relationRooted(), f.sourceExec(),
                                "frame_" + name, f.plan(), f.plannedInfo());
            }

            @Override
            public com.legend.compiler.spec.ResultEnvelopeSplice.View
                    inlineExecute(com.legend.compiler.spec.typed
                            .TypedNativeCall ec, boolean eager) {
                ExecFrame f = buildFrame(ec, letPrefix, eager, specs, env);
                return new com.legend.compiler.spec.ResultEnvelopeSplice
                        .View(f.chain(), f.relationRooted(), f.sourceExec());
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(
                    String frameName, long activityNumber) {
                ExecFrame f = allFrames.get(frameName);
                if (f == null || f.sourceExec() == null
                        || activityNumber != 0) {
                    // single-statement execution: exactly one
                    // RelationalActivity per execute; other indices (and
                    // alias frames, which lost the source call) fall to
                    // the activity ROWS
                    return null;
                }
                return com.legend.planner.PlanAllocations.activitySql(f.sourceExec(), f.chain(), letPrefix, specs, env.plan());
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(
                    com.legend.compiler.spec.typed.TypedNativeCall ec) {
                ExecFrame f = buildFrame(ec, letPrefix, false, specs, env);
                return com.legend.planner.PlanAllocations.activitySql(ec, f.chain(), letPrefix, specs, env.plan());
            }

        });
    }

    /** The EFFECT rows' registered arms — a REAL registry (LINQ's
     * dictionary, user push 2026-08-31: "or just moving the ifs into a
     * table?"): one map, method references, LOUD on a catalogued row
     * with no arm. {@link #registeredEffectKeys} feeds the governance
     * test pinning registry == catalog. */
    @FunctionalInterface
    private interface EffectRoutine {
        TypedSide run(java.util.List<TypedSpec> body,
                com.legend.compiler.spec.typed.TypedNativeCall nc,
                ExecEnv env);
    }

    private static final java.util.Map<String, EffectRoutine> EFFECT_ARMS = effectArms();

    private static java.util.Map<String, EffectRoutine> effectArms() {
        java.util.Map<String, EffectRoutine> m = new java.util.LinkedHashMap<>();
        m.put(com.legend.builtin.NativeFn.Effect.EXECUTE_IN_DB.fqn(), BodyDriver::executeInDb);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_TABLE_IN_DB.fqn(), BodyDriver::dropAndCreateTableInDb);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_SCHEMA_IN_DB.fqn(), BodyDriver::dropAndCreateSchemaInDb);
        m.put(com.legend.builtin.NativeFn.Effect.CREATE_TEMP_TABLE.fqn(), BodyDriver::createTempTable);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_TEMP_TABLE.fqn(), BodyDriver::dropTempTable);
        m.put(com.legend.builtin.NativeFn.Effect.LOAD_CSV_TO_DB_TABLE.fqn(), CsvLoad::loadCsvToDbTable);
        m.put(com.legend.builtin.NativeFn.Effect.SET_UP_DATA_SQLS.fqn(), SeedSqlForms::assertForm);
        m.put(com.legend.compiler.element.type.PlatformTypes.SET_UP_DATA_SQLS_V2, SeedSqlForms::assertForm);
        m.put(com.legend.builtin.NativeFn.Effect.PRINT.fqn(),
                (body, nc, env) -> new TypedSide.Scalar(null,
                        com.legend.compiler.element.type.Type.Primitive.STRING));
        // debug output: a NO-OP — the argument is NEVER evaluated (it may be an
        // unlowerable diagnostic); engine parity is the statement's inertness
        m.put(com.legend.builtin.NativeFn.Effect.PRINTLN.fqn(),
                (body, nc, env) -> new TypedSide.Scalar(null,
                        com.legend.compiler.element.type.Type.Primitive.STRING));
        m.put(com.legend.builtin.NativeFn.Effect.CONNECTION_BY_ELEMENT.fqn(),
                (body, nc, env) -> new TypedSide.Scalar(null, nc.info().type()));
        return java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(m));
    }

    /** Governance surface: the registry's keys — pinned equal to the
     * catalog's EFFECT rows by NativeDispatchTest. */
    public static java.util.Set<String> registeredEffectKeys() {
        return EFFECT_ARMS.keySet();
    }

    /** Governance surface: the staged-routine keys — pinned equal to
     * the catalog's JAVA_ROUTINE rows by NativeDispatchTest. */
    public static java.util.Set<String> registeredRoutineKeys() {
        return java.util.Set.of(
                com.legend.builtin.NativeFn.JavaRoutine.PLAN_TO_STRING.fqn(),
                com.legend.builtin.NativeFn.JavaRoutine.PLAN_TO_STRING_WITHOUT_FORMATTING.fqn(),
                com.legend.builtin.NativeFn.JavaRoutine.TO_SQL_STRING.fqn(),
                com.legend.builtin.NativeFn.JavaRoutine.TO_SQL_STRING_PRETTY.fqn(),
                com.legend.builtin.NativeFn.JavaRoutine.TO_NON_EXECUTABLE_SQL_STRING.fqn());
    }

    /**
     * The I&rarr;J&rarr;K tail over a resolved TYPED body — shared by
     * the statement loop and the K-native argument evaluation below.
     */
    /** The runtime's LocalH2 {@code testDataSetupSqls}: the engine
     * executes them when it ESTABLISHES the connection — here, on the
     * ambient session before the query (per-statement tolerance, engine
     * harness semantics). {@code fromChain} carries the unwrapped
     * top-level from() setups; nested from() (a graph query whose
     * serialize wraps the from) contribute via the walk. */
    /** A statement's execution contexts are ESTABLISHED before it runs:
     * every from() under it seeds its connection's inline test data (the
     * engine runs a LocalH2 connection's testDataSetupSqls when it opens
     * the connection, before the query) — once, here, for every route the
     * statement then takes (frames, plan text, verdict sides). */
    static void establishContexts(TypedSpec statement, ExecEnv env) {
        // ONCE PER SESSION: the engine runs a LocalH2 connection's
        // testDataSetupSqls / testDataSetupCsv when it ESTABLISHES the
        // connection, not before every query. A session that already
        // established exactly these setups, and has run no WRITING
        // statement since, is already in the established state — running
        // them again would only rebuild the same rows. A write in between
        // (the per-statement tolerance the harnesses rely on) re-seeds.
        //
        // THE KEY IS THE SEED'S SOURCES, never its rendered text (2026-09-16:
        // keying by the text meant regenerating it — CSV parsed, rows typed,
        // DDL and INSERTs rendered — before EVERY statement only to find the
        // session already seeded; the DuckDB shared lane went 20.7 s → 50 s
        // at the DDL leg). The text is a pure function of the from()'s inline
        // setup values and the named runtimes' definitions with their
        // connections' definitions (the CSV / SQL exactly as the author wrote
        // them) for the session's one dialect. Those are RECORDS: the key is
        // a record of them and value equality does the comparing — no
        // rendering, no string assembly, and never the model's identity (the
        // test runner overlays a fresh model per test over the same data).
        java.util.List<com.legend.compiler.spec.typed.TypedFrom> froms =
                com.legend.compiler.spec.typed.ExecutionContext.froms(statement);
        java.util.List<SeedSources.From> fromSources = new java.util.ArrayList<>(froms.size());
        for (com.legend.compiler.spec.typed.TypedFrom fr : froms) {
            String rtFqn = fr.context().runtime().map(r -> r.fullPath()).orElse(null);
            fromSources.add(new SeedSources.From(fr.sqlSetups(), fr.csvSetups(),
                    rtFqn == null ? null : declaredSeed(rtFqn, env.ctx())));
        }
        SeedSources key = new SeedSources(fromSources, env.runtimeFqn(),
                env.runtimeFqn() == null ? null : declaredSeed(env.runtimeFqn(), env.ctx()));
        if (key.namesNothing()) {
            return;   // nothing names test data
        }
        com.legend.exec.SessionState state = env.session().state();
        if (state.established(key)) {
            return;
        }
        java.util.List<com.legend.setup.CsvSeed.Step> setups = new java.util.ArrayList<>();
        for (com.legend.compiler.spec.typed.TypedFrom fr : froms) {
            for (String sql : fr.sqlSetups()) {
                // written in the database the from()'s connection declares (decision D2)
                setups.add(new com.legend.setup.CsvSeed.Step.Sql(sql, fr.context().rawLanguage()));
            }
            setups.addAll(com.legend.setup.CsvSeed.setupSteps(fr, env.ctx(), env.dialect()));
            // an ELEMENT runtime named by the from(): its declared
            // connections' test data (the instance form above carries the
            // same facts on the instance)
            fr.context().runtime().ifPresent(ref ->
                    setups.addAll(com.legend.setup.CsvSeed.declaredSteps(ref.fullPath(), env.ctx(), env.dialect())));
        }
        // the AMBIENT runtime (the driver's execution context): its
        // declared connections' test data seeds the session the same way
        if (env.runtimeFqn() != null) {
            setups.addAll(com.legend.setup.CsvSeed.declaredSteps(env.runtimeFqn(), env.ctx(), env.dialect()));
        }
        // the seeds as the plan side's setup steps (CsvSeed.planned: the one owner of their form, the plan's target
        // takes the same), run by the plan runner on the body's session under the SEED origin, each recorded for the
        // referee's ledger as it was asked (the doors, 2026-10-29; until then SetupRunner sent them on the session's
        // statements)
        java.util.List<com.legend.setup.CsvSeed.Planned> planned = com.legend.setup.CsvSeed.planned(setups,
                env.dialect());
        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.SEED)) {
            com.legend.exec.PlanRunner.setUp(planned.stream().map(com.legend.setup.CsvSeed.Planned::step).toList(),
                    env.target(), env.planSessions(), (i, rows) -> record(env, planned.get(i).text(), rows));
        }
        state.establish(key);
    }

    /** WHAT A SEED IS MADE OF — the values the seed text is a pure function
     *  of, compared by value (the key of the session's established state,
     *  {@link com.legend.exec.SessionState}): each from()'s inline setups and the runtime it
     *  names; the ambient runtime. A runtime contributes its definition and
     *  its bound connections' definitions (a LocalH2 specification carries
     *  the CSV / SQL strings as written). */
    record SeedSources(java.util.List<From> froms, @com.legend.base.Nullable String runtimeFqn,
            @com.legend.base.Nullable DeclaredSeed runtime) {
        record From(java.util.List<String> sqlSetups,
                java.util.List<com.legend.compiler.spec.typed.ExecutionContext.CsvSetup> csvSetups,
                @com.legend.base.Nullable DeclaredSeed runtime) {
        }

        record DeclaredSeed(String runtimeFqn,
                com.legend.model.@com.legend.base.Nullable RuntimeDefinition definition,
                java.util.List<com.legend.model.ConnectionDefinition> connections) {
        }

        boolean namesNothing() {
            return froms.isEmpty() && runtimeFqn == null;
        }
    }

    /** A named runtime's contribution to the seed sources. */
    private static SeedSources.DeclaredSeed declaredSeed(String runtimeFqn, ModelContext ctx) {
        java.util.Optional<com.legend.model.RuntimeDefinition> rt = ctx.findRuntime(runtimeFqn);
        java.util.List<com.legend.model.ConnectionDefinition> conns = new java.util.ArrayList<>();
        rt.ifPresent(r -> {
            for (var binding : r.connectionBindings().entrySet()) {
                for (String connFqn : binding.getValue()) {
                    ctx.findConnection(connFqn).ifPresent(conns::add);
                }
            }
        });
        return new SeedSources.DeclaredSeed(runtimeFqn, rt.orElse(null), conns);
    }

    /** THE SYSTEM DATABASE ROUTE (user ruling 2026-09-02): a body reading
     * the metamodel store executes on the GRAPH's own system database —
     * separate from every user connection, written once per graph, never
     * per execution ({@link SystemDatabase}); on a server
     * engine, the host's system connection, its tables read through the
     * route's renames (2026-10-11). The route is the planner's, a fact of the body's plan (the seam program's S2h and
     * S4a, 2026-10-31: the database and store it reads, the tables' names its plan is written with, a body mixing
     * the store with a user store refused by name); the session -- opened and seeded once per graph -- is the
     * driver's, opened here once the body is planned, and names the tables as planned. {@code env} as it is where
     * the body reads no system store. */
    private static ExecEnv openRoute(com.legend.planner.SystemRoutes.@com.legend.base.Nullable Route route,
            ExecEnv env) {
        if (route == null) {
            return env;
        }
        ModelContext ctx = env.ctx();
        SystemDatabase.Route opened = SystemDatabase.of(ctx).routeFor(route.engine(), env.dialect(), route.store(),
                table -> MetamodelSeeds.rows(table, ctx), env.options().systemConnections());
        java.util.Map<String, String> planned = com.legend.planner.SystemRoutes.renames(route);
        if (!opened.renames().equals(planned)) {
            throw new IllegalStateException("the system database on " + route.engine() + " names the store's tables "
                    + opened.renames() + ", its plan " + planned);
        }
        return env.withSession(opened.session());
    }

    /** A write into a store table: its body lowered to the DML statement (Lowerer.lowerStoreWrite), rendered by the
     *  dialect and run by the executor; its value the database's update count. A frame's post-processors rewrite a
     *  query's text, and no write is designed under them: refused by name. */
    private static TypedSide storeWrite(java.util.List<TypedSpec> body, TypedSpec write, ExecEnv env,
            boolean identity) {
        if (!env.tableReplace().isEmpty() || env.postProcessors().extractCtes()
                || env.postProcessors().nonExecutable()) {
            throw new com.legend.error.NotImplementedException("a write into a store table under the execution's"
                    + " post-processors (tableReplace, extractCtes, nonExecutable) is not designed (the writes design's"
                    + " leg 2 writes the table it names)");
        }
        java.util.List<TypedSpec> statements = new java.util.ArrayList<>(body.subList(0, body.size() - 1));
        statements.add(write);
        com.legend.sql.SqlDml dml = com.legend.planner.BodyPlanner.lowererFor(statements, env.plan(), env.ctx(), identity).lowerStoreWrite(
                com.legend.lowering.SeedableLets.withSeedableLetPrefix(statements, env.queryLets(), env.ctx()));
        String sql = env.dialect().render(dml);
        record(env, sql, false);
        long count = sendUpdate(env, sql);
        return new TypedSide.Scalar(count, write.info().type());
    }

    /** The temporary table of the session a staged let's rows are held in (Pure.Lite.STAGE; StagedRows.table, which
     *  a write plan's staging names too). */
    static String stagedTable(String let) {
        return com.legend.resolver.StagedRows.table(let);
    }

    /** A staged let's rows (Pure.Lite.STAGE, docs/WRITES_DESIGN_2026_10_18.md §8, leg 6): computed now, at the let's
     *  turn among the program's statements, into the session's temporary table {@code table}, which every statement
     *  after the let reads ({@link com.legend.resolver.StagedRows}). */
    private static TypedSide stageRows(java.util.List<TypedSpec> body, String table, ExecEnv env) {
        Prelude p = prelude(body, env, null, false);
        if (p.answered() != null) {
            throw new com.legend.error.NotImplementedException("rows staged (stage) from a statement the database does"
                    + " not query");
        }
        ExecEnv planned = p.plannedEnv();
        com.legend.compiler.element.type.Type.RelationType late = com.legend.compiler.element.type.Type.schemaView(
                p.plannedRoot().info().type());
        boolean grid = late != null && late.isLateBound();
        // a raw grid's rows (the seam program's S3c, 2026-10-30): no column is named, the table takes the ones its
        // query answers; any other relation's columns named as the staged relation names them, which the
        // statements after read by name
        com.legend.sql.SqlQuery plan = com.legend.planner.BodyPlanner.lowerAndPrepare(p.body(), planned.plan(), planned.ctx(), planned.dialect(),
                false, !grid);
        // (a grid's table a plain temporary one: it outlives the commits of the statements after it, which a
        // body's effects and verdicts make one by one, and is dropped when the body ends, dropTables)
        String sql = planned.dialect().render(grid ? new com.legend.sql.SqlDml.CreateTempTableAs(table, plan)
                : com.legend.sql.SqlDml.CreateTempTableAs.staged(table, plan,
                com.legend.resolver.StagedRows.columns(p.plannedRoot().info())));
        record(planned, sql, false);
        sendUpdate(planned, sql);
        return new TypedSide.Scalar(null, p.plannedRoot().info().type());
    }

    private static final java.util.concurrent.atomic.AtomicLong STAGED_GRIDS = new java.util.concurrent.atomic.AtomicLong();

    /** The temporary table a staged raw grid's rows are held in: the let's name under the platform's prefix and a
     *  number of its own, so a table whose drop failed (BodyCompiler drops it however the body ends; a drop can fail
     *  on a session the failure left unusable) is never the next body's. */
    static String stagedGridTable(String let) {
        return stagedTable(let) + "_" + STAGED_GRIDS.incrementAndGet();
    }

    /** {@code tables} dropped on the body's session, whatever the database does with staged tables as a transaction
     *  ends (a staged raw grid's table is a plain temporary one). */
    static void dropTables(ExecEnv env, java.util.List<String> tables) {
        for (String t : tables) {
            String sql = env.dialect().render(new com.legend.sql.SqlDdl.DropTable(null, t,
                    com.legend.sql.NameQuoting.BARE));
            record(env, sql, false);
            sendUpdate(env, sql);
        }
    }

    /** The raw read of a staged raw grid's temporary table, in the session's dialect (a plain {@code SELECT *} of
     *  it; StagedRows.gridBoundTo binds the let to it). */
    static String stagedGridRead(String table, ExecEnv env) {
        return env.dialect().render(com.legend.sql.SqlSelect.starOf(new com.legend.sql.SqlSource.Table(table, table,
                java.util.List.of(), false, java.util.Map.of(), com.legend.sql.NameQuoting.BARE)));
    }

    /** The store a staged let's table is read under (StagedRows.storeOf, which a write plan's staging reads too). */
    static String storeOf(java.util.List<TypedSpec> body, ExecEnv env) {
        return com.legend.resolver.StagedRows.storeOf(body, env.ctx(), env.runtimeFqn());
    }

    /** The staged lets' temporary tables dropped once the program has run (a failed program's transaction takes
     *  them back with its writes), unless the database drops them as the transaction ends
     *  (SqlDialect.dropsStagedTablesWithTheirTransaction). */
    static void dropStaged(ExecEnv env, java.util.List<String> tables) {
        if (env.dialect().dropsStagedTablesWithTheirTransaction()) {
            return;
        }
        for (String t : tables) {
            String sql = env.dialect().render(new com.legend.sql.SqlDdl.DropTable(null, t,
                    com.legend.sql.NameQuoting.BARE));
            record(env, sql, false);
            sendUpdate(env, sql);
        }
    }

    /** A change the body sends -- a store write, a staged table made or dropped -- as a plan's update on the body's
     *  session (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, phase 5: every statement a body sends is a plan's): the
     *  count of rows the database reports. */
    private static long sendUpdate(ExecEnv env, String sql) {
        try {
            return com.legend.exec.PlanRunner.update(new com.legend.executionplan.ExecutionPlan.Update(
                    statementOf(env, sql), null), com.legend.exec.PlanSessions.of(env.session(), env.trace()));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** A statement the body sends for what it does -- a raw statement a program wrote, a DDL, a script -- as a plan's
     *  effect on the body's session: whether it answered rows (not read). */
    private static boolean sendRaw(ExecEnv env, String sql) {
        try {
            return com.legend.exec.PlanRunner.effect(new com.legend.executionplan.ExecutionPlan.Effect(
                    statementOf(env, sql)), com.legend.exec.PlanSessions.of(env.session(), env.trace()));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** {@code sql} as a plan's statement for the body's session: no slot, its target the body's
     *  ({@link JudgePlans#target}). */
    private static com.legend.executionplan.ExecutionPlan.Sql statementOf(ExecEnv env, String sql) {
        return com.legend.executionplan.ExecutionPlan.Sql.of(sql, JudgePlans.target(env));
    }

    /** Leg 3.4 step 2: the SQL text of a plan for execution or prepare —
     * with the frame definitions it references at its head (the batch's,
     * database mode); the plan itself stays a bare select. */
    static String renderFor(ExecEnv env, com.legend.sql.SqlQuery plan) {
        var b = env.verdictBatch();
        return env.dialect().render(b == null ? plan : com.legend.sql.FrameCtes.attach(plan, b.frames()));
    }

    static @com.legend.base.Nullable TypedSide evalValue(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        return evalValue(value, letPrefix, specs, env, false, null);
    }

    /** F13c: {@code identity} asks for the identity lane -- assert-CONDITION sides (the value is a boolean; the
     *  in-SQL eq/equal arm needs instance identity to compile the engine's equality relation). V7 batch 2:
     *  {@code hook} non-null threads the statement loop's
     * result-envelope splice into the verdict side evaluation — the
     * SAME {@code UserCallInliner} hook ordinary statements get, so an
     * assert side reading an execute() frame compiles the spliced
     * chain (never a raw variable read). */
    static @com.legend.base.Nullable TypedSide evalValue(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            boolean identity,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        // the addDriverTablePkForProject option is part of the EXECUTION
        // a LITERAL-ONLY side (a golden text, a seed CSV, a bare number — measured 167 of
        // 168 sides, 2026-09-22) is a compile-time constant: the compiler folds it, nothing
        // is sent; everything else executes as before (Literals never guesses)
        java.util.List<TypedSpec> inlined = com.legend.planner.BodyPlanner.inlinedSide(value, letPrefix, specs, env.plan(), hook);
        TypedSide constant = foldedSide(inlined.get(inlined.size() - 1), inlined.subList(0, inlined.size() - 1));
        if (constant != null) {
            return constant;
        }
        try (var __o = com.legend.exec.StatementOrigin.enterIfUnmarked(com.legend.exec.StatementOrigin.SIDE)) {
            return executeTyped(com.legend.planner.BodyPlanner.stagedSide(inlined, specs, env.plan()), env, identity);
        }
    }

    /** The folded constant as the result the wire would have produced, or null. */
    private static @com.legend.base.Nullable TypedSide foldedSide(TypedSpec value,
            java.util.List<TypedSpec> lets) {
        Object v = com.legend.compiler.spec.typed.Literals.fold(value, lets);
        return switch (v) {
            case null -> null;
            case java.util.List<?> l -> new TypedSide.Collection(new java.util.ArrayList<>(l), value.info().type());
            default -> new TypedSide.Scalar(v, value.info().type());
        };
    }

    static TypedSide executeTyped(
            java.util.List<TypedSpec> body, ExecEnv env) {
        return executeTyped(body, env, false);
    }

    /** The typed-execution PRELUDE (leg 3.1 split): every arm that answers
     * WITHOUT a plan (effects, store navigation, DDL strings, handles, the
     * effectful map) returns {@link Prelude#answered}; otherwise the body,
     * root and environment the plan is lowered from. {@link #executeTyped}
     * and {@link #planValue} share it, so a side is planned exactly as it
     * would execute. */
    record Prelude(@com.legend.base.Nullable TypedSide answered,
            java.util.List<TypedSpec> body, @com.legend.base.Nullable TypedSpec root,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType declaredInfo,
            @com.legend.base.Nullable ExecEnv env) {
        static Prelude answered(TypedSide r) {
            return new Prelude(r, java.util.List.of(), null, null, null);
        }
        TypedSpec plannedRoot() {
            return java.util.Objects.requireNonNull(root, "an answered prelude has no root");
        }
        ExecEnv plannedEnv() {
            return java.util.Objects.requireNonNull(env, "an answered prelude has no env");
        }
    }

    private static Prelude prelude(
            java.util.List<TypedSpec> body, ExecEnv env,
            com.legend.lowering.@com.legend.base.Nullable CanonRider rider,
            boolean identityLane) {
        // the body's preparation -- its system route, its framing, and the kind of answer it is given with no plan --
        // the planner's (the shared preparation and S4a, 2026-10-31); the route's session opened, and the answers that
        // run, run here
        com.legend.planner.SidePlanner.Routed r = com.legend.planner.SidePlanner.prepare(body, env.plan());
        env = openRoute(r.route(), env);
        if (r.prepared() instanceof com.legend.planner.Prepared.Planned p) {
            return new Prelude(null, p.body(), p.root(), p.declaredInfo(), env.withPlan(p.env()));
        }
        return Prelude.answered(answer(r.prepared(), env, rider, identityLane));
    }

    /** A prepared kind given its answer with no plan, answered on {@code env} (its route's session): an effect's
     *  registered arm run, a store write sent, the plan-time texts written, a handle, an effectful map's elements
     *  run. */
    private static TypedSide answer(com.legend.planner.Prepared prepared, ExecEnv env,
            com.legend.lowering.@com.legend.base.Nullable CanonRider rider, boolean identityLane) {
        return switch (prepared) {
            case com.legend.planner.Prepared.Planned p ->
                    throw new IllegalStateException("a planned body is run through its plan, never answered");
            case com.legend.planner.Prepared.Effect e -> {
                EffectRoutine arm = EFFECT_ARMS.get(e.call().callee().qualifiedName());
                if (arm == null) {
                    throw new com.legend.error.NotImplementedException(
                            "catalog says '" + e.call().callee().qualifiedName()
                            + "' is EFFECT but no arm is registered");
                }
                yield arm.run(e.body(), e.call(), env.withPlan(e.env()));
            }
            case com.legend.planner.Prepared.StoreWrite w ->
                    storeWrite(w.body(), w.write(), env.withPlan(w.env()), rider != null || identityLane);
            case com.legend.planner.Prepared.DdlText d -> new TypedSide.Scalar(d.text(), d.type());
            case com.legend.planner.Prepared.Handle h -> new TypedSide.Scalar(null, h.type());
            case com.legend.planner.Prepared.DdlTexts c -> new TypedSide.Collection(c.texts(),
                    com.legend.compiler.element.type.Type.Primitive.STRING);
            case com.legend.planner.Prepared.EffectfulMap m -> effectfulMap(m.body(), m.map(), env.withPlan(m.env()));
        };
    }

    /** A map over an EFFECTFUL lambda ({@code $sqls->map(sql|executeInDb(...))}): the source collection evaluates
     *  through the pipeline; each element executes the lambda body with the parameter bound (the one
     *  statement-orchestration shape the corpus's setup bodies use). */
    private static TypedSide effectfulMap(java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedMap tm, ExecEnv env) {
        java.util.List<TypedSpec> src = new java.util.ArrayList<>(
                body.subList(0, body.size() - 1));
        src.add(tm.source());
        TypedSide seedForm = SeedSqlForms.mappedExecutionForm(body, tm, env);
        TypedSide values = seedForm != null ? seedForm
                : executeTyped(src, env);
        java.util.List<Object> vals = switch (values) {
            case TypedSide.Collection c -> c.values();
            case TypedSide.Scalar sc2 -> sc2.value() == null
                    ? java.util.List.of() : java.util.List.of(sc2.value());
            default -> throw new IllegalStateException(
                    "effectful map over a non-collection source");
        };
        String param = tm.mapper().parameters().get(0);
        TypedSide last = new TypedSide.Scalar(null,
                tm.info().type());
        for (Object v : vals) {
            if (!(v instanceof String sv)) {
                throw new IllegalStateException("effectful map element is"
                        + " not a string: " + v);
            }
            java.util.List<TypedSpec> one = new java.util.ArrayList<>(
                    body.subList(0, body.size() - 1));
            for (TypedSpec stmt2 : tm.mapper().body()) {
                one.add(com.legend.compiler.spec.UserCallInliner
                        .bindStringParam(stmt2, param, sv));
            }
            last = executeTyped(one, env);
        }
        return last;
    }

    /** A value statement's result: answered by the prelude where it needs no plan, a literal folded, else its plan
     *  run by the plan runner on the env's session ({@link JudgePlans#statementResult}: the typed rows shaped as the
     *  statement's shape; docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, phase 5 -- the body driver sends nothing
     *  itself). F13c: {@code identityLane} joins the identity lane (assert-condition sides). */
    static TypedSide executeTyped(
            java.util.List<TypedSpec> body, ExecEnv env, boolean identityLane) {
        return java.util.Objects.requireNonNull(executeTyped(body, env, identityLane, null),
                "a statement with no answer wanted collects its side");
    }

    /** {@link #executeTyped(java.util.List, ExecEnv, boolean)} with the statement's answer, where {@code answer} is
     *  given, STREAMED to it instead of collected (the body's last statement, the doors, 2026-10-29): a side the
     *  prelude or a fold gave is written as host rows; a planned statement's rows go through the plan runner to the
     *  sink ({@link JudgePlans#streamResult}), and null comes back. The answer is this statement's alone: nested
     *  runs collect, as before. */
    static @com.legend.base.Nullable TypedSide executeTyped(
            java.util.List<TypedSpec> body, ExecEnv env, boolean identityLane,
            Execution.@com.legend.base.Nullable Answer answer) {
        ModelContext ctx = env.ctx();
        Prelude p = prelude(body, env, null, identityLane);
        if (p.answered() != null) {
            return answered(p.answered(), answer);
        }
        body = p.body();
        env = p.plannedEnv();
        TypedSpec root = p.plannedRoot();
        com.legend.compiler.element.type.ExprType declaredInfo = p.declaredInfo();
        com.legend.judge.TypedSide folded = LiteralFold.fold(root);
        if (folded != null) {
            return answered(folded, answer);
        }
        if (System.getenv("LL_DUMP_RESOLVED") != null) {
            System.err.println("[resolved] " + body);
        }
        com.legend.sql.dialect.SqlDialect dialect = env.dialect();
        com.legend.sql.SqlQuery plan = com.legend.planner.BodyPlanner.lowerAndPrepare(body, env.plan(), ctx,
                dialect, identityLane);
        boolean collectionDeclared = com.legend.planner.SidePlanner.collectionDeclared(declaredInfo, root);
        if (System.getenv("LL_TMP_SQL") != null) {
            System.err.println("[exec-sql] " + renderFor(env, plan));
        }
        var sp = com.legend.planner.SidePlanner.wrap(plan, root, collectionDeclared ? declaredInfo : null, null,
                env.plan());
        WrappedSide side = new WrappedSide(sp.plan(), sp.shapeInfo(), sp.shape(), env.session(), false);
        if (answer != null) {
            JudgePlans.streamResult(side, toOneReader(root), env, answer);
            return null;
        }
        TypedSide res = JudgePlans.statementResult(side, env);
        enforceToOneReader(root, res);
        return res;
    }

    /** {@code side} written to {@code answer} as host rows where an answer is wanted; the side either way. */
    private static TypedSide answered(TypedSide side, Execution.@com.legend.base.Nullable Answer answer) {
        if (answer != null) {
            JudgePlans.host(side, answer);
        }
        return side;
    }


    /** The plan-execution tail of {@link #executeTyped}: V11 canon
     * wrap (when a rider asks) + the one Executor call. */
    /** A side's plan after the canon wrap (leg 3.1): what database mode
     * composes into its verdict statement without executing it. */
    record WrappedSide(com.legend.sql.SqlQuery plan,
            com.legend.compiler.element.type.ExprType shapeInfo,
            com.legend.plan.ResultShape shape,
            com.legend.exec.Sessions.Session session,
            boolean storeFree) {
        /** A side reading no store (a literal, a pure expression) runs on
         * ANY session; a side reading a store runs where it was routed. */
    }

    /** {@link #planValue}'s outcome: a planned side, or the value the
     * pipeline ANSWERED without a plan (a host constant — database mode
     * binds a primitive one as a literal side). */
    record PlannedValue(@com.legend.base.Nullable WrappedSide side,
            @com.legend.base.Nullable TypedSide answered,
            @com.legend.base.Nullable TypedSpec root, boolean joined) {
        /** The planned root, for a planned side (the to-one reader's check reads it). {@code joined}: an answered
         *  side whose constant the compiler JOINED from the program's constant tokens ({@code Literals.bare} false:
         *  {@code +}, {@code joinStrings}, {@code replace} over literals), not the tokens verbatim (S2f, 2026-10-30:
         *  the plan's constant side marks it). */
    }

    /** Plan a VALUE the way {@link #evalValue} would execute it, stopping
     * after the canon wrap: null when the value answers without a plan
     * (an orchestration handle, an effect, a host-evaluated navigation —
     * database mode reports those as UNJUDGED by name). */
    static PlannedValue planValue(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            com.legend.lowering.@com.legend.base.Nullable CanonRider rider,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        return planned(planBare(value, letPrefix, specs, env, rider, hook), rider);
    }

    /** The host judge's side on the plan path (the plan path program's step 6, 2026-10-18): planned exactly as
     *  {@link #executeTyped} prepared it for the legacy host route -- no canon rider, the identity lane when
     *  {@code identity} asks, the plan's slots the typer's own (no wire reconciliation: the literal channel's label
     *  survives to the decode) -- and, when {@code fold} (the sites the legacy folded: a rider-less evaluation), a
     *  literal-only side folded to its constant with nothing sent. The sites that rode a canon rider never folded: a
     *  literal there evaluates in the database, exactly as before (a DateTime golden's written precision meets the
     *  read value's as the database holds both). The plan JudgePlans runs. */
    static PlannedValue planHostSide(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env, boolean identity, boolean fold,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        java.util.List<TypedSpec> inlined = com.legend.planner.BodyPlanner.inlinedSide(value, letPrefix, specs, env.plan(), hook);
        TypedSide constant = fold ? foldedSide(inlined.get(inlined.size() - 1),
                inlined.subList(0, inlined.size() - 1)) : null;
        if (constant != null) {
            return new PlannedValue(null, constant, null, !com.legend.compiler.spec.typed.Literals.bare(
                    inlined.get(inlined.size() - 1), inlined.subList(0, inlined.size() - 1)));
        }
        BarePlan b;
        try (var __o = com.legend.exec.StatementOrigin.enterIfUnmarked(com.legend.exec.StatementOrigin.SIDE)) {
            b = planStaged(com.legend.planner.BodyPlanner.stagedSide(inlined, specs, env.plan()), value, env, null, identity, true);
        }
        if (fold && b.answered() == null) {
            // the planned root's own fold (executeTyped's second fold: a literal the staging exposed)
            TypedSide folded = LiteralFold.fold(b.root());
            if (folded != null) {
                return new PlannedValue(null, folded, null, false);
            }
        }
        return planned(b, null);
    }

    private static PlannedValue planned(BarePlan b, com.legend.lowering.@com.legend.base.Nullable CanonRider rider) {
        if (b.answered() != null) {
            return new PlannedValue(null, b.answered(), null, false);
        }
        var sp = com.legend.planner.SidePlanner.wrap(b.plan(), b.root(), b.declaredInfo(), rider, b.env().plan());
        return new PlannedValue(new WrappedSide(sp.plan(), sp.shapeInfo(), sp.shape(), b.env().session(),
                b.storeFree()), null, b.root(), false);
    }

    /** The test-data generator's VIEW fetch renderer (TestDataGenerator.ViewSql):
     *  the view's lifted function (E.5, reached through the include closure
     *  like a table) as the compiler's relation — the same node a user's
     *  accessor inlines to — planned like any query (the resolver
     *  materializes its joins), the fetched base tables renamed to their
     *  temps (the replaceTables pass — the engine's {@code fixTables}),
     *  rendered in the lane's own dialect (the text is executed on this
     *  connection). The generator's package never reaches the lowering: the
     *  driver hands it this. */
    static com.legend.testdatagen.TestDataGenerator.ViewSql viewSqlRenderer(
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        return (db, view, tableToTemp) -> com.legend.planner.BodyPlanner.viewSql(db, view, tableToTemp, specs,
                env.plan());
    }

    /** A value's plan before the canon wrap: the plan, its root, the
     * collection declaration (when a primitive collection is declared over a
     * relation root), the planned env (on its route's session), and whether it reads no store; or the side
     * answered with no plan. */
    record BarePlan(@com.legend.base.Nullable TypedSide answered,
            com.legend.sql.SqlQuery plan, TypedSpec root,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType declaredInfo,
            ExecEnv env, boolean storeFree) {
    }

    private static BarePlan planBare(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            com.legend.lowering.@com.legend.base.Nullable CanonRider rider,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        return planStaged(com.legend.planner.BodyPlanner.sideBody(value, letPrefix, specs, env.plan(), hook), value, env, rider, false, false);
    }

    /** {@link #planBare} over a side already inlined, staged and resolved ({@code body}); {@code identityLane} the
     *  identity lane without a rider (a rider implies it); {@code hostSide} the legacy host route's preparation
     *  ({@link #executeTyped}'s: the identity lane's flag alone, no wire reconciliation) where the database judge's
     *  side takes the verdict lane's. */
    private static BarePlan planStaged(java.util.List<TypedSpec> body, TypedSpec value, ExecEnv env,
            com.legend.lowering.@com.legend.base.Nullable CanonRider rider, boolean identityLane, boolean hostSide) {
        // planned with no session (S4a, 2026-10-31): the route's session opened once planned, a kind with no plan
        // answered on it
        com.legend.planner.SidePlanner.Staged s = com.legend.planner.SidePlanner.staged(body, env.plan(), rider,
                identityLane, hostSide);
        ExecEnv routed = openRoute(s.route(), env);
        return switch (s) {
            case com.legend.planner.SidePlanner.Unplanned u -> new BarePlan(answer(u.prepared(), routed, rider,
                    identityLane), com.legend.sql.SqlSelect.starOf(new com.legend.sql.SqlSource.Dual()), value, null,
                    env, true);
            case com.legend.planner.SidePlanner.Bare b -> new BarePlan(null, b.plan(), b.root(), b.declaredInfo(),
                    routed.withPlan(b.env()), b.storeFree());
        };
    }

    /** rows->toOne() READER enforcement (audit 22b F1): the lowering is
     * row-identical to the relation (engine toOne throws at the READER,
     * never in SQL) — the reader enforces exactly-one for a
     * TABULAR-consumed toOne root; the scalar arm's second-row guard
     * covers scalar reads, this covers whole-TDS consumption and the
     * ZERO-row lower bound. */
    private static void enforceToOneReader(TypedSpec root, TypedSide res) {
        if (toOneReader(root) && res instanceof TypedSide.Tabular tab && tab.rows().size() != 1) {
            throw new IllegalStateException("toOne() over a relation returned "
                    + tab.rows().size() + " row(s) — the exactly-one contract"
                    + " (engine reader semantics)");
        }
    }

    /** Whether {@code root} is {@code toOne()} over a relation: its answer is exactly one row (engine reader
     *  semantics), held on a collected side ({@link #enforceToOneReader}) and on a streamed one alike. */
    static boolean toOneReader(TypedSpec root) {
        return root instanceof com.legend.compiler.spec.typed.TypedNativeCall tw
                && com.legend.builtin.Pure.isToOneCall(tw.callee().qualifiedName())
                && !tw.args().isEmpty()
                && com.legend.compiler.element.type.Type.isRelation(tw.args().get(0).info().type());
    }

    /**
     * The K-native {@code executeInDb} (PlatformTypes.EXECUTE_IN_DB): the
     * engine's JDBC boundary. The SQL argument is an ordinary Pure
     * expression and is evaluated THROUGH the pipeline (Java orchestrates,
     * the database evaluates); the connection argument is NEVER evaluated —
     * there is exactly one ambient connection per execution context, and
     * the corpus's connection-resolution chains
     * ({@code testRuntime()->connectionByElement(...)}) exist only to
     * type-check. Let statements the SQL argument does not (transitively)
     * reference — crucially those connection chains — are dropped before
     * evaluation. The blob is dialect-adapted, split on top-level
     * {@code ;}, and executed statement by statement.
     */

    /** THE ONE SEND of an effect native (block-compiler stage 3, 2026-09-21): with a
     * sink on the environment the statement is COLLECTED into the effect segment
     * under construction (recorded for the referee's ledger when the script has run);
     * without one it executes now under its origin and is recorded at once — the
     * loop's own behavior. {@code recorded}: the ledger's text (the H2 spelling of a
     * DDL; the corpus-authored text of a raw statement), null when the statement was
     * never recorded. Returns whether the statement produced rows (known only when
     * executed now; the sink decides it statically). */
    static boolean sendEffect(ExecEnv env, String executed, @com.legend.base.Nullable String recorded,
            com.legend.exec.StatementOrigin origin, boolean generated) {
        com.legend.exec.EffectSink sink = env.effectSink();
        if (sink != null) {
            sink.add(executed, recorded);
            return false;
        }
        boolean query;
        try (var __o = generated ? com.legend.exec.StatementOrigin.enterGenerated(origin)
                : com.legend.exec.StatementOrigin.enterIfUnmarked(origin)) {
            query = sendRaw(env, executed);
        }
        if (recorded != null) {
            record(env, recorded, query);
        }
        return query;
    }

    /** Send an effect segment as ONE script (the dialect brackets it), then record its
     * statements for the referee's ledger: all of them on success; on a failure the
     * ones before the statement the engine names (H2), or none when the segment rolled
     * back as a unit (DuckDB). The failure is re-raised naming the segment. */
    static void sendScript(ExecEnv env, com.legend.exec.EffectSink sink, String where) {
        java.util.List<com.legend.exec.EffectSink.Entry> entries = sink.entries();
        java.util.List<String> texts = entries.stream().map(com.legend.exec.EffectSink.Entry::executed).toList();
        // the dialect's bracket only when this send OWNS the transaction; inside a
        // caller's (the harness's attempt) the statements run bare and the caller unwinds
        boolean bracketed = env.session().ownsTransaction();
        try {
            try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.BODY)) {
                sendRaw(env, bracketed ? env.dialect().script(texts) : String.join(";\n", texts) + ";");
            }
        } catch (com.legend.error.DataError e) {
            String abort = env.dialect().scriptAbort();
            if (bracketed && abort != null) {
                // the bracket the failure left open: closed before anything else runs
                sendRaw(env, abort);
            }
            java.util.OptionalInt failing = env.dialect().failingStatement(String.valueOf(e.getMessage()), texts);
            int applied = failing.isPresent() ? failing.getAsInt() : 0;
            for (int i = 0; i < applied; i++) {
                String rec = entries.get(i).recorded();
                if (rec != null) {
                    record(env, rec, entries.get(i).query());
                }
            }
            throw new com.legend.error.DataError(e.getMessage() + " — in the effect segment " + where
                    + (failing.isPresent() ? " (statement " + (failing.getAsInt() + 1) + " of " + texts.size() + ")"
                            : " (" + texts.size() + " statement(s), applied as a unit)"), e);
        }
        for (com.legend.exec.EffectSink.Entry en : entries) {
            String rec = en.recorded();
            if (rec != null) {
                record(env, rec, en.query());
            }
        }
    }

    static TypedSide executeInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        // its statements the planner's (split, translated to the session's dialect from the language its connection
        // declares), sent in order, each recorded as authored
        sendPlanned(body, call, env);
        // an opaque ResultSet handle: setup statements ignore it; a test
        // that READS it will surface loudly here when that day comes
        return new TypedSide.Scalar(null, call.info().type());
    }

    /** The K-native {@code dropAndCreateSchemaInDb}: the engine DROPS +
     * creates; here create-if-missing — the DDL seeds already own tables
     * in the schema, and the setup's own dropAndCreateTableInDb calls
     * recreate what it manages. Recorded on BOTH channels
     * (FULL_RESIDUE_CENSUS §9a root cause, 2026-08-30): the old
     * metadata-only recording WITHHELD a corpus-authored, executed
     * statement from the H2 row-replay ledger — violating the
     * recording's own mirror-executed-reality invariant. The mirror
     * only stayed alive because the module-DDL layer's recorded creates
     * happened to provide the schemas; without them one suppressed
     * create per family POISONED the family mirror and failed every
     * later advisory-dependent test (9 across 2 families, measured).
     * `Create schema if not exists` is valid H2 and idempotent — the
     * replay is safe on both fresh and live mirrors. The statement is the
     * IR's (SqlDdl.CreateSchema), rendered as dropAndCreateTableInDb's:
     * the session's dialect spells the executed copy, H2 the recorded one
     * (the dialect program's Phase 3, 2026-10-16: until then the text was
     * written here, so a database with no schemas, SQLite, refused it). */
    static TypedSide dropAndCreateSchemaInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        sendPlanned(body, call, env);
        return new TypedSide.Scalar(true, call.info().type());
    }

    /**
     * The K-native {@code dropAndCreateTableInDb}
     * (PlatformTypes.DROP_AND_CREATE_TABLE_IN_DB): the real engine spells
     * DDL by walking the Database metamodel; here it renders from the
     * compiled store model ({@link com.legend.setup.Ddl}) and executes over
     * the ambient connection — same connection convention as executeInDb.
     */
    static TypedSide dropAndCreateTableInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        sendPlanned(body, call, env);
        return new TypedSide.Scalar(true, call.info().type());
    }

    /**
     * The K-native {@code createTempTable(name, cols, sql, [relyOnFinally,] connection)}
     * (block-compiler stage 4, 2026-09-22): the engine calls the {@code sql} argument —
     * a per-database string builder — to spell the DDL; here the DDL is the dialect's
     * own {@link com.legend.sql.SqlDdl.CreateTable} (temporary), spelled from the TYPE
     * of each {@code ^Column(name=…, type=^Integer())} literal — DDL is SQL, the dialect
     * renders (task #6). The string-builder argument is NOT evaluated: it reaches this arm
     * already inlined (a lambda, so it cannot be recognized by name here), and MEASURED
     * over the engine corpus (2026-09-22) every one of its five callers passes one of the
     * spec's two {@code createTempTableStatement()} builders — {@code toDDL}'s and
     * {@code testDataGeneration}'s — both a per-DatabaseType TEXT spelling of the same
     * {@code CREATE [LOCAL] TEMPORARY TABLE name(cols)}, which this arm spells from its IR
     * for every target. Same connection convention and mirror recording as
     * {@link #dropAndCreateTableInDb}.
     */
    static TypedSide createTempTable(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        sendPlanned(body, call, env);
        return new TypedSide.Scalar(null, call.info().type());
    }

    /** The K-native {@code dropTempTable(name, connection)}: the dialect's own DropTable. */
    static TypedSide dropTempTable(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        sendPlanned(body, call, env);
        return new TypedSide.Scalar(null, call.info().type());
    }


    /** One toDDL string-generator call — engine golden spellings
     * (testDDL.pure:42-45); createTableStatement renders from the
     * compiled store model like dropAndCreateTableInDb. */

    /** {@code call}'s statements, the planner's ({@link com.legend.planner.EffectPlanner}, the seam program's S2d,
     *  2026-10-31): its String arguments evaluated here (a literal folded; a computed one run, the one place a value
     *  feeds a statement's text), the statements written from them, then sent in order as RAW (an effect segment
     *  collects them for its script). */
    private static void sendPlanned(java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        java.util.List<String> values = new java.util.ArrayList<>();
        for (TypedSpec arg : com.legend.planner.EffectPlanner.stringArgs(call)) {
            values.add(evalStringArg(body, arg, env));
        }
        for (var s : com.legend.planner.EffectPlanner.statements(call, values, body, env.plan())) {
            sendEffect(env, s.executed(), s.recorded(), com.legend.exec.StatementOrigin.RAW, s.ddl());
        }
    }

    /**
     * Evaluate one String[1] argument of a K-native THROUGH the pipeline:
     * the let statements it (transitively) references ride along; all
     * others — crucially connection chains — are dropped, never evaluated.
     */
    static String evalStringArg(java.util.List<TypedSpec> body, TypedSpec arg,
            ExecEnv env) {
        // a literal-only argument (a table name, a seed CSV) is folded, never sent
        if (com.legend.compiler.spec.typed.Literals.fold(arg, body.subList(0, body.size() - 1)) instanceof String folded) {
            return folded;
        }
        java.util.Set<String> needed = new java.util.HashSet<>();
        collectVariableRefs(arg, needed);
        java.util.List<TypedSpec> kept = new java.util.ArrayList<>();
        for (int i = body.size() - 2; i >= 0; i--) {
            TypedSpec stmt = body.get(i);
            if (!(stmt instanceof com.legend.compiler.spec.typed.TypedLet let)) {
                throw new IllegalStateException("K-native dispatch: non-let statement"
                        + " preceding the call is not supported: "
                        + stmt.getClass().getSimpleName());
            }
            if (needed.contains(let.name())) {
                kept.add(0, let);
                collectVariableRefs(let.value(), needed);
            }
        }
        kept.add(arg);
        TypedSide evaluated;
        try (var __o = com.legend.exec.StatementOrigin.enterIfUnmarked(com.legend.exec.StatementOrigin.SIDE)) {
            evaluated = executeTyped(kept, env);
        }
        if (!(evaluated instanceof TypedSide.Scalar sc)
                || !(sc.value() instanceof String str)) {
            throw new IllegalStateException("K-native dispatch: the argument"
                    + " must evaluate to one String, got " + evaluated);
        }
        return str;
    }

    /** Conservative free-variable scan (shadowed names over-collect — over-KEEPING lets is safe). */
    static void collectVariableRefs(TypedSpec node, java.util.Set<String> out) {
        if (node instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            out.add(v.name());
        }
        node.children().forEach(c -> collectVariableRefs(c, out));
    }
}
