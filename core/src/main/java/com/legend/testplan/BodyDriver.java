// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;


import com.legend.TypedQuery;

import com.legend.compiler.element.ModelContext;
import com.legend.testexecutionplan.TypedSide;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE BODY DRIVER (named for what it is 2026-10-29, the result boundary program's track D; the K-phase
 * {@code StatementExecutor} since audit 17): the orchestration of a TEST BODY -- statement sequencing over effectful
 * bodies, call frames, effect analysis, the judges and their arms, and the K-native dispatch arms (executeInDb,
 * dropAndCreateTableInDb/Schema, print). {@link Execution#runProgram} is the one entry and delegates here; nothing in
 * this class decides pipeline ORDER -- it runs already-typed statements, each through its plan.
 *
 * <p>The execution environment names a TARGET, never a session (I3, 2026-11-07): the planning of a test body's
 * frames and sides, and the shared environment its judges plan on ({@link ExecEnv}); the run opens the sessions.
 * Connection-valued expressions are never evaluated.
 *
 * <p>Public since step C (D73, 2026-10-30), for ONE caller: the body router ({@link TestPlanner}, since the
 * test-is-the-product plan's M5, 2026-11-01), which walks here every body of a class not yet moved onto the program
 * road; the execution door (TestExecution.runProgram) calls the router. The class was package-private by design while it
 * shared the root package with the door; ArchitectureTest.theDriverHasOneCaller keeps the design across the package
 * line.
 */
public final class BodyDriver {

    private BodyDriver() {
    }

    /** A body's environment at its start: the execution OPTIONS (the per-call ones ride each execute call's own from
     *  -- the engine's exeCtx overload binds them onto its bound context; the caller's ride the environment) and the
     *  query's protocol statements. The body router plans an assert program's asserts on it (the test-is-the-product
     *  plan's M6). No listener and no referee: planning tells no one anything and depends on no run-time service
     *  (holes 4 and 5, 2026-11-08). */
    static ExecEnv bodyEnv(TypedQuery typed, @com.legend.base.Nullable String runtimeFqn,
            com.legend.sql.dialect.SqlDialect dialect, PlanningOptions options) {
        ExecEnv env = ExecEnv.of(typed.context(), runtimeFqn, dialect, options);
        if (typed.resolved() instanceof com.legend.protocol.spec.LambdaFunction rlf
                && rlf.parameters().isEmpty()) {
            env = env.withProtocolBody(rlf.body());
        }
        return env;
    }

    // (ENGINE_TEXT, the engine golden DDL text's dialect held statically here until 2026-11-09:
    // com.legend.planner.EngineText.engineText(), one per use)

    /** The K-phase execution environment: the body's PLANNING half ({@link com.legend.planner.PlanEnv}: the model,
     * the runtime, the ONE dialect -- audit 17: recomputing it per arm invited a future mixed-dialect bug -- the
     * query's lets, the renames, the frame, the features, the site ids) and WHERE its statements run: the TARGET,
     * never a session (I3, 2026-11-07: planning names a target, the run opens its session -- the runner's
     * PlanSessions.Source maps one to the other), the trace, the verdict batch and the caller's execute options. Split in two by the seam program's S2g (2026-10-31) so planning helpers can leave the driver. */
    record ExecEnv(com.legend.planner.PlanEnv plan,
            com.legend.executionplan.ExecutionPlan.Target target,
            PlanningOptions options,
            com.legend.testplan.@com.legend.base.Nullable VerdictBatch verdictBatch,
            SqlTextVerdicts.@com.legend.base.Nullable Collector engineText,
            com.legend.lowering.VerdictSql.@com.legend.base.Nullable FailureMessage failureMessage,
            BodyDriver.@com.legend.base.Nullable ExecEnv appealOn,
            // the body's verdict ids, one counter shared by every copy of the environment and every planner of the body
            // (the plan-as-data design's step 0, 2026-11-08): planning state, never read at run
            com.legend.testplan.VerdictIds verdictIds,
            // the body's engine-text appeals by number, shared likewise (the plan-as-data design's step 4, 2026-11-08):
            // planning's registry, the plan carrying those its verdicts name (step 8b)
            EngineTextAppeals appeals) {

        /** A body's environment at its start: its planning half fresh ({@link com.legend.planner.PlanEnv#of}, the
         *  options' features), a fresh trace, no batch. */
        static ExecEnv of(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
                com.legend.sql.dialect.SqlDialect dialect, PlanningOptions options) {
            return new ExecEnv(com.legend.planner.PlanEnv.of(ctx, runtimeFqn, dialect, options.features(),
                    // the generator's plan names the engine's test connection, as every engine test of it does (its
                    // runtime argument is not read)
                    tg -> com.legend.testdatagen.TestDataGenerationNatives.planTextResult(tg, ctx, com.legend.planner.EngineText.engineText(),
                            "TestDatabaseConnection(type = \"" + com.legend.planner.EngineText.ENGINE_TEST_DATABASE.name()
                                    + "\")")), JudgePlans.target(com.legend.Compiler.executesOn(ctx, runtimeFqn), dialect),
                    options, null, null, null, null,
                    new com.legend.testplan.VerdictIds(), new EngineTextAppeals());
        }

        ModelContext ctx() {
            return plan.ctx();
        }

        /** {@code q} reading each {@code data:} source as this environment's database takes it (the JSON pass,
         *  2026-11-11: by the statement itself, in its dialect): what a judge's statement is planned from -- and what a
         *  probe renders, so it renders what will be planned. */
        com.legend.sql.SqlQuery read(com.legend.sql.SqlQuery q) {
            return com.legend.planner.DataSources.read(q, dialect());
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
                    : new ExecEnv(p, target, options, verdictBatch,
                            engineText, failureMessage, appealOn, verdictIds, appeals);
        }
        /** Leg 3.4: the body's deferred verdicts (database mode). */
        ExecEnv withVerdictBatch(com.legend.testplan.VerdictBatch b) {
            return new ExecEnv(plan, target, options, b, engineText,
                    failureMessage, appealOn, verdictIds, appeals);
        }
        /** The caller's execute options (the PCT wire render); their features are the planning half's. */
        ExecEnv withOptions(PlanningOptions o) {
            return new ExecEnv(plan.withFeatures(o.features()), target, o,
                    verdictBatch, engineText, failureMessage, appealOn, verdictIds, appeals);
        }
        /** The executing frame's bound context (post-processors, time zone,
         * options) — set where an execute frame is entered. */
        ExecEnv withFrame(com.legend.compiler.spec.typed.ExecutionContext f) {
            return withPlan(plan.withFrame(f));
        }
        ExecEnv withTableReplace(java.util.Map<String, String> tr) {
            return withPlan(plan.withTableReplace(tr));
        }
        /** The same environment with the engine-text arms' collector (an assert PLANNED for the program road, the
         *  test-is-the-product plan's M10b: {@link SqlTextVerdicts#plan}), or without one. */
        ExecEnv withEngineText(SqlTextVerdicts.@com.legend.base.Nullable Collector c) {
            return new ExecEnv(plan, target, options, verdictBatch, c,
                    failureMessage, appealOn, verdictIds, appeals);
        }
        /** The same environment with a caller's failure message for the assert its arm judges (the asserts-as-plan-nodes
         *  program, step 2, 2026-11-04): the database judge's verdict row carries it ({@code DatabaseJudge.runVerdict}),
         *  or none. */
        ExecEnv withFailureMessage(com.legend.lowering.VerdictSql.@com.legend.base.Nullable FailureMessage m) {
            return new ExecEnv(plan, target, options, verdictBatch,
                    engineText, m, appealOn, verdictIds, appeals);
        }
        /** The same environment with {@code on} the environment a compatibility APPEAL's rows are planned under (a
         *  system-routed body's engine-text assert under the database judge: its rows read the user's tables on the
         *  body's own environment while its verdict row runs on the program's; the walk deletion's step 3,
         *  2026-11-06), or none: the appeal planned under this environment. */
        ExecEnv withAppealOn(@com.legend.base.Nullable ExecEnv on) {
            return new ExecEnv(plan, target, options, verdictBatch,
                    engineText, failureMessage, on, verdictIds, appeals);
        }

        /** The same environment over another target -- the process's catalog for a body that reads the metamodel
         *  (targets compare by value: two sides on one database name one target) -- written in that target's dialect:
         *  the catalog's where it is not the runtime's own engine (B4, 2026-11-11: a side of a body on a server, which
         *  every judge plans by retargeting), the runtime's again off it. */
        ExecEnv withTarget(com.legend.executionplan.ExecutionPlan.Target other) {
            if (other.equals(target)) {
                return this;
            }
            com.legend.executionplan.@com.legend.base.Nullable DatabaseType from = besideCatalog(target);
            com.legend.executionplan.@com.legend.base.Nullable DatabaseType to = besideCatalog(other);
            com.legend.planner.PlanEnv p = from == to ? plan : plan.withDialect(options.spelling().dialect(
                    to != null ? com.legend.planner.ConnectionPlans.model(to)
                            : com.legend.Compiler.executesOn(ctx(), runtimeFqn()).type()));
            return new ExecEnv(p, other, options, verdictBatch, engineText, failureMessage, appealOn,
                    verdictIds, appeals);
        }

        /** The engine of the catalog {@code t} names where it is not the runtime's own engine (a body on a server);
         *  null for any other target. */
        private com.legend.executionplan.@com.legend.base.Nullable DatabaseType besideCatalog(
                com.legend.executionplan.ExecutionPlan.Target t) {
            return t.database() instanceof com.legend.executionplan.ExecutionPlan.Database.System s
                    && s.type() != com.legend.planner.ConnectionPlans.type(com.legend.Compiler.executesOn(ctx(),
                    runtimeFqn()).type()) ? s.type() : null;
        }

        /** The same environment over the process's CATALOG ({@link #systemTarget}) for a whole SYSTEM-ROUTED body
         *  ({@code route}), written in the catalog's dialect ({@link #withTarget}). */
        ExecEnv onCatalogBody(com.legend.planner.SystemRoutes.Route route) {
            return withTarget(systemTarget(ctx(), route));
        }

        /** Whether this environment's statements run on the graph's system database. */
        boolean onSystem() {
            return target.database() instanceof com.legend.executionplan.ExecutionPlan.Database.System;
        }

        /** The query's PROTOCOL statements (the source-shaped lets a
         * handle's rows are built from — the lineage scan walks the raw
         * query lambda). */
        ExecEnv withProtocolBody(
                java.util.List<com.legend.protocol.spec.ValueSpecification> body) {
            return withPlan(plan.withProtocolBody(body));
        }
    }

    /** The body's verdict batch: the fusion (flat WITH + UNION ALL), the judge, and the runner that sends each
     * statement as a plan (JudgePlans, step 6). */
    static com.legend.testplan.VerdictBatch newVerdictBatch(ExecEnv env) {
        return new com.legend.testplan.VerdictBatch(com.legend.lowering.VerdictSql::batch,
                com.legend.lowering.VerdictSql::either, com.legend.lowering.VerdictSql::selfContained, JudgePlans.runner(env),
                env.verdictIds());
    }

    /** Whether {@code nc} is a native of the asserts package, asked by its declaration: the verdict family's members
     *  there (every one but {@code assertTdsEquivalent}, the relation package's, and {@code toCSV}, the relational tests'),
     *  and {@code assertError}, the context owner (the identity guardrail: a family asked by the callee's id, never its
     *  name's prefix; the same set the package test read). */
    private static boolean assertsPackageNative(com.legend.compiler.spec.typed.TypedNativeCall nc) {
        com.legend.builtin.NativeFn.Verdict v = com.legend.builtin.NativeFn.Verdict.of(nc.callee().id()).orElse(null);
        return v != null && v != com.legend.builtin.NativeFn.Verdict.ASSERT_TDS_EQUIVALENT
                && v != com.legend.builtin.NativeFn.Verdict.TO_CSV
                || com.legend.builtin.NativeFn.ContextOwner.of(nc.callee().id()).isPresent();
    }

    /** The walk's rule for a statement-root HELPER call: a user call whose inlined, staged
     *  root is a verdict native asserting a CLASS VALUE or reading a STRING ENTRY is adjudicated as that assert (the
     *  corpus's {@code runLegendTest} wrappers, {@code toPostgresModel}'s {@code assertConversion}); every other
     *  helper root is a value -- but for an ASSERT HELPER ({@link #isAssertHelper}), whose root is its assert. */
    static boolean isHelperAssertRoot(TypedSpec bare, TypedSpec preRoot) {
        return bare instanceof com.legend.compiler.spec.typed.TypedUserCall
                && preRoot instanceof com.legend.compiler.spec.typed.TypedNativeCall
                && (com.legend.compiler.spec.VerdictRoutes.readsStringEntry(preRoot)
                        || com.legend.compiler.spec.VerdictRoutes.assertsClassValue(preRoot));
    }

    /** Whether {@code callee} IS an assert (the asserts-as-plan-nodes program, step 3, 2026-11-04): its body one
     *  statement, an assert of the family or a call of another assert helper -- a user's {@code check(x){
     *  assertEquals(2, $x) }}, the prelude's upstream {@code assertNotEq}, {@code assertNotSize}, {@code
     *  assertNotContains}, {@code assertIsNot}. Such a helper is its body's assert, as any function is its body; a helper
     *  that does more than assert (a setup that writes tables, then answers true) is not one, whatever asserts its
     *  library reaches (the first cut asked only whether an assert was reachable, and folded such setups away: 61 corpus
     *  bodies lost their tables). */
    static boolean isAssertHelper(com.legend.compiler.element.TypedFunction callee, SpecCompiler specs, int depth) {
        if (depth > 8) {
            return false;
        }
        java.util.List<TypedSpec> body = specs.compile(callee).body();
        if (body.size() != 1 || body.get(0) instanceof com.legend.compiler.spec.typed.TypedLet) {
            return false;
        }
        TypedSpec root = body.get(0);
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall nc) {
            return com.legend.builtin.NativeFn.Verdict.of(nc.callee().id())
                    .filter(v -> v != com.legend.builtin.NativeFn.Verdict.TO_CSV).isPresent();
        }
        return root instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                && isAssertHelper(uc.callee(), specs, depth + 1);
    }

    /** The assert a statement-root HELPER call reduces to, by the walk's rule ({@link #isHelperAssertRoot}: the call
     *  inlined with the lets before it, G½, and staged), or null where the rule does
     *  not apply (no helper, or a helper whose root is a value). The program road plans this root as the assert it
     *  is, under its own name, as the walk adjudicates it (the test-is-the-product plan's M10c item 4). */
    static @com.legend.base.Nullable TypedSpec helperAssertRoot(TypedSpec bare, java.util.List<TypedSpec> letPrefix,
            SpecCompiler specs, ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec, java.util.Set<String>, TypedSpec> hook) {
        if (!(bare instanceof com.legend.compiler.spec.typed.TypedUserCall call)) {
            return null;
        }
        // a necessary condition of the rule, read before the inlining (which stages the planning-time routines):
        // the callee's body reaches an asserts-package call or a string entry's read; a value helper (x::firms())
        // reaches neither and is a value without a pass over it
        java.util.Map<com.legend.model.FunctionId, Boolean> memo = new java.util.HashMap<>();
        boolean assertShaped = false;
        for (TypedSpec stmt : specs.compile(call.callee()).body()) {
            if (com.legend.compiler.spec.StatementEffects.reaches(stmt, n -> n instanceof
                    com.legend.compiler.spec.typed.TypedNativeCall nc && assertsPackageNative(nc)
                    || n instanceof com.legend.compiler.spec.typed.TypedJsonResult
                    || n instanceof com.legend.compiler.spec.typed.TypedJsonAccess, specs, memo)) {
                assertShaped = true;
                break;
            }
        }
        if (!assertShaped) {
            return null;
        }
        java.util.List<TypedSpec> single = new java.util.ArrayList<>(letPrefix);
        single.add(bare);
        var inliner = new com.legend.compiler.spec.UserCallInliner(specs, hook);
        java.util.List<TypedSpec> body = new java.util.ArrayList<>(inliner.inlineBody(single));
        env.queryLets().putAll(inliner.queryLets());
        final java.util.List<TypedSpec> stageEnv = body;
        body.replaceAll(b -> com.legend.compiler.spec.NativeDispatch
                .stage(b, stageEnv, com.legend.planner.EnginePlans.nativeRoutines(specs, env.plan())));
        TypedSpec preRoot = com.legend.compiler.spec.typed.Lets.bare(body.get(body.size() - 1));
        while (preRoot instanceof com.legend.compiler.spec.typed.TypedFrom pf) {
            preRoot = pf.source();
        }
        preRoot = foldPairProjection(preRoot);
        boolean assertHelper = isAssertHelper(call.callee(), specs, 0);
        if (assertHelper && preRoot instanceof com.legend.compiler.spec.typed.TypedCBoolean decided && decided.value()
                && body.size() == single.size()) {
            // the compiler decided the helper's assert (LiteralUnroll folds an assert over spelled values to true): the
            // assert it is, holding -- assert(true), planned by both judges as any assert (the asserts-as-plan-nodes
            // program, step 3: assertNotEq(1, 2) inlined to assert(!eq(1, 2), ...) folds so)
            return com.legend.compiler.spec.AssertMessages.holds(specs);
        }
        return isHelperAssertRoot(bare, preRoot) || assertHelper && preRoot instanceof
                com.legend.compiler.spec.typed.TypedNativeCall nc && assertsPackageNative(nc)
                && com.legend.builtin.NativeFn.Verdict.of(nc.callee().id()).isPresent() ? preRoot : null;
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
        // the union is the planner's (FramePlanner.renames, the test-is-the-product plan's M3f)
        java.util.Map<String, java.util.Map<String, String>> frames = new java.util.LinkedHashMap<>();
        execFrames.forEach((name, f) -> frames.put(name, f.tableReplace()));
        java.util.Map<String, String> union = com.legend.planner.FramePlanner.renames(stmt, frames, env.tableReplace(),
                letPrefix, specs);
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
     * scalar root, values IS the collection). A frame is planned, never run where it stands (I2, 2026-11-07: the
     * walk's eager run at the let deleted -- its result was never read). */
    record ExecFrame(TypedSpec chain, boolean relationRooted,
            java.util.Map<String, String> tableReplace,
            @com.legend.base.Nullable com.legend.compiler.spec.typed
                    .TypedNativeCall sourceExec,
            com.legend.sql.@com.legend.base.Nullable SqlQuery plan,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType plannedInfo,
            com.legend.sql.@com.legend.base.Nullable SqlSelect extentRows) {
        ExecFrame(TypedSpec chain, boolean relationRooted,
                java.util.Map<String, String> tableReplace,
                @com.legend.base.Nullable com.legend.compiler.spec.typed
                        .TypedNativeCall sourceExec) {
            this(chain, relationRooted, tableReplace, sourceExec, null, null, null);
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
        return buildFrame(ec, letPrefix, eager, specs, env, null);
    }

    /** A frame PRE-PLANNED for a moved body (the test-is-the-product plan's M9, M9c): built as the walk builds a let's
     *  frame but not run -- the program's check step runs it at the let -- its activity's comment the stamp parameter
     *  {@code stamp}, which that check binds when the plan runs and every statement on {@code env} binds as a
     *  parameter (the comment of a frame the walk ran is the run's; this frame's is the run's too, read at run). */
    static ExecFrame plannedFrame(
            com.legend.compiler.spec.typed.TypedNativeCall ec,
            java.util.List<TypedSpec> letPrefix, SpecCompiler specs, ExecEnv env, String stamp) {
        env.plan().stamp(stamp);
        // under a verdict batch (the database judge, M7) the frame's CTE is defined on the batch, which is what the
        // asserts' planner wants
        return buildFrame(ec, letPrefix, env.verdictBatch() != null, specs, env, stamp);
    }

    /** The stamp of an inline {@code execute(...)}'s activity: an execution of its own (the execution id as upstream,
     *  the user's ruling of 2026-11-12: upstream's interpreted execute mints a GUID per query it runs), named by its
     *  call site -- registered on the environment so every statement lowered on it binds it, and bound when the plan
     *  runs to the id its door minted. Until then ActivityComments' one constant comment under a verdict batch, shared
     *  by every such execute of every body, and none outside one (an empty comment). */
    private static String inlineStamp(com.legend.compiler.spec.typed.TypedNativeCall ec, ExecEnv env) {
        String stamp = com.legend.planner.ProgramFrames.STAMP + ".execute@"
                + com.legend.plan.PlanRows.scopeId(ec);
        env.plan().stamp(stamp);
        return stamp;
    }

    private static ExecFrame buildFrame(
            com.legend.compiler.spec.typed.TypedNativeCall ec0,
            java.util.List<TypedSpec> letPrefix, boolean eager,
            SpecCompiler specs, ExecEnv env0, @com.legend.base.Nullable String stampParameter) {
        // the frame's chain, planned (the seam program's S2g, 2026-10-31): the plan-execute normalization, the string
        // entry's envelope and activity SQL, the runtime's table renames and the chain's assembly -- the planner's
        com.legend.planner.FramePlanner.FrameChain fc =
                com.legend.planner.FramePlanner.chain(ec0, letPrefix, specs, env0.plan());
        com.legend.compiler.spec.typed.TypedNativeCall ec = fc.source();
        ExecEnv env = env0.withPlan(fc.env());
        com.legend.planner.PlanAllocations.registerActivityRows(ec, fc.chain(), letPrefix, specs, null,
                stampParameter == null ? inlineStamp(ec, env) : stampParameter, env.plan());
        if (fc.legendQuery()) {
            // the string entry's value is one string: no planned CTE, no read kind
            return new ExecFrame(fc.chain(), false, env.tableReplace(), ec);
        }
        // under the database judge a frame's readers reference its planned CTE where it has one (the planner's)
        com.legend.planner.FramePlanner.Cte cte = eager && env.verdictBatch() != null
                ? com.legend.planner.FramePlanner.cte(fc, letPrefix, specs) : com.legend.planner.FramePlanner.Cte.NONE;
        com.legend.sql.SqlQuery plan = cte.plan();
        com.legend.compiler.element.type.ExprType plannedInfo = cte.plannedInfo();
        com.legend.sql.SqlSelect extentRows = cte.extentRows();
        if (env.verdictBatch() != null) {
            com.legend.testplan.VerdictBatch.frame(extentRows != null ? com.legend.testplan.VerdictBatch.FrameRead.CLASS_CTE
                    : plan != null ? com.legend.testplan.VerdictBatch.FrameRead.CTE
                    : fc.relationRooted() ? com.legend.testplan.VerdictBatch.FrameRead.PASTED
                    : com.legend.testplan.VerdictBatch.FrameRead.CLASS);
        }
        return new ExecFrame(fc.chain(),
                fc.relationRooted(), env.tableReplace(), ec, plan, plannedInfo,
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
                // an eager read (size(execute(...))) runs nothing here: the test door places the execute's run as a
                // check step at its assert's turn, and the splice folds the size to the envelope's 1 (2026-11-06)
                ExecFrame f = buildFrame(ec, letPrefix, false, specs, env);
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
        // a statement an effect sends is a step of the body's plan (testplan.EffectPlanner, placed by the test door):
        // a side never runs one (I2, 2026-11-07: the walk's sending arms deleted -- measured unreached on every lane)
        m.put(com.legend.builtin.NativeFn.Effect.EXECUTE_IN_DB.fqn(), BodyDriver::effectIsAPlanStep);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_TABLE_IN_DB.fqn(), BodyDriver::effectIsAPlanStep);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_SCHEMA_IN_DB.fqn(), BodyDriver::effectIsAPlanStep);
        m.put(com.legend.builtin.NativeFn.Effect.CREATE_TEMP_TABLE.fqn(), BodyDriver::effectIsAPlanStep);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_TEMP_TABLE.fqn(), BodyDriver::effectIsAPlanStep);
        m.put(com.legend.builtin.NativeFn.Effect.LOAD_CSV_TO_DB_TABLE.fqn(), BodyDriver::effectIsAPlanStep);
        m.put(com.legend.builtin.NativeFn.Effect.SET_UP_DATA_SQLS.fqn(), SeedSqlForms::assertForm);
        m.put(com.legend.compiler.element.type.PlatformTypes.SET_UP_DATA_SQLS_V2, SeedSqlForms::assertForm);
        m.put(com.legend.builtin.NativeFn.Effect.PRINT.fqn(),
                (body, nc, env) -> new TypedSide.Scalar(null, "String"));
        // debug output: a NO-OP — the argument is NEVER evaluated (it may be an
        // unlowerable diagnostic); engine parity is the statement's inertness
        m.put(com.legend.builtin.NativeFn.Effect.PRINTLN.fqn(),
                (body, nc, env) -> new TypedSide.Scalar(null, "String"));
        m.put(com.legend.builtin.NativeFn.Effect.CONNECTION_BY_ELEMENT.fqn(),
                (body, nc, env) -> new TypedSide.Scalar(null, nc.info().type().typeName()));
        return java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(m));
    }

    /** An effect whose statement is sent: a step of the body's plan, never answered as a side. Reaching it is a lite
     *  bug (the test door places every effect as a plan step), refused by name. */
    private static TypedSide effectIsAPlanStep(java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall nc, ExecEnv env) {
        throw new IllegalStateException("'" + nc.callee().qualifiedName() + "' sends a statement: it is a step of the"
                + " body's plan (testplan.EffectPlanner), never a side answered while the body is planned");
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

    /** THE SYSTEM DATABASE ROUTE (user ruling 2026-09-02): a body reading the metamodel store executes on the
     * process's CATALOG -- separate from every user connection, made once per process, a model's rows registered in
     * the run's turn ({@link SystemDatabase}) -- on every database (B4, 2026-11-11: DuckDB's for a server). The route is
     * the planner's, a fact of the body's plan (the seam program's S2h and S4a, 2026-10-31: the catalog and store it
     * reads, the dialect its plan is written in, a body mixing the store with a user store refused by name); planning
     * NAMES its target ({@link #systemTarget}) and the run opens its session (I3, 2026-11-07: the session the walk
     * opened here deleted). {@code env} as it is where the body reads no system store. */
    private static ExecEnv openRoute(com.legend.planner.SystemRoutes.@com.legend.base.Nullable Route route,
            ExecEnv env) {
        if (route == null) {
            return env;
        }
        return env.withTarget(systemTarget(env.ctx(), route));
    }

    /** The target naming the process's catalog of {@code route}'s engine. */
    static com.legend.executionplan.ExecutionPlan.Target systemTarget(ModelContext ctx,
            com.legend.planner.SystemRoutes.Route route) {
        return com.legend.planner.SystemCatalog.target(ctx, route.engine());
    }

    /** The system store's route for a body the class rule found system-routed ({@code BodyClass.systemRouted}):
     *  the catalog of the runtime's database and the system metamodel store. */
    static com.legend.planner.SystemRoutes.Route systemRouteOf(ModelContext ctx, String runtimeFqn) {
        return com.legend.planner.SystemRoutes.route(ctx, runtimeFqn);
    }

    /** The store a staged let's table is read under (StagedRows.storeOf, which a write plan's staging reads too). */
    static String storeOf(java.util.List<TypedSpec> body, ExecEnv env) {
        return com.legend.resolver.StagedRows.storeOf(body, env.ctx(), env.runtimeFqn());
    }

    /** The folded constant as the result the wire would have produced, or null. */
    private static @com.legend.base.Nullable TypedSide foldedSide(TypedSpec value,
            java.util.List<TypedSpec> lets) {
        Object v = com.legend.compiler.spec.typed.Literals.fold(value, lets);
        return switch (v) {
            case null -> null;
            case java.util.List<?> l -> new TypedSide.Collection(new java.util.ArrayList<>(l), value.info().type().typeName());
            default -> new TypedSide.Scalar(v, value.info().type().typeName());
        };
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
            case com.legend.planner.Prepared.StoreWrite w -> throw new IllegalStateException("a write into a store"
                    + " table is a step of the body's plan, never a side answered while the body is planned");
            case com.legend.planner.Prepared.DdlText d -> new TypedSide.Scalar(d.text(), d.type().typeName());
            case com.legend.planner.Prepared.Handle h -> new TypedSide.Scalar(null, h.type().typeName());
            case com.legend.planner.Prepared.DdlTexts c -> new TypedSide.Collection(c.texts(), "String");
            case com.legend.planner.Prepared.EffectfulMap m -> throw new IllegalStateException("a map over an"
                    + " effectful lambda is a step of the body's plan, never a side answered while the body is planned");
        };
    }

    /** A side's plan after the canon wrap (leg 3.1): what database mode
     * composes into its verdict statement without executing it. */
    record WrappedSide(com.legend.sql.SqlQuery plan,
            com.legend.compiler.element.type.ExprType shapeInfo,
            com.legend.plan.ResultShape shape,
            com.legend.executionplan.ExecutionPlan.Target target,
            boolean storeFree) {
        /** A side reading no store (a literal, a pure expression) runs on
         * ANY target; a side reading a store runs where it was routed. */
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

    /** Plan a VALUE, stopping after the canon wrap: null when the value answers without a plan
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
     *  the legacy host route prepared it -- no canon rider, the identity lane when
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
        // a side the judge evaluates outside the body's statement: SIDE, where no arm has marked the texts it reads
        String priorOrigin = SqlTextVerdicts.marking(env, "SIDE", true);
        try {
            b = planStaged(com.legend.planner.BodyPlanner.stagedSide(inlined, specs, env.plan()), value, env, null, identity, true);
        } finally {
            SqlTextVerdicts.restoreMarking(env, priorOrigin);
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
        return new PlannedValue(new WrappedSide(sp.plan(), sp.shapeInfo(), sp.shape(), b.env().target(),
                b.storeFree()), null, b.root(), false);
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
     *  (the identity lane's flag alone, no wire reconciliation) where the database judge's
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

    /** One toDDL string-generator call — engine golden spellings
     * (testDDL.pure:42-45); createTableStatement renders from the
     * compiled store model like dropAndCreateTableInDb. */

    /** One String[1] argument of an effect native, folded: a literal, a let of one, their concatenation. A computed
     *  argument would run a statement while the body is planned -- refused by name (I2, 2026-11-07: the walk's
     *  evaluation through the pipeline deleted; a SQL text built at run time is S133's text hole). */
    static String evalStringArg(java.util.List<TypedSpec> body, TypedSpec arg,
            ExecEnv env) {
        if (com.legend.compiler.spec.typed.Literals.fold(arg, body.subList(0, body.size() - 1)) instanceof String folded) {
            return folded;
        }
        throw new IllegalStateException("an effect native's String argument that does not fold at planning (a"
                + " computed text) would run a statement while the body is planned");
    }

}
