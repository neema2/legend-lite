// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.exec.Ddl;
import com.legend.exec.ExecutionResult;
import com.legend.exec.Executor;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * The K-PHASE STATEMENT EXECUTOR — the orchestration layer extracted from
 * the driver (audit 17): statement sequencing over effectful bodies, call
 * frames, effect analysis, and the K-native dispatch arms (executeInDb,
 * dropAndCreateTableInDb/Schema, print). {@link com.legend.Compiler} stays
 * the ONE driver seam and delegates here; nothing in this class decides
 * pipeline ORDER — it executes already-resolved statements.
 *
 * <p>The execution environment is ONE ambient JDBC connection, one dialect,
 * one optional raw-SQL failure sink ({@link ExecEnv}) — connection-valued
 * expressions are never evaluated.
 */
final class StatementExecutor {

    private StatementExecutor() {
    }

    /** The G½→H→I→J→K back half over a name-RESOLVED query AST. */
    static @com.legend.base.Nullable ExecutionResult execute(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            com.legend.sql.dialect.SqlDialect dialect,
            java.sql.Connection connection) {
        return execute(resolved, ctx, runtimeFqn, dialect, connection, null,
                null);
    }

    /** Listener overload: {@code assertListener} observes every
     * statement-root assert verdict (the runner's scoring seam —
     * judging stays here, in one place); {@code replayOracle} is the
     * registered SQL-replay oracle (SQLTEXT charter §2 — same
     * registration seam, same nullable carriage; production passes
     * neither). */
    static @com.legend.base.Nullable ExecutionResult execute(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            com.legend.sql.dialect.SqlDialect dialect,
            java.sql.Connection connection,
            com.legend.exec.@com.legend.base.Nullable AssertListener assertListener,
            com.legend.exec.@com.legend.base.Nullable SqlReplayOracle replayOracle) {
        return execute(resolved, ctx, runtimeFqn, dialect, connection, assertListener,
                replayOracle, ExecuteOptions.NONE);
    }

    /** With the caller's execute OPTIONS (they ride the environment and the
     * result — no static slot). */
    static @com.legend.base.Nullable ExecutionResult execute(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            com.legend.sql.dialect.SqlDialect dialect,
            java.sql.Connection connection,
            com.legend.exec.@com.legend.base.Nullable AssertListener assertListener,
            com.legend.exec.@com.legend.base.Nullable SqlReplayOracle replayOracle,
            ExecuteOptions options) {
        SpecCompiler specs = new SpecCompiler(ctx);
        java.util.List<TypedSpec> typedBody = specs.typeQueryBody(resolved);
        // the execution OPTIONS: the per-call ones ride each execute call's
        // own from (the engine's exeCtx overload binds them onto its bound
        // context); the caller's ride the environment
        ExecEnv env0 = new ExecEnv(ctx, runtimeFqn, dialect, connection).withOptions(options);
        ExecEnv env = assertListener == null && replayOracle == null ? env0
                : env0.withListeners(assertListener, replayOracle);
        if (resolved instanceof com.legend.protocol.spec.LambdaFunction rlf
                && rlf.parameters().isEmpty()) {
            env = env.withProtocolBody(rlf.body());
        }
        return executeStatements(typedBody, new java.util.ArrayList<>(), specs, env);
    }

    /** The K-phase execution environment: ONE ambient connection, ONE
     * dialect (audit 17: recomputing it per arm invited a future
     * mixed-dialect bug), the driver runtime, and the
     * addDriverTablePkForProject execution option (#45 — see
     * the program's own execute call, the exeCtx overload). F7.1: the raw-SQL
     * failure sink is GONE — a failed raw statement THROWS (zero live
     * sink firings on both full sweeps; the corpus runner records
     * failures per SETUP UNIT and keeps its emptiness guard). */
    /** The raw-SQL ledger append (Phase 2b): the caller's recorder, when it
     * handed one through the options; the executor never keeps one. */
    /** The H2 replay ledger records DDL in H2's spelling on EVERY session. */
    private static final com.legend.sql.dialect.SqlDialect H2_DDL = new com.legend.sql.dialect.H2();

    /** The engine's golden DDL TEXT renderer (root layer only — invariant 4d). */
    static final com.legend.sql.dialect.SqlDialect ENGINE_TEXT =
            new com.legend.sql.dialect.EngineStyleH2();

    private static void record(ExecEnv env, String sql, boolean query) {
        var r = env.options().recorder();
        if (r != null) {
            r.recordExecuted(sql, query);
        }
    }

    record ExecEnv(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
            com.legend.sql.dialect.SqlDialect dialect,
            java.sql.Connection connection,
            java.util.Map<String, TypedSpec> queryLets,
            java.util.Map<String, String> tableReplace,
            com.legend.exec.InstanceIds instanceIds,
            com.legend.exec.@com.legend.base.Nullable AssertListener assertListener,
            com.legend.exec.@com.legend.base.Nullable SqlReplayOracle replayOracle,
            java.util.Map<String, java.util.Map<String, java.util.List<java.util.List<String>>>>
                    planRows,
            java.util.List<com.legend.protocol.spec.ValueSpecification> protocolBody,
            com.legend.compiler.spec.typed.@com.legend.base.Nullable ExecutionContext frame,
            ExecuteOptions options,
            com.legend.exec.ExecutionTrace trace,
            com.legend.exec.@com.legend.base.Nullable VerdictBatch verdictBatch,
            com.legend.exec.@com.legend.base.Nullable EffectSink effectSink) {
        /** Leg 3.4: the body's deferred verdicts (database mode). */
        ExecEnv withVerdictBatch(com.legend.exec.VerdictBatch b) {
            return new ExecEnv(ctx, runtimeFqn, dialect, connection, queryLets, tableReplace,
                    instanceIds, assertListener, replayOracle, planRows, protocolBody, frame,
                    options, trace, b, effectSink);
        }
        /** Block-compiler stage 3: the effect natives' sends land in this sink (an
         * effect segment under construction) instead of the connection. */
        ExecEnv withEffectSink(com.legend.exec.@com.legend.base.Nullable EffectSink sink) {
            return new ExecEnv(ctx, runtimeFqn, dialect, connection, queryLets, tableReplace,
                    instanceIds, assertListener, replayOracle, planRows, protocolBody, frame,
                    options, trace, verdictBatch, sink);
        }
        /** Without the protocol body (a handle's rows built off the typed
         * tree alone). */
        ExecEnv(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
                com.legend.sql.dialect.SqlDialect dialect,
                java.sql.Connection connection,
                java.util.Map<String, TypedSpec> queryLets,
                java.util.Map<String, String> tableReplace,
                com.legend.exec.InstanceIds instanceIds,
                com.legend.exec.@com.legend.base.Nullable AssertListener assertListener,
                com.legend.exec.@com.legend.base.Nullable SqlReplayOracle replayOracle,
                java.util.Map<String, java.util.Map<String, java.util.List<java.util.List<String>>>>
                        planRows) {
            this(ctx, runtimeFqn, dialect, connection, queryLets,
                    tableReplace, instanceIds, assertListener, replayOracle, planRows,
                    java.util.List.of(), null, ExecuteOptions.NONE,
                    new com.legend.exec.ExecutionTrace(), null, null);
        }
        /** The caller's execute options (the PCT wire render). */
        ExecEnv withOptions(ExecuteOptions o) {
            return new ExecEnv(ctx, runtimeFqn, dialect, connection,
                    queryLets, tableReplace, instanceIds, assertListener, replayOracle,
                    planRows, protocolBody, frame, o, trace, verdictBatch, effectSink);
        }
        /** The executing frame's bound context (post-processors, time zone,
         * options) — set where an execute frame is entered. */
        ExecEnv withFrame(com.legend.compiler.spec.typed.ExecutionContext f) {
            return new ExecEnv(ctx, runtimeFqn, dialect, connection, queryLets, tableReplace, instanceIds, assertListener, replayOracle,
                    planRows, protocolBody, f, options, trace, verdictBatch, effectSink);
        }
        ExecEnv withTableReplace(java.util.Map<String, String> tr) {
            return new ExecEnv(ctx, runtimeFqn, dialect, connection, queryLets, tr, instanceIds, assertListener, replayOracle,
                    planRows, protocolBody, frame, options, trace, verdictBatch, effectSink);
        }
        ExecEnv withListeners(com.legend.exec.@com.legend.base.Nullable AssertListener l,
                com.legend.exec.@com.legend.base.Nullable SqlReplayOracle o) {
            return new ExecEnv(ctx, runtimeFqn, dialect, connection, queryLets, tableReplace, instanceIds, l, o, planRows, protocolBody, frame, options, trace, verdictBatch, effectSink);
        }
        com.legend.compiler.spec.typed.ExecutionContext.PostProcessors postProcessors() {
            return frame == null ? com.legend.compiler.spec.typed.ExecutionContext.PostProcessors.NONE
                    : frame.postProcessors();
        }
        /** The frame connection's time zone (every DateTime literal spells in it). */
        @com.legend.base.Nullable String timeZone() {
            return frame == null ? null : frame.timeZone();
        }
        /** The same environment over another session — the system
         * database's connection for a body that reads the metamodel. */
        ExecEnv withConnection(java.sql.Connection other) {
            return other == connection ? this : new ExecEnv(ctx, runtimeFqn,
                    dialect, other, queryLets, tableReplace,
                    instanceIds, assertListener, replayOracle, planRows,
                    protocolBody, frame, options, trace, verdictBatch, effectSink);
        }

        /** The query's PROTOCOL statements (the source-shaped lets a
         * handle's rows are built from — the lineage scan walks the raw
         * query lambda). */
        ExecEnv withProtocolBody(
                java.util.List<com.legend.protocol.spec.ValueSpecification> body) {
            return new ExecEnv(ctx, runtimeFqn, dialect, connection,
                    queryLets, tableReplace, instanceIds,
                    assertListener, replayOracle, planRows, body, frame, options, trace, verdictBatch, effectSink);
        }

        ExecEnv(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
                com.legend.sql.dialect.SqlDialect dialect,
                java.sql.Connection connection,
                java.util.Map<String, TypedSpec> queryLets,
                java.util.Map<String, String> tableReplace,
                com.legend.exec.InstanceIds instanceIds) {
            this(ctx, runtimeFqn, dialect, connection, queryLets, tableReplace, instanceIds, null, null,
                    new java.util.LinkedHashMap<>());
        }

        ExecEnv(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
                com.legend.sql.dialect.SqlDialect dialect,
                java.sql.Connection connection,
                java.util.Map<String, TypedSpec> queryLets,
                java.util.Map<String, String> tableReplace) {
            // F13: one site-id minter per env — both sides of every
            // verdict share it, so identity ids agree across lowerings
            this(ctx, runtimeFqn, dialect, connection, queryLets, tableReplace, new com.legend.exec.InstanceIds());
        }

        ExecEnv(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
                com.legend.sql.dialect.SqlDialect dialect,
                java.sql.Connection connection,
                    java.util.Map<String, TypedSpec> queryLets) {
            // historical arity: no connection post-processor hooks
            this(ctx, runtimeFqn, dialect, connection,
                    queryLets, java.util.Map.of());
        }

        ExecEnv(ModelContext ctx, @com.legend.base.Nullable String runtimeFqn,
                com.legend.sql.dialect.SqlDialect dialect,
                java.sql.Connection connection) {
            // run-scoped accumulator of inliner-consumed lets: graph-tree
            // date args keep their source spelling (the serialize key), so
            // every resolver seeds its let env from here (engine
            // inScopeVars)
            this(ctx, runtimeFqn, dialect, connection,
                    new java.util.LinkedHashMap<>());
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
    static @com.legend.base.Nullable ExecutionResult executeStatements(
            java.util.List<TypedSpec> stmts, java.util.List<TypedSpec> letPrefix,
            SpecCompiler specs, ExecEnv env0) {
        // THE BLOCK COMPILER (stages 1–4, 2026-09-22): every body is walked ONCE by the
        // segment walk — verdict segments (deferred rows under the database judge; the
        // host arm judges at once, it has no batch), effect segments as scripts, values
        // prepared at compile and run at the segment's close. The statement-by-statement
        // loop that preceded it is deleted (stage 4); a body no arm claims walls loudly.
        return BodyCompiler.execute(stmts, letPrefix, specs, env0);
    }

    /** The body's verdict batch: the fusion (flat WITH + UNION ALL), its shape, the
     * one-row verdict shape, the judge. */
    static com.legend.exec.VerdictBatch newVerdictBatch() {
        return new com.legend.exec.VerdictBatch(com.legend.lowering.VerdictSql::batch,
                new com.legend.compiler.element.type.ExprType(
                        com.legend.lowering.VerdictSql.batchSchema(),
                        com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_MANY),
                AssertVerdicts.ONE_ROW, DatabaseJudge::verdictOf);
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
            @com.legend.base.Nullable ExecutionResult verdict,
            com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedNativeCall contextOwner,
            com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedNativeCall forcedFrame) {
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
                .stage(b, stageEnv, nativeRoutines(specs, env)));

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
            ExecutionResult inlinedVerdict = AssertVerdicts.tryAdjudicate(
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
            String catFqn = cat.callee().qualifiedName();
            if (com.legend.builtin.NativeFn.ContextOwner.of(catFqn).isPresent()) {
                return new PreparedValue(body, env, lets, null, cat, null);
            }
            if (com.legend.builtin.NativeFn.Handle.forcesAtValuePosition(catFqn)) {
                return new PreparedValue(body, env, lets, null, null, cat);
            }
        }
        com.legend.resolver.StoreResolver resolver =
                resolver(specs, env);
        body = resolver.resolve(body, env.runtimeFqn());              // Phase H
        // C2.2: stores bound to DIFFERENT connections cannot share
        // the one session connection — wall, never wrong-database rows
        CrossStoreGuard.check(body, env.ctx(), env.runtimeFqn());
        // the statement's env is widened LAST, after resolution — the loop's own order
        // (computing it before resolution changed a lowering: an order dependence
        // in the shared state the inliner touches, measured 2026-09-21, owed)
        return new PreparedValue(body, frameReplaceEnv(stmt, execFrames, env, letPrefix, specs),
                lets, null, null, null);
    }

    /** Run a prepared value statement: the run phase only. */
    static @com.legend.base.Nullable ExecutionResult runValue(PreparedValue pv, SpecCompiler specs,
            java.util.Deque<String> frames) {
        ExecutionResult verdict = pv.verdict();
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

    /**
     * The engine's SQL-text surface: lower the query lambda through the
     * platform's own G½->H->I against the MAPPING ARGUMENT and render with
     * the engine-style dialect. H2 only — other DatabaseTypes throw until
     * their renderers exist. Never lowers, never touches the connection.
     */
    private static @com.legend.base.Nullable ExecutionResult toSqlString(
            com.legend.compiler.spec.NativeDispatch.RoutineCall call,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
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
            db = dbBound == null ? "H2" : dbBound;
        }
        com.legend.sql.dialect.EngineStyleH2 renderer = switch (db) {
            case "H2" -> new com.legend.sql.dialect.EngineStyleH2();
            case "DB2" -> new com.legend.sql.dialect.EngineStyleDB2();
            // Composite = the engine-DEFAULT spellings (native trim/pad/
            // cbrt, plain char_length); divergent goldens fail honestly
            case "Composite" -> new com.legend.sql.dialect.EngineStyleComposite();
            default -> throw new com.legend.error.NotImplementedException(
                    "toSQLString for DatabaseType." + db
                    + " — only the H2/DB2 engine-style renderers are built");
        };
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
        return new ExecutionResult.Scalar(post == es.plan() ? es.sql()
                        : renderer.render(post),
                com.legend.compiler.element.type.Type.Primitive.STRING);
    }

    /** THE statement resolver: the query lets and the registered handle
     * rows ride every resolution (one construction, every site). */
    static com.legend.resolver.StoreResolver resolver(
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        return new com.legend.resolver.StoreResolver(env.ctx(), specs)
                .withLetBindings(env.queryLets()).withPlanRows(env.planRows())
                .withHandleRegistrar(pn -> {
                    // an INLINE handle met by the resolver: build its rows
                    // now (the let-time path for let-bound handles)
                    PlanAllocations.registerHandleRows("", pn, java.util.List.of(), specs, env);
                    return env.planRows().get(com.legend.plan.PlanRows.scopeId(pn));
                });
    }

    record EngineSql(com.legend.sql.SqlQuery plan, String sql,
            java.util.List<TypedSpec> body) {
    }

    static EngineSql engineSql(
            com.legend.compiler.spec.typed.TypedLambda lam,
            @com.legend.base.Nullable String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            ExecEnv env,
            com.legend.sql.dialect.EngineStyleH2 renderer) {
        return engineSql(lam.body(), mappingFqn, specs, env, renderer,
                java.util.Map.of(),
                java.util.function.UnaryOperator.identity());
    }

    /** The body form, with plan-TEMPLATE parameters: each named free
     * variable lowers to the engine's {@code ${name}} placeholder
     * (value = string-typed, driving the freemarker quote template). */
    static EngineSql engineSql(java.util.List<TypedSpec> raw,
            @com.legend.base.Nullable String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            ExecEnv env,
            com.legend.sql.dialect.EngineStyleH2 renderer,
            java.util.Map<String, com.legend.sql.SqlExpr.PlanParam>
                    planParams,
            java.util.function.UnaryOperator<String> tableRenames) {
        return engineSql(raw, mappingFqn, specs, env, renderer, planParams,
                tableRenames, java.util.List.of());
    }

    /** {@code chainMappings}: ModelChainConnection mappings from the plan
     * surface's runtime argument (M2M2R — ~src classes resolve through
     * them). */
    private static EngineSql engineSql(java.util.List<TypedSpec> raw,
            @com.legend.base.Nullable String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            ExecEnv env,
            com.legend.sql.dialect.EngineStyleH2 renderer,
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
        body = resolver(specs, env)
                .resolve(body, env.runtimeFqn(), mappingFqn, chainMappings);
        body = com.legend.resolver.RelationalRootForm.apply(
                body, env.ctx(), mappingFqn);
        // the query's feature flags: a LOWERING concern (Lowerer.withFeatures)
        com.legend.lowering.Lowerer lw = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(env.ctx(), t),
                f -> env.ctx().findClass(f).isPresent(), env.ctx().implementations())
                .withFeatures(featuresOf(env, body));
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
        if (!featuresOf(env, body).contains(
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
    private static java.util.Map<String, com.legend.compiler.spec
            .NativeDispatch.Routine> nativeRoutines(
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        com.legend.compiler.spec.NativeDispatch.Routine text =
                (call, letPrefix) -> {
                    ExecutionResult r = planToString(call, letPrefix,
                            specs, env);
                    return String.valueOf(((ExecutionResult.Scalar)
                            java.util.Objects.requireNonNull(r,
                                    "plan text")).value());
                };
        com.legend.compiler.spec.NativeDispatch.Routine sqlText =
                (call, letPrefix) -> {
                    ExecutionResult r = toSqlString(call, letPrefix, specs, env);
                    return String.valueOf(((ExecutionResult.Scalar)
                            java.util.Objects.requireNonNull(r,
                                    "sql text")).value());
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

    private static @com.legend.base.Nullable ExecutionResult planToString(
            com.legend.compiler.spec.NativeDispatch.RoutineCall call,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        return planToString(call, java.util.List.of(), specs, env);
    }

    private static @com.legend.base.Nullable ExecutionResult planToString(
            com.legend.compiler.spec.NativeDispatch.RoutineCall call,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        // the plan value chases through the LET PREFIX (getAll-76 lane:
        // `let plan = executionPlan(...)` then `$plan->planToString(...)`
        // inside an assert — the handle is a symbolic binding)
        TypedSpec a0 = com.legend.compiler.spec.typed.Lets.bound(call.args().get(0), letPrefix);
        if (a0 instanceof com.legend.compiler.spec.typed.TypedTestDataGen tg) {
            return com.legend.testdatagen.TestDataGenerationNatives.planTextResult(tg, env.ctx());
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
                ? boundContext(ep.args().get(2), specs) : null;
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
        if (com.legend.builtin.Pure.EXECUTION_PLAN__FUNCTION_DEFINITION_1__MAPPING_1__RUNTIME_1__EXECUTION_CONTEXT_1__EXTENSION_MANY
                .signatureKey().equals(ep.callee().signatureKey()) && ep.args().size() == 5) {
            flags.addAll((pc == null ? com.legend.compiler.spec.typed.ExecutionContext.NONE : pc)
                    .withOptions(ep.args().get(3), v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                    .features());
        }
        if (!flags.isEmpty()) {
            flags.addAll(env.options().features());
            env = env.withOptions(env.options().withFeatures(java.util.Set.copyOf(flags)));
        }
        boolean quote = pc != null && pc.quoteIdentifiers();
        String tz = pc != null ? pc.timeZone() : null;
        String fromConn = pc == null
                ? firstFromConnectionName(
                        lam.body().get(lam.body().size() - 1))
                : null;
        String connName = pc != null && pc.connectionName() != null ? pc.connectionName()
                : fromConn != null ? fromConn
                : "TestDatabaseConnection(type = \"H2\")";
        String dbType = pc != null && pc.databaseType() != null ? pc.databaseType() : "H2";
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
            EngineSql tes = engineSql(lam.body(), mappingFqn, specs, env, planDialect(dbType, quote, tz),
                    java.util.Map.of(), java.util.function.UnaryOperator.identity(), java.util.List.of());
            return new ExecutionResult.Scalar(com.legend.plan.PlanText.singleRelationRoot(env.ctx(), tr.store(),
                    tr.accessor(), tes.plan(), tes.sql(), lam.body(), connName),
                    com.legend.compiler.element.type.Type.Primitive.STRING);
        }
        java.util.List<String> chainMaps = new java.util.ArrayList<>(
                pc != null ? pc.chainMappings() : java.util.List.of());
        queryChain.stream().filter(m2 -> !chainMaps.contains(m2))
                .forEach(chainMaps::add);
        ExecutionResult crossDb = crossDbTdsPlan(lam, mappingFqn, specs,
                env, quote, tz, connName, dbType, rootClass, chainMaps);
        if (crossDb != null) {
            return crossDb;
        }
        EngineSql es = engineSql(lam.body(), mappingFqn, specs, env,
                planDialect(dbType, quote, tz), java.util.Map.of(),
                java.util.function.UnaryOperator.identity(), chainMaps);
        return new ExecutionResult.Scalar(
                com.legend.plan.PlanText.single(env.ctx(), rootClass,
                        mappingFqn, es.plan(), es.sql(),
                        // PRE-resolution body: the TDS-vs-Class shape and
                        // the documentation channel live in the G output
                        // (post-H everything is a relation)
                        lam.body(), connName, chainMaps, es.plan(), pushDownEnums(env, lam.body())),
                com.legend.compiler.element.type.Type.Primitive.STRING);
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
    private static @com.legend.base.Nullable ExecutionResult crossDbTdsPlan(
            com.legend.compiler.spec.typed.TypedLambda lam,
            String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            ExecEnv env, boolean quote, @com.legend.base.Nullable String tz,
            @com.legend.base.Nullable String connName,
            @com.legend.base.Nullable String dbType, String rootClass,
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
                    specs, env, planDialect(dbType, quote, tz),
                    java.util.Map.of(),
                    java.util.function.UnaryOperator.identity(), chainMaps);
            String aSql = prevVar == null ? aEs.sql()
                    : com.legend.plan.PlanText.spliceLeftVar(aEs.plan(),
                            prevVar,
                            planDialect(dbType, quote, tz)::render);
            if (aSql == null) { return null; }
            String[] aImpl = com.legend.lineage.ScanRelations.rootImpl(
                    env.ctx(), mappingFqn, aRoot, chainMaps);
            allocs.add(com.legend.plan.PlanText.allocation(var,
                    com.legend.plan.PlanText.typeBlock(env.ctx(), aRoot,
                            aImpl, aEs.plan(), java.util.List.of(at),
                            mappingFqn, pushDownEnums(env, java.util.List.of(at))),
                    com.legend.plan.PlanText.single(env.ctx(), aRoot,
                            mappingFqn, aEs.plan(), aSql,
                            java.util.List.of(at), connName, chainMaps,
                            // a SPLICED allocation's resultColumns type
                            // through the placeholder (every var-sourced
                            // column INT), like the terminal's
                            prevVar == null ? aEs.plan()
                                    : com.legend.plan.PlanText.colsPlanFor(
                                            aEs.plan(), prevVar),
                            pushDownEnums(env, java.util.List.of(at)))));
            prevVar = var;
        }
        EngineSql fullEs = engineSql(lam.body(), mappingFqn, specs, env,
                planDialect(dbType, quote, tz), java.util.Map.of(),
                java.util.function.UnaryOperator.identity(), chainMaps);
        String splicedSql = com.legend.plan.PlanText.spliceLeftVar(
                fullEs.plan(), java.util.Objects.requireNonNull(prevVar),
                planDialect(dbType, quote, tz)::render);
        if (splicedSql == null) { return null; }
        String terminal = com.legend.plan.PlanText.single(env.ctx(),
                rootClass, mappingFqn, fullEs.plan(), splicedSql,
                lam.body(), connName, chainMaps,
                com.legend.plan.PlanText.colsPlanFor(
                        fullEs.plan(), prevVar), pushDownEnums(env, lam.body()));
        String[] impl = com.legend.lineage.ScanRelations.rootImpl(
                env.ctx(), mappingFqn, rootClass, chainMaps);
        java.util.List<String> children = new java.util.ArrayList<>(allocs);
        children.add(terminal);
        return new ExecutionResult.Scalar(
                com.legend.plan.PlanText.sequence(
                        com.legend.plan.PlanText.typeBlock(env.ctx(),
                                rootClass, impl, fullEs.plan(),
                                lam.body(), mappingFqn, pushDownEnums(env, lam.body())),
                        children),
                com.legend.compiler.element.type.Type.Primitive.STRING);
    }

    /** Pre-order search for the first {@code TypedFrom} carrying a
     * connection-name hint (instance-runtime from()). */
    private static @com.legend.base.Nullable String firstFromConnectionName(
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
    private static @com.legend.base.Nullable ExecutionResult sequencePlan(
            com.legend.compiler.spec.typed.TypedLambda lam,
            String mappingFqn, com.legend.compiler.spec.SpecCompiler specs,
            ExecEnv env, boolean quote, @com.legend.base.Nullable String timeZone,
            @com.legend.base.Nullable String connName, @com.legend.base.Nullable String dbType) {
        var fnType = lam.functionType();
        java.util.LinkedHashMap<String, com.legend.sql.SqlExpr.PlanParam>
                params = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, String> paramSpells =
                new java.util.LinkedHashMap<>();
        java.util.List<String> children = new java.util.ArrayList<>();
        if (!lam.parameters().isEmpty()) {
            StringBuilder ps = new StringBuilder();
            for (int i = 0; i < lam.parameters().size(); i++) {
                var p = fnType.params().get(i);
                if (i > 0) {
                    ps.append(", ");
                }
                ps.append(lam.parameters().get(i)).append(':')
                        .append(com.legend.plan.PlanText
                                .pureTypeName(p.type()))
                        .append('[').append(com.legend.plan.PurePrint.sizeRange(p.multiplicity())).append(']');
                paramSpells.put(lam.parameters().get(i),
                        com.legend.plan.PlanText.pureTypeName(p.type())
                                + "[" + com.legend.plan.PurePrint.sizeRange(p.multiplicity()) + "]");
                boolean opt = p.multiplicity() instanceof
                        com.legend.compiler.element.type.Multiplicity
                                .Bounded ob
                        && ob.lower() == 0
                        && Integer.valueOf(1).equals(ob.upper());
                String emFn = p.type() instanceof com.legend.compiler
                        .element.type.Type.EnumType et
                        ? com.legend.plan.PlanText.enumMapFnOf(env.ctx(),
                                mappingFqn, et.fqn())
                        : null;
                params.put(lam.parameters().get(i),
                        new com.legend.sql.SqlExpr.PlanParam(
                                lam.parameters().get(i),
                                com.legend.lowering.PlanParams.kindOf(
                                        p.type()), opt, emFn));
            }
            children.add(com.legend.plan.PlanText
                    .functionParametersNode(ps.toString()));
        }
        for (int i = 0; i < lam.body().size() - 1; i++) {
            if (!(lam.body().get(i)
                    instanceof com.legend.compiler.spec.typed.TypedLet let)) {
                throw new com.legend.error.NotImplementedException(
                        "plan: non-let intermediate statement");
            }
            children.add(PlanAllocations.node(let, mappingFqn, specs, env,
                    params, paramSpells, quote, timeZone, dbType));
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
                            quote, timeZone, dbType)));
            if (children.size() != 1) {
                throw new com.legend.error.NotImplementedException(
                        "plan: trailing let in a multi-node sequence"
                        + " (envelope type block from the let pending)");
            }
            return new ExecutionResult.Scalar(children.get(0),
                    com.legend.compiler.element.type.Type.Primitive.STRING);
        }
        String rootClass = com.legend.plan.PlanText.rootGetAllClass(java.util.List.of(term));
        if (rootClass == null) {
            throw new com.legend.error.NotImplementedException(
                    "plan: sequence terminal without a getAll root");
        }
        EngineSql es = engineSql(java.util.List.of(term), mappingFqn, specs,
                env, planDialect(dbType, quote, timeZone), params,
                java.util.function.UnaryOperator.identity());
        String[] impl = com.legend.lineage.ScanRelations.rootImpl(
                env.ctx(), mappingFqn, rootClass);
        // temp-table IN protocol + envelope assembly (extracted at the
        // file guardrail — PlanEnvelope owns the block-vs-sequence rule)
        return PlanEnvelope.emit(es, children, env, rootClass, impl,
                mappingFqn, term, connName, dbType,
                (com.legend.sql.dialect.EngineStyleH2)
                        planDialect(dbType, quote, timeZone),
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
            com.legend.compiler.spec.typed.@com.legend.base.Nullable ExecutionContext pc, ExecEnv env) {
        if (pc == null) {
            return new com.legend.plan.PlanConn(
                    "TestDatabaseConnection", "H2", null,
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
                    "TestDatabaseConnection", "H2", null,
                    java.util.List.of(), null);
        }
        String kind = com.legend.compiler.element.type.PlatformTypes
                .relationalConnectionSimpleName(ni.classFqn());
        if (kind == null) {
            throw new IllegalStateException(
                    "plan connection: unmatched connection class " + ni.classFqn());
        }
        String type = com.legend.compiler.spec.typed.ExecutionContext.Reader.databaseType(ni);
        String csv = ni.properties().get("testDataSetupCsv")
                instanceof com.legend.compiler.spec.typed.TypedCString c
                ? c.value() : null;
        java.util.List<String> sqls = java.util.List.of();
        if (csv != null && db != null) {
            sqls = com.legend.exec.Ddl.setUpDataSqlsText(csv, db, lookup, ENGINE_TEXT);
        }
        com.legend.plan.PlanConn.DsSpec spec = null;
        if (ni.properties().get("datasourceSpecification")
                instanceof com.legend.compiler.spec.typed
                        .TypedNewInstance ds
                && com.legend.compiler.element.type.PlatformTypes
                        .LOCAL_H2_DATASOURCE_SPECIFICATION.equals(ds.classFqn())) {
            String specCsv = ds.properties().get("testDataSetupCsv")
                    instanceof com.legend.compiler.spec.typed
                            .TypedCString sc2
                    ? sc2.value() : null;
            java.util.List<String> specSqls = specCsv != null && db != null
                    ? com.legend.exec.Ddl.setUpDataSqlsText(specCsv, db,
                            lookup, ENGINE_TEXT)
                    : java.util.List.of();
            spec = new com.legend.plan.PlanConn.DsSpec(
                    "LocalH2DatasourceSpecification", specCsv, specSqls);
        }
        return new com.legend.plan.PlanConn(kind, type, csv, sqls, spec);
    }

    /** The execution context an executionPlan / execute / toSQLString call
     * binds through its RUNTIME argument: the argument is brought to its
     * VALUE (helper calls inlined) and read ONCE. */
    /** The feature flags an execution runs under: the executing frame's own
     *  (its execute call's context argument) plus the runner's defaults
     *  (ExecuteOptions.features) — the two sources, merged ONCE here. */
    /** PUSH_DOWN_ENUM_TRANSFORM on this plan's context (the plan printer's
     *  enum-tuple form follows it). */
    static boolean pushDownEnums(ExecEnv env, java.util.List<TypedSpec> body) {
        return featuresOf(env, body).contains(
                com.legend.platform.Feature.PUSH_DOWN_ENUM_TRANSFORM);
    }

    static java.util.Set<com.legend.platform.Feature> featuresOf(ExecEnv env,
            java.util.List<TypedSpec> body) {
        java.util.Set<com.legend.platform.Feature> all =
                java.util.EnumSet.noneOf(com.legend.platform.Feature.class);
        if (env.frame() != null) {
            all.addAll(env.frame().features());
        }
        all.addAll(env.options().features());
        all.addAll(com.legend.compiler.spec.typed.ExecutionContext.treeFeatures(body));
        return all.isEmpty() ? java.util.Set.of() : java.util.Set.copyOf(all);
    }

    static com.legend.compiler.spec.typed.ExecutionContext boundContext(
            TypedSpec runtimeArg, SpecCompiler specs) {
        TypedSpec value = new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(java.util.List.of(runtimeArg)).get(0);
        return com.legend.compiler.spec.typed.ExecutionContext.reader()
                .read(java.util.Optional.empty(), value);
    }

    /** The same read under a statement: the argument and the values it
     * names (a let-bound runtime, a let-bound hook operand) chase the
     * statement's preceding lets. */
    static com.legend.compiler.spec.typed.ExecutionContext boundContext(
            TypedSpec runtimeArg, java.util.List<TypedSpec> letPrefix, SpecCompiler specs) {
        TypedSpec value = new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(java.util.List.of(com.legend.compiler.spec.typed.Lets.bound(runtimeArg, letPrefix))).get(0);
        return com.legend.compiler.spec.typed.ExecutionContext.reader()
                .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                .read(java.util.Optional.empty(), value);
    }

    /** The engine-style PLAN renderer for a connection DatabaseType —
     * the plan goldens pin Composite to the DB2-family spelling
     * (paren-wrapped conjunctions, quoted boolean placeholders). */
    static com.legend.sql.dialect.EngineStyleH2 planDialect(
            @com.legend.base.Nullable String dbType, boolean quote,
            @com.legend.base.Nullable String tz) {
        if (dbType == null) {
            return new com.legend.sql.dialect.EngineStyleH2(quote, tz);
        }
        return switch (dbType) {
            case "DB2", "Composite" ->
                    new com.legend.sql.dialect.EngineStyleDB2(quote, tz);
            default ->
                    new com.legend.sql.dialect.EngineStyleH2(quote, tz);
        };
    }

    // ===== the plan-handle WALK vocabulary (plan node model) =========

    /** The PLAN NODE MODEL for an executionPlan call — same shapes the
     * text printer spells (Sequence / FunctionParametersValidation /
     * RelationalInstantiation / SQLExecution). */
    static com.legend.plan.PlanNode planModel(
            com.legend.compiler.spec.typed.TypedNativeCall ep,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
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
                ? boundContext(ep.args().get(2), specs) : null;
        boolean quote = pc2 != null && pc2.quoteIdentifiers();
        String tz = pc2 != null ? pc2.timeZone() : null;
        var fnType = lam.functionType();
        java.util.LinkedHashMap<String, com.legend.sql.SqlExpr.PlanParam>
                params = new java.util.LinkedHashMap<>();
        java.util.List<com.legend.plan.PlanNode.Param> fps =
                new java.util.ArrayList<>();
        for (int i = 0; i < lam.parameters().size(); i++) {
            var p = fnType.params().get(i);
            boolean many = !(p.multiplicity()
                    instanceof com.legend.compiler.element.type.Multiplicity
                            .Bounded ob
                    && Integer.valueOf(1).equals(ob.upper()));
            boolean opt = p.multiplicity()
                    instanceof com.legend.compiler.element.type.Multiplicity
                            .Bounded ob2
                    && ob2.lower() == 0
                    && Integer.valueOf(1).equals(ob2.upper());
            params.put(lam.parameters().get(i),
                    new com.legend.sql.SqlExpr.PlanParam(
                            lam.parameters().get(i),
                            com.legend.lowering.PlanParams.kindOf(p.type()),
                            opt));
            // supportsStream (engine storeContract:221 + executionPlan_
            // generation findParamsSupportedForStreamInput): the param
            // streams iff it is read under an in(...) call at least once
            // and NEVER in any other call position
            fps.add(new com.legend.plan.PlanNode.Param(
                    lam.parameters().get(i),
                    supportsStream(lam, lam.parameters().get(i),
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
                specs, env, new com.legend.sql.dialect.EngineStyleH2(quote,
                        tz), params, mapperRenames, chainMaps);
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
            @com.legend.base.Nullable ExecutionResult result,
            java.util.Map<String, String> tableReplace,
            @com.legend.base.Nullable com.legend.compiler.spec.typed
                    .TypedNativeCall sourceExec,
            com.legend.sql.@com.legend.base.Nullable SqlQuery plan,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType plannedInfo,
            com.legend.sql.@com.legend.base.Nullable SqlSelect extentRows) {
        /** Pre-activity-model constructor (alias frames keep it). */
        ExecFrame(TypedSpec chain, boolean relationRooted,
                @com.legend.base.Nullable ExecutionResult result,
                java.util.Map<String, String> tableReplace) {
            this(chain, relationRooted, result, tableReplace, null, null, null, null);
        }

        ExecFrame(TypedSpec chain, boolean relationRooted,
                @com.legend.base.Nullable ExecutionResult result,
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
    /** The executor's SchemaOracle: the LIMIT-0 probe with its checked
     * failure wrapped — F1.3 keeps the JDBC surface (its exception type
     * included) out of the resolver. */
    static com.legend.resolver.RawGridSchema.SchemaOracle gridOracle(
            java.sql.Connection connection, ExecEnv env) {
        return sql -> {
            try {
                return com.legend.exec.GridProbe.probeTypedColumns(
                        sql, connection, env.dialect());
            } catch (java.sql.SQLException e) {
                throw new IllegalStateException(e);
            }
        };
    }

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
            @com.legend.base.Nullable ExecutionResult run, ExecEnv env) {
        if (run != null) {
            return env.trace().lastComment();
        }
        return env.verdictBatch() != null ? env.trace().reserve() : null;
    }

    /** {@code resultNeeded}: the frame's own result is the value asked for
     * (a value-position execute) — it runs even under a verdict batch. */
    private static ExecFrame buildFrame(
            com.legend.compiler.spec.typed.TypedNativeCall ec,
            java.util.List<TypedSpec> letPrefix, boolean eager, boolean resultNeeded,
            SpecCompiler specs, ExecEnv env) {
        // PLAN-EXECUTE normalization (burn map: unlocks the TDG wall
        // cohort + the §5 program-replayer class): executionPlan::
        // execute(plan, values, ext) peels the plan argument to its
        // executionPlan(...) BUILD — the same positional shape (query,
        // mapping, runtime, extensions), so the ordinary frame
        // machinery serves it whole. Plan TEXT is engine-text and
        // never executes here (single-compiler tenet). Non-traceable
        // plans and non-empty parametersValues WALL counted
        // (values-binding is the §5 referee-binding cut, later).
        if (com.legend.compiler.element.type.PlatformTypes
                .EXECUTION_PLAN_EXECUTE.equals(
                        ec.callee().qualifiedName())) {
            TypedSpec plan = com.legend.compiler.spec.typed.Lets.bound(ec.args().get(0), letPrefix);
            if (!(plan instanceof com.legend.compiler.spec.typed
                            .TypedNativeCall pb
                    && com.legend.compiler.element.type.PlatformTypes
                            .EXECUTION_PLAN.equals(
                                    pb.callee().qualifiedName()))) {
                throw new com.legend.error.NotImplementedException(
                        "plan-execute: plan argument does not trace to"
                                + " an executionPlan(...) build");
            }
            // the values through the caller's lets (a helper's
            // parametersValues parameter bound to [] at the call)
            if (ec.args().size() > 1
                    && !(com.legend.compiler.spec.typed.Lets.bound(ec.args().get(1), letPrefix)
                            instanceof com.legend.compiler.spec.typed.TypedCollection pv
                            && pv.elements().isEmpty())) {
                throw new com.legend.error.NotImplementedException(
                        "plan-execute: parametersValues binding pending"
                                + " (the referee-binding cut)");
            }
            ec = pb;
        }
        // the STRING ENTRY (meta::legend::executeLegendQuery): the query
        // lambda's parameters bind from the vars pairs, the chain rides
        // the same frame, the READ is the engine's result JSON string
        // (envelope emitted over the chain — relationRooted is false:
        // the frame's value is one string). The eager run executes the
        // RAW chain (pipeline validation at the let, engine parity).
        if ((com.legend.builtin.NativeFn.Handle.of(ec.callee().qualifiedName()).orElse(null) == com.legend.builtin.NativeFn.Handle.EXECUTE_LEGEND_QUERY)) {
            var lq = com.legend.compiler.spec.ExecuteChainAssembly
                    .prepareLegendQuery(ec, letPrefix, specs);
            var lqChain = com.legend.compiler.spec.ExecuteChainAssembly
                    .chain(lq, ec, letPrefix, specs, env.runtimeFqn(),
                            env.queryLets());
            // the activity's SQL text (the engine records the SQL it
            // generated — goldens are engine-H2-spelled): the engine-style
            // render of the same chain, absent when no render exists
            String activitySql = null;
            if (lqChain.relationRooted()
                    || lqChain.chain().info().type() instanceof com.legend.compiler
                            .element.type.Type.ClassType) {
                String mappingFqn = firstMappingFqn(lqChain.chain());
                // a RELATION-rooted chain (the relation accessor) carries no
                // mapping and renders without one (the resolver and the root
                // form both take none); a class-rooted chain needs its mapping
                if (mappingFqn != null || lqChain.relationRooted()) {
                    try {
                        activitySql = engineSql(java.util.List.of(lqChain.chain()),
                                mappingFqn, specs, env,
                                new com.legend.sql.dialect.EngineStyleH2(),
                                java.util.Map.of(),
                                java.util.function.UnaryOperator.identity()).sql();
                    } catch (com.legend.error.LegendCompileException
                            | com.legend.error.NotImplementedException
                            | IllegalStateException e) {
                        // no engine-style render for this chain shape: the
                        // activities array stays empty (a read of it is
                        // then a loud missing member, never a fabrication)
                        activitySql = null;
                    }
                }
            }
            TypedSpec envelope = com.legend.compiler.spec.ExecuteChainAssembly
                    .legendQueryEnvelope(lqChain.chain(), env.ctx(), activitySql);
            ExecutionResult lqRun = null;
            if (eager && (env.verdictBatch() == null || resultNeeded)) {
                com.legend.resolver.StoreResolver lqResolver =
                        resolver(specs, env);
                try (var __o = com.legend.exec.StatementOrigin.enter(resultNeeded
                        ? com.legend.exec.StatementOrigin.VALUE : com.legend.exec.StatementOrigin.LET)) {
                lqRun = executeTyped(lqResolver.resolve(
                        java.util.List.of(lqChain.chain()), env.runtimeFqn()),
                        env);
                }
            }
            PlanAllocations.registerActivityRows(ec, envelope, letPrefix, specs,
                    activityComment(lqRun, env), env);
            return new ExecFrame(envelope, false, lqRun, env.tableReplace(), ec);
        }
        var prepared = com.legend.compiler.spec.ExecuteChainAssembly
                .prepare(ec, letPrefix, specs);
        // connection POST-PROCESSOR hooks ride the runtime argument
        // (sqlQueryPostProcessorsConnectionAware): inline the runtime
        // helper, recognize the replaceTables shape, thread the rename
        // map to the lowering seam (applied over OUR SQL IR)
        if (ec.args().size() >= 3) {
            TypedSpec rtArg = com.legend.compiler.spec.typed.Lets.bound(
                            ec.args().get(2), letPrefix);
            if (rtArg instanceof com.legend.compiler.spec.typed.TypedUserCall) {
                rtArg = new com.legend.compiler.spec.UserCallInliner(specs)
                        .inlineBody(java.util.List.of(rtArg)).get(0);
            }
            // the frame's table renames ride the statement's union channel
            // (ExecFrame.tableReplace); every other frame fact — CTE
            // extraction, the nonExecutable pass, the connection's time zone
            // (batch 86) — is read off the executed chain's own from
            // (executeTyped): the from is the one carrier, no static slot
            java.util.Map<String, String> tr = com.legend.compiler.spec.typed.ExecutionContext
                    .reader()
                    .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, letPrefix))
                    .read(java.util.Optional.empty(), rtArg).postProcessors().tableReplace();
            if (!tr.isEmpty()) {
                env = env.withTableReplace(tr);
            }
        }
        var assembled = com.legend.compiler.spec.ExecuteChainAssembly
                .chain(prepared, ec, letPrefix, specs, env.runtimeFqn(),
                        env.queryLets());
        ExecutionResult run = null;
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
                    resolver(specs, env);
            java.util.List<TypedSpec> body = chainResolver.resolve(
                    java.util.List.of(assembled.chain()), env.runtimeFqn());
            try (var __o = com.legend.exec.StatementOrigin.enter(resultNeeded
                    ? com.legend.exec.StatementOrigin.VALUE : com.legend.exec.StatementOrigin.LET)) {
            run = executeTyped(body, env);
            }
        }
        PlanAllocations.registerActivityRows(ec, assembled.chain(), letPrefix, specs,
                activityComment(run, env), env);
        // leg 3.4 step 2: a relation-rooted frame of STATIC schema that ran
        // (eager) is also PLANNED once — every assert side that reads it
        // references the plan as a CTE instead of pasting the chain
        com.legend.sql.SqlQuery plan = null;
        com.legend.compiler.element.type.ExprType plannedInfo = null;
        if (eager && assembled.relationRooted() && env.verdictBatch() != null) {
            var schema = com.legend.compiler.element.type.Type.schemaView(
                    assembled.chain().info().type());
            if (schema != null && !schema.isLateBound() && schema.dynamicColumns().isEmpty()
                    && !schema.columns().isEmpty()) {
                BarePlan b = planBare(assembled.chain(), letPrefix, specs, env, null, null);
                // the PLANNED root's type is the frame's schema as a side sees
                // it (the resolver's view: a validation's key columns are
                // typed here, not in the raw chain); a plan whose width is
                // not that schema's stays pasted
                var planned = b.answered() != null ? null
                        : com.legend.compiler.element.type.Type.schemaView(b.root().info().type());
                if (planned != null && !planned.isLateBound()
                        && planned.columns().size() == b.plan().outputs().size()) {
                    plan = b.plan();
                    // the reference keeps the CHAIN's relation carrier (a
                    // Relation<…> / TDS wrapper — the cast-to-TDS erasure and
                    // the render recognizers read the wrapper), over the
                    // PLANNED columns
                    var chainType = assembled.chain().info().type();
                    plannedInfo = chainType instanceof com.legend.compiler.element.type.Type.GenericType g
                            && com.legend.compiler.element.type.PlatformTypes.RELATION_CARRIERS
                                    .contains(g.rawFqn())
                            ? new com.legend.compiler.element.type.ExprType(
                                    new com.legend.compiler.element.type.Type.GenericType(g.rawFqn(),
                                            java.util.List.of(planned), g.multArguments()),
                                    assembled.chain().info().multiplicity())
                            : b.root().info();
                }
            }
        }
        com.legend.sql.SqlSelect extentRows = null;
        if (eager && !assembled.relationRooted() && env.verdictBatch() != null
                && assembled.chain() instanceof com.legend.compiler.spec.typed.TypedFrom fr
                && com.legend.compiler.spec.ResultEnvelopeSplice.plainExtentRoot(fr.source())
                        instanceof com.legend.compiler.spec.typed.TypedGetAll root
                && resolver(specs, env).plainClassPipeline(fr, root)) {
            // rung 12: a PLAIN class-rooted let (Class.all() under filters, the
            // class bound by filters over ONE table) planned ONCE — the fold's
            // root rows (its from / where over the root table's physical
            // columns) as a CTE standing for the class's mapped extent; every
            // reader ranges over that extent. The fold stays the frame's plan
            // of record (activity, the SQL-text referee). Any other chain or
            // pipeline shape stays pasted.
            BarePlan b = planBare(assembled.chain(), letPrefix, specs, env, null, null);
            if (b.answered() == null) {
                com.legend.sql.SqlSelect rows = com.legend.lowering.VerdictSql.classExtentRows(b.plan());
                if (rows != null) {
                    plan = b.plan();
                    plannedInfo = fr.info();
                    extentRows = rows;
                }
            }
        }
        if (env.verdictBatch() != null) {
            com.legend.exec.VerdictBatch.frame(extentRows != null ? com.legend.exec.VerdictBatch.FrameRead.CLASS_CTE
                    : plan != null ? com.legend.exec.VerdictBatch.FrameRead.CTE
                    : assembled.relationRooted() ? com.legend.exec.VerdictBatch.FrameRead.PASTED
                    : com.legend.exec.VerdictBatch.FrameRead.CLASS);
        }
        return new ExecFrame(assembled.chain(),
                assembled.relationRooted(), run, env.tableReplace(), ec, plan, plannedInfo,
                extentRows);
    }

    /** A callee body with a NON-LET statement before its last (a
     * statement sequence, not one expression). */

    /** The first from() mapping reference in the chain (pre-order), or null. */
    private static @com.legend.base.Nullable String firstMappingFqn(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedFrom fr
                && fr.mapping().isPresent()) {
            return fr.mapping().get().fullPath();
        }
        for (TypedSpec c : n.children()) {
            String m = firstMappingFqn(c);
            if (m != null) {
                return m;
            }
        }
        return null;
    }

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
     * {@code let tds = $r.values(->at(0)/->toOne()/->first())} over a RELATION-rooted
     * frame: the wrappers are the Result envelope and the alias IS the same
     * frame ({@code $tds->size()} keeps ONE-TDS semantics). {@code at(k>0)}
     * is loud — the envelope holds one TDS. Class/scalar roots return null:
     * their at/toOne are REAL selections and the binding is an ordinary let.
     */
    static @com.legend.base.Nullable ExecFrame aliasFrame(TypedSpec rhs,
            java.util.Map<String, ExecFrame> execFrames) {
        TypedSpec cur = rhs;
        boolean badIndex = false;
        while (true) {
            if (cur instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                    && pa.property().equals("values")) {
                cur = pa.source();
                continue;
            }
            if (cur instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                    && (com.legend.compiler.spec.ResultEnvelopeSplice.AT_FQN.equals(nc.callee().qualifiedName())
                            || com.legend.compiler.spec.ResultEnvelopeSplice.TO_ONE_FQN.equals(nc.callee().qualifiedName())
                            || com.legend.compiler.spec.ResultEnvelopeSplice.FIRST_FQN.equals(nc.callee().qualifiedName()))
                    && !nc.args().isEmpty()) {
                if (com.legend.compiler.spec.ResultEnvelopeSplice.AT_FQN.equals(nc.callee().qualifiedName())
                        && !(nc.args().size() == 2 && nc.args().get(1)
                                instanceof com.legend.compiler.spec.typed.TypedCInteger k
                                && k.value().longValue() == 0)) {
                    badIndex = true;
                }
                cur = nc.args().get(0);
                continue;
            }
            break;
        }
        if (cur instanceof com.legend.compiler.spec.typed.TypedVariable v
                && execFrames.containsKey(v.name())
                && execFrames.get(v.name()).relationRooted()) {
            if (badIndex) {
                throw new IllegalStateException("Result.values->at(k>0) on a"
                        + " relation-rooted query — the values envelope holds"
                        + " one TDS");
            }
            return execFrames.get(v.name());
        }
        return null;
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
                return PlanAllocations.activitySql(f.sourceExec(), f.chain(), letPrefix, specs, env);
            }

            @Override
            public @com.legend.base.Nullable String relationalActivitySql(
                    com.legend.compiler.spec.typed.TypedNativeCall ec) {
                ExecFrame f = buildFrame(ec, letPrefix, false, specs, env);
                return PlanAllocations.activitySql(ec, f.chain(), letPrefix, specs, env);
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
        ExecutionResult run(java.util.List<TypedSpec> body,
                com.legend.compiler.spec.typed.TypedNativeCall nc,
                ExecEnv env);
    }

    private static final java.util.Map<String, EffectRoutine> EFFECT_ARMS = effectArms();

    private static java.util.Map<String, EffectRoutine> effectArms() {
        java.util.Map<String, EffectRoutine> m = new java.util.LinkedHashMap<>();
        m.put(com.legend.builtin.NativeFn.Effect.EXECUTE_IN_DB.fqn(), StatementExecutor::executeInDb);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_TABLE_IN_DB.fqn(), StatementExecutor::dropAndCreateTableInDb);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_SCHEMA_IN_DB.fqn(), StatementExecutor::dropAndCreateSchemaInDb);
        m.put(com.legend.builtin.NativeFn.Effect.CREATE_TEMP_TABLE.fqn(), StatementExecutor::createTempTable);
        m.put(com.legend.builtin.NativeFn.Effect.DROP_TEMP_TABLE.fqn(), StatementExecutor::dropTempTable);
        m.put(com.legend.builtin.NativeFn.Effect.LOAD_CSV_TO_DB_TABLE.fqn(), CsvLoad::loadCsvToDbTable);
        m.put(com.legend.builtin.NativeFn.Effect.SET_UP_DATA_SQLS.fqn(), SeedSqlForms::assertForm);
        m.put(com.legend.compiler.element.type.PlatformTypes.SET_UP_DATA_SQLS_V2, SeedSqlForms::assertForm);
        m.put(com.legend.builtin.NativeFn.Effect.PRINT.fqn(),
                (body, nc, env) -> new ExecutionResult.Scalar(null,
                        com.legend.compiler.element.type.Type.Primitive.STRING));
        // debug output: a NO-OP — the argument is NEVER evaluated (it may be an
        // unlowerable diagnostic); engine parity is the statement's inertness
        m.put(com.legend.builtin.NativeFn.Effect.PRINTLN.fqn(),
                (body, nc, env) -> new ExecutionResult.Scalar(null,
                        com.legend.compiler.element.type.Type.Primitive.STRING));
        m.put(com.legend.builtin.NativeFn.Effect.CONNECTION_BY_ELEMENT.fqn(),
                (body, nc, env) -> new ExecutionResult.Scalar(null, nc.info().type()));
        return java.util.Map.copyOf(m);
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

    /**
     * Does this expression (transitively, through user calls) reach the
     * {@code executeInDb} K-native? Memoized per callee signature; a cycle
     * scores the in-progress callee non-effectful — real recursion is
     * caught loudly at execution time.
     */
    static boolean containsEffect(TypedSpec node, SpecCompiler specs,
            java.util.Map<String, Boolean> memo) {
        if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && com.legend.builtin.NativeFn.Effect.isDbEffect(nc.callee().qualifiedName())) {
            return true;
        }
        if (node instanceof com.legend.compiler.spec.typed.TypedUserCall uc
                && com.legend.builtin.Subsumed.of(uc.callee().qualifiedName()).isPresent()) {
            // a SUBSUMED engine program's body is never compiled or run
            // here (Subsumed.java): no effect
            return false;
        }
        if (node instanceof com.legend.compiler.spec.typed.TypedUserCall uc) {
            String key = uc.callee().signatureKey();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in-progress: cycles score false
                boolean effectful = false;
                java.util.List<TypedSpec> calleeBody;
                try {
                    calleeBody = specs.compile(uc.callee()).body();
                } catch (com.legend.compiler.spec.TypeInferenceException e) {
                    // an UN-TYPEABLE callee (a dead match arm's library
                    // closure — toPostgresModel's SemiStructuredObjectNavigation
                    // arm reaching sqlQueryToString's string recursion) cannot
                    // execute in EITHER channel: this over-approximating
                    // reachability scan must not decide the test on it — the
                    // SQL channel's inliner walls the LIVE arm loudly if it is
                    // ever reached (WORLD_MAP rule 5)
                    memo.put(key, false);
                    return false;
                }
                for (TypedSpec stmt : calleeBody) {
                    if (containsEffect(stmt, specs, memo)) {
                        effectful = true;
                        break;
                    }
                }
                memo.put(key, effectful);
                known = effectful;
            }
            if (known) {
                return true;
            }
        }
        // post-processor CONFIG properties carry plan-time SQL-rewrite
        // hooks, never DDL/executeInDb effects — compiling them drags in
        // relational-metamodel vocabulary the prelude does not declare
        // (ledger cluster 63)
        if (node instanceof com.legend.compiler.spec.typed
                .TypedNewInstance ni9) {
            for (var pe : ni9.properties().entrySet()) {
                if (!com.legend.compiler.element.type.PlatformTypes
                                .isPostProcessorConfigProperty(pe.getKey())
                        && containsEffect(pe.getValue(), specs, memo)) {
                    return true;
                }
            }
            return false;
        }
        if (node instanceof com.legend.compiler.spec.typed
                .TypedCopyInstance cp9) {
            if (containsEffect(cp9.source(), specs, memo)) {
                return true;
            }
            for (var pe : cp9.overrides().entrySet()) {
                if (!com.legend.compiler.element.type.PlatformTypes
                                .isPostProcessorConfigProperty(pe.getKey())
                        && containsEffect(pe.getValue(), specs, memo)) {
                    return true;
                }
            }
            return false;
        }
        for (TypedSpec c : node.children()) {
            if (containsEffect(c, specs, memo)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The I&rarr;J&rarr;K tail over a resolved TYPED body — shared by
     * {@link #executeResolved} and the K-native argument evaluation below.
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
        Established state = ESTABLISHED.computeIfAbsent(env.connection(),
                c -> new Established());
        if (!state.dirty && state.done.contains(key)) {
            return;
        }
        java.util.List<com.legend.exec.CsvSeed.Step> setups = new java.util.ArrayList<>();
        for (com.legend.compiler.spec.typed.TypedFrom fr : froms) {
            for (String sql : fr.sqlSetups()) {
                setups.add(new com.legend.exec.CsvSeed.Step.Sql(sql));
            }
            setups.addAll(com.legend.exec.CsvSeed.setupSteps(fr, env.ctx(), env.dialect()));
            // an ELEMENT runtime named by the from(): its declared
            // connections' test data (the instance form above carries the
            // same facts on the instance)
            fr.context().runtime().ifPresent(ref ->
                    setups.addAll(com.legend.exec.CsvSeed.declaredSteps(ref.fullPath(), env.ctx(), env.dialect())));
        }
        // the AMBIENT runtime (the driver's execution context): its
        // declared connections' test data seeds the session the same way
        if (env.runtimeFqn() != null) {
            setups.addAll(com.legend.exec.CsvSeed.declaredSteps(env.runtimeFqn(), env.ctx(), env.dialect()));
        }
        for (com.legend.exec.CsvSeed.Step step : setups) {
            switch (step) {
                case com.legend.exec.CsvSeed.Step.Sql blob -> {
                    for (String stmt : com.legend.sql.RawSql.splitStatements(blob.text())) {
                        boolean query;
                        try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.SEED)) {
                            query = Executor.executeRaw(env.connection(), adaptRaw(stmt, env));
                        }
                        record(env, stmt, query);
                    }
                }
                // test data's ROWS: the engine's bulk load when it has one; the
                // referee's ledger records the one insert that lands the same rows
                case com.legend.exec.CsvSeed.Step.Rows rows -> {
                    try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.SEED)) {
                        Executor.load(env.connection(), env.dialect(), rows.load());
                    }
                    if (env.options().recorder() != null) {
                        record(env, rows.text(env.dialect()), false);
                    }
                }
            }
        }
        state.done.add(key);
        state.dirty = false;
    }

    /** A session's established state: the setup texts run on it, and
     *  whether a writing statement ran since (then the next establishment
     *  re-seeds). Keyed by the session itself — the fact lives exactly as
     *  long as the connection does. */
    private static final class Established {
        /** the seed SOURCES established on the session (value keys) */
        final java.util.Set<SeedSources> done = new java.util.HashSet<>();
        /** the database's reported column types per prepared statement
         *  text (leg 3.3, {@link com.legend.exec.WireTypes}) */
        final java.util.Map<String, java.util.List<com.legend.exec.WireTypes.ReportedColumn>> wireTypes =
                new java.util.HashMap<>();
        boolean dirty;
    }

    /** WHAT A SEED IS MADE OF — the values the seed text is a pure function
     *  of, compared by value: each from()'s inline setups and the runtime it
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

    private static final java.util.Map<java.sql.Connection, Established> ESTABLISHED =
            java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    /** The session's memo of the database's reported column types per
     *  prepared statement text (leg 3.3, {@link com.legend.exec.WireTypes}). */
    static java.util.Map<String, java.util.List<com.legend.exec.WireTypes.ReportedColumn>> wireMemo(
            java.sql.Connection connection) {
        return ESTABLISHED.computeIfAbsent(connection, c -> new Established()).wireTypes;
    }

    /** A statement that WRITES is about to run on {@code connection}: the
     *  next establishment on it re-seeds. */
    static void markWriting(java.sql.Connection connection) {
        Established state = ESTABLISHED.get(connection);
        if (state != null) {
            state.dirty = true;
        }
    }

    /** THE SYSTEM DATABASE ROUTE (user ruling 2026-09-02): a body reading
     * the metamodel store executes on the GRAPH's own system database —
     * separate from every user connection, written once per graph, never
     * per execution ({@link com.legend.exec.SystemDatabase}). A body reading
     * BOTH the store and a user store has no single session: LOUD (the
     * corpus census — none fired at this landing); a body reading no
     * store pays one tree walk and nothing else. */
    private static ExecEnv routeSystemDatabase(java.util.List<TypedSpec> body,
            ExecEnv env) {
        java.util.Set<String> stores = new java.util.TreeSet<>();
        for (TypedSpec n : body) {
            collectStores(n, stores);
        }
        String storeFqn = com.legend.builtin.SystemMetamodel.STORE_FQN;
        if (!stores.contains(storeFqn)) {
            return env;
        }
        if (stores.size() > 1) {
            throw new com.legend.error.NotImplementedException("a query reading"
                    + " the system metamodel store AND a user store " + stores
                    + " has no single session — the system database is"
                    + " separate from user connections");
        }
        var store = env.ctx().findDatabase(storeFqn).orElseThrow(() ->
                new IllegalStateException("the system metamodel store is"
                        + " not in the model — injection regressed"));
        ModelContext ctx = env.ctx();
        return env.withConnection(com.legend.exec.SystemDatabase.of(ctx)
                .connectionFor(env.connection(), env.dialect(), store,
                        table -> MetamodelSeeds.rows(table, ctx)));
    }

    private static void collectStores(TypedSpec n, java.util.Set<String> out) {
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
    private static com.legend.sql.SqlQuery lowerAndPrepare(
            java.util.List<TypedSpec> body, ExecEnv env, ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect,
            java.sql.Connection connection, boolean identity) {
        // F13: identity-bearing layouts (keyless classes gain the __id
        // field, minted per construction site) ride ONLY the verdict-
        // side lane (canon rider) — golden-SQL text lanes and corpus
        // value lanes keep the plain layout, unperturbed.
        com.legend.lowering.Lowerer lowerer = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t,
                        identity),
                f -> ctx.findClass(f).isPresent(), ctx.implementations()).withEngineExistsJoinForm()
                .withDbTimeZone(env.timeZone());
        // D91: the <<equality.Key>> resolver rides EVERY lane — equal()
        // over keyed instances is the key relation on the execute path
        // too (the structural fallback erased class identity and read
        // non-key fields). Identity minting stays verdict-lane-only.
        lowerer = lowerer.withInstanceKeys(t2 -> com.legend.compiler
                .element.EqualityKeys.resolve(ctx, t2));
        if (identity) {
            lowerer = lowerer.withInstanceIds(env.instanceIds()::idOf);
        }
        // the query's feature flags: a LOWERING concern (Lowerer.withFeatures)
        lowerer = lowerer.withFeatures(featuresOf(env, body));
        com.legend.sql.SqlQuery plan =
                lowerer.lower(com.legend.lowering.SeedableLets
                        .withSeedableLetPrefix(body, env.queryLets(), ctx));
        // post-process, then two-phase dynamic pivot (DynamicPivot doc)
        plan = com.legend.exec.DynamicPivot.staticize(
                com.legend.lowering.SqlPostProcessors.applyRecorded(plan,
                        env.tableReplace(),
                        env.postProcessors().extractCtes(),
                        env.postProcessors().nonExecutable()),
                dialect, connection);
        // DEFERRED relation-toString (dynamic-pivot inners): the column
        // list exists only NOW, post-staticize. The LOWERING layer owns
        // the composition pass (invariant 6d — exec never calls the
        // middle-end); THIS orchestrator supplies the LIMIT-0 probe
        // (a SCHEMA read, DynamicPivot's two-phase discipline).
        if (!lowerer.deferredTds().isEmpty()) {
            final var fDialect = dialect;
            final var fConn = connection;
            class ProbeFailed extends RuntimeException {
                ProbeFailed(java.sql.SQLException c) { super(c); }
            }
            // the seam: PctProbe raises DataError (unchecked) itself —
            // the ProbeFailed checked-exception tunnel is gone
            plan = com.legend.lowering.Render.resolveAllDeferredTds(
                    plan, lowerer.deferredTds(),
                    inner -> com.legend.exec.PctProbe.probe(
                            inner, fDialect, fConn),
                    com.legend.exec.Executor::pureOfSqlType);
        }
        return plan;
    }

    /** Leg 3.4 step 2: the SQL text of a plan for execution or prepare —
     * with the frame definitions it references at its head (the batch's,
     * database mode); the plan itself stays a bare select. */
    static String renderFor(ExecEnv env, com.legend.sql.SqlQuery plan) {
        var b = env.verdictBatch();
        return env.dialect().render(b == null ? plan : com.legend.sql.FrameCtes.attach(plan, b.frames()));
    }

    static @com.legend.base.Nullable ExecutionResult evalValue(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        return evalValue(value, letPrefix, specs, env, null, false);
    }

    /** V11 rider entry: the canon rides the value query itself — one
     * execution serves the host referee AND the byte verdict. */
    static @com.legend.base.Nullable ExecutionResult evalValue(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            com.legend.exec.@com.legend.base.Nullable CanonRider rider) {
        return evalValue(value, letPrefix, specs, env, rider, false);
    }

    /** F13c: {@code identity} asks for the identity lane WITHOUT a
     * canon rider — assert-CONDITION sides (the value is a boolean; the
     * in-SQL eq/equal arm needs instance identity to compile the
     * engine's equality relation). A rider implies identity. */
    static @com.legend.base.Nullable ExecutionResult evalValue(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            com.legend.exec.@com.legend.base.Nullable CanonRider rider,
            boolean identity) {
        return evalValue(value, letPrefix, specs, env, rider, identity, null);
    }

    /** V7 batch 2: {@code hook} non-null threads the statement loop's
     * result-envelope splice into the verdict side evaluation — the
     * SAME {@code UserCallInliner} hook ordinary statements get, so an
     * assert side reading an execute() frame compiles the spliced
     * chain (never a raw variable read). */
    static @com.legend.base.Nullable ExecutionResult evalValue(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            com.legend.exec.@com.legend.base.Nullable CanonRider rider,
            boolean identity,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        // the addDriverTablePkForProject option is part of the EXECUTION
        // a LITERAL-ONLY side (a golden text, a seed CSV, a bare number — measured 167 of
        // 168 sides, 2026-09-22) is a compile-time constant: the compiler folds it, nothing
        // is sent; everything else executes as before (Literals never guesses). A side a
        // CANON RIDER rides is NOT folded: its canonical text is the database's own render
        // (V11 — the byte verdict of record), never a second spelling here; the executor's
        // own literal arm keeps the same rule (`folded != null && rider == null`).
        java.util.List<TypedSpec> inlined = inlinedSide(value, letPrefix, specs, env, hook);
        ExecutionResult constant = rider == null
                ? foldedSide(inlined.get(inlined.size() - 1), inlined.subList(0, inlined.size() - 1))
                : null;
        if (constant != null) {
            return constant;
        }
        try (var __o = com.legend.exec.StatementOrigin.enterIfUnmarked(com.legend.exec.StatementOrigin.SIDE)) {
            return executeTyped(stagedSide(inlined, specs, env), env, rider, identity);
        }
    }

    /** The folded constant as the result the wire would have produced, or null. */
    private static @com.legend.base.Nullable ExecutionResult foldedSide(TypedSpec value,
            java.util.List<TypedSpec> lets) {
        Object v = com.legend.compiler.spec.typed.Literals.fold(value, lets);
        return switch (v) {
            case null -> null;
            case java.util.List<?> l -> new ExecutionResult.Collection(new java.util.ArrayList<>(l), value.info().type());
            default -> new ExecutionResult.Scalar(v, value.info().type());
        };
    }

    /** A VALUE as the executor's body: the let prefix spliced, user calls
     * inlined (with the caller's envelope hook), natives staged, stores
     * resolved — the ONE road both {@link #evalValue} and
     * {@link #planValue} take (leg 3.1b: the audit's one real duplication). */
    private static java.util.List<TypedSpec> sideBody(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        return stagedSide(inlinedSide(value, letPrefix, specs, env, hook), specs, env);
    }

    /** The inlined side: the let prefix spliced and user calls inlined — the
     * form a literal-only side can be FOLDED from (a helper-built golden text is
     * literal only after its call is inlined). */
    private static java.util.List<TypedSpec> inlinedSide(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
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
    private static java.util.List<TypedSpec> stagedSide(java.util.List<TypedSpec> body,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        final java.util.List<TypedSpec> stageEnv = body;
        body.replaceAll(b -> com.legend.compiler.spec.NativeDispatch
                .stage(b, stageEnv, nativeRoutines(specs, env)));
        return resolver(specs, env).resolve(body, env.runtimeFqn());
    }

    static ExecutionResult executeTyped(
            java.util.List<TypedSpec> body, ExecEnv env) {
        return executeTyped(body, env, null, false);
    }

    static ExecutionResult executeTyped(
            java.util.List<TypedSpec> body, ExecEnv env,
            com.legend.exec.@com.legend.base.Nullable CanonRider rider) {
        return executeTyped(body, env, rider, false);
    }

    /** V11: {@code rider} non-null asks the SQL path to carry the
     * canonical renders as appended columns (wrapWithCanon); every
     * non-SQL arm leaves the rider's initial "non-sql-arm" decline in
     * place — counted at the verdict seam, never silent. F13c:
     * {@code identityLane} joins the identity lane without a rider
     * (assert-condition sides). */
    /** The typed-execution PRELUDE (leg 3.1 split): every arm that answers
     * WITHOUT a plan (effects, store navigation, DDL strings, handles, the
     * effectful map) returns {@link Prelude#answered}; otherwise the body,
     * root and environment the plan is lowered from. {@link #executeTyped}
     * and {@link #planValue} share it, so a side is planned exactly as it
     * would execute. */
    record Prelude(@com.legend.base.Nullable ExecutionResult answered,
            java.util.List<TypedSpec> body, @com.legend.base.Nullable TypedSpec root,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType declaredInfo,
            @com.legend.base.Nullable ExecEnv env) {
        static Prelude answered(ExecutionResult r) {
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
            com.legend.exec.@com.legend.base.Nullable CanonRider rider,
            boolean identityLane) {
        ModelContext ctx = env.ctx();
        String runtimeFqn = env.runtimeFqn();
        env = routeSystemDatabase(body, env);
        java.sql.Connection connection = env.connection();
        // late-bound grids: FIRST-query schema pin — staged compilation:
        // the resolver pass takes the probed roster through the oracle
        body = com.legend.resolver.RawGridSchema.stamp(body,
                gridOracle(connection, env));
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
                && com.legend.builtin.NativeFn.Effect.of(nc.callee().qualifiedName()).isPresent()) {
            EffectRoutine arm = EFFECT_ARMS.get(nc.callee().qualifiedName());
            if (arm == null) {
                throw new com.legend.error.NotImplementedException(
                        "catalog says '" + nc.callee().qualifiedName()
                        + "' is EFFECT but no arm is registered");
            }
            return Prelude.answered(arm.run(body, nc, env));
        }
        // DDL STRING generators (toDDL deprecated forms): evaluated HERE —
        // the engine walks its Database metamodel, we render from the
        // compiled store model (the lowerer has no model access)
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall ds
                && com.legend.builtin.NativeFn.DdlStatement.of(ds.callee().qualifiedName()).isPresent()) {
            return Prelude.answered(new ExecutionResult.Scalar(ddlStatementString(ds, env),
                    ds.info().type()));
        }
        ExecutionResult handle = orchestrationHandleArm(root, env);
        if (handle != null) {
            return Prelude.answered(handle);
        }
        // a COLLECTION whose elements include DDL string generators (the
        // aggregationAware setup shape: [dropSchemaStatement(..), ...]
        // ->map(s|executeInDb(..))): every element evaluates here
        if (root instanceof com.legend.compiler.spec.typed.TypedCollection ddlColl
                && ddlColl.elements().stream().anyMatch(e ->
                        e instanceof com.legend.compiler.spec.typed.TypedNativeCall enc
                        && com.legend.builtin.NativeFn.DdlStatement.of(enc.callee().qualifiedName()).isPresent())) {
            java.util.List<Object> strs = new java.util.ArrayList<>();
            for (TypedSpec e : ddlColl.elements()) {
                if (e instanceof com.legend.compiler.spec.typed.TypedNativeCall enc
                        && com.legend.builtin.NativeFn.DdlStatement.of(enc.callee().qualifiedName()).isPresent()) {
                    strs.add(ddlStatementString(enc, env));
                } else if (e instanceof com.legend.compiler.spec.typed.TypedCString cs2) {
                    strs.add(cs2.value());
                } else {
                    throw new IllegalStateException("DDL statement collection"
                            + " carries a non-DDL, non-literal element: "
                            + e.getClass().getSimpleName());
                }
            }
            return Prelude.answered(new ExecutionResult.Collection(strs,
                    com.legend.compiler.element.type.Type.Primitive.STRING));
        }
        // map over an EFFECTFUL lambda ($sqls->map(sql|executeInDb(...))):
        // the source collection evaluates through the pipeline; each
        // element executes the lambda body with the parameter bound (the
        // one statement-orchestration shape the corpus's setup bodies use).
        if (root instanceof com.legend.compiler.spec.typed.TypedMap tm
                && containsEffectfulNode(java.util.List.of(tm.mapper()))) {
            java.util.List<TypedSpec> src = new java.util.ArrayList<>(
                    body.subList(0, body.size() - 1));
            src.add(tm.source());
            ExecutionResult seedForm = SeedSqlForms.mappedExecutionForm(body, tm, env);
            ExecutionResult values = seedForm != null ? seedForm
                    : executeTyped(src, env);
            java.util.List<Object> vals = switch (values) {
                case ExecutionResult.Collection c -> c.values();
                case ExecutionResult.Scalar sc2 -> sc2.value() == null
                        ? java.util.List.of() : java.util.List.of(sc2.value());
                default -> throw new IllegalStateException(
                        "effectful map over a non-collection source");
            };
            String param = tm.mapper().parameters().get(0);
            ExecutionResult last = new ExecutionResult.Scalar(null,
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
            return Prelude.answered(last);
        }
        // V11: a canon-riding side SKIPS the literal fold — the fold is
        // a value-fetch optimization, but a requested canon is computed
        // BY THE DATABASE, so the side executes (same cost as the
        // deleted runCanon round trip; the values now come through the
        // full pipeline too, one road for both). The fold survives as
        // the LAST-RESORT value source for literals SQL cannot spell
        // (NUL-bearing strings — DuckDB VARCHAR is NUL-free): the
        // executePlan tunnel returns it with a counted decline.
        return new Prelude(null, body, root, declaredInfo, env);
    }

    static ExecutionResult executeTyped(
            java.util.List<TypedSpec> body, ExecEnv env,
            com.legend.exec.@com.legend.base.Nullable CanonRider rider,
            boolean identityLane) {
        ModelContext ctx = env.ctx();
        Prelude p = prelude(body, env, rider, identityLane);
        if (p.answered() != null) {
            return p.answered();
        }
        body = p.body();
        env = p.plannedEnv();
        java.sql.Connection connection = env.connection();
        TypedSpec root = p.plannedRoot();
        com.legend.compiler.element.type.ExprType declaredInfo = p.declaredInfo();
        com.legend.exec.ExecutionResult folded = LiteralFold.fold(root);
        if (folded != null && rider == null) {
            return folded;
        }
        if (System.getenv("LL_DUMP_RESOLVED") != null) {
            System.err.println("[resolved] " + body);
        }
        com.legend.sql.dialect.SqlDialect dialect = env.dialect();
        com.legend.sql.SqlQuery plan = lowerAndPrepare(body, env, ctx,
                dialect, connection, rider != null || identityLane);
        boolean collectionDeclared = declaredInfo != null
                && declaredInfo.type()
                        instanceof com.legend.compiler.element.type.Type.Primitive
                && declaredInfo.multiplicity()
                        .requireBounded("result shape").isMany()
                && com.legend.compiler.element.type.Type
                        .isRelation(root.info().type());
        if (System.getenv("LL_TMP_SQL") != null) {
            System.err.println("[exec-sql] " + renderFor(env, plan));
        }
        // E1 (JAVA_EVICTION_PLAN): post-staticize wrap — the plan
        // emits the PCT wire text as one Scalar String
        if (env.options().pctRender()
                && com.legend.compiler.element.type.Type
                        .isRelation(root.info().type())) {
            return executePctTds(plan, root, env);
        }
        ExecutionResult res = executePlan(plan, root,
                collectionDeclared ? declaredInfo : null, rider, folded, env);
        enforceToOneReader(root, res);
        return res;
    }

    /** ORCHESTRATION-HANDLE arms of {@link #executeTyped} (extracted at
     * the file guard, V11): connection/runtime values never lower.
     * Null = not a handle (the caller continues). */
    private static @com.legend.base.Nullable ExecutionResult orchestrationHandleArm(
            TypedSpec root, ExecEnv env) {
        if (root instanceof com.legend.compiler.spec.typed.TypedCast castC
                && castC.source()
                        instanceof com.legend.compiler.spec.typed.TypedNativeCall cbe2
                && com.legend.builtin.NativeFn.Effect.CONNECTION_BY_ELEMENT.fqn()
                        .equals(cbe2.callee().qualifiedName())) {
            return new ExecutionResult.Scalar(null, castC.info().type());
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
            return new ExecutionResult.Scalar(null, rni.info().type());
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
            return new ExecutionResult.Scalar(null, root.info().type());
        }
        return null;
    }

    /** The plan-execution tail of {@link #executeTyped}: V11 canon
     * wrap (when a rider asks) + the one Executor call. */
    /** A side's plan after the canon wrap (leg 3.1): what database mode
     * composes into its verdict statement without executing it. */
    record WrappedSide(com.legend.sql.SqlQuery plan,
            com.legend.compiler.element.type.ExprType shapeInfo,
            com.legend.exec.ResultShape shape,
            java.sql.Connection connection,
            boolean storeFree) {
        /** A side reading no store (a literal, a pure expression) runs on
         * ANY connection; a side reading a store runs where it was routed. */
    }

    /** {@link #planValue}'s outcome: a planned side, or the value the
     * pipeline ANSWERED without a plan (a host constant — database mode
     * binds a primitive one as a literal side). */
    record PlannedValue(@com.legend.base.Nullable WrappedSide side,
            @com.legend.base.Nullable ExecutionResult answered) {
    }

    /** Plan a VALUE the way {@link #evalValue} would execute it, stopping
     * after the canon wrap: null when the value answers without a plan
     * (an orchestration handle, an effect, a host-evaluated navigation —
     * database mode reports those as UNJUDGED by name). */
    static PlannedValue planValue(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            com.legend.exec.CanonRider rider,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        BarePlan b = planBare(value, letPrefix, specs, env, rider, hook);
        if (b.answered() != null) {
            return new PlannedValue(null, b.answered());
        }
        WrappedSide ws = wrapSide(b.plan(), b.root(), b.declaredInfo(), rider, b.env());
        return new PlannedValue(new WrappedSide(ws.plan(), ws.shapeInfo(), ws.shape(),
                ws.connection(), b.storeFree()), null);
    }

    /** A value's plan before the canon wrap: the plan, its root, the
     * collection declaration (when a primitive collection is declared over a
     * relation root), the planned env, and whether it reads no store. */
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
        return (db, view, tableToTemp) -> {
            com.legend.compiler.element.TypedFunction fn = env.ctx().findViewFunction(db, view)
                    .orElseThrow(() -> new com.legend.error.NotImplementedException(
                            "testDataGen: view '" + view + "' is not a view of '" + db + "'"));
            BarePlan b = planBare(specs.viewRelation(fn), java.util.List.of(), specs, env,
                    null, null);
            if (b.answered() != null) {
                throw new com.legend.error.NotImplementedException(
                        "testDataGen: view '" + view + "' planned to a host answer, not a query");
            }
            com.legend.sql.SqlQuery plan = com.legend.lowering.SqlPostProcessors.apply(
                    b.plan(), t -> tableToTemp.getOrDefault(t, t));
            return env.dialect().render(plan);
        };
    }

    record BarePlan(@com.legend.base.Nullable ExecutionResult answered,
            com.legend.sql.SqlQuery plan, TypedSpec root,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType declaredInfo,
            ExecEnv env, boolean storeFree) {
    }

    private static BarePlan planBare(TypedSpec value,
            java.util.List<TypedSpec> letPrefix,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env,
            com.legend.exec.@com.legend.base.Nullable CanonRider rider,
            java.util.function.@com.legend.base.Nullable BiFunction<TypedSpec,
                    java.util.Set<String>, TypedSpec> hook) {
        java.util.List<TypedSpec> body = sideBody(value, letPrefix, specs, env, hook);
        java.util.Set<String> stores = new java.util.TreeSet<>();
        for (TypedSpec n : body) {
            collectStores(n, stores);
        }
        Prelude p = prelude(body, env, rider, false);
        if (p.answered() != null) {
            return new BarePlan(p.answered(), com.legend.sql.SqlSelect.starOf(
                    new com.legend.sql.SqlSource.Dual()), value, null, env, true);
        }
        ExecEnv penv = p.plannedEnv();
        TypedSpec root = p.plannedRoot();
        com.legend.sql.SqlQuery plan = lowerAndPrepare(p.body(), penv, penv.ctx(),
                penv.dialect(), penv.connection(), true);
        if (rider != null && rider.canonicalJsonKeys()) {
            // the verdict plan's JSON objects with keys SORTED — one IR
            // pass over the plan, the builders untouched
            plan = com.legend.sql.JsonKeyOrder.sort(plan);
        }
        boolean collectionDeclared = p.declaredInfo() != null
                && p.declaredInfo().type()
                        instanceof com.legend.compiler.element.type.Type.Primitive
                && p.declaredInfo().multiplicity()
                        .requireBounded("result shape").isMany()
                && com.legend.compiler.element.type.Type
                        .isRelation(root.info().type());
        if (!stores.isEmpty()) {
            // leg 3.3: a store-reading side's wire-decided kinds are the
            // database's (a store declaration is not a cast); a store-free
            // side's stamp is the compiler's own fact
            var batch = penv.verdictBatch();
            plan = com.legend.exec.WireTypes.reconcile(plan,
                    collectionDeclared ? java.util.Objects.requireNonNull(p.declaredInfo())
                            : com.legend.exec.ResultShape.valueInfo(root.info()),
                    penv.dialect(), penv.connection(), wireMemo(penv.connection()),
                    batch == null ? java.util.Map.of() : batch.frames());
        }
        return new BarePlan(null, plan, root,
                collectionDeclared ? p.declaredInfo() : null, penv, stores.isEmpty());
    }

    /** The canon wrap of {@link #executePlan}, alone. */
    private static WrappedSide wrapSide(com.legend.sql.SqlQuery plan,
            TypedSpec root,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType declaredInfo,
            com.legend.exec.@com.legend.base.Nullable CanonRider rider, ExecEnv env) {
        com.legend.compiler.element.type.ExprType shapeInfo =
                declaredInfo != null ? declaredInfo
                        : com.legend.exec.ResultShape.valueInfo(root.info());
        com.legend.exec.ResultShape shape = declaredInfo != null
                ? com.legend.exec.ResultShape.COLLECTION
                : com.legend.exec.ResultShape.of(root);
        if (rider != null && shape == com.legend.exec.ResultShape.TABULAR) {
            // V7 §8 leg 1 — a TABULAR side rides the GRID canon: one
            // per-ROW canonical text as the appended last column (the
            // fusion-spike F2 shape); the tabular decode strips it into
            // the rider, row-aligned.
            var schema = com.legend.compiler.element.type.Type.schemaView(shapeInfo.type());
            if (plan.outputs().isEmpty() && (schema == null || schema.isLateBound())) {
                // leg 3.3: a raw grid (executeInDbToTDS) has no typed outputs —
                // the database's reported columns frame it (WireTypes)
                plan = com.legend.exec.WireTypes.staticized(plan, env.dialect(),
                        env.connection(), wireMemo(env.connection()));
                schema = com.legend.compiler.spec.VerdictQueries.wireSchema(plan.outputs());
            }
            var gw = com.legend.lowering.CanonicalRenderSql.wrapTdsCanon(plan, schema);
            if (gw.declineReason() != null) {
                rider.decline(gw.declineReason());
            } else {
                rider.tdsWrap(plan.outputs().size());
                plan = gw.plan();
            }
        } else if (rider != null) {
            var w = com.legend.lowering.CanonicalRenderSql.wrapWithCanon(
                    plan, shapeInfo, rider.canonicalOrder(),
                    // substitution-aware (Pair-of-Pairs): the stamp's
                    // instantiation decides key nesting
                    com.legend.compiler.element.EqualityKeys
                            .resolve(env.ctx(), shapeInfo.type()),
                    true,
                    com.legend.lowering.CanonicalRenderSql.nameValued(
                            shapeInfo.type(), env.ctx()::tracksClassifier),
                    rider.enumFrame());
            if (w.declineReason() != null) {
                rider.decline(w.declineReason());
            } else {
                rider.wrap(w.kinds(), w.many(), w.literalIndex());
                plan = w.plan();
            }
        }
        return new WrappedSide(plan, shapeInfo, shape, env.connection(), false);
    }

    private static ExecutionResult executePlan(com.legend.sql.SqlQuery plan,
            TypedSpec root,
            com.legend.compiler.element.type.@com.legend.base.Nullable ExprType declaredInfo,
            com.legend.exec.@com.legend.base.Nullable CanonRider rider,
            @com.legend.base.Nullable ExecutionResult folded, ExecEnv env) {
        com.legend.sql.SqlQuery bare = plan;
        WrappedSide ws = wrapSide(plan, root, declaredInfo, rider, env);
        plan = ws.plan();
        com.legend.compiler.element.type.ExprType shapeInfo = ws.shapeInfo();
        com.legend.exec.ResultShape shape = ws.shape();
        com.legend.sql.dialect.SqlDialect dialect = env.dialect();
        if (rider == null) {
            return Executor.execute(renderFor(env, plan), plan, shapeInfo,
                    shape, env.connection(), dialect, null, env.trace());
        }
        try {
            return Executor.execute(renderFor(env, plan), plan, shapeInfo,
                    shape, env.connection(), dialect, rider, env.trace());
        } catch (RuntimeException e) {
            // THE DECLINE TUNNEL, V11 form (DataError included — the
            // seam made the boundary translation a RuntimeException) (prepCanon/runCanon caught
            // exactly this class — a caught failure becomes the DESIGNED
            // decline sentinel, counted, never a rescue): a canon column
            // must never poison the value fetch. Witness: the
            // MIXED-ELEMENT IDENTITY carrier (F10) — pure PRINT-FORM
            // VARCHAR ('7.345D') errors under the candidate casts,
            // undetectable at wrap time (OutputCol types are
            // stamp-derived, the V6 circularity). The side re-executes
            // BARE (pure SELECT — effectful statements never reach the
            // K-arm) and the canon declines.
            boolean wasWrapped = rider.wrapped() || rider.tdsWrapped();
            boolean hadDroppableLiteral = rider.literalIndex() >= 1;
            rider.rows().clear();
            if (wasWrapped) {
                rider.decline("canon-exec: "
                        + String.valueOf(e.getMessage()).split("\\n")[0]);
            }
            // MIDDLE RUNG (F10 v1): a TYPED side whose failure may be
            // its literal candidate alone (stamp-derived column types
            // lie about the wire — the BLOB byte carrier under a STRING
            // stamp) re-wraps WITHOUT the literal channel: the bare
            // candidates keep byte-deciding instead of demoting to the
            // host. A second failure falls through to the bare rung.
            if (wasWrapped && hadDroppableLiteral) {
                try {
                    var w2 = com.legend.lowering.CanonicalRenderSql
                            .wrapWithCanon(bare, shapeInfo,
                                    rider.canonicalOrder(),
                                    com.legend.compiler.element.EqualityKeys
                                            .resolve(env.ctx(),
                                                    shapeInfo.type()),
                                    false,
                                    com.legend.lowering.CanonicalRenderSql.nameValued(
                                            shapeInfo.type(), env.ctx()::tracksClassifier));
                    if (w2.declineReason() == null) {
                        rider.wrap(w2.kinds(), w2.many(), w2.literalIndex());
                        return Executor.execute(
                                renderFor(env, w2.plan()), w2.plan(),
                                shapeInfo, shape, env.connection(), dialect,
                                rider, env.trace());
                    }
                } catch (RuntimeException e15) {
                    rider.rows().clear();
                    rider.decline("canon-exec: "
                            + String.valueOf(e15.getMessage())
                                    .split("\\n")[0]);
                }
            }
            try {
                if (!wasWrapped) {
                    // the canon never rode (wrap already declined) —
                    // the failure is the side's own
                    throw e;
                }
                return Executor.execute(renderFor(env, bare), bare,
                        shapeInfo, shape, env.connection(), dialect, null, env.trace());
            } catch (RuntimeException e2) {
                // the BARE side itself cannot execute: an unSQLable
                // literal (NUL-bearing string — DuckDB VARCHAR is
                // NUL-free). The literal fold answers, canon declines.
                if (folded != null) {
                    rider.decline("unsqlable-literal: "
                            + String.valueOf(e2.getMessage()).split("\\n")[0]);
                    return folded;
                }
                throw e2;
            }
        }
    }

    /** E1: probe (pivot plans only) → lowering-side wrap → SCALAR
     * String execution — the wire text is the plan's projection. */
    private static ExecutionResult executePctTds(com.legend.sql.SqlQuery plan,
            TypedSpec root, ExecEnv env) {
        com.legend.sql.dialect.SqlDialect dialect = env.dialect();
        java.sql.Connection connection = env.connection();
        com.legend.sql.PlanProbe probe =
                com.legend.lowering.PctTdsWrap.pivots(plan).isEmpty() ? null
                        : com.legend.exec.PctProbe.probe(plan, dialect,
                                connection);
        com.legend.sql.SqlQuery rendered =
                com.legend.lowering.PctTdsWrap.wrap(plan,
                        com.legend.compiler.element.type.Type
                                .requireRelationSchema(root.info().type()),
                        probe, com.legend.exec.Executor::pureOfSqlType);
        ExecutionResult text = Executor.execute(renderFor(env, rendered), rendered,
                com.legend.compiler.element.type.ExprType.one(
                        com.legend.compiler.element.type.Type.Primitive
                                .STRING),
                com.legend.exec.ResultShape.SCALAR, connection, dialect, null);   // PCT wire render: no activity reads its trace
        return new ExecutionResult.TdsText(
                String.valueOf(((ExecutionResult.Scalar) text).value()),
                com.legend.compiler.element.type.Type.Primitive.STRING);
    }

    /** rows->toOne() READER enforcement (audit 22b F1): the lowering is
     * row-identical to the relation (engine toOne throws at the READER,
     * never in SQL) — the reader enforces exactly-one for a
     * TABULAR-consumed toOne root; the scalar arm's second-row guard
     * covers scalar reads, this covers whole-TDS consumption and the
     * ZERO-row lower bound. */
    private static void enforceToOneReader(TypedSpec root, ExecutionResult res) {
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall tw
                && com.legend.builtin.Pure.isToOneCall(tw.callee().qualifiedName())
                && !tw.args().isEmpty()
                && com.legend.compiler.element.type.Type
                        .isRelation(tw.args().get(0).info().type())
                && res instanceof ExecutionResult.Tabular tab
                && tab.rows().size() != 1) {
            throw new IllegalStateException("toOne() over a relation returned "
                    + tab.rows().size() + " row(s) — the exactly-one contract"
                    + " (engine reader semantics)");
        }
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
    /** Corpus-authored raw H2 adapts to the SESSION: identity on a
     * dialect that executes H2 natively, the boundary translator for the
     * DuckDB reference target (H2_BACKEND.md §12 step 12 — the rewrite
     * is a DuckDB-target adaptation, never generic). */
    private static String adaptRaw(String sql, ExecEnv env) {
        return env.dialect().rawH2IsNative() ? sql
                : com.legend.sql.dialect.RawSqlBoundary.h2ToDuckDb(sql);
    }

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
            query = Executor.executeRaw(env.connection(), executed);
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
        boolean bracketed = Executor.ownsTransaction(env.connection());
        try {
            try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.BODY)) {
                Executor.executeScript(env.connection(),
                        bracketed ? env.dialect().script(texts) : String.join(";\n", texts) + ";");
            }
        } catch (com.legend.error.DataError e) {
            String abort = env.dialect().scriptAbort();
            if (bracketed && abort != null) {
                // the bracket the failure left open: closed before anything else runs
                Executor.executeRaw(env.connection(), abort);
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

    static ExecutionResult executeInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        String raw = evalStringArg(body, call.args().get(0), env);
        // split FIRST: adaptation is per-statement (its recognizers anchor
        // at statement start). Corpus-authored raw H2 goes through THE
        // boundary translator — never a dialect renderer (R0 rule).
        for (String stmt : com.legend.sql.RawSql.splitStatements(raw)) {
            // recorded AFTER it executes, with its kind: the ledger
            // mirrors executed reality by construction
            sendEffect(env, adaptRaw(stmt, env), stmt, com.legend.exec.StatementOrigin.RAW, false);
        }
        // an opaque ResultSet handle: setup statements ignore it; a test
        // that READS it will surface loudly here when that day comes
        return new ExecutionResult.Scalar(null, call.info().type());
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
     * replay is safe on both fresh and live mirrors. */
    static ExecutionResult dropAndCreateSchemaInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall sc, ExecEnv env) {
        String schemaDdl = "Create schema if not exists "
                + evalStringArg(body, sc.args().get(0), env);
        sendEffect(env, schemaDdl, schemaDdl, com.legend.exec.StatementOrigin.RAW, true);
        return new ExecutionResult.Scalar(true, sc.info().type());
    }

    /**
     * The K-native {@code dropAndCreateTableInDb}
     * (PlatformTypes.DROP_AND_CREATE_TABLE_IN_DB): the real engine spells
     * DDL by walking the Database metamodel; here it renders from the
     * compiled store model ({@link com.legend.exec.Ddl}) and executes over
     * the ambient connection — same connection convention as executeInDb.
     */
    static ExecutionResult dropAndCreateTableInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        ModelContext ctx = env.ctx();
        java.sql.Connection connection = env.connection();
        if (!(call.args().get(0)
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef db)) {
            throw new IllegalStateException("dropAndCreateTableInDb: the database"
                    + " argument must be a store reference, got "
                    + call.args().get(0).getClass().getSimpleName());
        }
        boolean hasSchema = call.args().size() == 4;
        String schema = hasSchema
                ? evalStringArg(body, call.args().get(1), env)
                : "default";
        String table = evalStringArg(body, call.args().get(hasSchema ? 2 : 1), env);
        String lookup = "default".equals(schema) ? table : schema + "." + table;
        com.legend.model.DatabaseDefinition.TableDefinition def =
                ctx.findTableDefinition(db.fullPath(), lookup)
                        .orElseThrow(() -> new IllegalStateException(
                                "dropAndCreateTableInDb: no table '" + lookup
                                        + "' in store " + db.fullPath()));
        // F7.4: model-derived DDL routes DIRECTLY — spelled for the
        // target from the TYPE, never rendered-H2-then-regexed. The H2
        // advisory mirror still needs its H2-flavored stream: the SAME
        // model spells it a second time (recorded only after the session
        // executed — the recording mirrors executed reality).
        // the replay ledger carries the H2 spelling of the DDL on EVERY
        // session (Phase 0.6): the H2 lane's fresh replays inserted into
        // tables nobody created because this recording was gated on the
        // DuckDB session (17 `ADDRESSTABLE not found` declines)
        sendEffect(env, env.dialect().render(Ddl.dropTable(schema, table)),
                H2_DDL.render(Ddl.dropTable(schema, table)), com.legend.exec.StatementOrigin.RAW, true);
        // engine parity (batch 71 experiment): the native's DDL carries the
        // declared key and nullability, exactly like the engine's
        // dropAndCreateTableInDb (applyConstraints defaults true)
        sendEffect(env, env.dialect().render(Ddl.createTable(def, schema)),
                H2_DDL.render(Ddl.createTable(def, schema)), com.legend.exec.StatementOrigin.RAW, true);
        // (the ENGINE's dropAndCreateTableInDb applies PRIMARY KEY constraints;
        // a record-only ALTER ledger carried them for a metadata replay that
        // no longer exists — deleted with the meta ledger, batch 137)
        return new ExecutionResult.Scalar(true, call.info().type());
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
    static ExecutionResult createTempTable(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        String table = evalStringArg(body, call.args().get(0), env);
        java.util.List<com.legend.sql.SqlDdl.Column> cols = new java.util.ArrayList<>();
        TypedSpec colsArg = call.args().get(1);
        java.util.List<TypedSpec> elements = colsArg instanceof com.legend.compiler.spec.typed.TypedCollection c
                ? c.elements() : java.util.List.of(colsArg);
        for (TypedSpec e : elements) {
            if (!(e instanceof com.legend.compiler.spec.typed.TypedNewInstance col)
                    || !(col.properties().get("name") instanceof com.legend.compiler.spec.typed.TypedCString name)
                    || !(col.properties().get("type") instanceof com.legend.compiler.spec.typed.TypedNewInstance type)) {
                throw new IllegalStateException("createTempTable: each column must be a literal"
                        + " ^Column(name='…', type=^<datatype>()), got " + e.getClass().getSimpleName());
            }
            cols.add(new com.legend.sql.SqlDdl.Column(name.value(), false,
                    Ddl.columnType(datatypeLiteral(type)), false, false));
        }
        com.legend.sql.SqlDdl.CreateTable ct = new com.legend.sql.SqlDdl.CreateTable(null, table, cols, true);
        sendEffect(env, env.dialect().render(ct), H2_DDL.render(ct), com.legend.exec.StatementOrigin.RAW, true);
        return new ExecutionResult.Scalar(null, call.info().type());
    }

    /** The K-native {@code dropTempTable(name, connection)}: the dialect's own DropTable. */
    static ExecutionResult dropTempTable(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env) {
        String table = evalStringArg(body, call.args().get(0), env);
        com.legend.sql.SqlDdl.DropTable dt = Ddl.dropTable(null, table);
        sendEffect(env, env.dialect().render(dt), H2_DDL.render(dt), com.legend.exec.StatementOrigin.RAW, true);
        return new ExecutionResult.Scalar(null, call.info().type());
    }

    /** A {@code ^meta::relational::metamodel::datatype::<T>(…)} literal as the store
     * model's data type, by the class's EXACT FQN ({@link com.legend.compiler.element.type.PlatformTypes});
     * the sized / scaled kinds read their literal arguments. */
    private static com.legend.model.RelationalDataType datatypeLiteral(
            com.legend.compiler.spec.typed.TypedNewInstance type) {
        java.util.function.Function<com.legend.compiler.spec.typed.TypedNewInstance,
                com.legend.model.RelationalDataType> reader = DATATYPE_LITERALS.get(type.classFqn());
        if (reader == null) {
            throw new com.legend.error.NotImplementedException(
                    "createTempTable: not a store data type: " + type.classFqn());
        }
        return reader.apply(type);
    }

    /** The data-type classes by exact FQN. */
    private static final java.util.Map<String, java.util.function.Function<
            com.legend.compiler.spec.typed.TypedNewInstance, com.legend.model.RelationalDataType>> DATATYPE_LITERALS =
            java.util.Map.ofEntries(
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_BIGINT, t -> new com.legend.model.RelationalDataType.BigInt()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_SMALLINT, t -> new com.legend.model.RelationalDataType.SmallInt()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_TINYINT, t -> new com.legend.model.RelationalDataType.TinyInt()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_INTEGER, t -> new com.legend.model.RelationalDataType.Integer_()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_FLOAT, t -> new com.legend.model.RelationalDataType.Float_()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_DOUBLE, t -> new com.legend.model.RelationalDataType.Double_()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_REAL, t -> new com.legend.model.RelationalDataType.Real()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_BIT, t -> new com.legend.model.RelationalDataType.Bit()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_TIMESTAMP, t -> new com.legend.model.RelationalDataType.Timestamp()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_DATE, t -> new com.legend.model.RelationalDataType.Date_()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_DISTINCT, t -> new com.legend.model.RelationalDataType.Distinct()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_OTHER, t -> new com.legend.model.RelationalDataType.Other()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_SEMI_STRUCTURED, t -> new com.legend.model.RelationalDataType.SemiStructured()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_VARCHAR, t -> new com.legend.model.RelationalDataType.Varchar(literalInt(t, "size"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_CHAR, t -> new com.legend.model.RelationalDataType.Char_(literalInt(t, "size"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_BINARY, t -> new com.legend.model.RelationalDataType.Binary(literalInt(t, "size"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_VARBINARY, t -> new com.legend.model.RelationalDataType.Varbinary(literalInt(t, "size"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_DECIMAL, t -> new com.legend.model.RelationalDataType.Decimal(literalInt(t, "precision"), literalInt(t, "scale"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_NUMERIC, t -> new com.legend.model.RelationalDataType.Numeric(literalInt(t, "precision"), literalInt(t, "scale"))));

    private static int literalInt(com.legend.compiler.spec.typed.TypedNewInstance type, String property) {
        if (!(type.properties().get(property) instanceof com.legend.compiler.spec.typed.TypedCInteger n)) {
            throw new IllegalStateException("createTempTable: " + type.classFqn() + " needs a literal " + property);
        }
        return n.value().intValue();
    }

    /** One toDDL string-generator call — engine golden spellings
     * (testDDL.pure:42-45); createTableStatement renders from the
     * compiled store model like dropAndCreateTableInDb. */
    private static String ddlStatementString(
            com.legend.compiler.spec.typed.TypedNativeCall ds, ExecEnv env) {
        String fqn = ds.callee().qualifiedName();
        if (com.legend.compiler.element.type.PlatformTypes
                .DROP_SCHEMA_STATEMENT.equals(fqn)
                || com.legend.compiler.element.type.PlatformTypes
                        .CREATE_SCHEMA_STATEMENT.equals(fqn)) {
            if (!(ds.args().get(0)
                    instanceof com.legend.compiler.spec.typed.TypedCString sc)) {
                throw new IllegalStateException(fqn + ": only literal schema"
                        + " names are supported, got "
                        + ds.args().get(0).getClass().getSimpleName());
            }
            return com.legend.compiler.element.type.PlatformTypes
                    .DROP_SCHEMA_STATEMENT.equals(fqn)
                    ? "Drop schema if exists " + sc.value() + " cascade;"
                    : "Create Schema if not exists " + sc.value() + ";";
        }
        // 2-arg forms default the schema (toDDL.pure:34-42)
        boolean twoArg = ds.args().size() == 2;
        if (!(ds.args().get(0)
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef db)
                || !(ds.args().get(twoArg ? 1 : 1)
                        instanceof com.legend.compiler.spec.typed.TypedCString a1)
                || (!twoArg && !(ds.args().get(2)
                        instanceof com.legend.compiler.spec.typed.TypedCString))) {
            throw new IllegalStateException(fqn + ": literal"
                    + " (database, ['schema',] 'table') arguments required");
        }
        String sch = twoArg ? "default" : a1.value();
        String tbl = twoArg ? a1.value()
                : ((com.legend.compiler.spec.typed.TypedCString)
                        ds.args().get(2)).value();
        if (com.legend.compiler.element.type.PlatformTypes
                .DROP_TABLE_STATEMENT.equals(fqn)) {
            return ENGINE_TEXT.render(Ddl.dropTable(sch, tbl));
        }
        String lookup = "default".equals(sch) ? tbl : sch + "." + tbl;
        com.legend.model.DatabaseDefinition.TableDefinition def =
                env.ctx().findTableDefinition(db.fullPath(), lookup)
                        .orElseThrow(() -> new IllegalStateException(
                                "createTableStatement: no table '" + lookup
                                        + "' in store " + db.fullPath()));
        // the ENGINE_TEXT flavor of the ONE generator (NOT NULL /
        // PRIMARY KEY constraints) — the EXECUTION flavors stay
        // constraint-free for DuckDB re-seeds
        return ENGINE_TEXT.render(Ddl.createTable(def, sch));
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
        ExecutionResult evaluated;
        try (var __o = com.legend.exec.StatementOrigin.enterIfUnmarked(com.legend.exec.StatementOrigin.SIDE)) {
            evaluated = executeTyped(kept, env);
        }
        if (!(evaluated instanceof ExecutionResult.Scalar sc)
                || !(sc.value() instanceof String str)) {
            throw new IllegalStateException("K-native dispatch: the argument"
                    + " must evaluate to one String, got " + evaluated);
        }
        return str;
    }

    /** Does any node in these (post-inline) trees carry a REAL K effect?
     * An executeInDb whose SQL is a literal SELECT is provably read-only
     * (the corpus prints such probes) and does not count. */
    static boolean containsEffectfulNode(java.util.List<TypedSpec> nodes) {
        for (TypedSpec n : nodes) {
            if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                    && com.legend.builtin.NativeFn.Effect.isDbEffect(nc.callee().qualifiedName())
                    && !isLiteralSelect(nc)) {
                return true;
            }
            if (containsEffectfulNode(n.children())) {
                return true;
            }
        }
        return false;
    }

    static boolean isLiteralSelect(
            com.legend.compiler.spec.typed.TypedNativeCall nc) {
        String fqn = nc.callee().qualifiedName();
        boolean sqlCarrier = com.legend.builtin.NativeFn.Effect.EXECUTE_IN_DB.fqn().equals(fqn);
        return sqlCarrier && !nc.args().isEmpty()
                && nc.args().get(0)
                        instanceof com.legend.compiler.spec.typed.TypedCString cs
                && cs.value().strip().toLowerCase(java.util.Locale.ROOT)
                        .startsWith("select");
    }

    /** Conservative free-variable scan (shadowed names over-collect — over-KEEPING lets is safe). */
    static void collectVariableRefs(TypedSpec node, java.util.Set<String> out) {
        if (node instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            out.add(v.name());
        }
        node.children().forEach(c -> collectVariableRefs(c, out));
    }
}
