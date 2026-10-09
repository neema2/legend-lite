// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;

/**
 * THE EXECUTION FRONT DOOR (C2a, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md): a query planned by
 * {@link Compiler} (the planner, which never touches a database — its library has no {@code exec}) run on a
 * session — handed by the caller and checked, or opened from the target the planner decided
 * ({@code Compiler.executesOn}, {@code exec.Sessions}). Execution reaches the planner only through its public API:
 * the compiled model, the resolved query, the target, and the lowered query ({@link Compiler.LoweredQuery}).
 */
public final class Execution {

    private Execution() {
    }

    /**
     * STREAMING execution — the {@link #planStreaming} lowering pushed all
     * the way through {@link com.legend.exec.Executor#stream}: JSON rows go
     * to {@code out} as they arrive from JDBC, O(one row) regardless of
     * result size. The dialect binds to the ACTUAL SESSION (the H5.4
     * reconciliation), unlike the plan-only surface which has no connection
     * to consult. {@code out} is flushed per row and never closed.
     */
    public static void executeStreaming(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, java.sql.Connection connection,
            java.io.Writer out) throws java.io.IOException {
        executeStreaming(model, query, runtimeFqn, com.legend.exec.Sessions.given(connection), out);
    }

    /** {@link #executeStreaming(String, String, String, java.sql.Connection, java.io.Writer)} on the session
     *  {@code sessions} opens for the target this query's runtime declares ({@link Compiler#executesOn}): the
     *  connection is decided from the compiled model, then opened (C3b). */
    public static void executeStreaming(String model, String query,
            @com.legend.base.Nullable String runtimeFqn, com.legend.exec.Sessions.Source sessions,
            java.io.Writer out) throws java.io.IOException {
        Compiler.LoweredQuery l = Compiler.query(Compiler.compileModel(model), query).lower(runtimeFqn, true);
        try (com.legend.exec.Sessions.Session session = sessions.open(Compiler.executesOn(l.ctx(), runtimeFqn), l.ctx())) {
            streamOn(l, runtimeFqn, session.connection(), out);
        }
    }

    private static void streamOn(Compiler.LoweredQuery l, @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection, java.io.Writer out) throws java.io.IOException {
        com.legend.sql.dialect.SqlDialect dialect =
                dialectOf(l.ctx(), runtimeFqn, connection);
        switch (com.legend.plan.ResultShape.of(l.root())) {
            // E5: the JSON rows are PLAN-RENDERED (WireRender) — the
            // executor writes bytes and array punctuation only
            case GRAPH -> com.legend.exec.Executor.streamGraph(
                    dialect.render(l.plan()), connection, dialect, out);
            case TABULAR -> com.legend.exec.Executor.streamWireRows(
                    dialect.render(com.legend.lowering.WireRender.rows(
                            l.plan())), connection, out);
            case SCALAR, COLLECTION -> {
                out.write(com.legend.exec.Executor.wireText(
                        dialect.render(com.legend.lowering.WireRender.wrap(
                                l.plan(), com.legend.lowering.WireRender.schema(l.root().info()),
                                com.legend.lowering.WireRender.Format.JSON)),
                        connection));
                out.flush();
            }
        }
    }

    /**
     * E5 (JAVA_EVICTION_PLAN): the PRODUCT WIRE execution — the plan
     * renders the result text ({@link com.legend.lowering.WireRender})
     * and the DATABASE produces the bytes; Java writes them through.
     * Returns the typed COLUMN NAMES (a plan fact — the response
     * envelope's columns, correct even for a zero-row result). GRAPH
     * results are already DB-built JSON and pass verbatim (JSON only).
     */
    public static java.util.List<String> executeWire(String model,
            String query, @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection,
            com.legend.lowering.WireRender.Format format, java.io.Writer out)
            throws java.io.IOException {
        return executeWire(model, query, runtimeFqn, com.legend.exec.Sessions.given(connection), format, out);
    }

    /** {@link #executeWire(String, String, String, java.sql.Connection, com.legend.lowering.WireRender.Format,
     *  java.io.Writer)} on the session {@code sessions} opens for the target the query's runtime declares (C3b). */
    public static java.util.List<String> executeWire(String model,
            String query, @com.legend.base.Nullable String runtimeFqn,
            com.legend.exec.Sessions.Source sessions,
            com.legend.lowering.WireRender.Format format, java.io.Writer out)
            throws java.io.IOException {
        Compiler.LoweredQuery l = Compiler.query(Compiler.compileModel(model), query).lower(runtimeFqn, false);
        try (com.legend.exec.Sessions.Session session = sessions.open(Compiler.executesOn(l.ctx(), runtimeFqn), l.ctx())) {
            return wireOn(l, runtimeFqn, session.connection(), format, out);
        }
    }

    private static java.util.List<String> wireOn(Compiler.LoweredQuery l, @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection, com.legend.lowering.WireRender.Format format, java.io.Writer out)
            throws java.io.IOException {
        com.legend.sql.dialect.SqlDialect dialect =
                dialectOf(l.ctx(), runtimeFqn, connection);
        com.legend.plan.ResultShape shape =
                com.legend.plan.ResultShape.of(l.root());
        if (shape == com.legend.plan.ResultShape.GRAPH) {
            if (format != com.legend.lowering.WireRender.Format.JSON) {
                throw new com.legend.error.NotImplementedException(
                        "graph results have no CSV wire");
            }
            var r = com.legend.exec.Executor.execute(dialect.render(l.plan()),
                    l.plan(), l.root().info(), shape, connection, dialect, null);
            out.write(r instanceof com.legend.exec.ExecutionResult.Graph g
                    && g.json() != null ? g.json() : "[]");
            return java.util.List.of();
        }
        com.legend.compiler.element.type.Type.RelationType schema =
                com.legend.lowering.WireRender.schema(l.root().info());
        out.write(com.legend.exec.Executor.wireText(
                dialect.render(com.legend.lowering.WireRender.wrap(
                        l.plan(), schema, format)), connection));
        return schema.columns().stream()
                .map(com.legend.compiler.element.type.Type.Column::name)
                .toList();
    }

    /**
     * Upstream {@code pure/v1/execution/execute} (E8) for an already-parsed relation
     * query: the runtime's connections are ESTABLISHED as legend-engine establishes them
     * on every acquisition (a LocalH2 connection's declared test data runs first), then the
     * database renders the rows as the JSON wire ({@code [row, ...]}) onto {@code out}.
     * Returns the plan: its SQL (the activity the engine reports) and its root type.
     */
    public static com.legend.plan.QueryPlan executeWire(String model,
            com.legend.protocol.spec.ValueSpecification query, String runtimeFqn,
            java.sql.Connection connection, java.io.Writer out) throws java.io.IOException {
        return executeWire(model, query, runtimeFqn, com.legend.exec.Sessions.given(connection), out);
    }

    /** {@link #executeWire(String, com.legend.protocol.spec.ValueSpecification, String, java.sql.Connection,
     *  java.io.Writer)} on the session {@code sessions} opens for the target the query's runtime declares (C3b). */
    public static com.legend.plan.QueryPlan executeWire(String model,
            com.legend.protocol.spec.ValueSpecification query, String runtimeFqn,
            com.legend.exec.Sessions.Source sessions, java.io.Writer out) throws java.io.IOException {
        Compiler.LoweredQuery l = Compiler.query(Compiler.compileModel(model), query).lower(runtimeFqn, false);
        try (com.legend.exec.Sessions.Session session = sessions.open(Compiler.executesOn(l.ctx(), runtimeFqn), l.ctx())) {
            return wireOn(l, runtimeFqn, session.connection(), out);
        }
    }

    private static com.legend.plan.QueryPlan wireOn(Compiler.LoweredQuery l, String runtimeFqn, java.sql.Connection connection,
            java.io.Writer out) throws java.io.IOException {
        com.legend.plan.ResultShape shape = com.legend.plan.ResultShape.of(l.root());
        if (shape == com.legend.plan.ResultShape.GRAPH) {
            // a graph fetch: the database renders the objects' JSON array, as the text
            // path above does (the Query app's G2)
            com.legend.sql.dialect.SqlDialect dialect = dialectOf(l.ctx(), runtimeFqn, connection);
            com.legend.exec.SetupRunner.run(com.legend.setup.CsvSeed.declaredSteps(runtimeFqn, l.ctx(), dialect),
                    connection, dialect, null);
            String sql = dialect.render(l.plan());
            var r = com.legend.exec.Executor.execute(sql, l.plan(), l.root().info(), shape, connection, dialect, null);
            out.write(r instanceof com.legend.exec.ExecutionResult.Graph g && g.json() != null ? g.json() : "[]");
            return new com.legend.plan.QueryPlan(sql, l.root().info(), shape);
        }
        if (shape != com.legend.plan.ResultShape.TABULAR) {
            throw new com.legend.error.NotImplementedException(
                    "execute: a " + shape + " result's serialization is unprobed");
        }
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(l.ctx(), runtimeFqn, connection);
        com.legend.exec.SetupRunner.run(com.legend.setup.CsvSeed.declaredSteps(runtimeFqn, l.ctx(), dialect),
                connection, dialect, null);
        out.write(com.legend.exec.Executor.wireText(dialect.render(com.legend.lowering.WireRender.wrap(
                l.plan(), com.legend.lowering.WireRender.schema(l.root().info()),
                com.legend.lowering.WireRender.Format.JSON)), connection));
        return new com.legend.plan.QueryPlan(dialect.render(l.plan()), l.root().info(), shape);
    }

    /**
     * THE dialect of a query that executes on {@code connection}: the database its runtime executes on
     * ({@link Compiler#executesOn}, upstream's {@code createDbConfig(connection.type)}), refined by the server's
     * version ({@code SqlDialect.forServer}), with its session setup run once. The session is only
     * CHECKED ({@code Sessions.check}): connected to another database than the one declared is refused,
     * never reinterpreted.
     */
    static com.legend.sql.dialect.SqlDialect dialectOf(ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection) {
        com.legend.model.ConnectionDefinition.DatabaseType declared = Compiler.executesOn(ctx, runtimeFqn).type();
        com.legend.exec.Sessions.check(connection, declared);
        com.legend.sql.dialect.SqlDialect dialect = com.legend.database.Databases.dialect(declared)
                .forServer(com.legend.exec.Sessions.version(connection));
        // B6: session setup rides the connection-dialect resolution -- the ONE seam every
        // connection-bearing entry passes through; the dialect states the FACTS, the exec funnel executes
        for (String s : dialect.sessionSetup()) {
            try (var __o = com.legend.exec.StatementOrigin.enter(com.legend.exec.StatementOrigin.SESSION)) {
                com.legend.exec.Executor.executeRaw(connection, s);
            }
        }
        return dialect;
    }

    /**
     * The core QUERY SERVICE: frontend + Phase G + lowering + rendering +
     * EXECUTION over the caller's connection, shaped per the result-type
     * classification ({@link com.legend.plan.ResultShape}). The corpus
     * bridge's target (PHASE_K_EXECUTION.md). Class queries need an
     * execution context in the query itself ({@code ->from(...)}) on this
     * overload; the 4-arg overload supplies a driver runtime.
     */
    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult execute(
            String model, String query,
            java.sql.Connection connection) {
        return execute(model, query, null, connection);
    }

    /**
     * The full pipeline with a DRIVER-SUPPLIED execution context — the
     * service shape: queries carry no {@code ->from(...)}; the runtime
     * arrives as an API argument (PHASE_K_EXECUTION.md §4). Phase H
     * resolves class queries against the runtime's mapping between G and
     * I; an explicit {@code from()} in the query always wins.
     */
    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult execute(
            String model, String query,
            @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection) {
        return execute(model, query, null, runtimeFqn, connection);
    }

    /**
     * {@link #execute(String, String, String, java.sql.Connection)} with a
     * SECTION import scope: the query resolves under {@code imports} (plus
     * the prelude) against the model's element universe — real pure's rule
     * for a query written in an import-bearing section. A {@code null}
     * scope is the sectionless-query behavior.
     */
    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult execute(
            String model, String query,
            com.legend.model.@com.legend.base.Nullable ImportScope imports,
            @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection) {
        return execute(model, query, imports, runtimeFqn, connection, ExecuteOptions.NONE);
    }

    /** With the caller's execute OPTIONS (the PCT adapter's wire render). */
    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult execute(
            String model, String query,
            com.legend.model.@com.legend.base.Nullable ImportScope imports,
            @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection, ExecuteOptions options) {
        ModelContext ctx = Compiler.compileModel(model);
        // the ONE front door (resolveQuery: names, the statement splice, the
        // desugars) — a text query is its statements under its section scope
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(query);
        java.util.List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf
                        && lf.parameters().isEmpty() ? lf.body() : java.util.List.of(parsed);
        return executeResolved(
                Compiler.resolveQuery(statements,
                        imports == null ? new com.legend.model.ImportScope(java.util.List.of())
                                : imports, ctx),
                ctx, runtimeFqn, connection, null, null, options);
    }

    /** {@link #execute(String, String, com.legend.model.ImportScope, String, java.sql.Connection, ExecuteOptions)}
     *  on the session {@code sessions} opens for the target the query's runtime declares ({@link Compiler#executesOn}):
     *  the model compiled once, the connection decided from it, then opened (C3b). */
    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult execute(
            String model, String query, @com.legend.base.Nullable String runtimeFqn,
            com.legend.exec.Sessions.Source sessions, ExecuteOptions options) {
        ModelContext ctx = Compiler.compileModel(model);
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(query);
        java.util.List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf
                        && lf.parameters().isEmpty() ? lf.body() : java.util.List.of(parsed);
        var resolved = Compiler.resolveQuery(statements, new com.legend.model.ImportScope(java.util.List.of()), ctx);
        try (com.legend.exec.Sessions.Session session = sessions.open(Compiler.executesOn(ctx, runtimeFqn), ctx)) {
            return executeResolved(resolved, ctx, runtimeFqn, session.connection(), null, null, options);
        }
    }

    /**
     * Phases G&frac12;&rarr;K for an already NAME-RESOLVED query AST — THE
     * one back-half sequence. Every driver path (text queries above,
     * EngineTestExecutor's handle-splice path) comes through here; a second
     * hand-rolled sequence is an orchestrator bug (audit 15 unified two).
     */
    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult executeResolved(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection) {
        return StatementExecutor.execute(resolved, ctx,
                runtimeFqn, dialectOf(ctx, runtimeFqn, connection), connection);
    }

    /** Listener overload — the runner's scoring seam: observes each
     * statement-root assert verdict; the platform keeps the judgment. */
    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult executeResolved(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection,
            com.legend.exec.@com.legend.base.Nullable AssertListener assertListener) {
        return executeResolved(resolved, ctx, runtimeFqn, connection,
                assertListener, null);
    }

    /** Registration overload (SQLTEXT charter §2): the harness supplies
     * its {@link com.legend.exec.SqlReplayOracle} beside the listener;
     * the env carries both. Production never calls this arity. */
    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult executeResolved(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection,
            com.legend.exec.@com.legend.base.Nullable AssertListener assertListener,
            com.legend.exec.@com.legend.base.Nullable SqlReplayOracle replayOracle) {
        return executeResolved(resolved, ctx, runtimeFqn, connection, assertListener,
                replayOracle, ExecuteOptions.NONE);
    }

    public static com.legend.exec.@com.legend.base.Nullable ExecutionResult executeResolved(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn,
            java.sql.Connection connection,
            com.legend.exec.@com.legend.base.Nullable AssertListener assertListener,
            com.legend.exec.@com.legend.base.Nullable SqlReplayOracle replayOracle,
            ExecuteOptions options) {
        return StatementExecutor.execute(resolved, ctx,
                runtimeFqn, dialectOf(ctx, runtimeFqn, connection), connection,
                assertListener, replayOracle, options);
    }
}
