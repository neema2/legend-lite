package com.legend;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.normalizer.ModelNormalizer;
import com.legend.parser.SpecParser;
import com.legend.parser.NormalizedModel;
import com.legend.parser.ElementParser;
import com.legend.parser.ParsedModel;

import java.util.Objects;

/**
 * Top-level entry point for the Legend Lite compiler pipeline.
 *
 * <p>Drives the steps listed in {@code package-info.java}:
 * lex → parse-element → parse-spec → resolve-names → normalize (E) →
 * compile-element (F) → compile-spec (G) → resolve-mapping (H) →
 * build-sql (I) → render-sql (J) → execute (K).
 *
 * <p><strong>Status.</strong> {@link #compileModel} runs lex→F;
 * {@link #compileQuery} carries a query through Phase G to its typed HIR;
 * {@link #execute} is the FULL pipeline — lower (I), render (J), execute (K)
 * over the caller's connection with a typed result. Phase H (class sources /
 * mappings) is the remaining gap; {@link #compile(String, String, String)}
 * throws until a runtime-resolved plan-string form is worth having.
 *
 * <p>This is the single orchestration point: it owns step <em>ordering</em>
 * only. Each step is the same method its own unit tests exercise &mdash; there
 * is no orchestrator-only code path.
 */
public final class Compiler {

    private Compiler() {}

    /**
     * Frontend pipeline: Pure model source &rarr; typed {@link ModelContext}.
     *
     * <p>Drives steps 1&ndash;6 (the steps implemented in {@code core/} today;
     * the query/spec and backend steps land later):
     * <ol>
     *   <li><b>parse</b> &mdash; {@link ElementParser#parse} (lex + parse-element).</li>
     *   <li><b>resolve-names</b> &mdash; {@link NameResolver#resolve(ParsedModel)}
     *       rewrites simple names to FQNs against the user imports + platform
     *       prelude (the prelude is owned by the resolver, not this driver).</li>
     *   <li><b>normalize</b> (Phase E) &mdash; {@link ModelNormalizer#normalize}
     *       externalizes body sites into synthesized functions.</li>
     *   <li><b>element-compile</b> (Phase F) &mdash; {@link PureModelContext#from}
     *       builds the typed model; synth functions flatten into
     *       {@code findFunction} uniformly with user functions.</li>
     * </ol>
     *
     * <p>This is the single orchestration point: it owns step <em>ordering</em>.
     * Each step is the same method its own unit tests exercise &mdash; there is
     * no orchestrator-only code path.
     *
     * @param model Pure model source (classes, enums, associations, databases,
     *              mappings, services, runtimes, ...).
     * @return the populated, queryable {@link ModelContext}.
     */
    public static ModelContext compileModel(String model) {
        Objects.requireNonNull(model, "model");
        ParsedModel parsed = ElementParser.parse(model);
        try {
            ParsedModel resolved = NameResolver.resolve(parsed);
            NormalizedModel normalized = ModelNormalizer.normalize(resolved);
            return PureModelContext.from(normalized);
        } catch (com.legend.error.ModelException e) {
            // Decorate with the offending ELEMENT's [line:col] — the offsets
            // live on the original parse (resolution rebuilds ParsedModel
            // without them), so the driver is where source meets failure.
            Integer off = e.element() == null ? null
                    : parsed.elementOffsets().get(e.element());
            if (off == null || parsed.source() == null) {
                throw e;
            }
            throw new com.legend.error.ModelException(e.phase(),
                    com.legend.error.LegendCompileException.position(parsed.source(), off)
                            + " " + e.getMessage(), e.element());
        }
    }

    /**
     * Compile a Pure model + query against a runtime to a SQL execution plan.
     * The plan half of {@link #execute(String, String, String, java.sql.Connection)}:
     * the same pipeline (frontend &rarr; G &rarr; H resolve against the
     * driver-supplied runtime &rarr; lower &rarr; render) WITHOUT executing
     * &mdash; the {@code planSql} seam for SQL-shape assertions and plan
     * inspection.
     *
     * @param model      Pure model source (classes, mappings, stores, runtimes, ...).
     * @param query      Pure query expression (a {@code ValueSpecification} in legacy terms).
     * @param runtime    FQN of the runtime to compile against.
     * @return rendered SQL in the runtime's dialect.
     */
    public static String compile(String model, String query, String runtime) {
        return plan(model, query, runtime).sql();
    }

    /**
     * {@link #compile} with the full plan contract: rendered SQL plus the
     * root's {@link com.legend.compiler.element.type.ExprType} and
     * {@link com.legend.exec.ResultShape} &mdash; exactly what
     * {@link com.legend.exec.Executor} would consume, minus execution.
     * Bridges re-wrap these fields verbatim (no invented metadata).
     */
    public static com.legend.exec.QueryPlan plan(String model, String query, String runtime) {
        ModelContext ctx = compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        java.util.List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(SpecParser.parse(query)));
        body = new com.legend.resolver.StoreResolver(ctx, specs)
                .resolve(body, runtime);                          // Phase H
        TypedSpec root = body.get(body.size() - 1);
        String sql = dialectOf(ctx, runtime)
                .render(new com.legend.lowering.Lowerer().lower(body));
        return new com.legend.exec.QueryPlan(sql, root.info(),
                com.legend.exec.ResultShape.of(root.info()));
    }

    /**
     * The runtime's SQL dialect: its connections' declared
     * {@code DatabaseType} selects the renderer; an undeclared type is LOUD.
     * A runtime with no relational connection binding (or no runtime at all
     * &mdash; the caller-supplied-connection path) defaults to DuckDB, the
     * reference dialect.
     */
    private static com.legend.sql.dialect.SqlDialect dialectOf(ModelContext ctx, String runtimeFqn) {
        if (runtimeFqn != null) {
            var rt = ctx.findRuntime(runtimeFqn);
            if (rt.isPresent()) {
                for (String connFqn : rt.get().connectionBindings().values()) {
                    var conn = ctx.findConnection(connFqn);
                    if (conn.isEmpty()) {
                        continue;
                    }
                    switch (conn.get().databaseType()) {
                        case DuckDB -> {
                            return new com.legend.sql.dialect.DuckDb();
                        }
                        case SQLite -> {
                            return new com.legend.sql.dialect.Sqlite();
                        }
                        default -> throw new com.legend.error.NotImplementedException(
                                "SQL dialect for database type '" + conn.get().databaseType()
                                        + "' (connection '" + connFqn + "' of runtime '"
                                        + runtimeFqn + "') is not implemented yet");
                    }
                }
            }
        }
        return new com.legend.sql.dialect.DuckDb();
    }

    /**
     * The core QUERY SERVICE: frontend + Phase G + lowering + rendering +
     * EXECUTION over the caller's connection, shaped per the result-type
     * classification ({@link com.legend.exec.ResultShape}). The corpus
     * bridge's target (PHASE_K_EXECUTION.md). Class queries need an
     * execution context in the query itself ({@code ->from(...)}) on this
     * overload; the 4-arg overload supplies a driver runtime.
     */
    public static com.legend.exec.ExecutionResult execute(String model, String query,
            java.sql.Connection connection) throws java.sql.SQLException {
        return execute(model, query, null, connection);
    }

    /**
     * The full pipeline with a DRIVER-SUPPLIED execution context — the
     * service shape: queries carry no {@code ->from(...)}; the runtime
     * arrives as an API argument (PHASE_K_EXECUTION.md §4). Phase H
     * resolves class queries against the runtime's mapping between G and
     * I; an explicit {@code from()} in the query always wins.
     */
    public static com.legend.exec.ExecutionResult execute(String model, String query,
            String runtimeFqn, java.sql.Connection connection) throws java.sql.SQLException {
        ModelContext ctx = compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        java.util.List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(SpecParser.parse(query)));
        body = new com.legend.resolver.StoreResolver(ctx, specs)
                .resolve(body, runtimeFqn);                       // Phase H
        TypedSpec root = body.get(body.size() - 1);
        com.legend.sql.SqlQuery plan = new com.legend.lowering.Lowerer().lower(body);
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(ctx, runtimeFqn);
        return com.legend.exec.Executor.execute(
                dialect.render(plan), plan, root.info(), connection, dialect);
    }

    /**
     * Frontend + Phase G for a standalone query: Pure model source + query
     * expression &rarr; the query's typed HIR (the FRONT half only; use
     * {@link #execute} for the full pipeline).
     *
     * <p>The query is parsed by {@link SpecParser}, name-resolved under real
     * legend-engine's <em>sectionless-lambda</em> scope
     * ({@link NameResolver#resolveQuery}: the platform prelude is always in
     * scope &mdash; {@code JoinKind.INNER} works bare &mdash; while user
     * elements require full paths, e.g. {@code test::Person.all()}), then
     * type-checked against the compiled model snapshot.
     *
     * @param model Pure model source.
     * @param query Pure query expression (user elements fully qualified).
     * @return the type-checked query (schema/type on {@link TypedSpec#info()}).
     */
    public static TypedSpec compileQuery(String model, String query) {
        Objects.requireNonNull(query, "query");
        ModelContext ctx = compileModel(model);
        return new SpecCompiler(ctx).typeExpression(
                NameResolver.resolveQuery(SpecParser.parse(query)));
    }
}
