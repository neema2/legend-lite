package com.legend;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.normalizer.ModelNormalizer;
import com.legend.parser.SpecParser;
import com.legend.normalizer.NormalizedModel;
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
 * <p><strong>Status.</strong> The FRONT half is real: {@link #compileModel}
 * runs lex→F and {@link #compileQuery} carries a query through Phase G to its
 * typed HIR ({@link TypedSpec}). The BACK half (H–K, lowering to a SQL plan)
 * is not built; {@link #compile(String, String, String)} says so loudly.
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
        ParsedModel resolved = NameResolver.resolve(parsed);
        NormalizedModel normalized = ModelNormalizer.normalize(resolved);
        return PureModelContext.from(normalized);
    }

    /**
     * Compile a Pure model + query against a runtime to a SQL execution plan.
     *
     * @param model      Pure model source (classes, mappings, stores, runtimes, ...).
     * @param query      Pure query expression (a {@code ValueSpecification} in legacy terms).
     * @param runtime    FQN of the runtime to compile against.
     * @return SQL execution plan in the runtime's dialect.
     */
    public static String compile(String model, String query, String runtime) {
        throw new UnsupportedOperationException(
            "lowering (phases H-J, typed HIR -> SQL plan) is not built yet;"
                + " compileQuery(model, query) yields the typed HIR the lowering will consume");
    }

    /**
     * Frontend + Phase G for a standalone query: Pure model source + query
     * expression &rarr; the query's typed HIR. This is the pipeline's current
     * end-to-end &mdash; everything up to (not including) lowering.
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
    /**
     * The core QUERY SERVICE: frontend + Phase G + lowering + rendering +
     * EXECUTION over the caller's connection, shaped per the result-type
     * classification ({@link com.legend.exec.ResultShape}). The corpus
     * bridge's target (PHASE_K_EXECUTION.md).
     */
    public static com.legend.exec.ExecutionResult execute(String model, String query,
            java.sql.Connection connection) throws java.sql.SQLException {
        ModelContext ctx = compileModel(model);
        java.util.List<TypedSpec> body = new SpecCompiler(ctx).typeQueryBody(
                NameResolver.resolveQuery(SpecParser.parse(query)));
        TypedSpec root = body.get(body.size() - 1);
        com.legend.sql.SqlQuery plan = new com.legend.lowering.Lowerer().lower(body);
        com.legend.sql.dialect.SqlDialect dialect = new com.legend.sql.dialect.DuckDb();
        return com.legend.exec.Executor.execute(
                dialect.render(plan), plan, root.info(), connection, dialect);
    }

    public static TypedSpec compileQuery(String model, String query) {
        Objects.requireNonNull(query, "query");
        ModelContext ctx = compileModel(model);
        return new SpecCompiler(ctx).typeExpression(
                NameResolver.resolveQuery(SpecParser.parse(query)));
    }
}
