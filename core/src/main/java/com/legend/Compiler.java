package com.legend;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.normalizer.ModelNormalizer;
import com.legend.parser.SpecParser;
import com.legend.model.NormalizedModel;
import com.legend.parser.ElementParser;
import com.legend.model.ParsedModel;

import java.util.List;
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
            return buildModel(parsed);
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

    /** One named source unit of a multi-file model (a MODULE member). */
    public record ModelSource(String name, String text) {
        public ModelSource {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(text, "text");
        }
    }

    /**
     * A parsed multi-source MODULE: the merged {@link ParsedModel} plus the
     * duplicate elements that were dropped (first definition wins; each
     * loser is reported as {@code kind fqn (source, kept source)} so the
     * caller can wall it) and the per-unit texts for error decoration.
     */
    public record ParsedModule(ParsedModel model, List<String> duplicateElements,
                               java.util.Map<String, String> sourceTexts) {
        public ParsedModule {
            duplicateElements = List.copyOf(duplicateElements);
            sourceTexts = java.util.Map.copyOf(sourceTexts);
        }
    }

    /**
     * Parse each source as its OWN unit — per-file import sections, per-file
     * positions — and merge into one model: the MODULE compile every real
     * legend project needs (the engine compiles a repository's files
     * together; cross-file references are normal). Imports never leak
     * across units: each element resolves against its own section's scope,
     * and the merged model's GLOBAL scope is empty (per-element scopes are
     * total, so the fallback never widens).
     */
    public static ParsedModule parseSources(List<ModelSource> sources) {
        Objects.requireNonNull(sources, "sources");
        List<com.legend.model.PackageableElement> elements = new java.util.ArrayList<>();
        java.util.Map<String, Integer> offsets = new java.util.HashMap<>();
        java.util.Map<String, com.legend.model.ImportScope> elementImports =
                new java.util.HashMap<>();
        java.util.Map<String, String> elementSources = new java.util.HashMap<>();
        java.util.Map<String, String> sourceTexts = new java.util.LinkedHashMap<>();
        java.util.Map<String, String> seen = new java.util.HashMap<>();   // key -> source
        List<String> duplicates = new java.util.ArrayList<>();
        for (ModelSource src : sources) {
            sourceTexts.put(src.name(), src.text());
            ParsedModel unit = ElementParser.parse(src.text());
            for (com.legend.model.PackageableElement el : unit.elements()) {
                // FUNCTIONS overload: same FQN with different signatures is
                // NOT a duplicate — the dedup key carries the parameter
                // shape (dropping overloads silently lost the corpus's own
                // executeInDb wrappers)
                String key = el instanceof com.legend.model.FunctionDefinition fd
                        ? "Function::" + fd.qualifiedName() + "(" + fd.parameters()
                                .stream().map(pd -> String.valueOf(pd.type())
                                        + String.valueOf(pd.multiplicity()))
                                .reduce("", (x, y) -> x + "," + y) + ")"
                        : el.getClass().getSimpleName() + "::" + el.qualifiedName();
                String prior = seen.putIfAbsent(key, src.name());
                if (prior != null) {
                    // FIRST definition wins (the corpus carries alternative
                    // models in parent directories); the drop is REPORTED,
                    // never silent
                    duplicates.add(key + " (" + src.name()
                            + ", kept " + prior + ")");
                    continue;
                }
                elements.add(el);
                String fqn = el.qualifiedName();
                Integer off = unit.elementOffsets().get(fqn);
                if (off != null) {
                    offsets.put(fqn, off);
                }
                com.legend.model.ImportScope own = unit.elementImports().get(fqn);
                if (own != null) {
                    elementImports.put(fqn, own);
                }
                elementSources.put(fqn, src.name());
            }
        }
        return new ParsedModule(
                new ParsedModel(elements, com.legend.model.ImportScope.empty(),
                        null, offsets, elementImports, elementSources),
                duplicates, sourceTexts);
    }

    /**
     * The back half of {@link #compileModel(String)} over an
     * already-parsed model: resolve names, normalize, build the context.
     * Multi-source callers decorate errors themselves (they hold the
     * per-unit texts).
     */
    public static ModelContext buildModel(ParsedModel parsed) {
        ParsedModel resolved = NameResolver.resolve(parsed);
        NormalizedModel normalized = ModelNormalizer.normalize(resolved);
        return PureModelContext.from(normalized);
    }

    /** A module built TOLERANTLY: the context over every element that
     * compiles, plus the walls (element FQN => first error line) for every
     * element that does not — the engine-parity behavior for compiling a
     * repository (compile what compiles, report the rest). */
    public record BuiltModule(ModelContext context,
                              java.util.Map<String, String> walls) {
        public BuiltModule {
            walls = java.util.Collections.unmodifiableMap(
                    new java.util.LinkedHashMap<>(walls));
        }
    }

    /**
     * Tolerant module build — POISON, DON'T DROP: every element stays in
     * the model; the walls map records each broken element's FIRST failure
     * reason (eager DIAGNOSIS over the whole module). A broken element
     * harms nothing that merely references it — the failure fires at USE
     * time (compiling the function on call, materializing the binding),
     * loudly, when something actually enters the quarantine. Dropping
     * instead cascaded: removing a walled helper failed every element
     * referencing it, and every test touching THOSE — 182 corpus tests
     * died in the blast radius of functions they never called.
     * One exception: a mapping that fails to NORMALIZE has no canonical
     * form to keep and is excluded (its absence is walled; the legacy
     * per-family harness behaved identically). Unattributed failures
     * still throw — a genuine bug must fail the build.
     */
    public static BuiltModule buildModule(ParsedModel parsed) {
        java.util.Map<String, String> walls = new java.util.LinkedHashMap<>();
        ParsedModel resolved = NameResolver.resolve(parsed, walls);
        NormalizedModel normalized = ModelNormalizer.normalize(resolved, walls);
        PureModelContext ctx = PureModelContext.from(normalized, walls);
        return new BuiltModule(ctx, walls);
    }

    /**
     * Compile a multi-source MODULE. Errors carry the offending element's
     * SOURCE NAME and [line:col] within that source.
     */
    public static ModelContext compileModel(List<ModelSource> sources) {
        ParsedModule module = parseSources(sources);
        try {
            return buildModel(module.model());
        } catch (com.legend.error.ModelException e) {
            String fqn = e.element();
            String srcName = fqn == null ? null
                    : module.model().elementSources().get(fqn);
            Integer off = fqn == null ? null
                    : module.model().elementOffsets().get(fqn);
            if (srcName == null || off == null) {
                throw e;
            }
            throw new com.legend.error.ModelException(e.phase(),
                    srcName + " " + com.legend.error.LegendCompileException
                            .position(module.sourceTexts().get(srcName), off)
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
        body = new com.legend.compiler.spec.UserCallInliner(specs).inlineBody(body);   // Phase G½
        body = new com.legend.resolver.StoreResolver(ctx, specs)
                .resolve(body, runtime);                          // Phase H
        TypedSpec root = body.get(body.size() - 1);
        String sql = dialectOf(ctx, runtime)
                .render(new com.legend.lowering.Lowerer(
                        t -> com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t),
                        f -> ctx.findClass(f).isPresent()).lower(body));
        return new com.legend.exec.QueryPlan(sql, root.info(),
                com.legend.exec.ResultShape.of(root));
    }

    /**
     * The runtime's SQL dialect: its connections' declared
     * {@code DatabaseType} selects the renderer; an undeclared type is LOUD.
     * A runtime with no relational connection binding (or no runtime at all
     * &mdash; the caller-supplied-connection path) defaults to DuckDB, the
     * reference dialect.
     */
    static com.legend.sql.dialect.SqlDialect dialectOf(ModelContext ctx, String runtimeFqn) {
        if (runtimeFqn == null) {
            return new com.legend.sql.dialect.DuckDb();
        }
        var rt = ctx.findRuntime(runtimeFqn);
        if (rt.isEmpty()) {
            return new com.legend.sql.dialect.DuckDb();
        }
        // EVERY binding is inspected, in sorted (deterministic) order —
        // connection bindings are an unordered map, and first-match-wins
        // was nondeterministic AND skipped later unsupported types (audit).
        var types = new java.util.TreeMap<String,
                com.legend.model.ConnectionDefinition.DatabaseType>();
        for (String connFqn : new java.util.TreeSet<>(
                rt.get().connectionBindings().values())) {
            var conn = ctx.findConnection(connFqn);
            if (conn.isEmpty()) {
                throw new com.legend.error.MappingResolutionException(
                        "connection '" + connFqn + "' of runtime '" + runtimeFqn
                                + "' is not defined", runtimeFqn);
            }
            types.put(connFqn, conn.get().databaseType());
        }
        var distinct = new java.util.TreeSet<String>();
        for (var e : types.entrySet()) {
            switch (e.getValue()) {
                case DuckDB, SQLite -> distinct.add(e.getValue().name());
                // H2 rides the ANSI-flavored DuckDB renderer: the corpus
                // executes H2-typed connections on the session's DuckDB, and
                // every emission H2 sees is the ANSI subset.
                case H2 -> distinct.add("DuckDB");
                default -> throw new com.legend.error.NotImplementedException(
                        "SQL dialect for database type '" + e.getValue()
                                + "' (connection '" + e.getKey() + "' of runtime '"
                                + runtimeFqn + "') is not implemented yet");
            }
        }
        if (distinct.size() > 1) {
            throw new com.legend.error.NotImplementedException(
                    "runtime '" + runtimeFqn + "' mixes database types "
                            + distinct + " — one dialect per query is supported");
        }
        return distinct.contains("SQLite")
                ? new com.legend.sql.dialect.Sqlite()
                : new com.legend.sql.dialect.DuckDb();
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
        return execute(model, query, null, runtimeFqn, connection);
    }

    /**
     * {@link #execute(String, String, String, java.sql.Connection)} with a
     * SECTION import scope: the query resolves under {@code imports} (plus
     * the prelude) against the model's element universe — real pure's rule
     * for a query written in an import-bearing section. A {@code null}
     * scope is the sectionless-query behavior.
     */
    public static com.legend.exec.ExecutionResult execute(String model, String query,
            com.legend.model.ImportScope imports, String runtimeFqn,
            java.sql.Connection connection) throws java.sql.SQLException {
        ModelContext ctx = compileModel(model);
        return executeResolved(
                imports == null
                        ? NameResolver.resolveQuery(SpecParser.parse(query))
                        : NameResolver.resolveQuery(SpecParser.parse(query),
                                imports, ctx.elementFqns()),
                ctx, runtimeFqn, connection);
    }

    /**
     * Phases G&frac12;&rarr;K for an already NAME-RESOLVED query AST — THE
     * one back-half sequence. Every driver path (text queries above,
     * TestBody's handle-splice path) comes through here; a second
     * hand-rolled sequence is an orchestrator bug (audit 15 unified two).
     */
    public static com.legend.exec.ExecutionResult executeResolved(
            com.legend.model.spec.ValueSpecification resolved, ModelContext ctx,
            String runtimeFqn, java.sql.Connection connection)
            throws java.sql.SQLException {
        return executeResolved(resolved, ctx, runtimeFqn, connection, null);
    }

    /**
     * {@code rawSqlFailureSink}: OPTIONAL per-statement tolerance at the
     * {@code executeInDb} boundary — a failed raw statement is reported
     * to the sink and the setup CONTINUES (the engine's own harness
     * semantics: one dialect-incompatible INSERT must not abort the whole
     * seed; the caller's ledger feeds its emptiness guard). Null = throw.
     */
    public static com.legend.exec.ExecutionResult executeResolved(
            com.legend.model.spec.ValueSpecification resolved, ModelContext ctx,
            String runtimeFqn, java.sql.Connection connection,
            java.util.function.Consumer<String> rawSqlFailureSink)
            throws java.sql.SQLException {
        SpecCompiler specs = new SpecCompiler(ctx);
        ExecEnv env = new ExecEnv(ctx, runtimeFqn, connection, rawSqlFailureSink);
        return executeStatements(specs.typeQueryBody(resolved),
                new java.util.ArrayList<>(), specs, env,
                new java.util.ArrayDeque<>());
    }

    /** The K-phase execution environment: ONE ambient connection, the
     * driver runtime, and the optional raw-SQL failure sink. */
    private record ExecEnv(ModelContext ctx, String runtimeFqn,
            java.sql.Connection connection,
            java.util.function.Consumer<String> rawSqlFailureSink) {
    }

    /**
     * STATEMENT SEQUENCING — the K-phase orchestration layer. Pure bodies
     * (lets + one result expression) take exactly the classic path:
     * inline (G&frac12;) &rarr; resolve (H) &rarr; lower/execute, one
     * statement. EFFECTFUL bodies — corpus setup functions: a sequence of
     * {@code executeInDb} statements — cannot &beta;-reduce to one
     * expression; each statement executes in order through the full
     * pipeline, and a statement-position call to an effectful function
     * expands as a statement sequence in a FRESH call frame (parameters
     * bound as lets; closed bodies make frames capture-proof — no
     * &alpha;-renaming needed). Value evaluation still ALWAYS lowers to
     * SQL; only the sequencing lives host-side.
     */
    private static com.legend.exec.ExecutionResult executeStatements(
            java.util.List<TypedSpec> stmts, java.util.List<TypedSpec> letPrefix,
            SpecCompiler specs, ExecEnv env, java.util.Deque<String> frames)
            throws java.sql.SQLException {
        com.legend.exec.ExecutionResult result = null;
        java.util.Map<String, Boolean> effectMemo = new java.util.HashMap<>();
        for (int i = 0; i < stmts.size(); i++) {
            TypedSpec stmt = stmts.get(i);
            boolean last = i == stmts.size() - 1;
            if (stmt instanceof com.legend.compiler.spec.typed.TypedLet let && !last) {
                if (containsEffect(let.value(), specs, effectMemo)) {
                    // β-substitution would DROP the effect if the binding is
                    // unused (or double it if used twice) — refuse loudly
                    throw new IllegalStateException("effectful let binding ('"
                            + let.name() + "' reaches executeInDb) is not supported");
                }
                letPrefix.add(let);
                continue;
            }
            // a trailing let IS its value (real pure)
            TypedSpec bare = stmt instanceof com.legend.compiler.spec.typed.TypedLet l
                    ? l.value() : stmt;
            if (bare instanceof com.legend.compiler.spec.typed.TypedUserCall call
                    && containsEffect(call, specs, effectMemo)) {
                result = executeCallStatement(call, letPrefix, specs, env, frames);
                continue;
            }
            java.util.List<TypedSpec> single = new java.util.ArrayList<>(letPrefix);
            single.add(stmt);
            java.util.List<TypedSpec> body =
                    new com.legend.compiler.spec.UserCallInliner(specs)
                            .inlineBody(single);                          // Phase G½
            body = new com.legend.resolver.StoreResolver(env.ctx(), specs)
                    .resolve(body, env.runtimeFqn());                     // Phase H
            result = executeTyped(body, env);
        }
        return result;
    }

    /**
     * A statement-position call to an EFFECTFUL function: bind the caller's
     * arguments as parameter lets (caller lets substituted in — the callee
     * body is otherwise closed) and run the body as a statement sequence.
     */
    private static com.legend.exec.ExecutionResult executeCallStatement(
            com.legend.compiler.spec.typed.TypedUserCall call,
            java.util.List<TypedSpec> letPrefix, SpecCompiler specs, ExecEnv env,
            java.util.Deque<String> frames) throws java.sql.SQLException {
        String key = call.callee().signatureKey();
        if (frames.contains(key)) {
            throw new IllegalStateException("recursive effectful call: "
                    + call.callee().qualifiedName());
        }
        frames.push(key);
        try {
            java.util.List<TypedSpec> frame = new java.util.ArrayList<>();
            for (int p = 0; p < call.callee().parameters().size(); p++) {
                java.util.List<TypedSpec> argBody = new java.util.ArrayList<>(letPrefix);
                argBody.add(call.args().get(p));
                TypedSpec argValue = new com.legend.compiler.spec.UserCallInliner(specs)
                        .inlineBody(argBody).get(0);
                frame.add(new com.legend.compiler.spec.typed.TypedLet(
                        call.callee().parameters().get(p).name(), argValue,
                        argValue.info()));
            }
            return executeStatements(specs.compile(call.callee()).body(), frame,
                    specs, env, frames);
        } finally {
            frames.pop();
        }
    }

    /**
     * Does this expression (transitively, through user calls) reach the
     * {@code executeInDb} K-native? Memoized per callee signature; a cycle
     * scores the in-progress callee non-effectful — real recursion is
     * caught loudly at execution time.
     */
    private static boolean containsEffect(TypedSpec node, SpecCompiler specs,
            java.util.Map<String, Boolean> memo) {
        if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && com.legend.compiler.element.type.PlatformTypes
                        .isKNative(nc.callee().qualifiedName())) {
            return true;
        }
        if (node instanceof com.legend.compiler.spec.typed.TypedUserCall uc) {
            String key = uc.callee().signatureKey();
            Boolean known = memo.get(key);
            if (known == null) {
                memo.put(key, false);   // in-progress: cycles score false
                boolean effectful = false;
                for (TypedSpec stmt : specs.compile(uc.callee()).body()) {
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
    private static com.legend.exec.ExecutionResult executeTyped(
            java.util.List<TypedSpec> body, ExecEnv env)
            throws java.sql.SQLException {
        ModelContext ctx = env.ctx();
        String runtimeFqn = env.runtimeFqn();
        java.sql.Connection connection = env.connection();
        TypedSpec root = body.get(body.size() - 1);
        // from() is context-only: shape AND root type come from the same
        // looked-through node — a resolved source may be relation-shaped
        // (scalar map lowers to a one-column project) while the from
        // wrapper still carries the pre-resolution scalar info.
        while (root instanceof com.legend.compiler.spec.typed.TypedFrom fr) {
            root = fr.source();
        }
        // K-NATIVE dispatch: executeInDb never lowers — it IS the phase-K
        // boundary (raw SQL over the ambient JDBC connection).
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && (com.legend.compiler.element.type.PlatformTypes.EXECUTE_IN_DB
                        .equals(nc.callee().qualifiedName())
                        || com.legend.compiler.element.type.PlatformTypes.EXECUTE_IN_DB_DEBUG
                                .equals(nc.callee().qualifiedName()))) {
            return executeInDb(body, nc, env);
        }
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall dc
                && com.legend.compiler.element.type.PlatformTypes.DROP_AND_CREATE_TABLE_IN_DB
                        .equals(dc.callee().qualifiedName())) {
            return dropAndCreateTableInDb(body, dc, env);
        }
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall pn
                && (com.legend.compiler.element.type.PlatformTypes.PRINT
                        .equals(pn.callee().qualifiedName())
                        || com.legend.compiler.element.type.PlatformTypes.PRINTLN
                                .equals(pn.callee().qualifiedName()))) {
            // debug output: a NO-OP — the argument is NEVER evaluated (it
            // may introspect a ResultSet, which never materializes host-
            // side). Divergence from the engine (which prints) is deliberate
            // harness behavior; an effect nested inside print is dropped.
            return new com.legend.exec.ExecutionResult.Scalar(null, pn.info().type());
        }
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall sc
                && com.legend.compiler.element.type.PlatformTypes.DROP_AND_CREATE_SCHEMA_IN_DB
                        .equals(sc.callee().qualifiedName())) {
            // the engine DROPS + creates; here create-if-missing — the DDL
            // seeds already own tables in the schema, and the setup's own
            // dropAndCreateTableInDb calls recreate what it manages
            com.legend.exec.Executor.executeRaw(connection,
                    "Create schema if not exists "
                            + evalStringArg(body, sc.args().get(0), env));
            return new com.legend.exec.ExecutionResult.Scalar(true, sc.info().type());
        }
        com.legend.sql.SqlQuery plan = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent()).lower(body);
        com.legend.sql.dialect.SqlDialect dialect = dialectOf(ctx, runtimeFqn);
        return com.legend.exec.Executor.execute(
                dialect.render(plan), plan, root.info(),
                com.legend.exec.ResultShape.of(root), connection, dialect);
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
    private static com.legend.exec.ExecutionResult executeInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env)
            throws java.sql.SQLException {
        String raw = evalStringArg(body, call.args().get(0), env);
        com.legend.sql.dialect.SqlDialect dialect =
                dialectOf(env.ctx(), env.runtimeFqn());
        // split FIRST: adaptation is per-statement (its recognizers anchor
        // at statement start)
        for (String stmt : com.legend.sql.RawSql.splitStatements(raw)) {
            try {
                com.legend.exec.Executor.executeRaw(env.connection(),
                        dialect.adaptRawSql(stmt));
            } catch (java.sql.SQLException e) {
                if (env.rawSqlFailureSink() == null) {
                    throw e;
                }
                // per-statement tolerance (engine-harness semantics): report
                // and CONTINUE — the caller's ledger drives its emptiness guard
                env.rawSqlFailureSink().accept(stmt.strip().split("\\n")[0]
                        + " => " + String.valueOf(e.getMessage()).split("\\n")[0]);
            }
        }
        // an opaque ResultSet handle: setup statements ignore it; a test
        // that READS it will surface loudly here when that day comes
        return new com.legend.exec.ExecutionResult.Scalar(null, call.info().type());
    }

    /**
     * The K-native {@code dropAndCreateTableInDb}
     * (PlatformTypes.DROP_AND_CREATE_TABLE_IN_DB): the real engine spells
     * DDL by walking the Database metamodel; here it renders from the
     * compiled store model ({@link com.legend.exec.Ddl}) and executes over
     * the ambient connection — same connection convention as executeInDb.
     */
    private static com.legend.exec.ExecutionResult dropAndCreateTableInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env)
            throws java.sql.SQLException {
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
        com.legend.sql.dialect.SqlDialect dialect =
                dialectOf(ctx, env.runtimeFqn());
        com.legend.exec.Executor.executeRaw(connection,
                dialect.adaptRawSql(com.legend.exec.Ddl.dropTable(schema, table)));
        com.legend.exec.Executor.executeRaw(connection,
                dialect.adaptRawSql(com.legend.exec.Ddl.createTable(def, schema)));
        return new com.legend.exec.ExecutionResult.Scalar(true, call.info().type());
    }

    /**
     * Evaluate one String[1] argument of a K-native THROUGH the pipeline:
     * the let statements it (transitively) references ride along; all
     * others — crucially connection chains — are dropped, never evaluated.
     */
    private static String evalStringArg(java.util.List<TypedSpec> body, TypedSpec arg,
            ExecEnv env) throws java.sql.SQLException {
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
        com.legend.exec.ExecutionResult evaluated = executeTyped(kept, env);
        if (!(evaluated instanceof com.legend.exec.ExecutionResult.Scalar sc)
                || !(sc.value() instanceof String str)) {
            throw new IllegalStateException("K-native dispatch: the argument"
                    + " must evaluate to one String, got " + evaluated);
        }
        return str;
    }

    /** Conservative free-variable scan (shadowed names over-collect — over-KEEPING lets is safe). */
    private static void collectVariableRefs(TypedSpec node, java.util.Set<String> out) {
        if (node instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            out.add(v.name());
        }
        node.children().forEach(c -> collectVariableRefs(c, out));
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
