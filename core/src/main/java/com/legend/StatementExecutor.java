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
    static ExecutionResult execute(
            com.legend.model.spec.ValueSpecification resolved, ModelContext ctx,
            String runtimeFqn, com.legend.sql.dialect.SqlDialect dialect,
            java.sql.Connection connection,
            java.util.function.Consumer<String> rawSqlFailureSink)
            throws java.sql.SQLException {
        SpecCompiler specs = new SpecCompiler(ctx);
        ExecEnv env = new ExecEnv(ctx, runtimeFqn, dialect, connection,
                rawSqlFailureSink);
        return executeStatements(specs.typeQueryBody(resolved),
                new java.util.ArrayList<>(), specs, env,
                new java.util.ArrayDeque<>());
    }

    /** The K-phase execution environment: ONE ambient connection, ONE
     * dialect (audit 17: recomputing it per arm invited a future
     * mixed-dialect bug), the driver runtime, and the optional raw-SQL
     * failure sink. */
    record ExecEnv(ModelContext ctx, String runtimeFqn,
            com.legend.sql.dialect.SqlDialect dialect,
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
    static ExecutionResult executeStatements(
            java.util.List<TypedSpec> stmts, java.util.List<TypedSpec> letPrefix,
            SpecCompiler specs, ExecEnv env, java.util.Deque<String> frames)
            throws java.sql.SQLException {
        ExecutionResult result = null;
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
            // toSQLString dispatches PRE-H: its query lambda resolves
            // against the EXPLICIT mapping argument, never the ambient
            // runtime's (audit 19d B3 — the K-native replacing the
            // harness's name-interception)
            TypedSpec preRoot = body.get(body.size() - 1);
            if (preRoot instanceof com.legend.compiler.spec.typed.TypedLet pl) {
                preRoot = pl.value();
            }
            while (preRoot instanceof com.legend.compiler.spec.typed.TypedFrom pf) {
                preRoot = pf.source();
            }
            if (preRoot instanceof com.legend.compiler.spec.typed.TypedNativeCall tsc
                    && com.legend.compiler.element.type.PlatformTypes.TO_SQL_STRING
                            .equals(tsc.callee().qualifiedName())) {
                result = toSqlString(tsc, specs, env);
                continue;
            }
            body = new com.legend.resolver.StoreResolver(env.ctx(), specs)
                    .resolve(body, env.runtimeFqn());                     // Phase H
            result = executeTyped(body, env);
        }
        return result;
    }

    /**
     * The engine's SQL-text surface: lower the query lambda through the
     * platform's own G½->H->I against the MAPPING ARGUMENT and render with
     * the engine-style dialect. H2 only — other DatabaseTypes throw until
     * their renderers exist. Never lowers, never touches the connection.
     */
    private static ExecutionResult toSqlString(
            com.legend.compiler.spec.typed.TypedNativeCall call,
            com.legend.compiler.spec.SpecCompiler specs, ExecEnv env) {
        String db = typedEnumTail(call.args().get(2));
        if (!"H2".equals(db)) {
            throw new com.legend.error.NotImplementedException(
                    "toSQLString for DatabaseType." + db
                    + " — only the H2 engine-style renderer is built");
        }
        if (!(call.args().get(0)
                instanceof com.legend.compiler.spec.typed.TypedLambda lam)) {
            throw new com.legend.error.NotImplementedException(
                    "toSQLString whose query argument is not a lambda literal");
        }
        if (!(call.args().get(1)
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr)) {
            throw new com.legend.error.NotImplementedException(
                    "toSQLString mapping argument must be a mapping reference");
        }
        java.util.List<TypedSpec> body =
                new com.legend.compiler.spec.UserCallInliner(specs)
                        .inlineBody(lam.body());
        body = new com.legend.resolver.StoreResolver(env.ctx(), specs)
                .resolve(body, env.runtimeFqn(), pr.fullPath());
        body = com.legend.resolver.RelationalRootForm.apply(
                body, env.ctx(), pr.fullPath());
        com.legend.sql.SqlQuery plan = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(env.ctx(), t),
                f -> env.ctx().findClass(f).isPresent()).lower(body);
        return new ExecutionResult.Scalar(
                new com.legend.sql.dialect.EngineStyleH2().render(plan),
                com.legend.compiler.element.type.Type.Primitive.STRING);
    }

    /** The member name of a typed enum-shaped read (DatabaseType.H2). */
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
     * A statement-position call to an EFFECTFUL function: bind the caller's
     * arguments as parameter lets (caller lets substituted in — the callee
     * body is otherwise closed) and run the body as a statement sequence.
     */
    static ExecutionResult executeCallStatement(
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
                if (containsEffectfulNode(java.util.List.of(argValue))) {
                    // same rule as the effectful-let guard: the frame binds
                    // arguments as lets, and β-substitution drops an unused
                    // one (or doubles a twice-used one) — refuse loudly
                    // (audit 17: ignore(executeInDb(...)) silently lost the
                    // insert)
                    throw new IllegalStateException("effectful argument to '"
                            + call.callee().qualifiedName()
                            + "' (parameter '"
                            + call.callee().parameters().get(p).name()
                            + "' binds an executeInDb-family call) is not"
                            + " supported");
                }
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
    static boolean containsEffect(TypedSpec node, SpecCompiler specs,
            java.util.Map<String, Boolean> memo) {
        if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall nc
                && com.legend.compiler.element.type.PlatformTypes
                        .isEffectfulNative(nc.callee().qualifiedName())) {
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
    static ExecutionResult executeTyped(
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
                && com.legend.compiler.element.type.PlatformTypes.EXECUTE_IN_DB
                        .equals(nc.callee().qualifiedName())) {
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
            // harness behavior. A REAL effect nested inside the argument
            // would be dropped — that must never be silent (audit 17): it
            // feeds the failure ledger (arming the emptiness guard), or
            // throws when no ledger is listening.
            if (containsEffectfulNode(pn.args())) {
                if (env.rawSqlFailureSink() == null) {
                    throw new IllegalStateException("print/println argument"
                            + " contains an executeInDb-family call; the print"
                            + " arm never evaluates arguments, so the effect"
                            + " would be dropped");
                }
                env.rawSqlFailureSink().accept(
                        "print => argument contains an executeInDb-family call;"
                        + " not evaluated (effect dropped)");
            }
            return new ExecutionResult.Scalar(null, pn.info().type());
        }
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall sc
                && com.legend.compiler.element.type.PlatformTypes.DROP_AND_CREATE_SCHEMA_IN_DB
                        .equals(sc.callee().qualifiedName())) {
            // the engine DROPS + creates; here create-if-missing — the DDL
            // seeds already own tables in the schema, and the setup's own
            // dropAndCreateTableInDb calls recreate what it manages
            Executor.executeRaw(connection,
                    "Create schema if not exists "
                            + evalStringArg(body, sc.args().get(0), env));
            return new ExecutionResult.Scalar(true, sc.info().type());
        }
        com.legend.sql.SqlQuery plan = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent()).lower(body);
        com.legend.sql.dialect.SqlDialect dialect = env.dialect();
        return Executor.execute(
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
    static ExecutionResult executeInDb(
            java.util.List<TypedSpec> body,
            com.legend.compiler.spec.typed.TypedNativeCall call, ExecEnv env)
            throws java.sql.SQLException {
        String raw = evalStringArg(body, call.args().get(0), env);
        com.legend.sql.dialect.SqlDialect dialect = env.dialect();
        // split FIRST: adaptation is per-statement (its recognizers anchor
        // at statement start)
        for (String stmt : com.legend.sql.RawSql.splitStatements(raw)) {
            try {
                Executor.executeRaw(env.connection(),
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
        return new ExecutionResult.Scalar(null, call.info().type());
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
        com.legend.sql.dialect.SqlDialect dialect = env.dialect();
        Executor.executeRaw(connection,
                dialect.adaptRawSql(Ddl.dropTable(schema, table)));
        Executor.executeRaw(connection,
                dialect.adaptRawSql(Ddl.createTable(def, schema)));
        return new ExecutionResult.Scalar(true, call.info().type());
    }

    /**
     * Evaluate one String[1] argument of a K-native THROUGH the pipeline:
     * the let statements it (transitively) references ride along; all
     * others — crucially connection chains — are dropped, never evaluated.
     */
    static String evalStringArg(java.util.List<TypedSpec> body, TypedSpec arg,
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
        ExecutionResult evaluated = executeTyped(kept, env);
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
                    && com.legend.compiler.element.type.PlatformTypes
                            .isEffectfulNative(nc.callee().qualifiedName())
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
        boolean sqlCarrier = com.legend.compiler.element.type.PlatformTypes
                .EXECUTE_IN_DB.equals(fqn);
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
