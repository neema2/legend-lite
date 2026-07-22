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
        java.util.Map<String, ExecFrame> execFrames = new java.util.LinkedHashMap<>();
        for (int i = 0; i < stmts.size(); i++) {
            TypedSpec stmt = stmts.get(i);
            boolean last = i == stmts.size() - 1;
            if (stmt instanceof com.legend.compiler.spec.typed.TypedLet let && !last) {
                // let tds = $r.values(->at(0)/->toOne()): over a RELATION-
                // rooted frame these wrappers are the Result ENVELOPE — the
                // alias IS the same frame (audit 19d B2: the splice rules
                // move verbatim from the harness). Class/scalar roots fall
                // through: their at/toOne are REAL selections.
                ExecFrame alias = aliasFrame(let.value(), execFrames);
                if (alias != null) {
                    execFrames.put(let.name(), alias);
                    continue;
                }
                TypedSpec rhs = let.value();
                while (rhs instanceof com.legend.compiler.spec.typed.TypedFrom rf) {
                    rhs = rf.source();
                }
                if (rhs instanceof com.legend.compiler.spec.typed.TypedNativeCall ec
                        && com.legend.compiler.element.type.PlatformTypes.EXECUTE
                                .equals(ec.callee().qualifiedName())) {
                    // EAGER run (engine parity, audit 16 F1): a broken
                    // pipeline surfaces AT the let even when nothing reads
                    // the frame.
                    execFrames.put(let.name(),
                            buildFrame(ec, letPrefix, true, specs, env));
                    continue;
                }
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
                    new com.legend.compiler.spec.UserCallInliner(specs,
                            spliceHook(execFrames, letPrefix, specs, env))
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
            // execute() in RESULT position: the eager frame run IS the value
            // (the Result envelope is typing-only — the chain's rows are what
            // a reader observes).
            if (preRoot instanceof com.legend.compiler.spec.typed.TypedNativeCall xc
                    && com.legend.compiler.element.type.PlatformTypes.EXECUTE
                            .equals(xc.callee().qualifiedName())) {
                result = buildFrame(xc, letPrefix, true, specs, env).result();
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
            ExecutionResult result) {
    }

    /** Envelope-read recognizers — generic natives identified by EXACT FQN
     * (never suffix matching). */
    private static final String AT_FQN = "meta::pure::functions::collection::at";
    private static final String FIRST_FQN = "meta::pure::functions::collection::first";
    private static final String TO_ONE_FQN =
            "meta::pure::functions::multiplicity::toOne";
    private static final java.util.Set<String> SIZE_FQNS = java.util.Set.of(
            "meta::pure::functions::relation::size",
            "meta::pure::functions::collection::size");

    /**
     * Build the frame for one {@code execute(f, mapping, runtime, ext)}
     * call: fold the query lambda's (and the caller's) lets, attach the
     * EXPLICIT mapping argument as the chain's execution context, and — for
     * a let binding — run it eagerly through the pipeline.
     */
    private static ExecFrame buildFrame(
            com.legend.compiler.spec.typed.TypedNativeCall ec,
            java.util.List<TypedSpec> letPrefix, boolean eager,
            SpecCompiler specs, ExecEnv env) throws java.sql.SQLException {
        TypedSpec q = letBound(ec.args().get(0), letPrefix);
        if (!(q instanceof com.legend.compiler.spec.typed.TypedLambda lam)
                || !lam.parameters().isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "execute() whose query argument is not a lambda");
        }
        if (!(letBound(ec.args().get(1), letPrefix)
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef mref)) {
            throw new com.legend.error.NotImplementedException(
                    "execute() mapping argument must be a mapping reference");
        }
        java.util.List<TypedSpec> qb = new java.util.ArrayList<>(letPrefix);
        qb.addAll(lam.body());
        TypedSpec chain = new com.legend.compiler.spec.UserCallInliner(specs)
                .inlineBody(qb).get(0);
        if (!containsTypedFrom(chain)) {
            java.util.Optional<com.legend.compiler.spec.typed.TypedPackageableRef>
                    runtime = env.runtimeFqn() == null ? java.util.Optional.empty()
                            : java.util.Optional.of(
                                    new com.legend.compiler.spec.typed.TypedPackageableRef(
                                            env.runtimeFqn(), mref.info()));
            chain = new com.legend.compiler.spec.typed.TypedFrom(chain,
                    java.util.Optional.of(mref), runtime, chain.info());
        }
        boolean relationRooted = chain.info().type()
                instanceof com.legend.compiler.element.type.Type.RelationType;
        ExecutionResult run = null;
        if (eager) {
            java.util.List<TypedSpec> body =
                    new com.legend.resolver.StoreResolver(env.ctx(), specs)
                            .resolve(java.util.List.of(chain), env.runtimeFqn());
            run = executeTyped(body, env);
        }
        return new ExecFrame(chain, relationRooted, run);
    }

    /** A let-bound argument resolves through the caller's let prefix
     * ({@code let q = |...|; execute($q, ...)}). */
    private static TypedSpec letBound(TypedSpec arg,
            java.util.List<TypedSpec> letPrefix) {
        if (arg instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            for (int i = letPrefix.size() - 1; i >= 0; i--) {
                if (letPrefix.get(i)
                        instanceof com.legend.compiler.spec.typed.TypedLet let
                        && let.name().equals(v.name())) {
                    return let.value();
                }
            }
        }
        return arg;
    }

    private static boolean containsTypedFrom(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedFrom) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsTypedFrom(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@code let tds = $r.values(->at(0)/->toOne()/->first())} over a RELATION-rooted
     * frame: the wrappers are the Result envelope and the alias IS the same
     * frame ({@code $tds->size()} keeps ONE-TDS semantics). {@code at(k>0)}
     * is loud — the envelope holds one TDS. Class/scalar roots return null:
     * their at/toOne are REAL selections and the binding is an ordinary let.
     */
    private static ExecFrame aliasFrame(TypedSpec rhs,
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
                    && (AT_FQN.equals(nc.callee().qualifiedName())
                            || TO_ONE_FQN.equals(nc.callee().qualifiedName())
                            || FIRST_FQN.equals(nc.callee().qualifiedName()))
                    && !nc.args().isEmpty()) {
                if (AT_FQN.equals(nc.callee().qualifiedName())
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
     * The TYPED splice — rides the inliner's per-node hook: {@code $r.values}
     * becomes the frame's query chain; {@code ->at(0)}/{@code ->toOne()}
     * over it collapse for a relation root (real selections for a class or
     * scalar root); {@code $r->size()} over a relation-rooted frame is the
     * envelope's ONE; an inline {@code execute(...).values} splices in place.
     */
    private static java.util.function.UnaryOperator<TypedSpec> spliceHook(
            java.util.Map<String, ExecFrame> execFrames,
            java.util.List<TypedSpec> letPrefix, SpecCompiler specs, ExecEnv env) {
        return n -> {
            // the Typer's `.rows` MARKER (identity over a relation value):
            // it exists so the arms below can tell a REAL row index
            // ($r.values.rows->at(k)) from the Result envelope
            // ($r.values->at(k)) — once seen, it erases to its source.
            if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess rp
                    && rp.property().equals("rows")
                    && rp.source().info().type() instanceof
                            com.legend.compiler.element.type.Type.RelationType) {
                return rp.source();
            }
            // $r->size() / $tds->size(): ONE TDS value, never the row count
            if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall sz
                    && SIZE_FQNS.contains(sz.callee().qualifiedName())
                    && sz.args().size() == 1
                    && sz.args().get(0)
                            instanceof com.legend.compiler.spec.typed.TypedVariable sv
                    && execFrames.containsKey(sv.name())
                    && execFrames.get(sv.name()).relationRooted()) {
                return new com.legend.compiler.spec.typed.TypedCInteger(1L,
                        sz.info());
            }
            // $r.values->at(k) / ->toOne(): collapse (relation root) or a
            // REAL selection over the spliced chain (class/scalar root)
            if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall w
                    && (AT_FQN.equals(w.callee().qualifiedName())
                            || TO_ONE_FQN.equals(w.callee().qualifiedName())
                            || FIRST_FQN.equals(w.callee().qualifiedName()))
                    && !w.args().isEmpty()) {
                TypedSpec spliced = spliceValuesRead(w.args().get(0),
                        execFrames, letPrefix, specs, env);
                if (spliced != null) {
                    // relation-rootedness IS the spliced chain's root type
                    boolean relation = spliced.info().type() instanceof
                            com.legend.compiler.element.type.Type.RelationType;
                    if (relation) {
                        if (AT_FQN.equals(w.callee().qualifiedName())
                                && !(w.args().size() == 2 && w.args().get(1)
                                        instanceof com.legend.compiler.spec.typed
                                                .TypedCInteger k
                                        && k.value().longValue() == 0)) {
                            throw new IllegalStateException(
                                    "Result.values->at(k>0) on a relation-rooted"
                                    + " query — the values envelope holds one TDS");
                        }
                        return spliced;
                    }
                    java.util.List<TypedSpec> args =
                            new java.util.ArrayList<>(w.args());
                    args.set(0, spliced);
                    return new com.legend.compiler.spec.typed.TypedNativeCall(
                            w.callee(), args, w.info());
                }
            }
            // $r.values / execute(...).values → the spliced chain
            TypedSpec direct = spliceValuesRead(n, execFrames, letPrefix,
                    specs, env);
            if (direct != null) {
                return direct;
            }
            // a BARE frame variable reads as the chain (harness parity)
            if (n instanceof com.legend.compiler.spec.typed.TypedVariable bv
                    && execFrames.containsKey(bv.name())) {
                return execFrames.get(bv.name()).chain();
            }
            return n;
        };
    }

    /** The frame behind a {@code <frameVar>.values} read; null otherwise. */
    private static ExecFrame valuesFrame(TypedSpec n,
            java.util.Map<String, ExecFrame> execFrames) {
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.property().equals("values")
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable v
                && execFrames.containsKey(v.name())) {
            return execFrames.get(v.name());
        }
        return null;
    }

    /** Splice a {@code .values} read (over a frame variable or an INLINE
     * execute call) into the underlying typed query chain; null when the
     * node is not a values read the frames can answer. */
    private static TypedSpec spliceValuesRead(TypedSpec n,
            java.util.Map<String, ExecFrame> execFrames,
            java.util.List<TypedSpec> letPrefix, SpecCompiler specs, ExecEnv env) {
        ExecFrame f = valuesFrame(n, execFrames);
        if (f != null) {
            return f.chain();
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.property().equals("values")) {
            TypedSpec src = pa.source();
            while (src instanceof com.legend.compiler.spec.typed.TypedFrom sf) {
                src = sf.source();
            }
            if (src instanceof com.legend.compiler.spec.typed.TypedNativeCall ec
                    && com.legend.compiler.element.type.PlatformTypes.EXECUTE
                            .equals(ec.callee().qualifiedName())) {
                try {
                    // inline read: the value is observed where it stands —
                    // no separate eager run (it would execute twice)
                    return buildFrame(ec, letPrefix, false, specs, env).chain();
                } catch (java.sql.SQLException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        return null;
    }

    /** Bind an effectful map's parameter: TypedVariable(param) reads in
     * the body's native-call arguments replace with the STRING literal
     * (the corpus shape: executeInDb($sql, $connection)); a read anywhere
     * deeper is loud — never silently unbound. */
    private static TypedSpec bindParam(TypedSpec node, String param, String value) {
        var lit = new com.legend.compiler.spec.typed.TypedCString(value,
                com.legend.compiler.element.type.ExprType.one(
                        com.legend.compiler.element.type.Type.Primitive.STRING));
        if (node instanceof com.legend.compiler.spec.typed.TypedVariable tv
                && tv.name().equals(param)) {
            return lit;
        }
        if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall nc) {
            java.util.List<TypedSpec> args = new java.util.ArrayList<>();
            for (TypedSpec a : nc.args()) {
                args.add(a instanceof com.legend.compiler.spec.typed.TypedVariable v2
                        && v2.name().equals(param) ? lit : a);
            }
            return new com.legend.compiler.spec.typed.TypedNativeCall(
                    nc.callee(), args, nc.info());
        }
        if (referencesVar(node, param)) {
            throw new IllegalStateException("effectful map body reads the"
                    + " parameter '" + param + "' in an unsupported position");
        }
        return node;
    }

    private static boolean referencesVar(TypedSpec node, String name) {
        if (node instanceof com.legend.compiler.spec.typed.TypedVariable tv
                && tv.name().equals(name)) {
            return true;
        }
        for (TypedSpec c : node.children()) {
            if (referencesVar(c, name)) {
                return true;
            }
        }
        return false;
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
        // The engine's CSV-seed SQL generator: strings from the parsed
        // store's column types (CsvSeed) — dbConfig is never evaluated.
        // The corpus's own setupTestData body maps the result through
        // executeInDb, which the TypedMap arm below sequences.
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall gen
                && com.legend.compiler.element.type.PlatformTypes.SET_UP_DATA_SQLS_V2
                        .equals(gen.callee().qualifiedName())) {
            String csv = evalStringArg(body, gen.args().get(0), env);
            String dbFqn = gen.args().get(1)
                    instanceof com.legend.compiler.spec.typed.TypedPackageableRef pr
                    ? pr.fullPath() : null;
            java.util.List<Object> sqls = new java.util.ArrayList<>(
                    com.legend.exec.CsvSeed.sqls(csv, dbFqn, ctx));
            return new ExecutionResult.Collection(sqls,
                    com.legend.compiler.element.type.Type.Primitive.STRING);
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
            ExecutionResult values = executeTyped(src, env);
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
                    one.add(bindParam(stmt2, param, sv));
                }
                last = executeTyped(one, env);
            }
            return last;
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
        ExecutionResult res = Executor.execute(
                dialect.render(plan), plan, root.info(),
                com.legend.exec.ResultShape.of(root), connection, dialect);
        // rows->toOne() READER enforcement (audit 22b F1): the lowering is
        // row-identical to the relation (engine toOne throws at the READER,
        // never in SQL) — so THE reader enforces exactly-one here for a
        // TABULAR-consumed toOne root; the scalar arm's second-row guard
        // covers scalar reads, this covers whole-TDS consumption and the
        // ZERO-row lower bound.
        if (root instanceof com.legend.compiler.spec.typed.TypedNativeCall tw
                && "meta::pure::functions::multiplicity::toOne"
                        .equals(tw.callee().qualifiedName())
                && !tw.args().isEmpty()
                && tw.args().get(0).info().type()
                        instanceof com.legend.compiler.element.type.Type.RelationType
                && res instanceof ExecutionResult.Tabular tab
                && tab.rows().size() != 1) {
            throw new IllegalStateException("toOne() over a relation returned "
                    + tab.rows().size() + " row(s) — the exactly-one contract"
                    + " (engine reader semantics)");
        }
        return res;
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
        // split FIRST: adaptation is per-statement (its recognizers anchor
        // at statement start). Corpus-authored raw H2 goes through THE
        // boundary translator — never a dialect renderer (R0 rule).
        for (String stmt : com.legend.sql.RawSql.splitStatements(raw)) {
            try {
                Executor.executeRaw(env.connection(),
                        com.legend.exec.RawSqlBoundary.h2ToDuckDb(stmt));
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
        Executor.executeRaw(connection,
                com.legend.exec.RawSqlBoundary.h2ToDuckDb(Ddl.dropTable(schema, table)));
        Executor.executeRaw(connection,
                com.legend.exec.RawSqlBoundary.h2ToDuckDb(Ddl.createTable(def, schema)));
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
