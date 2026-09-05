// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;



import com.legend.Compiler;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.protocol.spec.KeyExpression;
import com.legend.model.ImportScope;
import com.legend.parser.SpecParser;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Map;

/**
 * NATIVE test-body execution &mdash; runs a real pure {@code <<test.Test>>}
 * function body (a STATEMENT SEQUENCE of lets, {@code execute(...)} calls
 * and {@code assert*} calls) through the ordinary compile-to-SQL pipeline.
 *
 * <p><strong>No interpreter</strong> (tenet #1): {@code let r =
 * execute(|Q, ...)} binds a lazy handle + execution context; downstream
 * reads SPLICE into ONE SQL statement; {@code assert*} natives are the
 * orchestration boundary — both sides execute through the pipeline, Java
 * compares wire values strictly (one shared wire convention).
 *
 * <p><strong>The one driver-level form.</strong> execute()'s runtime/
 * extensions args are engine-harness plumbing (runtime objects
 * legend-lite deliberately does not model): the driver consumes the
 * QUERY (arg 0) and the MAPPING (arg 1, caller-import-resolved);
 * trailing config args are un-typed and the CALLER supplies the physical
 * connection + runtime — the same boundary the engine's own execute
 * crosses into Java.
 *
 * <p><strong>Failure polarity.</strong> Anything unrecognized is
 * {@link Outcome.Unsupported} (named, loud), never a silent skip; a
 * compile error propagates; assertion evaluation STOPS at the first
 * failing assert (real pure {@code assert} raises).
 */
public final class EngineTestExecutor {

    /**
     * HARNESS comparison policy (D-arc 2026-08-21): the harness's
     * EXPECTED side is decoded from engine golden TEXT, so a temporal
     * expectation arrives as a STRING while the wire carries
     * {@code PureDateLiteral} (THE temporal type — the production
     * string-carrier bridge is DEAD). The decode obligation is the
     * HARNESS's, at its own seam: a string beside a wire temporal
     * parses to the pure date value first (space-separated grid
     * spellings normalize), then the ONE production lattice judges.
     * Non-parsing strings stay strings and fail like pure. These arms
     * delete wholesale with the R2 render cutover.
     */
    private static boolean goldenEqualScalar(@com.legend.Nullable Object e,
            @com.legend.Nullable Object a) {
        Object de = goldenTemporalDecode(e, a);
        Object da = goldenTemporalDecode(a, e);
        // V10a (derived, replacing the instant-blind fit): temporal
        // golden compares follow THE ENGINE'S OWN CONVENTION —
        // DateFunctions.fromSQLTimestamp makes every DB-derived value a
        // NINE-DIGIT DateWithSubsecond, so both sides normalize
        // time-bearing precision to nine digits and compare by EXACT
        // record equality (AbstractPureDate.equals: components + exact
        // subsecond string). STRICTER than the old instant compare:
        // a date-only value never equals a midnight datetime — exactly
        // as the engine rules it.
        if (de instanceof com.legend.values.PureDateLiteral pe
                && da instanceof com.legend.values.PureDateLiteral pa) {
            return engineNine(pe).equals(engineNine(pa));
        }
        // X-audit lane doctrine: the PRODUCTION lattice is engine-exact
        // (no cross-kind numeric equality) — but THIS seam's expected
        // side is DECODED FROM GOLDEN TEXT, which cannot carry kind or
        // scale fidelity (a golden '22.3' decodes at the text's scale
        // while the wire carries the column's declared scale). The
        // golden seam therefore compares numerics BY VALUE — the one
        // place the old grants legitimately belonged, quarantined here,
        // mirroring the temporal engineNine normalization above.
        // The arm is NARROW: only pairs touching a BigDecimal and free
        // of integral values, because golden text DOES carry the
        // integral-vs-fractional distinction ('30' vs '30.0' — pure
        // kind semantics stay strict, numericKindIsStrict pins it);
        // what text cannot carry is Decimal SCALE and the decode
        // CARRIER (a golden '22.3' decodes at the text's scale/as a
        // Double while the wire carries the column's declared scale).
        if (de instanceof Number ne && da instanceof Number na
                && !(de instanceof com.legend.values.PureDateLiteral)
                && (ne instanceof java.math.BigDecimal
                        || na instanceof java.math.BigDecimal)
                && !integral(ne) && !integral(na)
                && numericValue(ne) != null && numericValue(na) != null) {
            java.math.BigDecimal ve = numericValue(ne);
            java.math.BigDecimal va = numericValue(na);
            if (ve != null && va != null && ve.compareTo(va) == 0) {
                return true;
            }
            // value-differing pairs FALL THROUGH: the lattice keeps its
            // engine-exact kind rules AND the declared 2-ULP dialect-
            // arithmetic policy (OPEN_REGISTER §5 — cross-libm last-ULP
            // drift on transcendentals; user-ratified 2026-08-22)
        }
        return com.legend.exec.PureAsserts.equalScalar(de, da);
    }

    private static boolean integral(Number n) {
        return n instanceof Long || n instanceof Integer
                || n instanceof Short || n instanceof Byte
                || n instanceof java.math.BigInteger;
    }

    /** Finite numeric value, or null for non-finite floats (those keep
     * the lattice's IEEE rules). */
    private static java.math.@com.legend.Nullable BigDecimal numericValue(
            Number n) {
        if ((n instanceof Double d && !Double.isFinite(d))
                || (n instanceof Float f && !Float.isFinite(f))) {
            return null;
        }
        return n instanceof java.math.BigDecimal bd ? bd
                : n instanceof java.math.BigInteger bi
                        ? new java.math.BigDecimal(bi)
                        : new java.math.BigDecimal(String.valueOf(n));
    }

    /** The engine wire convention ({@code fromSQLTimestamp %09d}):
     * time-bearing values carry nine subsecond digits; date-only
     * precisions are untouched (the engine's StrictDate stays
     * StrictDate). */
    private static com.legend.values.PureDateLiteral engineNine(
            com.legend.values.PureDateLiteral d) {
        if (!d.precision().atLeast(
                com.legend.values.PureDateLiteral.Precision.HOUR)) {
            return d;
        }
        java.time.LocalDateTime f = d.toInstantFloor();
        return new com.legend.values.PureDateLiteral.DateWithSubsecond(
                f.getYear(), f.getMonthValue(), f.getDayOfMonth(),
                f.getHour(), f.getMinute(), f.getSecond(),
                String.format("%09d", f.getNano()));
    }

    private static @com.legend.Nullable Object goldenTemporalDecode(
            @com.legend.Nullable Object v, @com.legend.Nullable Object other) {
        if (v instanceof String s
                && other instanceof com.legend.values.PureDateLiteral) {
            try {
                return com.legend.values.PureDateLiteral.parse(
                        s.trim().replace(' ', 'T').replaceFirst("Z$", "+0000"));
            } catch (IllegalArgumentException notADate) {
                return v;   // the typing-bug catch stays the parse
            }
        }
        return v;
    }

    /** F3.2e: ONE substitution engine — the compiler's (SourceSubst,
     * semantics pinned by SourceSubstTest + SubstitutionParityTest).
     * CORPUS_FOLD is the driver-injected PostFold carrying the harness's
     * two wire concerns: the metaprogramming fold (payload grammar =
     * the native's own contract, engine grammar per LegendCompile.java:57)
     * and the TDSNull wire sentinel (a real cell value for wireEquals,
     * never a pure empty). HarnessSubstitution is DELETED; its other
     * extras moved to their owners (pair fold + lambda-local scoping ->
     * SourceSubst; serialize-key aliases -> ElqSplice pre-stamping). */
    static final com.legend.compiler.spec.SourceSubst.PostFold CORPUS_FOLD =
            node -> {
                if (node instanceof com.legend.protocol.spec.NewInstance tn
                        && (tn.className().equals("TDSNull")
                                || tn.className().equals(
                                        "meta::pure::tds::TDSNull"))) {
                    return new com.legend.protocol.spec.CString("TDSNull");
                }
                // POST-ORDER means the inner ^TDSNull() has ALREADY
                // folded to the sentinel by the time the new(...) wrapper
                // is offered — accept both spellings of the payload
                if (node instanceof com.legend.protocol.spec.AppliedFunction nf
                        && nf.function().equals("new")
                        && nf.parameters().size() == 2
                        && (nf.parameters().get(1)
                                instanceof com.legend.protocol.spec.NewInstance tn2
                                && (tn2.className().equals("TDSNull")
                                        || tn2.className().equals(
                                                "meta::pure::tds::TDSNull"))
                            || nf.parameters().get(1)
                                instanceof com.legend.protocol.spec.CString cs2
                                && cs2.value().equals("TDSNull"))) {
                    return new com.legend.protocol.spec.CString("TDSNull");
                }
                if (node instanceof com.legend.protocol.spec.AppliedFunction af) {
                    return com.legend.parser.QuotedSpecParser.fold(af,
                            com.legend.parser.Dialect.LEGEND_ENGINE);
                }
                // pair(a, b).first/.second constant fold (real pure
                // anonymousCollections semantics) — corpus-wire concern:
                // the datetime plan helpers return Pair<plan, text>
                if (node instanceof com.legend.protocol.spec.AppliedProperty app
                        && app.receiver()
                                instanceof com.legend.protocol.spec.AppliedFunction pf
                        && simpleName(pf.function()).equals("pair")
                        && pf.parameters().size() == 2) {
                    if (app.property().equals("first")) {
                        return pf.parameters().get(0);
                    }
                    if (app.property().equals("second")) {
                        return pf.parameters().get(1);
                    }
                }
                return null;
            };

    static com.legend.protocol.spec.ValueSpecification subst(
            com.legend.protocol.spec.ValueSpecification v,
            java.util.Map<String, com.legend.protocol.spec.ValueSpecification> lets) {
        return substitute(v, lets);
    }

    static com.legend.protocol.spec.ValueSpecification substitute(
            com.legend.protocol.spec.ValueSpecification v,
            java.util.Map<String, com.legend.protocol.spec.ValueSpecification> lets) {
        return com.legend.compiler.spec.SourceSubst.substitute(v,
                resolvedLets(lets), CORPUS_FOLD);
    }

    /** Harness lets maps can hold RAW statement pulls whose values read
     * other lets (the per-driver toSQLString loop) — the base engine is
     * pure capture-at-binding (A8 pin), so the BRIDGE pre-resolves each
     * value through the map with ITSELF removed (cycle-safe: a
     * self-referential let terminates). Lexical-equivalent absent
     * rebinding, which the F3.2 corpus differential proved. */
    private static java.util.Map<String, com.legend.protocol.spec.ValueSpecification>
            resolvedLets(java.util.Map<String,
                    com.legend.protocol.spec.ValueSpecification> lets) {
        if (lets.isEmpty()) {
            return lets;
        }
        // FIXPOINT (bounded by map size — each round resolves at least
        // one more chain level; self-reads removed per entry so cycles
        // terminate): after this, every value is capture-complete and
        // the pure base engine substitutes it verbatim
        java.util.Map<String, com.legend.protocol.spec.ValueSpecification> cur =
                new java.util.LinkedHashMap<>(lets);
        for (int round = 0; round <= lets.size(); round++) {
            java.util.Map<String, com.legend.protocol.spec.ValueSpecification>
                    next = new java.util.LinkedHashMap<>(cur.size());
            for (var e : cur.entrySet()) {
                java.util.Map<String,
                        com.legend.protocol.spec.ValueSpecification> without =
                        new java.util.LinkedHashMap<>(cur);
                without.remove(e.getKey());
                next.put(e.getKey(), com.legend.compiler.spec.SourceSubst
                        .substitute(e.getValue(), without, CORPUS_FOLD));
            }
            if (next.equals(cur)) {
                break;
            }
            cur = next;
        }
        return cur;
    }

    private EngineTestExecutor() {
    }

    /** The result of driving one test body. */
    public sealed interface Outcome {

        /** The body ran to completion or first assert failure.
         * verified = row/value asserts run; advisory = golden-SQL
         * recognized not compared (our SQL is our dialect's, by design);
         * executed = statements run THROUGH the platform (an assert-free
         * executed body is an engine-parity pass, not hollow);
         * failures = first assert failure (empty = all held). */
        record Ran(int verified, int advisory, int executed,
                List<String> failures, List<String> sqlDiffs) implements Outcome {
            public Ran(int verified, int advisory, int executed,
                    List<String> failures) {
                this(verified, advisory, executed, failures, List.of());
            }
        }

        /** A statement/assert shape the driver does not support yet — NAMED. */
        record Unsupported(String reason) implements Outcome {
        }
    }

    // execute() bindings and every read over them run PLATFORM-SIDE (audit
    // 19d B2): the statements forward VERBATIM to the statement executor's
    // result frame — the harness no longer owns any envelope semantics
    // (the values/at/toOne/size splice rules live in StatementExecutor).

    /** Does the expression (transitively) contain an {@code execute()} call? */
    static boolean containsExecute(ValueSpecification v) {
        if (v instanceof AppliedFunction af && isExecuteCall(af)) {
            return true;
        }
        return switch (v) {
            case AppliedFunction af -> af.parameters().stream()
                    .anyMatch(EngineTestExecutor::containsExecute);
            case AppliedProperty ap -> containsExecute(ap.receiver());
            case PureCollection pc -> pc.values().stream()
                    .anyMatch(EngineTestExecutor::containsExecute);
            case LambdaFunction lf -> lf.body().stream()
                    .anyMatch(EngineTestExecutor::containsExecute);
            default -> false;
        };
    }

    /** Does the expression read any of the given variables? (No shadow
     * tracking — execute bindings are never usefully shadowed, and
     * over-forwarding a statement prefix is safe.) */
    static boolean referencesAny(ValueSpecification v,
            java.util.Set<String> names) {
        return switch (v) {
            case Variable var -> names.contains(var.name());
            case AppliedFunction af -> af.parameters().stream()
                    .anyMatch(p -> referencesAny(p, names));
            case AppliedProperty ap -> referencesAny(ap.receiver(), names);
            case PureCollection pc -> pc.values().stream()
                    .anyMatch(p -> referencesAny(p, names));
            case LambdaFunction lf -> lf.body().stream()
                    .anyMatch(p -> referencesAny(p, names));
            default -> false;
        };
    }

    /**
     * ORDER-POLICY VIEW ONLY: rewrite {@code $r.values(->at(0)/->toOne())}
     * reads to the bound query's chain expression so {@link #endsInSort}
     * sees a sort INSIDE the query lambda (the platform frame owns the
     * actual evaluation; this rewrite never executes).
     */
    private static ValueSpecification orderView(ValueSpecification v,
            Map<String, ValueSpecification> execChains) {
        if (v instanceof AppliedProperty ap && ap.property().equals("values")
                && ap.receiver() instanceof Variable var) {
            ValueSpecification chain = execChains.get(var.name());
            if (chain != null) {
                return chain;
            }
        }
        if (v instanceof Variable var) {
            ValueSpecification chain = execChains.get(var.name());
            if (chain != null) {
                return chain;
            }
        }
        return switch (v) {
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream()
                            .map(p -> orderView(p, execChains)).toList());
            case AppliedProperty ap -> new AppliedProperty(
                    orderView(ap.receiver(), execChains), ap.property());
            default -> v.mapChildren(x -> orderView(x, execChains));
        };
    }

    private static final String NOT_ENVELOPE = "\u0000notEnvelope";

    /** {@code assertSize($r.values[->at(0)/toOne()/first()], n)}: a TDS
     * is ONE carrier even through a 0-pick (engine parity, cluster 34);
     * an INSTANCE-rooted 0-pick is a REAL element pick — generic path.
     * No-.rows-traversal gated; instance collections SPLAT. */
    @SuppressWarnings("StringEquality")
    private static @com.legend.Nullable String envelopeSizeCheck(Object n,
            ValueSpecification arg, Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts, java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        ValueSpecification subst0 = subst(arg, lets), peel0 = subst0;
        while (peel0 instanceof AppliedFunction pf0
                && !pf0.parameters().isEmpty()
                && (simpleName(pf0.function()).equals("toOne")
                    || simpleName(pf0.function()).equals("first")
                    || (simpleName(pf0.function()).equals("at")
                        && pf0.parameters().size() == 2
                        && pf0.parameters().get(1) instanceof CInteger pi0
                        && pi0.value().longValue() == 0))) {
            peel0 = pf0.parameters().get(0);
        }
        if (!(peel0 instanceof AppliedProperty vp
                && vp.property().equals("values")
                && vp.receiver() instanceof Variable rv
                && execChains.containsKey(rv.name()))) {
            return NOT_ENVELOPE;
        }
        Eval av = eval(peel0, lets, execStmts, execVars, execChains, ctx,
                imports, runtimeFqn, conn);
        // Phase 3: the envelope-arity RULE is the model's
        // (ExecutionResult.envelopeCarriers — the K pin retired into it);
        // this arm keeps only recognition + eval (Phase 5's kill list).
        // ONE-carrier result == the TDS envelope; a PEELED read over a
        // non-envelope value is a real element pick (generic path).
        boolean oneCarrier = av.result() != null
                && av.result().envelopeCarriers(0) == 1L;
        if (peel0 != subst0 && !oneCarrier) {
            return NOT_ENVELOPE;
        }
        long carriers = oneCarrier ? 1L : av.size();
        return (n instanceof Number cn && cn.longValue() == carriers) ? null
                : "assertSize(result.values): expected " + n + ", got "
                        + carriers + " (TDS = one carrier; collections splat)";
    }

    /** The query CHAIN of a forwarded execute binding ({@code let name =
     * execute(|chain, ...)}) — for the order-policy view; aliases follow. */
    private static void recordExecChain(String name, ValueSpecification rhs,
            Map<String, ValueSpecification> execChains) {
        ValueSpecification cur = rhs;
        while (true) {
            if (cur instanceof AppliedProperty ap
                    && ap.property().equals("values")) {
                cur = ap.receiver();
                continue;
            }
            if (cur instanceof AppliedFunction w
                    && (w.function().equals("at") || w.function().equals("toOne"))
                    && !w.parameters().isEmpty()) {
                cur = w.parameters().get(0);
                continue;
            }
            break;
        }
        if (cur instanceof Variable var && execChains.containsKey(var.name())) {
            execChains.put(name, execChains.get(var.name()));
            return;
        }
        if (cur instanceof AppliedFunction ex && isExecuteCall(ex)
                && ex.parameters().get(0) instanceof LambdaFunction lf
                && !lf.body().isEmpty()) {
            execChains.put(name, lf.body().get(lf.body().size() - 1));
        }
    }

    /**
     * Drive one test body.
     *
     * @param ctx        the compiled model (compile once per model text,
     *                   reuse across the file's tests)
     * @param body       the test function's body source (statements between
     *                   the braces)
     * @param imports    the enclosing section's import scope (plus the
     *                   test's own package)
     * @param runtimeFqn the driver-supplied runtime (connections; also the
     *                   dialect)
     */
    public static @com.legend.Nullable Outcome run(ModelContext ctx, String body, ImportScope imports,
            String runtimeFqn, Connection conn,
            com.legend.parser.Dialect dialect) throws java.sql.SQLException {
        return run(ctx, body, imports, runtimeFqn, conn, false, dialect);
    }

    /**
     * {@code emptinessUnverifiable}: the caller knows the database may be
     * missing rows for environmental reasons (failed seed replay) — an
     * emptiness-shaped assertion (assertEmpty, assertSize 0, an empty
     * expected grid) proves nothing then and the body reports Unsupported
     * instead of a hollow pass.
     *
     * <p>Adjudicated (documented-debts 2026-08-18; the tenet audit
     * listed this flag as "gated on a runtime fact and uncounted"): the
     * gate DECLINES hollow passes, it never grants one — the runtime
     * fact (did seeding fail?) is irreducibly runtime. And it IS
     * counted, twice: the sweep prints {@code failed seeds: N} (0 on
     * every healthy run — the 18 sites are inert then), and a fired
     * gate surfaces as a NON-pass the scoreboard's gate-before-write
     * comparison catches as a family delta.
     */
    public static @com.legend.Nullable Outcome run(ModelContext ctx, String body, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable,
            com.legend.parser.Dialect dialect)
            throws java.sql.SQLException {
        return run(ctx, SpecParser.parseCodeBlock(body, dialect), imports,
                runtimeFqn, conn, emptinessUnverifiable);
    }

    /**
     * AST entry (Phase C): the test body arrives ALREADY PARSED — the
     * harness discovers test functions from the parsed model, so their
     * statement lists come straight off the FunctionDefinition, no
     * re-parse of extracted text.
     */
    public static @com.legend.Nullable Outcome run(ModelContext ctx,
            java.util.List<ValueSpecification> statements, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable)
            throws java.sql.SQLException {
        return run(ctx, statements, imports, runtimeFqn, conn,
                emptinessUnverifiable, java.util.List.of());
    }

    /**
     * {@code seedFailures}: the caller's failed-seed LEDGER — setup calls
     * the body makes report per-statement raw-SQL failures here instead of
     * aborting (engine-harness tolerance), and a non-empty ledger makes
     * emptiness-shaped assertions unverifiable from that point on.
     */
    public static @com.legend.Nullable Outcome run(ModelContext ctx,
            java.util.List<ValueSpecification> statements, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable,
            java.util.List<String> seedFailures)
            throws java.sql.SQLException {
        Outcome flipped = WholeTestFlip.tryFlip(ctx, statements, imports,
                runtimeFqn, conn, emptinessUnverifiable, seedFailures);
        if (flipped != null) {
            return flipped;
        }
        Outcome walked = runWalk(ctx, statements, imports, runtimeFqn,
                conn, emptinessUnverifiable, seedFailures);
        FlipProbe.probe(walked, ctx, statements, imports, runtimeFqn, conn);
        return walked;
    }

    private static @com.legend.Nullable Outcome runWalk(ModelContext ctx,
            java.util.List<ValueSpecification> statements, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable,
            java.util.List<String> seedFailures)
            throws java.sql.SQLException {
        ElqSplice.ELQ_PARAMS.get().clear();   // per-test param-let names
        Preamble pre = preamble(ctx, statements, imports, runtimeFqn);
        WholeTestCensus.probe(ctx, pre.lineage() != null ? statements
                : pre.statements(), imports,
                com.legend.exec.CanonicalDivergence.CONTEXT_SOURCE.get());
        if (pre.lineage() != null) {
            return pre.lineage();
        }
        statements = pre.statements();
        java.util.ArrayDeque<ValueSpecification> work =
                new java.util.ArrayDeque<>(statements);
        Map<String, ValueSpecification> lets = new LinkedHashMap<>();
        // the PLATFORM-forwarded statements (execute bindings + reads over
        // them, in order) and their bound names; execChains is the
        // order-policy view of each binding's query chain
        List<ValueSpecification> execStmts = new ArrayList<>();
        java.util.Set<String> execVars = new java.util.HashSet<>();
        Map<String, ValueSpecification> execChains = new LinkedHashMap<>();
        // #46 state — see tdgLetArm/checkTdgAssert for the semantics of
        // each surface (generator results, plan-transparent executionPlan
        // bindings, inert plan-text lets)
        Map<String, com.legend.testdatagen.TestDataGenerator.Result> tdg =
                new LinkedHashMap<>();
        TDG_GOLDENS.get().clear();
        TDG_TRANSCRIPTS.get().clear();
        Map<String, AppliedFunction> planLets = new LinkedHashMap<>();
        java.util.Set<String> planText = new java.util.HashSet<>();
        int verified = 0;
        int advisory = 0;
        List<String> sqlDiffs = new ArrayList<>();
        int executed = 0;
        while (!work.isEmpty()) {
            ValueSpecification stmt = work.poll();
            // print/println: the OUTPUT is noise, but the engine still
            // EVALUATES the argument (plan-print bodies: executionPlan ->
            // planToString -> println IS the test's whole contract) — a
            // clean run counts as engine-parity execution; a wall keeps
            // the old skip (tolerant: print text is never asserted)
            if (stmt instanceof AppliedFunction pln
                    && resolvesTo(pln, ctx, PRINT_FQNS)) {
                if (pln.parameters().size() == 1
                        && !(pln.parameters().get(0) instanceof CString)) {
                    try {
                        evalSpliced(subst(pln.parameters().get(0), lets),
                                execStmts, execVars, ctx, imports,
                                runtimeFqn, conn);
                        executed++;
                    } catch (com.legend.error.NotImplementedException
                            | java.sql.SQLException walled) {
                        // unported print material — noise either way
                    }
                }
                continue;
            }
            // engine test-harness WRAPPERS: the lambda argument's body IS
            // the test — inline its statements at the front of the worklist
            if (stmt instanceof AppliedFunction wrap
                    && resolvesTo(wrap, ctx, TEST_WRAPPER_FQNS)) {
                LambdaFunction inner = null;
                // mayExecute* carries TWO legs (alloy-lambda, pure-lambda):
                // legend-lite executes the in-process Alloy-shaped path, so
                // the PARAMETERIZED alloy leg is the test — inline it when
                // its clientVersion/serverVersion/host/port parameters are
                // decorative (unreferenced). A leg that really reads them
                // (dials a server) falls through to the zero-arg leg.
                if (simpleName(wrap.function()).startsWith("mayExecute")) {
                    for (ValueSpecification arg : wrap.parameters()) {
                        ValueSpecification a2 = arg instanceof Variable av
                                && lets.get(av.name()) != null
                                ? lets.get(av.name()) : arg;
                        if (a2 instanceof LambdaFunction lfA
                                && !lfA.parameters().isEmpty()) {
                            java.util.Set<String> ps = new java.util.HashSet<>();
                            lfA.parameters().forEach(p -> ps.add(p.name()));
                            if (lfA.body().stream()
                                    .noneMatch(st -> referencesAny(st, ps))) {
                                inner = lfA;
                            }
                            break;
                        }
                    }
                }
                if (inner == null) {
                    inner = zeroArgLambdaArg(wrap, lets);
                }
                if (inner != null) {
                    List<ValueSpecification> bodyStmts =
                            new ArrayList<>(inner.body());
                    for (int i = bodyStmts.size() - 1; i >= 0; i--) {
                        work.addFirst(bodyStmts.get(i));
                    }
                    continue;
                }
                // PARAMETERIZED lambda + pair-bound variables (the
                // WithVariables idiom): β-bind the pairs into the query and
                // synthesize the wrapper's OWN assertions in the corpus
                // spellings the harness already evaluates — engine parity
                // (executeLegendQuery binds vars, the wrapper asserts the
                // flattened values / SQL + count).
                List<ValueSpecification> synth = etaExpandWrapper(wrap, lets);
                if (synth != null) {
                    for (int i = synth.size() - 1; i >= 0; i--) {
                        work.addFirst(synth.get(i));
                    }
                    continue;
                }
                return new Outcome.Unsupported("harness wrapper '"
                        + simpleName(wrap.function())
                        + "' carries no zero-arg lambda body");
            }
            List<ValueSpecification> unrolledLoop = spliceForms(stmt);
            if (unrolledLoop != null) {
                for (int i = unrolledLoop.size() - 1; i >= 0; i--) {
                    work.addFirst(unrolledLoop.get(i));
                }
                continue;
            }
            // let name = rhs
            if (stmt instanceof AppliedFunction af && af.function().equals("letFunction")
                    && af.parameters().size() == 2
                    && af.parameters().get(0) instanceof CString name) {
                // bind-time folds: literal-if thunks + parse-through-
                // our-own-parser grammar strings (foldLiteralIf / clgArm)
                ValueSpecification rhs = clgArm(foldLiteralIf(
                        subst(af.parameters().get(1), lets)), lets);
                // #46 arms: generateTestData binding / literal read
                // inlining / plan-transparent executionPlan chain
                TdgLet tl = tdgLetArm(name, rhs, lets, tdg, planLets,
                        planText, ctx, imports, conn);
                if (tl.wall() != null) {
                    return tl.wall();
                }
                if (tl.consumed()) {
                    executed++;
                    continue;
                }
                rhs = requireNonNull(tl.rhs(), "tdg arm not rewritten");
                List<ValueSpecification> elq = ElqSplice.splice(name, rhs, lets);
                if (elq != null) {
                    for (int i = elq.size() - 1; i >= 0; i--) {
                        work.addFirst(elq.get(i));
                    }
                    continue;
                }
                // an execute() binding — or any read over one — forwards to
                // the PLATFORM's result frame (audit 19d B2). Forwarding is
                // EAGER (audit 16 F1, engine parity): the statement executor
                // runs the query AT the let, so a broken pipeline surfaces
                // even when no assert ever reads the binding.
                // let-arm HOST FOLDS (ConnEquality.letFold): JSON plumbing
                // defers, predicate verdicts bind, objectReferences build
                ValueSpecification lf0 = ConnEquality.letFold(rhs,
                        subst(rhs, lets), ctx, imports);
                if (lf0 != null) {
                    lets.put(name.value(), lf0);
                    continue;
                }
                java.util.function.Function<ValueSpecification, Object>
                        parsedEval = e2 -> {
                            try {
                                Object r = jsonValueOf(eval(e2, lets,
                                        execStmts, execVars, execChains, ctx,
                                        imports, runtimeFqn, conn));
                                return r == null ? "" : r;   // non-List = miss
                            } catch (java.sql.SQLException
                                    | com.legend.error.DataError
                                    | com.legend.error.AssertFailed se) {
                                throw new IllegalStateException(se);
                            }
                        };
                ValueSpecification exd = JsonAssertCanon.extractStrings(rhs,
                        parsedEval);
                if (exd != null) {
                    lets.put(name.value(), exd);
                    continue;
                }
                if (containsExecute(rhs) || referencesAny(rhs, execVars)) {
                    execStmts.add(new AppliedFunction("letFunction",
                            List.of(name, rhs)));
                    execVars.add(name.value());
                    recordExecChain(name.value(), rhs, execChains);
                    evalStatements(execStmts, ctx, imports, runtimeFqn, conn);
                    executed++;
                    continue;
                }
                Outcome sw = letSetupArm(rhs, lets, tdg, ctx, imports,
                        runtimeFqn, conn, seedFailures);
                if (sw != null) {
                    return sw;
                }
                // a PLAIN let carrying an inline testDataSetupCsv runtime
                // copy seeds NOW (engine: the test connection's own data;
                // the query that names this runtime sees it) — the
                // execute-binding path collects via evalStatements, this
                // arm covers the executeLegendQuery/from() shapes
                List<ValueSpecification[]> csvs = new ArrayList<>();
                collectInlineCsv(rhs, csvs);
                for (ValueSpecification[] csvExpr : csvs) {
                    seedInlineCsv(csvExpr, imports, ctx, conn);
                }
                lets.put(name.value(),
                        subst(purifiedSetup(rhs, ctx), lets));   // F3.2a
                continue;
            }
            // The per-driver golden idiom:
            //   $expected->map(p| let driver = $p.first; let expectedSql =
            //   $p.second; ...; assertEquals(...);)->distinct() == [true]
            // — HOST-side orchestration (the multi-statement lambda is
            // harness vocabulary, not a query). Every declared driver must
            // be H2: verifying an H2 subset of a multi-driver list would be
            // silent partial verification.
            if (stmt instanceof AppliedFunction eqf
                    && simpleName(eqf.function()).equals("equal")
                    && eqf.parameters().size() == 2) {
                List<AppliedFunction> pairs = new ArrayList<>();
                LambdaFunction perDriver = driverPairLoop(
                        eqf.parameters().get(0), lets, pairs);
                if (perDriver != null) {
                    int[] counters = {verified, advisory};
                    Outcome o = runPerDriverLoop(pairs, perDriver, lets,
                            execStmts, execVars, execChains, ctx, imports,
                            runtimeFqn, conn,
                            emptinessUnverifiable || seedFailures != null
                                    && !seedFailures.isEmpty(), counters);
                    verified = counters[0];
                    advisory = counters[1];
                    if (o != null) {
                        return o;
                    }
                    continue;
                }
            }
            if (stmt instanceof AppliedFunction af
                    && resolvesTo(af, ctx, ASSERT_FORM_FQNS)) {
                String failure = checkAssert(af, lets, execStmts, execVars,
                        execChains, ctx, imports,
                        runtimeFqn, conn, emptinessUnverifiable
                                || seedFailures != null && !seedFailures.isEmpty(),
                        tdg, planText);
                v7DualChannel(af, failure, lets, execStmts, execVars, ctx,
                        imports, runtimeFqn, conn,
                        tdg.keySet(), planText);
                int[] cs = {verified, advisory};
                Outcome oc = scoreAssert(af, failure, cs, sqlDiffs,
                        executed);
                verified = cs[0];
                advisory = cs[1];
                if (oc != null) {
                    return oc;
                }
                continue;
            }
            if (stmt instanceof CBoolean) {   // conventional trailing true
                continue;
            }
            // runtime-conditional if (RuntimeIfForm): branch re-enters
            if (RuntimeIfForm.splice(subst(stmt, lets), lets, execStmts,
                    execVars, execChains, ctx, imports, runtimeFqn, conn,
                    work)) {
                executed++;
                continue;
            }
            // assert loop over materialised values — AssertLoopForm
            if (stmt instanceof AppliedFunction mapAf
                    && AssertLoopForm.consume(mapAf, work, lets, execStmts,
                            execVars, execChains, ctx, imports,
                            runtimeFqn, conn)) {
                executed++;
                continue;
            }
            // K-natives arc (S4): any other EXPRESSION STATEMENT runs
            // through the platform (setup calls are ordinary pure code).
            // SQLExceptions propagate (honest ERROR); compile/type
            // failures report Unsupported (body data untrusted after).
            if (stmt instanceof AppliedFunction af3) {
                try {
                    // S4: no harness pre-inlining — the platform's TDG
                    // carrier folds dataCsvString/sqls reads itself
                    ValueSpecification sub = subst(stmt, lets);
                    ValueSpecification wrapped =
                            referencesAny(sub, execVars)
                                    ? new LambdaFunction(List.of(),
                                            append(execStmts, sub))
                                    : sub;
                    // F7.1 fail-loud: no per-statement sink — a failed
                    // setup statement throws (zero live firings on both
                    // full sweeps when the tolerance was deleted)
                    Compiler.executeResolved(
                            NameResolver.resolveQuery(wrapped,
                                    imports, ctx.elementFqns()),
                            ctx, runtimeFqn, conn);
                    executed++;
                    continue;
                    // (the seam: platform failures are AssertFailed/
                    // DataError — RuntimeExceptions that propagate to
                    // the runner's scorer exactly as the old rethrown
                    // SQLException did; the vacuous catch is gone)
                } catch (com.legend.error.NotImplementedException e) {
                    // a VOCABULARY gap — honestly SHAPE; any OTHER
                    // RuntimeException is a real pipeline defect and must
                    // surface as ERROR, not hide in the SHAPE bucket
                    // (audit 17) — it propagates to the runner's scorer
                    return new Outcome.Unsupported("statement '" + af3.function()
                            + "' failed through the pipeline: "
                            + String.valueOf(e.getMessage()).split("\\n")[0]);
                }
            }
            return new Outcome.Unsupported("unsupported statement: "
                    + stmt.getClass().getSimpleName());
        }
        return new Outcome.Ran(verified, advisory, executed, List.of(),
                List.copyOf(sqlDiffs));
    }

    private record Preamble(java.util.List<ValueSpecification> statements,
            @com.legend.Nullable Outcome lineage) {
    }

    /** FEATURE-TRACK preprocessing before statement routing:
     * validate(...) desugars to the engine's own synthesized query over
     * the ORDINARY execute path (#45 — before routing, so the exec-frame
     * machinery sees the execute binding; a fired desugar runs the body
     * with the addDriverTablePkForProject option, set FRESH every run),
     * and the canonical scanColumns lineage form (#44) routes whole to
     * the real analyzer (see LineageForm's why-not-K-natives note). */
    private static Preamble preamble(ModelContext ctx,
            java.util.List<ValueSpecification> statements,
            ImportScope imports, String runtimeFqn) {
        java.util.List<ValueSpecification> desugared =
                new ArrayList<>(statements.size());
        boolean fired = false;
        for (ValueSpecification s : statements) {
            ValueSpecification r = com.legend.validation.ValidateDesugar
                    .rewrite(s, ctx, imports.wildcards());
            desugared.add(r);
            fired |= r != s;
        }
        com.legend.validation.DriverPkOption.set(fired);
        Outcome lineage = LineageForm.tryRun(ctx, desugared, imports,
                runtimeFqn);
        if (lineage == null) {
            lineage = LineageRelationsForm.tryRun(ctx, desugared, imports,
                    runtimeFqn);
        }
        return new Preamble(desugared, lineage);
    }

    /** Fold {@code if(<literal>, |a, |b)} (zero-param thunks, one body
     * expression each) to the chosen branch — the checked/unchecked
     * helper idiom resolves to a plain query lambda. */
    /** The first zero-arg lambda among {@code wrap}'s arguments, looking
     * through let-bound variables and through
     * {@code meta::pure::router::preeval::preval(query, extensions)} — the
     * engine's PLAN-TIME pre-evaluation, identity for row semantics: the
     * wrapped query IS the query. */
    private static @com.legend.Nullable LambdaFunction zeroArgLambdaArg(
            AppliedFunction wrap, Map<String, ValueSpecification> lets) {
        for (ValueSpecification arg : wrap.parameters()) {
            ValueSpecification a2 = arg instanceof Variable av
                    && lets.get(av.name()) != null
                    ? lets.get(av.name()) : arg;
            if (a2 instanceof AppliedFunction pf
                    && pf.function().equals(
                            "meta::pure::router::preeval::preval")
                    && !pf.parameters().isEmpty()) {
                a2 = pf.parameters().get(0);
                if (a2 instanceof Variable av2
                        && lets.get(av2.name()) != null) {
                    a2 = lets.get(av2.name());
                }
            }
            if (a2 instanceof LambdaFunction lf0
                    && lf0.parameters().isEmpty()) {
                return lf0;
            }
        }
        return null;
    }

    private static @com.legend.Nullable ValueSpecification foldLiteralIf(ValueSpecification v) {
        while (v instanceof AppliedFunction f && f.function().equals("if")
                && f.parameters().size() == 3
                && f.parameters().get(0)
                        instanceof com.legend.protocol.spec.CBoolean b
                && f.parameters().get(1) instanceof LambdaFunction t
                && t.parameters().isEmpty() && t.body().size() == 1
                && f.parameters().get(2) instanceof LambdaFunction e
                && e.parameters().isEmpty() && e.body().size() == 1) {
            v = b.value() ? t.body().get(0) : e.body().get(0);
        }
        return v;
    }

    /** Strip JSON canonicalization wrappers (parseJSON / toPrettyJSONString)
     * from an assertJsonStringsEqual argument — the assert parses and
     * deep-compares both sides itself, so the wrappers are identity. */
    private static com.legend.protocol.spec.@com.legend.Nullable ValueSpecification stripJsonCanon(
            com.legend.protocol.spec.ValueSpecification v) {
        while (v instanceof com.legend.protocol.spec.AppliedFunction af
                && af.parameters().size() == 1
                && (af.function().equals("parseJSON")
                        || af.function().equals("toPrettyJSONString")
                        || af.function().endsWith("::parseJSON")
                        || af.function().endsWith("::toPrettyJSONString"))) {
            v = af.parameters().get(0);
        }
        return v;
    }

    /** One side of a JSON assert as a PARSED structure: a GRAPH result's
     * envelope, or a String value holding JSON text. Null = not JSON-shaped
     * (the caller reports Unsupported, never a false verdict). */
    private static @com.legend.Nullable Object jsonValueOf(Eval e) {
        if (e.result instanceof com.legend.exec.ExecutionResult.Graph g) {
            return com.legend.sql.Json.parse(g.json());
        }
        List<Object> vals = e.values();
        if (vals.size() == 1 && vals.get(0) instanceof String str) {
            try {
                // parseOne: real pure parseJSON reads the LEADING value
                // (a golden with stray text after the root still compares)
                return com.legend.sql.Json.parseOne(str);
            } catch (RuntimeException notJson) {
                return null;
            }
        }
        return null;
    }

    private static @com.legend.Nullable String abbreviate(String s) {
        return s.length() <= 160 ? s : s.substring(0, 157) + "...";
    }

    /** The elements of a CONSTANT string collection ({@code ['a'+'b', $x]}
     * with let-resolved, concat-folded elements), or null if any element
     * is not a compile-time string. */
    private static @com.legend.Nullable List<String> constantStrings(ValueSpecification v) {
        List<ValueSpecification> elems =
                v instanceof PureCollection pc ? pc.values() : List.of(v);
        List<String> out = new ArrayList<>(elems.size());
        for (ValueSpecification e : elems) {
            String sv = constantString(e);
            if (sv == null) {
                return null;
            }
            out.add(sv);
        }
        return out;
    }

    private static @com.legend.Nullable String constantString(ValueSpecification v) {
        if (v instanceof CString cs) {
            return cs.value();
        }
        if (v instanceof AppliedFunction af && af.parameters().size() == 2
                && ("plus".equals(af.function()) || "+".equals(af.function()))) {
            String l = constantString(af.parameters().get(0));
            String r = constantString(af.parameters().get(1));
            return l != null && r != null ? l + r : null;
        }
        if (v instanceof AppliedFunction af && "plus".equals(af.function())
                && af.parameters().size() == 1
                && af.parameters().get(0) instanceof PureCollection pc) {
            StringBuilder sb = new StringBuilder();
            for (ValueSpecification e : pc.values()) {
                String sv = constantString(e);
                if (sv == null) {
                    return null;
                }
                sb.append(sv);
            }
            return sb.toString();
        }
        return null;
    }

    /** One CSV seed block: {@code schema\ntable\nHEADER\nrows...} —
     * DROP + CREATE from the model's OWN table definition (engine
     * setUpDataSQLsV2 semantics: the test connection holds exactly the
     * CSV tables, so a bulk-seeded base table sharing the name cannot
     * shadow the family's — audit: 37 modelJoin binder errors), then
     * typed INSERTs ('default' schema is bare; empty tokens are NULL;
     * numerics ride bare, everything else quotes). */


    // ===== assert dispatch =====

    static final String UNSUPPORTED_MARKER = new String("unsupported");

    /** C0.3: the marker is an IDENTITY sentinel — the wall reason is lost
     * by construction. Sites that KNOW their reason set it via
     * {@link #unsupported}; the two marker consumers read and CLEAR it. */
    static final ThreadLocal<String> UNSUPPORTED_REASON = new ThreadLocal<>();

    static @com.legend.Nullable String unsupported(String reason) {
        UNSUPPORTED_REASON.set(reason);
        return UNSUPPORTED_MARKER;
    }

    private static @com.legend.Nullable String takeUnsupportedReason() {
        String why = UNSUPPORTED_REASON.get();
        UNSUPPORTED_REASON.remove();
        return why;
    }
    private static final String ADVISORY_MARKER = new String("advisory");
    private static final String NOT_TDG_MARKER = new String("not-tdg");

    /** The statement-splice forms, first match wins: per-driver golden
     * loops, result-var loops, the alloy fallback. */
    private static @com.legend.Nullable List<ValueSpecification> spliceForms(
            ValueSpecification stmt) {
        List<ValueSpecification> out = enumDriverLoop(stmt);
        if (out == null) {
            out = resultVarLoop(stmt);
        }
        if (out == null) {
            out = alloyFallback(stmt);
        }
        return out;
    }

    /** {@code mayExecuteAlloyTest(serverThunk, fallbackThunk)}: no Alloy
     * server exists in this environment, so the FALLBACK thunk's body
     * splices — the engine's own no-server CI takes the same branch
     * (usually {@code {|true}}). A non-lambda fallback returns null and
     * the statement walls loudly downstream. */
    private static @com.legend.Nullable List<ValueSpecification> alloyFallback(
            ValueSpecification stmt) {
        if (stmt instanceof AppliedFunction af
                && simpleName(af.function()).equals("mayExecuteAlloyTest")
                && af.parameters().size() == 2
                && af.parameters().get(1) instanceof LambdaFunction fb
                && fb.parameters().isEmpty()) {
            return new ArrayList<>(fb.body());
        }
        return null;
    }

    /** {@code meta::legend::compileLegendGrammar(<foldable string>)}
     * behind optional {@code ->at(i)}/{@code ->cast(@...)} wraps: parse
     * the grammar with the platform's own parser and return the selected
     * FunctionDefinition's BODY as a zero-arg lambda; any other shape
     * passes through untouched. */
    private static @com.legend.Nullable ValueSpecification clgArm(
            @com.legend.Nullable ValueSpecification rhs,
            Map<String, ValueSpecification> lets) {
        ValueSpecification cur = rhs;
        long idx = 0;
        while (cur instanceof AppliedFunction af
                && !af.parameters().isEmpty()) {
            String n = simpleName(af.function());
            if (n.equals("cast") || n.equals("toOne")) {
                cur = af.parameters().get(0);
            } else if (n.equals("at") && af.parameters().size() == 2
                    && af.parameters().get(1)
                            instanceof com.legend.protocol.spec.CInteger ci) {
                idx = ci.value().longValue();
                cur = af.parameters().get(0);
            } else {
                break;
            }
        }
        if (!(cur instanceof AppliedFunction clg)
                || !resolvesTo(clg, null, COMPILE_LEGEND_GRAMMAR_FQNS)
                || clg.parameters().size() != 1) {
            return rhs;
        }
        String src = TestDataGenForm.foldString(
                subst(clg.parameters().get(0), lets));
        if (src == null) {
            return rhs;
        }
        List<com.legend.model.FunctionDefinition> fns = new ArrayList<>();
        // engine's two-level seam: the payload is USER GRAMMAR by contract
        for (com.legend.model.PackageableElement el
                : com.legend.parser.ElementParser.parse(src,
                        // LegendCompile parses quote/eval payloads with the USER grammar
                        com.legend.parser.Dialect.LEGEND_ENGINE).elements()) {
            if (el instanceof com.legend.model.FunctionDefinition fd) {
                fns.add(fd);
            }
        }
        if (idx < 0 || idx >= fns.size()) {
            return rhs;
        }
        return new LambdaFunction(List.of(),
                new ArrayList<>(fns.get((int) idx).body()));
    }

    /** One assert's terminal outcome from its checkAssert result, or
     * null to continue; {@code counters} = {verified, advisory}. A
     * divergent golden text records into {@code sqlDiffs} — rows stay
     * the contract for tests that verify anything else; a test with NO
     * other verification fails on the diff (runner scoring). */
    /** assertContains(collection, value[, message…]) — real pure
     * membership (assertContains.pure:20); message args ignored. */
    private static @com.legend.Nullable String assertContainsCheck(List<ValueSpecification> args,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts, java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn,
            boolean emptinessUnverifiable) throws java.sql.SQLException {
        if (args.size() < 2) {
            return UNSUPPORTED_MARKER;
        }
        Eval col = eval(args.get(0), lets, execStmts, execVars, execChains,
                ctx, imports, runtimeFqn, conn);
        if (emptinessUnverifiable && col.size() == 0) {
            return UNSUPPORTED_MARKER;   // see the assertEquals guard
        }
        Eval val = eval(args.get(1), lets, execStmts, execVars, execChains,
                ctx, imports, runtimeFqn, conn);
        if (val.values().size() != 1) {
            return UNSUPPORTED_MARKER;
        }
        for (Object x : col.values()) {
            if (goldenEqualScalar(x, val.values().get(0))) {
                return null;
            }
        }
        return "assertContains: " + col.render()
                + " does not contain " + val.render();
    }

    private static @com.legend.Nullable Outcome scoreAssert(
            AppliedFunction af, @com.legend.Nullable String failure,
            int[] counters, List<String> sqlDiffs, int executed) {
        if (failure == UNSUPPORTED_MARKER) {
            String why = takeUnsupportedReason();
            // ATTRIBUTION (E2E burndown §2.1, goal #18 step 1b): when a
            // PLATFORM cause exists it is the PRIMARY message — the old
            // stamp buried it after an em-dash, so 82 of 95 SHAPE rows
            // read as harness gaps when they were platform walls (nobody
            // can prioritise a column that says "harness" and means it
            // 3 times). Only a genuinely bare marker is harness-shaped.
            return new Outcome.Unsupported(why != null
                    ? why + " [surfaced via assert form '" + af.function()
                            + "/" + af.parameters().size() + "']"
                    : "assert form '" + af.function() + "/"
                            + af.parameters().size()
                            + "' is not supported yet");
        }
        if (failure == ADVISORY_MARKER) {
            counters[1]++;
            return null;
        }
        if (failure != null && failure.startsWith("sql-text: ")) {
            counters[1]++;
            sqlDiffs.add(failure);
            return null;
        }
        counters[0]++;
        if (failure != null) {
            return new Outcome.Ran(counters[0], counters[1], executed,
                    List.of(failure));
        }
        return null;
    }

    /** The ENGINE's own contract for golden-SQL asserts: render the
     * SAME query through the toSQLString surface (the EngineStyleH2
     * dialect over the one SQL IR — a sibling of the DuckDB renderer,
     * no side-band conversion) and compare LITERALLY. Byte-exact match
     * verifies; a text diff falls back to the #67 H2 row-replay (rows
     * equal = execution-equivalent, SQL divergence stays visible in the
     * census); when neither verifies, the TEXT DIFF is the failure —
     * never a silent advisory skip. */
    /** The TDG sqls-text verify (the 49er replay): golden and OURS are
     * both FETCH texts. Byte match or row-equivalent replay = VERIFIED
     * (exec-pass; a divergent-text row match is the RESCUE, counted);
     * row divergence = exec-diverged (REAL); an unreplayable side keeps
     * the counted diff-noreplay with its cause. */
    private static @com.legend.Nullable String tdgSqlTextVerify(
            List<ValueSpecification> args,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts,
            java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        String golden = null;
        ValueSpecification actual = null;
        for (ValueSpecification a : args) {
            String sfold = TestDataGenForm.foldString(subst(a, lets));
            if (sfold == null && a instanceof AppliedFunction srf
                    && resolvesTo(srf, ctx, java.util.Set.of(
                            com.legend.compiler.spec.ResultEnvelopeSplice
                                    .SQL_REMOVE_FORMATTING_FQN))
                    && srf.parameters().size() == 1
                    && TestDataGenForm.foldString(
                            subst(srf.parameters().get(0), lets)) != null) {
                // the H2Compatible pair spells its goldens
                // sqlRemoveFormatting('literal') (testQualifier hop 0) —
                // the String overload is ordinary string code: evaluate
                // AS WRITTEN through the platform for the flattened
                // golden spelling (one owner of the flatten semantics)
                try {
                    if (evalScalar(subst(a, lets), lets, execStmts,
                            execVars, execChains, ctx, imports, runtimeFqn,
                            conn) instanceof String s0) {
                        sfold = s0;
                    }
                } catch (RuntimeException | java.sql.SQLException e) {
                    H2Verify.decline("golden sqlRemoveFormatting fold: "
                            + (e.getMessage() == null
                                    ? e.getClass().getSimpleName()
                                    : e.getMessage()));
                }
            }
            if (sfold != null && golden == null) {
                golden = sfold;
            } else {
                actual = a;
            }
        }
        String ours = evalSideText(actual, lets, execStmts, execVars,
                execChains, ctx, imports, runtimeFqn, conn);
        if (golden == null || ours == null) {
            // no two texts to referee — the generic path owns it
            return sqlTextVerify(args, lets, execStmts, execVars,
                    execChains, ctx, imports, runtimeFqn, conn);
        }
        // the (var, index) address of the actual side — the same at(i)
        // the test uses; cache the golden for DESCENDANT hops (a chained
        // hop's parent temp content IS the parent's golden result)
        TestDataGenForm.Read aread = actual == null ? null
                : readTdg(actual, lets);
        if (aread != null && "sqls".equals(aread.kind())
                && aread.index() >= 0) {
            TDG_GOLDENS.get().computeIfAbsent(aread.var(),
                    k -> new LinkedHashMap<>()).put(aread.index(), golden);
        }
        boolean match = golden.equals(ours);
        try {
            String rows = ours.contains("tdg_")
                    ? tdgChainedVerify(aread, golden, ours, actual, lets,
                            ctx, imports, conn)
                    : ReplayOracle.tdgSqlReplay(
                            com.legend.sql.dialect.RawSqlBoundary.recording(),
                            golden, conn, ours);
            if (rows == null) {
                if (!match) {
                    H2Verify.M1_RESCUED.increment();
                    H2Verify.verdict("rescued");
                }
                sqlTextOutcome("exec-pass");
                return null;
            }
            sqlTextOutcome("exec-diverged");
            return "h2-exec: " + rows;
        } catch (H2Verify.Unverifiable u) {
            sqlTextOutcome((match ? "match-noreplay" : "diff-noreplay")
                    + " :: " + u.getMessage());
            // batch 69: a matched text without a row replay is advisory
            return match ? ADVISORY_MARKER
                    : "sql-text: expected " + golden + ", got " + ours;
        }
    }

    /** Per-test TDG referee state (live-session refereeing, census §10o
     * leg 1), cleared at {@code run()} entry — the SQL_TEXT_OUTCOME
     * channel idiom. Goldens key by (let var, sqls index): a chained
     * hop's engine parent temp holds exactly its parent GOLDEN's rows,
     * so ancestor goldens cached from the test's own earlier asserts
     * (engine tests assert in index order) drive the mirror synthesis.
     * The transcript memo holds ONE referee-time platform generator run
     * per binding (the carrier's chartered per-statement re-evaluation
     * model; byte-equal sqls texts are the determinism receipt). */
    private static final ThreadLocal<Map<String, Map<Integer, String>>>
            TDG_GOLDENS = ThreadLocal.withInitial(LinkedHashMap::new);

    private static final ThreadLocal<Map<String,
            com.legend.testdatagen.TestDataGenerator.Result>>
            TDG_TRANSCRIPTS = ThreadLocal.withInitial(LinkedHashMap::new);

    /** The CHAINED-fetch verify: our recorded text references the
     * generator's {@code tdg_*} temps — session artifacts that dropped
     * with the generator's own finally (the engine drops its temps the
     * same way). OUR side referees by the LIVE-SESSION transcript (the
     * per-fetch rows the generator captured from the materialized temp
     * before dropping it); the GOLDEN side executes on the mirror with
     * its ancestor {@code testDataGen_Temp_*} tables synthesized from
     * the test's own earlier goldens. Every decline is a NAMED
     * {@link H2Verify.Unverifiable}. */
    private static @com.legend.Nullable String tdgChainedVerify(
            TestDataGenForm.@com.legend.Nullable Read aread, String golden,
            String ours, @com.legend.Nullable ValueSpecification actual,
            Map<String, ValueSpecification> lets, ModelContext ctx,
            ImportScope imports, Connection conn) {
        if (aread == null || !"sqls".equals(aread.kind())
                || aread.index() < 0) {
            throw new H2Verify.Unverifiable(
                    "chained fetch — sqls index unreadable", null);
        }
        com.legend.testdatagen.TestDataGenerator.Result r =
                TDG_TRANSCRIPTS.get().get(aread.var());
        if (r == null) {
            try {
                r = TestDataGenForm.transcript(subst(
                        java.util.Objects.requireNonNull(actual), lets),
                        ctx, imports, conn);
            } catch (com.legend.error.NotImplementedException
                    | java.sql.SQLException e) {
                throw new H2Verify.Unverifiable("chained fetch —"
                        + " transcript run: "
                        + firstLine(String.valueOf(e.getMessage())), e);
            }
            if (r == null) {
                throw new H2Verify.Unverifiable("chained fetch — no"
                        + " generateTestData call on the assert side", null);
            }
            TDG_TRANSCRIPTS.get().put(aread.var(), r);
        }
        List<com.legend.testdatagen.TestDataGenerator.Fetch> fetches =
                r.fetches();
        if (fetches == null || aread.index() >= fetches.size()) {
            throw new H2Verify.Unverifiable(
                    "chained fetch — transcript index out of range", null);
        }
        if (!r.sqls().get(aread.index()).equals(ours)) {
            throw new H2Verify.Unverifiable("chained fetch — transcript"
                    + " text mismatch (determinism receipt failed)", null);
        }
        com.legend.testdatagen.TestDataGenerator.Fetch f =
                fetches.get(aread.index());
        if (f.parentIndex() < 0) {
            throw new H2Verify.Unverifiable("chained fetch — view fetch"
                    + " over temps (no single-parent chain)", null);
        }
        List<Integer> chain = new ArrayList<>();
        for (int j = f.parentIndex(); j >= 0;
                j = fetches.get(j).parentIndex()) {
            chain.add(0, j);
        }
        Map<Integer, String> goldens = TDG_GOLDENS.get()
                .getOrDefault(aread.var(), Map.of());
        List<String[]> ancestors = new ArrayList<>();
        for (int j : chain) {
            String gj = goldens.get(j);
            if (gj == null) {
                throw new H2Verify.Unverifiable("chained fetch — ancestor"
                        + " golden (index " + j + ") not asserted before"
                        + " this hop", null);
            }
            ancestors.add(new String[]{
                    "testDataGen_Temp_" + fetches.get(j).table(), gj});
        }
        // receipt: this hop's golden must reference the temp the
        // transcript derives for its DIRECT parent — a mismatch means
        // the parentage model and the engine's spelling disagree
        String parentTemp = "testDataGen_Temp_"
                + fetches.get(f.parentIndex()).table();
        if (!golden.toLowerCase().contains(parentTemp.toLowerCase())) {
            throw new H2Verify.Unverifiable("chained fetch — golden does"
                    + " not reference derived parent temp " + parentTemp,
                    null);
        }
        return ReplayOracle.tdgChainedReplay(
                com.legend.sql.dialect.RawSqlBoundary.recording(),
                ancestors, golden,
                H2Verify.transcriptRows(f.columns(), f.rows()));
    }

    private static @com.legend.Nullable String sqlTextVerify(List<ValueSpecification> args,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts,
            java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        String golden = null;
        ValueSpecification actual = null;
        for (ValueSpecification a : args) {
            String s = TestDataGenForm.foldString(subst(a, lets));
            if (s != null && golden == null) {
                golden = s;
            } else {
                actual = a;
            }
        }
        long gt0 = System.nanoTime();   // GOLDEN_NANOS perf instrument
        String sql = evalSideText(actual, lets, execStmts,
                execVars, execChains, ctx, imports, runtimeFqn, conn);
        H2Verify.GOLDEN_NANOS.addAndGet(System.nanoTime() - gt0);
        if (golden == null && args.size() == 2 && sql != null) {
            // NO golden literal: the contract is the two sides' SQL being
            // IDENTICAL (slice-0-is-take shape) — both texts are OURS, so
            // the compare verifies without any engine-text parity
            String other = evalSideText(args.get(0), lets,
                    execStmts, execVars, execChains, ctx, imports,
                    runtimeFqn, conn);
            if (other != null) {
                sqlTextOutcome("both-ours");
                return other.equals(sql) ? null
                        : "sql sides differ: " + other + " vs " + sql;
            }
        }
        if (golden != null && sql != null) {
            if (golden.equals(sql)) {
                // MILESTONE 1 (H2_BACKEND.md §12.5): the matched text
                // IS our rendering — execute it on H2, hold its rows
                // to our DuckDB rows; a divergence is a REAL renderer
                // bug (H5.1 class), never advisory.
                String h2rows = h2Upgrade(args, lets, execStmts,
                        execVars, execChains, ctx, imports,
                        runtimeFqn, conn);
                if (h2rows == null) {
                    H2Verify.M1_VERIFIED.increment();
                    H2Verify.verdict("textmatch");
                    sqlTextOutcome("exec-pass");
                    return null;
                }
                if (java.util.Objects.equals(h2rows, ADVISORY_MARKER)) {
                    // batch 69 (user direction 2026-09-04: delete the
                    // harness code that does the platform's job): a
                    // byte-equal golden the referee could NOT replay is
                    // text agreement, not a row verdict — advisory,
                    // never counted verified (it was, as "match-noreplay")
                    H2Verify.M1_UNVERIFIABLE.increment();
                    sqlTextOutcome("match-noreplay");
                    return ADVISORY_MARKER;
                }
                H2Verify.M1_DIVERGED.increment();
                sqlTextOutcome("exec-diverged");
                return "h2-exec: OUR byte-matched SQL on H2 diverged"
                        + " from our DuckDB rows — " + h2rows;
            }
            // divergent text: execution-equivalence may still verify —
            // and a null return here is the RESCUE (rows matched despite
            // divergent text): counted (F2.2), never silent
            String rows = h2Upgrade(args, lets, execStmts, execVars,
                    execChains, ctx, imports, runtimeFqn, conn);
            if (rows != ADVISORY_MARKER) {
                if (rows == null) {
                    H2Verify.M1_RESCUED.increment();
                    H2Verify.verdict("rescued");
                    sqlTextOutcome("exec-pass");
                } else {
                    H2Verify.verdict("execfail");
                    sqlTextOutcome("exec-diverged");
                }
                return rows;
            }
            // burndown census: the decline CAUSE rides the outcome (the
            // replay attempt above recorded exactly one decline)
            String cause = H2Verify.LAST_DECLINE.get();
            sqlTextOutcome("diff-noreplay"
                    + (cause != null ? " :: " + cause : ""));
            if (System.getenv("LL_TMP_DEBUG") != null) {
                System.err.println("[diff-noreplay] ["
                        + H2Verify.CURRENT_TEST.get() + "]\n  golden: "
                        + golden + "\n  ours:   " + sql);
            }
            return "sql-text: expected " + golden + ", got " + sql;
        }
        // no reachable generator for OUR side — the golden may still
        // row-verify on H2 (rows vs our DuckDB rows)
        String tail = h2Upgrade(args, lets, execStmts, execVars, execChains,
                ctx, imports, runtimeFqn, conn);
        sqlTextOutcome(tail == null ? "exec-pass"
                : java.util.Objects.equals(tail, ADVISORY_MARKER)
                        ? "no-generator-noreplay" : "exec-diverged");
        return tail;
    }



    /** ONE decline channel for every h2-replay early-out (§12.4) —
     * printing and the per-reason census live in {@link H2Verify#decline}. */
    private static void h2Decline(String reason) {
        H2Verify.decline(reason);
    }

    /** OUR side's SQL text by REAL EVALUATION (slice 3, equality half):
     * a side that already spells a sql-producer call evaluates AS
     * WRITTEN (the splice folds activity-log reads; toSQLString runs as
     * the K-native; string transforms like ->replace apply natively); a
     * raw Result side evaluates through the corpus body's OWN
     * definition — assertSameSQL(s,r) ≡ assertEquals(s,
     * r->sqlRemoveFormatting()) (testAssert.pure:20) — spelled by exact
     * FQN, never a name. Replaces ExecCallFinder.sideSqlText's terminal
     * surgery (find the generator call, hand-rebuild a toSQLString
     * invocation). Null when the side still walls — the caller's
     * existing null-handling (golden-only replay / advisory) applies. */
    private static @com.legend.Nullable String evalSideText(
            @com.legend.Nullable ValueSpecification side,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts,
            java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn) {
        if (side == null) {
            return null;
        }
        // OUTCOME-driven, never shape-guessed: the side evaluates AS
        // WRITTEN first — a producer call, a let-bound rendered string,
        // a transform chain all yield their String directly (wrapping a
        // string side would strip formatting its golden expects). Only
        // a side whose value is NOT a string (the raw Result envelope of
        // assertSameSQL(s, $result)) evaluates through the corpus body's
        // own definition — ≡ $result->sqlRemoveFormatting()
        // (testAssert.pure:20), spelled by exact FQN.
        try {
            if (evalScalar(side, lets, execStmts, execVars, execChains,
                    ctx, imports, runtimeFqn, conn)
                    instanceof String s) {
                return s;
            }
            return evalScalar(new AppliedFunction(
                            com.legend.compiler.spec.ResultEnvelopeSplice
                                    .SQL_REMOVE_FORMATTING_FQN,
                            List.of(side)),
                    lets, execStmts, execVars, execChains,
                    ctx, imports, runtimeFqn, conn)
                    instanceof String s2 ? s2 : null;
        } catch (RuntimeException | java.sql.SQLException e) {
            // F2.3 discipline carried over from sideSqlText: a side that
            // cannot produce its text is a COUNTED decline, never a
            // silent null — the caller falls back to golden-only replay
            // or the named advisory
            H2Verify.decline("sql-text side: "
                    + (e.getMessage() == null
                            ? e.getClass().getSimpleName()
                            : e.getMessage()));
            return null;
        }
    }

    /** #67: a pure golden-SQL assert upgrades to ROW-VERIFIED when the
     * H2 second target can replay the test's raw seeds (recorded at the
     * RawSqlBoundary — H2-flavored BY DEFINITION) and execute the golden
     * on the engine's own dialect: golden-H2 rows vs our DuckDB rows,
     * order-insensitive. null = verified match (a REAL verification, not
     * a hollow pass); text = divergence FAIL; unverifiable inputs return
     * the advisory marker — exactly the pre-#67 behavior. */
    private static @com.legend.Nullable String h2Upgrade(List<ValueSpecification> args,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts,
            java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn) {
        if (!H2Verify.ready()
                || com.legend.sql.dialect.RawSqlBoundary.recording() == null
                || args.size() != 2) {
            // COUNTED decline (H2_BACKEND.md §12 step 4): these
            // early-outs were the two silent ADVISORY_MARKER paths —
            // without the print the sweep's unverifiable total lied low
            h2Decline(!H2Verify.ready() ? "h2 driver not ready"
                    : com.legend.sql.dialect.RawSqlBoundary.recording() == null
                            ? "no recorded seed statements"
                            : "assert arity " + args.size() + " != 2");
            return ADVISORY_MARKER;
        }
        String golden = null;
        ValueSpecification actual = null;
        for (ValueSpecification a : args) {
            String s = TestDataGenForm.foldString(subst(a, lets));
            if (s != null && golden == null) {
                golden = s;
            } else {
                actual = a;
            }
        }
        String var = actual == null ? null
                : rootExecVar(actual, execVars, lets);
        if (golden == null || var == null) {
            h2Decline(golden == null ? "no foldable golden string"
                    : "no root exec variable in the actual arg");
            return ADVISORY_MARKER;
        }
        try {
            Eval rows = eval(new AppliedProperty(
                    new Variable(var, null, null), "values"), lets,
                    execStmts, execVars, execChains, ctx, imports,
                    runtimeFqn, conn);
            // the Graph-frame per-key enum decode, TYPE-driven from the
            // class model + the exec call's mapping (the tabular
            // enumDecodeFor's label-mapped twin): null = not enum;
            // empty = enum with no derivable decode (counted decline);
            // else the golden's raw codes decode to the frame's names.
            com.legend.compiler.element.type.Type rt = rows.result()
                    .returnType();
            final var actualSide = actual;
            java.util.function.Function<String,
                    java.util.Map<String, String>> enumProp = key -> {
                if (!(rt instanceof com.legend.compiler.element.type.Type
                        .ClassType ct)) {
                    return null;
                }
                var prop = ctx.findProperty(ct.fqn(), key);
                if (prop.isEmpty() || !(prop.get().type() instanceof
                        com.legend.compiler.element.type.Type.EnumType et)) {
                    return null;
                }
                String mfqn = H2Verify.mappingFqnOf(actualSide, lets,
                        execStmts, ctx, imports);
                var dec = mfqn == null ? null
                        : H2Verify.decodeOf(ctx, mfqn, et.fqn());
                return dec == null ? java.util.Map.of() : dec;
            };
            // session-direct on an H2 backend, seed-replay elsewhere —
            // the routing lives with the oracle (ReplayOracle.verifyAuto)
            java.util.List<String> seeds =
                    com.legend.sql.dialect.RawSqlBoundary.recording();
            java.util.List<String> extra = null;
            if (golden.toLowerCase().contains("temptableforin_")) {
                extra = tempTableSeeds(golden,
                        actual, lets, execStmts, execVars, execChains, ctx,
                        imports, runtimeFqn, conn);
            }
            // row-13 adjudication (SQLTEXT charter §6.1, 2026-09-01):
            // the graph compare's golden-side fan-out collapse is gated
            // on the STATIC extent-subset fact of the exec-bound query
            // chain (the §7 order-policy doctrine applied to
            // multiplicity) — computed here, where the chain lives.
            // §7 proper: the STATIC order fact rides beside it — the
            // walk's own endsInSort judgment of the same chain gates
            // the oracle's in-order vs multiset compare.
            ValueSpecification qchain = execChains.get(var);
            H2Verify.EXTENT_SUBSET.set(extentSubset(qchain));
            boolean orderedQ = qchain != null
                    && endsInSort(orderView(qchain, execChains));
            H2Verify.ORDERED_QUERY.set(orderedQ);
            H2Verify.SORT_KEYS.set(orderedQ
                    ? sortKeyCols(orderView(qchain, execChains)) : null);
            try {
                return ReplayOracle.verifyAuto(conn,
                        seeds, extra, golden,
                        rows.result(), H2Verify.enumDecodeFor(rows.result(),
                                actual, lets, execStmts, ctx, imports), enumProp);
            } finally {
                H2Verify.EXTENT_SUBSET.remove();
                H2Verify.ORDERED_QUERY.remove();
                H2Verify.SORT_KEYS.remove();
            }
        } catch (java.sql.SQLException | RuntimeException e) {
            // audit (TENET V2.1): this decline was visible ONLY under
            // LL_H2_DEBUG — a row-verification opportunity silently fell
            // back to advisory. The fallback stays (pre-#67 status quo;
            // hardening it to FAIL waits on the CsvSeed producer fix),
            // but every sweep now COUNTS it: grep '\[h2-unverifiable\]'.
            h2Decline("replay/verify failed: "
                    + String.valueOf(e.getMessage()).replace('\n', ' '));
            return ADVISORY_MARKER;
        }
    }

    /** tempTableForIn CHAINED REPLAY (sql-exec burn 2026-08-30): the
     * engine's IN-collection strategy materializes a RUNTIME temp table
     * ({@code tempTableForIn_<letVar>}, one
     * {@code ColumnForStoringInCollection} column) from an in-lambda
     * let, and the golden references it — standalone replay finds no
     * table. The CONTENTS are fully derivable: evaluate the let's own
     * expression through OUR platform with the SAME exec call's
     * mapping/runtime and seed the table from the values. ONE temp var
     * per golden is the witnessed shape; anything else keeps the
     * counted decline. */
    private static java.util.@com.legend.Nullable List<String> tempTableSeeds(
            String golden, @com.legend.Nullable ValueSpecification actual,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts,
            java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn) {
        var m = java.util.regex.Pattern
                .compile("tempTableForIn_([A-Za-z0-9]+)")
                .matcher(golden);
        java.util.Set<String> vars = new java.util.LinkedHashSet<>();
        while (m.find()) {
            vars.add(m.group(1));
        }
        if (vars.size() != 1) {
            return null;
        }
        String var = vars.iterator().next();
        AppliedFunction exec = ExecCallFinder.find(actual, lets, execStmts);
        if (exec == null || exec.parameters().isEmpty()) {
            return null;
        }
        ValueSpecification q = subst(exec.parameters().get(0), lets);
        if (q instanceof AppliedFunction dq && dq.parameters().size() == 1
                && simpleName(dq.function()).equals("deferred")) {
            q = dq.parameters().get(0);
        }
        if (!(q instanceof LambdaFunction qlam)) {
            return null;
        }
        ValueSpecification expr = null;
        for (ValueSpecification b : qlam.body()) {
            if (b instanceof AppliedFunction lf
                    && simpleName(lf.function()).equals("letFunction")
                    && lf.parameters().size() == 2
                    && lf.parameters().get(0) instanceof CString cs
                    && cs.value().equals(var)) {
                expr = lf.parameters().get(1);
            }
        }
        if (expr == null && var.matches("\\d+")) {
            // NUMBERED temp table = an INLINE literal in-collection (the
            // engine numbers it by plan node); the values are the
            // query's own literals — unambiguous when exactly ONE
            // inline in() collection exists in the lambda
            java.util.List<PureCollection> colls = new ArrayList<>();
            collectInCollections(qlam, colls);
            if (colls.size() == 1) {
                return literalTempSeeds(var, colls.get(0));
            }
            return null;
        }
        if (expr == null) {
            return null;
        }
        List<ValueSpecification> ps = new ArrayList<>(exec.parameters());
        ps.set(0, new LambdaFunction(java.util.List.of(),
                java.util.List.of(expr), null));
        AppliedFunction subExec = new AppliedFunction(exec.function(), ps,
                exec.candidateFqns(), null, false, false, false);
        try {
            Eval fed = eval(subExec,
                    lets, execStmts, execVars, execChains, ctx, imports,
                    runtimeFqn, conn);
            var res = fed.result();
            if (res == null || res.columns().size() != 1) {
                return null;
            }
            java.util.List<Object> vals = new ArrayList<>();
            for (com.legend.exec.Row r : res.rows()) {
                vals.add(r.values().get(0));
            }
            boolean allNum = vals.stream().allMatch(v ->
                    v instanceof Long || v instanceof Integer);
            boolean allStr = vals.stream().allMatch(v -> v instanceof String);
            if (!allNum && !allStr) {
                return null;   // witnessed shapes only: ints or strings
            }
            java.util.List<String> out = new ArrayList<>();
            // drop-first (§9a cursor fix): extras re-execute on the LIVE
            // family mirror on every verify — creation must be
            // re-runnable
            out.add("DROP TABLE IF EXISTS tempTableForIn_" + var);
            out.add("CREATE LOCAL TEMPORARY TABLE tempTableForIn_" + var
                    + " (ColumnForStoringInCollection "
                    + (allStr ? "VARCHAR(1024)" : "BIGINT") + ")");
            for (Object v : vals) {
                out.add("INSERT INTO tempTableForIn_" + var + " VALUES ("
                        + (allStr ? "'" + String.valueOf(v)
                                .replace("'", "''") + "'"
                                  : String.valueOf(v)) + ")");
            }
            return out;
        } catch (java.sql.SQLException | RuntimeException e) {
            return null;   // derivation failed — the counted decline stands
        }
    }

    private static void collectInCollections(ValueSpecification v,
            java.util.List<PureCollection> out) {
        if (v instanceof AppliedFunction af
                && simpleName(af.function()).equals("in")
                && af.parameters().size() == 2
                && af.parameters().get(1) instanceof PureCollection pc) {
            out.add(pc);
        }
        for (ValueSpecification c : childrenOfSpec(v)) {
            collectInCollections(c, out);
        }
    }

    private static java.util.List<ValueSpecification> childrenOfSpec(
            ValueSpecification v) {
        if (v instanceof AppliedFunction af) {
            return af.parameters();
        }
        if (v instanceof AppliedProperty ap) {
            return java.util.List.of(ap.receiver());
        }
        if (v instanceof LambdaFunction lf) {
            return lf.body();
        }
        if (v instanceof PureCollection pc) {
            return pc.values();
        }
        return java.util.List.of();
    }

    /** Seeds for a NUMBERED temp table from the inline literal
     * collection (dates/datetimes/strings/ints — the witnessed kinds). */
    private static java.util.@com.legend.Nullable List<String> literalTempSeeds(
            String var, PureCollection pc) {
        java.util.List<String> lits = new ArrayList<>();
        String colType = null;
        for (ValueSpecification v : pc.values()) {
            if (v instanceof com.legend.protocol.spec.CDate cd) {
                // GMT offset suffixes strip — pure date literals are
                // UTC and H2's TIMESTAMP parser takes the bare form
                String t = cd.value().toString()
                        .replaceAll("(\\+0000|Z)$", "");
                boolean timed = t.contains("T");
                colType = timed ? "TIMESTAMP" : "DATE";
                lits.add((timed ? "TIMESTAMP '" : "DATE '")
                        + t.replace('T', ' ') + "'");
            } else if (v instanceof CString cs) {
                colType = "VARCHAR(1024)";
                lits.add("'" + cs.value().replace("'", "''") + "'");
            } else if (v instanceof CInteger ci) {
                colType = "BIGINT";
                lits.add(String.valueOf(ci.value()));
            } else {
                return null;   // unwitnessed literal kind — keep decline
            }
        }
        if (colType == null) {
            return null;
        }
        java.util.List<String> out = new ArrayList<>();
        // drop-first (§9a cursor fix) — extras re-execute on the LIVE
        // family mirror every verify; BOTH synthesizer paths must be
        // re-runnable (the numbered path missing this was the 2-row
        // exec-passing residue under the provisioning experiment:
        // 'Table "TEMPTABLEFORIN_4" already exists')
        out.add("DROP TABLE IF EXISTS tempTableForIn_" + var);
        out.add("CREATE LOCAL TEMPORARY TABLE tempTableForIn_" + var
                + " (ColumnForStoringInCollection " + colType + ")");
        for (String l : lits) {
            out.add("INSERT INTO tempTableForIn_" + var + " VALUES ("
                    + l + ")");
        }
        return out;
    }

    /** {@code assertEquals/assertSameElements(cols, pkOfFunc(fnRef))} —
     * PK auto-inference (#78): the referenced corpus function's parsed
     * body walks through {@link com.legend.lineage.PkInference}; list
     * equality VERIFIES (assertSameElements order-insensitively). */
    private static @com.legend.Nullable String pkAssert(AppliedFunction af,
            List<ValueSpecification> args, ModelContext ctx) {
        String fn = simpleName(af.function());
        if (!(fn.equals("assertEquals") || fn.equals("assertSameElements"))
                || args.size() != 2) {
            return NOT_TDG_MARKER;
        }
        AppliedFunction pk = args.get(1) instanceof AppliedFunction c
                && simpleName(c.function()).equals("pkOfFunc") ? c : null;
        if (pk == null || pk.parameters().size() != 1
                || !(pk.parameters().get(0)
                        instanceof com.legend.protocol.spec
                                .PackageableElementPtr ptr)) {
            return NOT_TDG_MARKER;
        }
        String path = ptr.fullPath();
        int mangle = path.indexOf("__");
        String fqn = mangle > 0 ? path.substring(0, mangle) : path;
        var fd = ctx.findFunctionDefinition(fqn);
        if (fd.isEmpty() || fd.get().body().isEmpty()) {
            return NOT_TDG_MARKER;
        }
        List<String> got = com.legend.lineage.PkInference.infer(ctx,
                fd.get().body().get(0));
        List<String> expected = new ArrayList<>();
        ValueSpecification e = args.get(0);
        List<ValueSpecification> items = e instanceof PureCollection pc
                ? pc.values() : List.of(e);
        for (ValueSpecification it : items) {
            if (!(it instanceof CString cs)) {
                return NOT_TDG_MARKER;
            }
            expected.add(cs.value());
        }
        boolean ok = fn.equals("assertSameElements")
                ? new java.util.HashSet<>(expected)
                        .equals(new java.util.HashSet<>(got))
                : expected.equals(got);
        return ok ? null : "pkOfFunc: expected " + expected + ", got " + got;
    }


    static boolean walkHasProp(@com.legend.Nullable ValueSpecification v, String name) {
        if (v instanceof AppliedProperty ap) {
            return name.equals(ap.property())
                    || walkHasProp(ap.receiver(), name);
        }
        if (v instanceof AppliedFunction af) {
            return af.parameters().stream()
                    .anyMatch(x -> walkHasProp(x, name));
        }
        return false;
    }

    static boolean walkHasCall(@com.legend.Nullable ValueSpecification v) {
        if (v instanceof AppliedFunction af) {
            return simpleName(af.function()).equals("executionPlan")
                    || af.parameters().stream()
                            .anyMatch(EngineTestExecutor::walkHasCall);
        }
        if (v instanceof AppliedProperty ap) {
            return walkHasCall(ap.receiver());
        }
        return false;
    }




    /** The exec-frame variable an expression reads through (receiver /
     * first-arg chains), or null. */
    private static @com.legend.Nullable String rootExecVar(ValueSpecification v,
            java.util.Set<String> execVars,
            Map<String, ValueSpecification> lets) {
        v = substitute(v, lets);
        while (true) {
            if (v instanceof Variable var) {
                return execVars.contains(var.name()) ? var.name() : null;
            }
            if (v instanceof AppliedProperty ap) {
                v = ap.receiver();
            } else if (v instanceof AppliedFunction af
                    && !af.parameters().isEmpty()) {
                v = af.parameters().get(0);
            } else {
                return null;
            }
        }
    }

    /** #46 let-arm result: a wall, a consumed binding, or a (possibly
     * rewritten) rhs for the ordinary let path. */
    private record TdgLet(@com.legend.Nullable Outcome wall, @com.legend.Nullable ValueSpecification rhs,
            boolean consumed) {
    }

    /** A let-bound SETUP HELPER (a corpus function whose body issues
     * executeInDb DDL/inserts — {@code let runtime = model::setUp()})
     * runs NOW for its side effects through the platform; the binding
     * itself still rides lazily (its value is the runtime handle).
     * Returns null normally, an Outcome wall on compile failure. */
    private static @com.legend.Nullable Outcome letSetupArm(ValueSpecification rhs,
            Map<String, ValueSpecification> lets,
            Map<String, com.legend.testdatagen.TestDataGenerator.Result> tdg,
            ModelContext ctx, ImportScope imports, String runtimeFqn,
            Connection conn, List<String> seedFailures)
            throws java.sql.SQLException {
        if (!(rhs instanceof AppliedFunction af)) {
            return null;
        }
        var fd = ctx.findFunctionDefinition(af.function());
        if (fd.isEmpty()) {
            for (String c : af.candidateFqns()) {
                fd = ctx.findFunctionDefinition(c);
                if (fd.isPresent()) {
                    break;
                }
            }
        }
        if (fd.isEmpty() || !hasExecuteInDb(fd.get().body())) {
            return null;
        }
        try {
            Compiler.executeResolved(NameResolver.resolveQuery(
                    subst(rhs, lets), imports, ctx.elementFqns()),
                    ctx, runtimeFqn, conn);
            return null;
        } catch (com.legend.error.NotImplementedException
                | com.legend.error.LegendCompileException e) {
            return new Outcome.Unsupported("let-bound setup: "
                    + String.valueOf(e.getMessage()).split("\\n")[0]);
        }
    }

    /** The lazy binding a RAN setup helper leaves behind: its RETURN
     * EXPRESSION (body's last statement, own lets substituted forward,
     * executed side-effect statements dropped). A raw multi-statement
     * call would hit the inliner's non-let wall when a consumer reads
     * the binding — but the statements already ran through the platform
     * (letSetupArm), so the value IS the remainder. 0-arg helpers only;
     * anything else keeps the raw call (walls stay honest). */
    private static @com.legend.Nullable ValueSpecification purifiedSetup(ValueSpecification rhs,
            ModelContext ctx) {
        if (!(rhs instanceof AppliedFunction af)
                || !af.parameters().isEmpty()) {
            return rhs;
        }
        var fd = ctx.findFunctionDefinition(af.function());
        if (fd.isEmpty()) {
            for (String c : af.candidateFqns()) {
                fd = ctx.findFunctionDefinition(c);
                if (fd.isPresent()) {
                    break;
                }
            }
        }
        if (fd.isEmpty() || fd.get().body().isEmpty()
                || !fd.get().parameters().isEmpty()) {
            return rhs;
        }
        List<ValueSpecification> body = fd.get().body();
        // ONLY the genuine setup shape purifies: statement-position
        // executeInDb side effects (the setUp() DDL/seed idiom). An
        // extension BUILDER whose executeInDb hides inside constructor
        // lambdas keeps its raw call — inlining its body would drag
        // module-private references into the consumer's compile scope.
        boolean setupShape = false;
        for (int i = 0; i < body.size() - 1; i++) {
            if (body.get(i) instanceof AppliedFunction sf
                    && !sf.function().equals("letFunction")
                    && hasExecuteInDb(List.of(body.get(i)))) {
                setupShape = true;
                break;
            }
        }
        if (!setupShape) {
            return rhs;
        }
        Map<String, ValueSpecification> inner = new java.util.LinkedHashMap<>();
        for (int i = 0; i < body.size() - 1; i++) {
            if (body.get(i) instanceof AppliedFunction lf
                    && lf.function().equals("letFunction")
                    && lf.parameters().size() == 2
                    && lf.parameters().get(0) instanceof CString ln) {
                inner.put(ln.value(),
                        substitute(lf.parameters().get(1), inner));
            }
            // non-let side-effect statements already executed — dropped
        }
        ValueSpecification last = body.get(body.size() - 1);
        if (last instanceof AppliedFunction lf2
                && lf2.function().equals("letFunction")
                && lf2.parameters().size() == 2) {
            last = lf2.parameters().get(1);
        }
        return substitute(last, inner);
    }

    private static boolean hasExecuteInDb(List<ValueSpecification> body) {
        for (ValueSpecification v : body) {
            if (v instanceof AppliedFunction af
                    && (simpleName(af.function()).equals("executeInDb")
                            || hasExecuteInDb(af.parameters()))) {
                return true;
            }
            if (v instanceof AppliedFunction af2
                    && hasExecuteInDb(af2.parameters())) {
                return true;
            }
        }
        return false;
    }

    /** Test-level lets the plan lambda reads, injected as LEADING
     * lambda-local lets in first-use order (engine inScopeVars — each
     * prints as an Allocation node). */
    private static @com.legend.Nullable AppliedFunction injectOpenLets(AppliedFunction ep,
            Map<String, ValueSpecification> lets) {
        if (!(ep.parameters().get(0) instanceof LambdaFunction plam)) {
            return ep;
        }
        java.util.LinkedHashSet<String> open = new java.util.LinkedHashSet<>();
        java.util.Set<String> bound = new java.util.HashSet<>();
        plam.parameters().forEach(p -> bound.add(p.name()));
        for (ValueSpecification st : plam.body()) {
            collectOpenVars(st, lets.keySet(), bound, open);
            if (st instanceof AppliedFunction lfn
                    && lfn.function().equals("letFunction")
                    && lfn.parameters().size() == 2
                    && lfn.parameters().get(0) instanceof CString ln) {
                bound.add(ln.value());
            }
        }
        if (open.isEmpty()) {
            return ep;
        }
        List<ValueSpecification> body = new ArrayList<>();
        for (String n : open) {
            body.add(new AppliedFunction("letFunction", List.of(
                    new CString(n), substitute(lets.get(n), lets))));
        }
        body.addAll(plam.body());
        List<ValueSpecification> ps = new ArrayList<>(ep.parameters());
        ps.set(0, new LambdaFunction(plam.parameters(), body));
        return ep.withParameters(ps);
    }

    private static void collectOpenVars(ValueSpecification v,
            java.util.Set<String> lets, java.util.Set<String> bound,
            java.util.LinkedHashSet<String> out) {
        switch (v) {
            case Variable var -> {
                if (lets.contains(var.name()) && !bound.contains(var.name())) {
                    out.add(var.name());
                }
            }
            case AppliedFunction af -> af.parameters()
                    .forEach(x -> collectOpenVars(x, lets, bound, out));
            case AppliedProperty ap -> collectOpenVars(ap.receiver(),
                    lets, bound, out);
            case PureCollection pc -> pc.values()
                    .forEach(x -> collectOpenVars(x, lets, bound, out));
            case LambdaFunction lf -> {
                java.util.Set<String> inner = new java.util.HashSet<>(bound);
                lf.parameters().forEach(p -> inner.add(p.name()));
                lf.body().forEach(x -> collectOpenVars(x, lets, inner, out));
            }
            case NewInstance ni -> ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::expression).toList().forEach(
                    ke -> collectOpenVars(ke.value(), lets, bound, out));
            default -> {
            }
        }
    }

    /** The #46 let-arm rewrites: a generateTestData binding runs NOW
     * (setup statements above already executed — engine parity, all data
     * work in the database); testDataGen reads inline as literals so the
     * corpus's loadAndTestExecution tail runs through the platform
     * unchanged; executionPlan bindings are PLAN-TRANSPARENT — the handle
     * only ever flows into {@code $plan->execute(...)}, which re-forms as
     * the execute native (identical row semantics; plan text is never
     * inspected here). */
    private static TdgLet tdgLetArm(CString name, @com.legend.Nullable ValueSpecification rhs,
            Map<String, ValueSpecification> lets,
            Map<String, com.legend.testdatagen.TestDataGenerator.Result> tdg,
            Map<String, AppliedFunction> planLets,
            java.util.Set<String> planText, ModelContext ctx,
            ImportScope imports, Connection conn)
            throws java.sql.SQLException {
        if (TestDataGenForm.hasPlanGenerate(rhs)) {
            // the binding rides lets so a plan-text assert can
            // substitute $plan back to the planTestDataGeneration call
            // (checkTdgAssert builds the MultiResultSequence text);
            // wrapper-only tests that never read the plan keep their
            // engine-parity pass
            planText.add(name.value());
            lets.put(name.value(), subst(rhs, lets));   // F3.2a
            return new TdgLet(null, null, true);
        }
        // generateSeedDataString is a PLATFORM construct now (S3 tail):
        // the let rides the ordinary lazy path (carrier fold).
        // csvCensus (getRelationalCSVDataFromQuery) is a PLATFORM
        // construct now (TDG lane S1): the CHECKER folds it to instance
        // literals (CsvCensusChecker), so the let rides the ordinary
        // lazy path and its navigation lowers.
        if (TestDataGenForm.hasGenerate(rhs)) {
            // S2: the binding FLOWS to the platform (the checker's carrier
            // executes the extraction and splices literals — size and
            // row-contract asserts route as REAL verdicts); the harness
            // copy stays ONLY for the sqls-TEXT advisory (S3 converts it,
            // S4 deletes this arm).
            // S4: NO duplicate generator run — the map entry is a
            // NAME-ONLY classifier (the platform carrier executes the
            // one real extraction)
            tdg.put(name.value(), TestDataGenForm.NAME_ONLY);
            return new TdgLet(null, rhs, false);
        }
        // (S4: no harness pre-inlining — the platform carrier folds reads)
        if (rhs instanceof AppliedFunction ep
                && simpleName(ep.function()).equals("executionPlan")
                && (ep.function().equals("executionPlan")
                        || ep.function().startsWith("meta::"))
                && ep.parameters().size() >= 3) {
            // OPEN VARIABLES become Allocations (engine inScopeVars): a
            // test-level let the plan lambda reads is injected as a
            // LAMBDA-LOCAL leading let — the local shadows the outer
            // binding under substitute(), so the plan printer sees the
            // let (name + value) instead of an inlined literal
            ep = injectOpenLets(ep, lets);
            // recorded for the plan->execute desugar; the binding ALSO
            // rides the ordinary lazy let so planToString reads type
            // through the platform (the #47 plan-text K-native)
            planLets.put(name.value(), ep);
            return new TdgLet(null, ep, false);
        }
        // the plan binding ALSO rides the lazy lets (planToString typing),
        // so rhs arrives with $plan already substituted to the
        // executionPlan CALL — match either spelling
        AppliedFunction planSrc = null;
        if (rhs instanceof AppliedFunction pe0
                && simpleName(pe0.function()).equals("execute")
                && !pe0.parameters().isEmpty()) {
            ValueSpecification p0 = pe0.parameters().get(0);
            if (p0 instanceof Variable pv && planLets.containsKey(pv.name())) {
                planSrc = planLets.get(pv.name());
            } else if (p0 instanceof AppliedFunction epc
                    && simpleName(epc.function()).equals("executionPlan")
                    && epc.parameters().size() >= 3) {
                planSrc = epc;
            }
        }
        if (planSrc != null && rhs instanceof AppliedFunction pe) {
            if (pe.parameters().size() >= 2
                    && !(substitute(pe.parameters().get(1), lets)
                            instanceof PureCollection epc
                            && epc.values().isEmpty())) {
                return new TdgLet(new Outcome.Unsupported(
                        "plan->execute with bound parameters"), null, false);
            }
            AppliedFunction plan = planSrc;
            rhs = new AppliedFunction("execute",
                    List.of(plan.parameters().get(0),
                            plan.parameters().get(1),
                            plan.parameters().get(2),
                            plan.parameters().size() > 3
                                    ? plan.parameters().get(3)
                                    : new PureCollection(List.of())));
            // rides the caller's exec-forward arm
        }
        return new TdgLet(null, rhs, false);
    }

    /** Plan-text literal compare (toSQLString doctrine) with NAMED walls
     * staying SHAPE. 3-arg H2Compatible = (legacy, h2New, actual): the
     * ACTUAL is always LAST, and EITHER golden may match (h2New is our
     * own dialect generation). */


    /** testDataGen assert arms (#46): assertTestData is the ROW contract
     * (typed set compare in the database), .sqls text is engine H2 SQL —
     * advisory (the golden-SQL doctrine), .sqls COUNTS verify. Returns
     * {@link #NOT_TDG_MARKER} when the assert doesn't touch a
     * generateTestData binding. */
    /** {@code read} on the RAW arg first (S2: generate bindings ride the
     * lets now, so substitution inlines the call and erases the Variable
     * the reader keys on), falling back to the substituted form for
     * lets-of-lets spellings. */
    private static TestDataGenForm.@com.legend.Nullable Read readTdg(
            ValueSpecification a, Map<String, ValueSpecification> lets) {
        TestDataGenForm.Read r = TestDataGenForm.read(a);
        return r != null ? r : TestDataGenForm.read(subst(a, lets));
    }

    private static @com.legend.Nullable String checkTdgAssert(AppliedFunction af,
            List<ValueSpecification> args,
            Map<String, ValueSpecification> lets,
            Map<String, com.legend.testdatagen.TestDataGenerator.Result> tdg,
            java.util.Set<String> planText,
            List<ValueSpecification> execStmts, java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        switch (simpleName(af.function())) {
            case "assertTestData" -> {
                // S2: a USER function over platform-owned natives
                // (setUpDataSQLs + assertSameElements) — the platform IS
                // the implementation, so the host verdict is its own
                // evaluation (failures surface through the DB error
                // channel, the verdict-in-DB idiom)
                try {
                    evalSpliced(subst(af, lets), execStmts, execVars, ctx,
                            imports, runtimeFqn, conn);
                    return null;
                } catch (java.sql.SQLException
                        | com.legend.error.DataError
                        | com.legend.error.AssertFailed fail) {
                    // the seam: platform failures arrive as AssertFailed/
                    // DataError now — same verdict classification
                    return "assertTestData: " + firstLine(fail.getMessage());
                } catch (com.legend.error.NotImplementedException wall) {
                    return UNSUPPORTED_MARKER;
                }
            }
            case "assertSqlEquals" -> {
                TestDataGenForm.Read r = readTdg(args.size() == 2
                        ? args.get(1) : args.get(0), lets);
                if (r == null || !tdg.containsKey(r.var())) {
                    return UNSUPPORTED_MARKER;
                }
                // S3 + 49er replay: byte compare, then BOTH fetch texts
                // execute (golden on the H2 mirror, ours on DuckDB) and
                // rows referee — outcome-bucketed
                return tdgSqlTextVerify(args, lets, execStmts, execVars,
                        execChains, ctx, imports, runtimeFqn, conn);
            }
            default -> {
            }
        }
        if (!tdg.isEmpty() && !args.isEmpty()) {
            TestDataGenForm.Read r0 = readTdg(args.get(0), lets);
            if (r0 != null && tdg.containsKey(r0.var())
                    && "sqls".equals(r0.kind())) {
                if (simpleName(af.function()).equals("assertSize")
                        && args.size() == 2) {
                    return NOT_TDG_MARKER;   // S2: the COUNT routes
                }
                // S3 + 49er replay (see tdgSqlTextVerify)
                return tdgSqlTextVerify(args, lets, execStmts, execVars,
                        execChains, ctx, imports, runtimeFqn, conn);
            }
            for (ValueSpecification a : args) {
                TestDataGenForm.Read r = readTdg(a, lets);
                if (r != null && tdg.containsKey(r.var())
                        && "sqls".equals(r.kind())) {
                    return tdgSqlTextVerify(args, lets, execStmts,
                            execVars, execChains, ctx, imports,
                            runtimeFqn, conn);
                }
            }
        }
        // generateSeedDataString ROUTES (S3 tail): the carrier folds it
        // to a string literal and the assert compares in the DB.
        if (!planText.isEmpty()) {
            for (ValueSpecification arg : args) {
                if (referencesAnyVar(arg, planText)) {
                    String text;
                    try {
                        text = TestDataGenForm.planText(
                                subst(arg, lets), ctx, imports);
                    } catch (com.legend.error.NotImplementedException e) {
                        if (System.getenv("LL_TMP_DEBUG") != null) {
                            System.err.println("[tdg-plan-wall] " + e);
                        }
                        return unsupported(String.valueOf(
                                e.getMessage()).split("\\n")[0]);
                    }
                    if (text == null) {
                        return UNSUPPORTED_MARKER;
                    }
                    // literal plan-text compare — EITHER golden of the
                    // H2Compatible pair may match
                    for (ValueSpecification g : args) {
                        if (g == arg) {
                            continue;
                        }
                        if (text.equals(TestDataGenForm.foldString(
                                subst(g, lets)))) {
                            return null;
                        }
                    }
                    return "assertEquals: expected "
                            + TestDataGenForm.foldString(
                                    subst(args.get(0), lets))
                            + ", got " + text;
                }
            }
        }
        return NOT_TDG_MARKER;
    }

    /** getRelationalCSVDataFromQuery reads: {@code $x.tables->size()}
     * and the schema/table/values map-join idiom — host-side over the
     * census triples. */
    /** The lambda body's property-read names in source order (the
     * census join idiom pin — anything else stays a wall). */
    private static @com.legend.Nullable List<String> propertyReadOrder(LambdaFunction ml) {
        List<String> out = new ArrayList<>();
        java.util.ArrayDeque<ValueSpecification> work =
                new java.util.ArrayDeque<>(ml.body());
        while (!work.isEmpty()) {
            ValueSpecification v = work.poll();
            if (v instanceof AppliedProperty ap) {
                out.add(ap.property());
            } else if (v instanceof AppliedFunction f) {
                // left-to-right over plus chains
                for (int i = f.parameters().size() - 1; i >= 0; i--) {
                    work.addFirst(f.parameters().get(i));
                }
            } else if (v instanceof PureCollection pc) {
                for (int i = pc.values().size() - 1; i >= 0; i--) {
                    work.addFirst(pc.values().get(i));
                }
            }
        }
        return out;
    }

    private static boolean referencesAnyVar(ValueSpecification v,
            java.util.Set<String> names) {
        if (v instanceof Variable var) {
            return names.contains(var.name());
        }
        if (v instanceof AppliedFunction af2) {
            for (ValueSpecification p : af2.parameters()) {
                if (referencesAnyVar(p, names)) {
                    return true;
                }
            }
        } else if (v instanceof AppliedProperty ap) {
            return referencesAnyVar(ap.receiver(), names);
        } else if (v instanceof PureCollection pc) {
            for (ValueSpecification e : pc.values()) {
                if (referencesAnyVar(e, names)) {
                    return true;
                }
            }
        } else if (v instanceof LambdaFunction lf) {
            for (ValueSpecification b2 : lf.body()) {
                if (referencesAnyVar(b2, names)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** null = held; ADVISORY_MARKER = golden-SQL; UNSUPPORTED_MARKER; else the failure text. */
    private static @com.legend.Nullable String checkAssert(AppliedFunction af,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts, java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn,
            boolean emptinessUnverifiable,
            Map<String, com.legend.testdatagen.TestDataGenerator.Result> tdg,
            java.util.Set<String> planText)
            throws java.sql.SQLException {
        List<ValueSpecification> args = af.parameters();
        // the sql-text OUTCOME is PER ASSERT: an earlier assert whose
        // classification returned early (plan-let) must not lend its
        // "plan-literal" to the next one (batch 72b: a stale outcome
        // moved testGroupByWithJoinDB2 between lanes with the run order)
        SQL_TEXT_OUTCOME.remove();
        // testDataGen reads (#46) route to the bound generator result —
        // extracted arm (checkAssert length guardrail)
        String tdgOut = checkTdgAssert(af, args, lets, tdg, planText,
                execStmts, execVars, execChains, ctx, imports, runtimeFqn,
                conn);
        if (tdgOut != NOT_TDG_MARKER) {
            return tdgOut;
        }
        String pkOut = pkAssert(af, args, ctx);
        if (pkOut != NOT_TDG_MARKER) {
            return pkOut;
        }
        switch (simpleName(af.function())) {
            case "assert", "assertFalse" -> {
                if (args.isEmpty()) {
                    return UNSUPPORTED_MARKER;
                }
                if (PlanAsserts.containsPlanToString(subst(args.get(0), lets))) {
                    return PlanAsserts.planPredicateAssert(af, args, lets, execStmts,
                            execVars, execChains, ctx, imports,
                            runtimeFqn, conn);
                }
                if (containsSqlProducer(args.get(0), ctx)) {
                    // SLICE 3 (real evaluation): sql()/sqlRemoveFormatting()
                    // fold to the compiler's rendered SQL at the splice —
                    // the predicate is an ordinary boolean now; RUN it.
                    // Audit 9's mixed-conjunct worry dissolves when every
                    // conjunct actually evaluates. Guards kept: failed
                    // seeds + value reads = hollow-pass risk (audit 16 F4);
                    // a shape that still walls falls back to the NAMED
                    // advisory/unsupported it always was — never silent.
                    boolean mixed = containsValuesRead(args.get(0));
                    if (emptinessUnverifiable && mixed) {
                        return UNSUPPORTED_MARKER;
                    }
                    try {
                        Object pv = evalScalar(args.get(0), lets, execStmts,
                                execVars, execChains, ctx, imports,
                                runtimeFqn, conn);
                        if (pv instanceof Boolean) {
                            boolean want = simpleName(af.function())
                                    .equals("assert");   // F6.9 polarity
                            if (Boolean.valueOf(want).equals(pv)) {
                                // batch 69: a predicate over OUR OWN
                                // generated SQL text holding is a text
                                // contract, not a row verdict — advisory
                                // (it was counted a "REAL verified pass";
                                // the platform's ledger names the same
                                // assert sql-text-assert)
                                sqlTextOutcome("predicate-held");
                                return ADVISORY_MARKER;
                            }
                            // dialect-owned text: a failed text-predicate
                            // is a RECORDED divergence, never a hard test
                            // failure — the SAME verdict policy as an
                            // assertSameSQL text mismatch; no golden
                            // exists to row-replay a fragment check. The
                            // "sql-text: " prefix rides the existing
                            // divergence-recording channel (sqlDiffs).
                            sqlTextOutcome("predicate-diverged");
                            return "sql-text: assert" + (want ? "" : "False")
                                    + " predicate over generated SQL did"
                                    + " not hold";
                        }
                    } catch (com.legend.error.NotImplementedException
                            | com.legend.error.LegendCompileException e) {
                        // fall through to the named non-verdict below
                    }
                    sqlTextOutcome(mixed ? "mixed" : "predicate-wall");
                    return mixed ? UNSUPPORTED_MARKER : ADVISORY_MARKER;
                }
                if (emptinessUnverifiable) {
                    // seeds failed: a predicate like isEmpty(...) would
                    // hollow-PASS over the tables the failed seeds left
                    // empty — same guard as the equals/size-0 spellings
                    // (audit 16 F4); assert over verifiable state is rare
                    // enough that blanket-unsupported stays honest
                    return UNSUPPORTED_MARKER;
                }
                // forAll-contains SUBSET assert (functionvariables idiom
                // assert($expected->forAll(e|$results->contains($e)),|m)):
                // both sides evaluate through the pipeline; the forAll
                // fold is assert-level logic — DuckDB cannot host a
                // subquery inside a SQL lambda (Binder), and pure's own
                // evaluation of this shape is in-memory too.
                ValueSpecification[] fc = AssertLoopForm.forAllContains(
                        subst(args.get(0), lets));
                if (fc != null) {
                    Eval need = eval(fc[0], lets, execStmts, execVars,
                            execChains, ctx, imports, runtimeFqn, conn);
                    Eval have = eval(fc[1], lets, execStmts, execVars,
                            execChains, ctx, imports, runtimeFqn, conn);
                    List<Object> missing = need.values().stream()
                            .filter(n2 -> have.values().stream()
                                    .noneMatch(h -> goldenEqualScalar(n2, h)))
                            .toList();
                    boolean holds = missing.isEmpty();
                    boolean want = simpleName(af.function()).equals("assert");   // F6.9: FQN-spelled asserts keep polarity
                    return holds == want ? null
                            : "assert" + (want ? "" : "False")
                                    + " (forAll-contains subset): missing "
                                    + missing + " from " + have.render();
                }
                // connection-equality contract folds HOST-side (ConnEquality)
                Object v = ConnEquality.tryEval(subst(args.get(0), lets), ctx, imports);
                v = v != null ? v : evalScalar(args.get(0), lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
                boolean expect = simpleName(af.function()).equals("assert");   // F6.9
                return Boolean.valueOf(expect).equals(v) ? null
                        : "assert" + (expect ? "" : "False") + " did not hold (" + v + ")";
            }
            case "assertEquals", "assertEq", "assertEqualsH2Compatible", "assertNotEquals" -> {
                if (args.size() < 2) {
                    return UNSUPPORTED_MARKER;
                }
                // plan-text asserts read planToString — their goldens
                // CONTAIN sql text but the compare is the LITERAL plan
                // string through the K-native (toSQLString doctrine):
                // skip the golden-SQL advisory routing entirely
                if (PlanAsserts.wantsPlanText(args, lets)) {
                    sqlTextOutcome("plan-literal");
                    return PlanAsserts.planTextAssert(args, lets, execStmts, execVars,
                            execChains, ctx, imports, runtimeFqn, conn);
                } else {
                // legacy 3-arg H2-compat: (legacySql, h2NewSql, actual) —
                // the NEW golden is H2 2.1.214, exactly the advisory
                // second target's dialect: verify by ROWS through it
                if (args.size() == 3 && simpleName(af.function())
                        .equals("assertEqualsH2Compatible")) {
                    return sqlTextVerify(List.of(args.get(1), args.get(2)),
                            lets, execStmts, execVars, execChains, ctx,
                            imports, runtimeFqn, conn);
                }
                // golden-SQL spellings are advisory: our SQL is DuckDB's.
                // A MIXED side (sql text AND value reads) is loud instead —
                // skipping its value conjuncts would be silent (audit 9).
                // #67: a PURE golden-SQL assert upgrades to ROW-VERIFIED
                // when the H2 second target can replay the seeds and run
                // the golden (h2Upgrade; unverifiable stays advisory).
                if (containsSqlProducer(args.get(args.size() - 1), ctx)
                        || containsSqlProducer(args.get(0), ctx)) {
                    if (containsValuesRead(args.get(0))
                            || containsValuesRead(args.get(args.size() - 1))) {
                        sqlTextOutcome("mixed");
                        return UNSUPPORTED_MARKER;
                    }
                    return sqlTextVerify(args, lets, execStmts, execVars,
                            execChains, ctx, imports, runtimeFqn, conn);
                }
                }
                Eval e = eval(args.get(0), lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
                if (emptinessUnverifiable && e.size() == 0) {
                    // seeds failed: an EMPTY expectation would hollow-PASS
                    // against the empty tables (audit 9 — the assertSize-0/
                    // assertEmpty guard alone missed the equals spellings)
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(1), lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
                // assertEq = assert(eq(e, a)) — eq is IDENTITY-or-
                // primitive (P2-5, 2026-08-19 deep audit): the platform
                // owner refuses non-primitives LOUDLY; the old conflation
                // with equal risked a silent structural true where pure
                // answers false by identity
                if (simpleName(af.function()).equals("assertEq")) {
                    if (e.size() != 1 || a.size() != 1) {
                        return "assertEq: both sides must be [1] —"
                                + " expected arity " + e.size()
                                + ", actual arity " + a.size();
                    }
                    String d = com.legend.exec.PureAsserts.assertEq(
                            e.values().get(0), a.values().get(0));
                    return d == null ? null : "assertEq:" + d;
                }
                boolean equal = compare(e, a, /* ordered */ true);
                if (simpleName(af.function()).equals("assertNotEquals")) {   // F6.9
                    return equal ? "assertNotEquals: both sides are " + e.render() : null;
                }
                if (!equal && System.getenv("LEGEND_LITE_CMP_DEBUG") != null) {
                    System.err.println("[cmp] assertEquals FAIL arg0=" + args.get(0)
                            + "\n[cmp] e.sortedChain=" + e.sortedChain()
                            + " a.sortedChain=" + a.sortedChain()
                            + "\n[cmp] e types=" + e.values().stream().map(o ->
                                    o == null ? "null" : o.getClass().getSimpleName()).toList()
                            + "\n[cmp] a types=" + a.values().stream().map(o ->
                                    o == null ? "null" : o.getClass().getSimpleName()).toList());
                }
                if (equal) {
                    return null;
                }
                String er = e.render();
                String ar = a.render();
                // COMPARATOR HONESTY (E2E burndown §3.2, goal #18 step
                // 2): when both sides RENDER identically the failure is
                // on invisible grounds (cell type identity, arity,
                // TDSNull-vs-null) — a message that cannot show its own
                // reason also cannot be audited, and a comparator that
                // fails invisibly can pass invisibly. Render types+arity
                // whenever the plain renders agree.
                if (er.equals(ar)) {
                    return "assertEquals: expected " + er + ", got " + ar
                            + " — renders equal, comparison differs:"
                            + " expected types=" + e.values().stream()
                                    .map(o -> o == null ? "null"
                                            : o.getClass().getSimpleName())
                                    .toList()
                            + " arity=" + e.size()
                            + "; got types=" + a.values().stream()
                                    .map(o -> o == null ? "null"
                                            : o.getClass().getSimpleName())
                                    .toList()
                            + " arity=" + a.size();
                }
                return "assertEquals: expected " + er + ", got " + ar;
            }
            case "assertSameElements" -> {
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Eval e = eval(args.get(0), lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
                if (emptinessUnverifiable && e.size() == 0) {
                    return UNSUPPORTED_MARKER;   // see the assertEquals guard
                }
                Eval a = eval(args.get(1), lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
                return compare(e, a, /* ordered */ false) ? null
                        : "assertSameElements: expected " + e.render() + ", got " + a.render()
                                + " [expected types=" + e.values().stream()
                                        .map(o -> o == null ? "null" : o.getClass().getSimpleName()).toList()
                                + "; got types=" + a.values().stream()
                                        .map(o -> o == null ? "null" : o.getClass().getSimpleName()).toList() + "]";
            }
            case "assertContains" -> {
                return assertContainsCheck(args, lets, execStmts, execVars, execChains,
                        ctx, imports, runtimeFqn, conn, emptinessUnverifiable);
            }
            case "assertEqWithinTolerance" -> {
                if (args.size() != 3) {
                    return UNSUPPORTED_MARKER;
                }
                Object e = evalScalar(args.get(0), lets, execStmts, execVars, execChains, ctx, imports,
                        runtimeFqn, conn);
                Object a = evalScalar(args.get(1), lets, execStmts, execVars, execChains, ctx, imports,
                        runtimeFqn, conn);
                Object tol = evalScalar(args.get(2), lets, execStmts, execVars, execChains, ctx,
                        imports, runtimeFqn, conn);
                if (!(e instanceof Number en && a instanceof Number an
                        && tol instanceof Number tn)) {
                    return "assertEqWithinTolerance: non-numeric operand ("
                            + e + "/" + (e == null ? "null" : e.getClass().getSimpleName())
                            + ", " + a + "/" + (a == null ? "null" : a.getClass().getSimpleName())
                            + ", " + tol + "/" + (tol == null ? "null" : tol.getClass().getSimpleName())
                            + ")";
                }
                // Phase 2: the math + spec message live with the owner
                return com.legend.exec.PureAsserts
                        .assertEqWithinTolerance(en, an, tn);
            }
            case "assertSize" -> {
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Object n = evalScalar(args.get(1), lets, execStmts, execVars, execChains, ctx, imports,
                        runtimeFqn, conn);
                // F6.8 (audit A-hole): the guard runs BEFORE the carrier
                // arm — a failed-seed empty envelope must never hollow-
                // PASS an expected 0 through envelopeSizeCheck
                if (emptinessUnverifiable && n instanceof Number zn && zn.longValue() == 0) {
                    return UNSUPPORTED_MARKER;
                }
                String env0 = envelopeSizeCheck(n, args.get(0), lets,
                        execStmts, execVars, execChains, ctx, imports,
                        runtimeFqn, conn);
                if (env0 != NOT_ENVELOPE) {
                    return env0;
                }
                Eval a = eval(args.get(0), lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
                long actual = a.size();
                return (n instanceof Number num && num.longValue() == actual) ? null
                        : "assertSize: expected " + n + ", got " + actual;
            }
            case "assertEmpty" -> {
                if (args.isEmpty() || args.size() > 2) {
                    return UNSUPPORTED_MARKER;   // optional message arg
                }
                if (emptinessUnverifiable) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(0), lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
                return a.size() == 0 ? null : "assertEmpty: got " + a.size() + " values";
            }
            case "assertNotEmpty" -> {
                if (args.isEmpty() || args.size() > 2) {
                    return UNSUPPORTED_MARKER;   // optional message arg
                }
                Eval a = eval(args.get(0), lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
                return a.size() > 0 ? null : "assertNotEmpty: got 0 values";
            }
            // assertInstanceOf: the PLATFORM's verdict (AssertVerdicts over
            // the wire's __type) — the harness arm that string-matched the
            // deleted metamodel walk's NodeH handles is GONE (batch 55a)
            case "assertTdsEquivalent" -> {
                return args.size() == 3 || args.size() == 4
                        ? TdsEquivalence.assertArm(args, lets, execStmts, execVars,
                                execChains, ctx, imports, runtimeFqn, conn)
                        : UNSUPPORTED_MARKER;
            }
            case "assertSameSQL" -> {
                // planToString/planWalk operands are LITERAL plan-text
                // compares (same pre-check as assertEquals)
                if (!args.isEmpty() && PlanAsserts.wantsPlanText(args, lets)) {
                    return PlanAsserts.planTextAssert(args, lets,
                            execStmts, execVars, execChains, ctx,
                            imports, runtimeFqn, conn);
                }
                return sqlTextVerify(af.parameters(), lets, execStmts,
                        execVars, execChains, ctx, imports, runtimeFqn,
                        conn);
            }
            case "assertJsonStringsEqual" -> {
                // engine semantics: object keys order-INSENSITIVE, arrays
                // order-SENSITIVE — deep equality over PARSED structures
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                // canon wrappers = identity; JSONArray sort host-side
                var sc0 = JsonAssertCanon.sortCanon(subst(args.get(0), lets));
                var sc1 = JsonAssertCanon.sortCanon(subst(args.get(1), lets));
                args = java.util.List.of(
                        stripJsonCanon(sc0 != null ? sc0.inner() : args.get(0)),
                        stripJsonCanon(sc1 != null ? sc1.inner() : args.get(1)));
                Eval e = eval(args.get(0), lets, execStmts, execVars, execChains, ctx, imports,
                        runtimeFqn, conn);
                if (emptinessUnverifiable) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(1), lets, execStmts, execVars, execChains, ctx, imports,
                        runtimeFqn, conn);
                Object expected = jsonValueOf(e);
                Object actual = jsonValueOf(a);
                if (expected == null || actual == null) {
                    return UNSUPPORTED_MARKER;
                }
                if (sc0 != null) {
                    expected = JsonAssertCanon.sortByKey(expected, sc0.key());
                }
                if (sc1 != null) {
                    actual = JsonAssertCanon.sortByKey(actual, sc1.key());
                }
                // pure's [x] ≡ x value semantics at the ROOT: the engine
                // serializes a one-element result as the bare object; our
                // envelope always arrays. Bridge exactly that case — an
                // object-shaped expectation against a singleton array.
                if (!(expected instanceof List) && actual instanceof List<?> al
                        && al.size() == 1) {
                    actual = al.get(0);
                }
                String diff = com.legend.exec.JsonCompare.document(expected, actual);
                return diff == null ? null
                        : "assertJsonStringsEqual: FIRST DIFF at " + diff
                                + " | expected "
                                + abbreviate(String.valueOf(expected))
                                + ", got " + abbreviate(String.valueOf(actual));
            }
            default -> {
                return UNSUPPORTED_MARKER;
            }
        }
    }

    /** The ASSERT-FORM register: DERIVED from the platform registry
     * (every native in the meta::pure::functions::asserts package) plus
     * the corpus-defined assert functions, by exact FQN. Replaces the
     * old name-shape gate (harnessVocabName + startsWith("assert") —
     * the same sniffing class slice 2 deleted for sql producers). */
    static final java.util.Set<String> ASSERT_FORM_FQNS;
    static {
        java.util.Set<String> s = new java.util.LinkedHashSet<>();
        for (var f : com.legend.builtin.Pure.all()) {
            String q = f.qualifiedName();
            if (q.startsWith("meta::pure::functions::asserts::")) {
                s.add(q);
            }
        }
        // corpus-defined assert forms (engine .pure sources, exact FQNs)
        s.add("meta::relational::functions::asserts::assertSameSQL");
        s.add("meta::relational::functions::sqlQueryToString::h2"
                + "::assertEqualsH2Compatible");
        s.add("meta::relational::testDataGeneration::tests::assertSqlEquals");
        s.add("meta::relational::testDataGeneration::tests::assertTestData");
        s.add("meta::pure::functions::relation::assertTdsEquivalent");
        ASSERT_FORM_FQNS = java.util.Set.copyOf(s);
    }

    /** Engine test-harness WRAPPER functions whose lambda argument's
     * body IS the test (exact corpus FQNs). */
    static final java.util.Set<String> TEST_WRAPPER_FQNS = java.util.Set.of(
            "meta::relational::tests::query::runLegendTest",
            "meta::relational::tests::query::paginate::helper::runTest",
            "meta::external::query::graphQL::transformation::queryToPure"
                    + "::dynamic::tests::objectValueToExpression::runTest",
            "meta::external::query::graphQL::transformation::queryToPure"
                    + "::dynamic::tests::queryToLambda::runTest",
            "meta::relational::tests::query::paginate::helper"
                    + "::runGraphFetchTest",
            "meta::alloy::test::mayExecuteAlloyTest",
            "meta::legend::test::mayExecuteLegendTest");

    static final java.util.Set<String> PRINT_FQNS = java.util.Set.of(
            "meta::pure::functions::io::print",
            "meta::pure::functions::io::println");

    static final java.util.Set<String> COMPILE_LEGEND_GRAMMAR_FQNS =
            java.util.Set.of("meta::legend::compileLegendGrammar");

    static final java.util.Set<String> MAP_FQNS =
            java.util.Set.of("meta::pure::functions::collection::map");

    /** The WithVariables wrapper idiom (runLegendTest($f, pairs,
     * expected) / runTest($f, vars, sql, count), $f a PARAMETERIZED query
     * lambda): β-bind pair values over the params and return the
     * wrapper's assertions in spellings the harness already evaluates
     * (flattened .rows.values; advisory golden SQL + row count). Null =
     * not this idiom (the caller keeps its wall). */
    private static @com.legend.Nullable List<ValueSpecification> etaExpandWrapper(
            AppliedFunction wrap, Map<String, ValueSpecification> lets) {
        String fn = simpleName(wrap.function());
        List<ValueSpecification> args = wrap.parameters();
        boolean legend = fn.equals("runLegendTest") && args.size() == 3;
        boolean paginate = fn.equals("runTest") && args.size() == 4;
        if (!legend && !paginate) {
            return null;
        }
        if (!(substitute(args.get(0), lets) instanceof LambdaFunction lf)
                || lf.parameters().isEmpty() || lf.body().size() != 1) {
            return null;
        }
        ValueSpecification varsArg = substitute(args.get(1), lets);
        List<ValueSpecification> pairSpecs = varsArg instanceof PureCollection pc
                ? pc.values() : List.of(varsArg);
        Map<String, ValueSpecification> binding = new LinkedHashMap<>();
        for (ValueSpecification p : pairSpecs) {
            if (!(p instanceof AppliedFunction pf)
                    || !simpleName(pf.function()).equals("pair")
                    || pf.parameters().size() != 2
                    || !(pf.parameters().get(0) instanceof CString key)) {
                return null;
            }
            binding.put(key.value(), pf.parameters().get(1));
        }
        for (var prm : lf.parameters()) {
            if (!binding.containsKey(prm.name())) {
                return null;
            }
        }
        ValueSpecification bound = subst(lf.body().get(0), binding);
        if (legend) {
            return List.of(new AppliedFunction("assertEquals", List.of(
                    args.get(2),
                    new AppliedProperty(
                            new AppliedProperty(bound, "rows"), "values"))));
        }
        return List.of(
                new AppliedFunction("assertSameSQL",
                        List.of(args.get(2), bound)),
                new AppliedFunction("assertSize",
                        List.of(bound, args.get(3))));
    }


    /** The per-driver golden loop body — null when every pair verified
     * clean; counters = {verified, advisory} accumulate in place. */
    private static @com.legend.Nullable Outcome runPerDriverLoop(List<AppliedFunction> pairs,
            LambdaFunction perDriver, Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts, java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn,
            boolean unverifiable, int[] counters)
            throws java.sql.SQLException {
            for (AppliedFunction pair : pairs) {
                String db = enumTail(pair.parameters().get(0));
                if (!"H2".equals(db) && !"DB2".equals(db)
                        && !"Composite".equals(db)) {
                    return new Outcome.Unsupported(
                            "per-driver golden loop declares"
                            + " DatabaseType." + db
                            + " — only the H2/DB2 renderers are built");
                }
            }
            for (AppliedFunction pair : pairs) {
                Map<String, ValueSpecification> loopLets =
                        new LinkedHashMap<>(lets);
                for (ValueSpecification ls : perDriver.body()) {
                    ValueSpecification s2 = substPairReads(ls,
                            perDriver.parameters().get(0).name(),
                            pair.parameters().get(0),
                            pair.parameters().get(1));
                    if (s2 instanceof AppliedFunction lf
                            && lf.function().equals("letFunction")
                            && lf.parameters().size() == 2
                            && lf.parameters().get(0)
                                    instanceof CString ln) {
                        loopLets.put(ln.value(), lf.parameters().get(1));
                        continue;
                    }
                    if (s2 instanceof AppliedFunction af2
                            && resolvesTo(af2, ctx, ASSERT_FORM_FQNS)) {
                        String failure = checkAssert(af2, loopLets,
                                execStmts, execVars, execChains, ctx,
                                imports, runtimeFqn, conn,
                                unverifiable, Map.of(), java.util.Set.of());
                        v7DualChannel(af2, failure, loopLets, execStmts,
                                execVars, ctx, imports, runtimeFqn, conn,
                                java.util.Set.of(), java.util.Set.of());
                        if (failure == UNSUPPORTED_MARKER) {
                            String why2 = takeUnsupportedReason();
                            return new Outcome.Unsupported(
                                    "assert form '" + af2.function()
                                    + "' in a per-driver golden loop"
                                    + (why2 == null ? "" : " — " + why2));
                        }
                        if (failure == ADVISORY_MARKER) {
                            counters[1]++;
                            continue;
                        }
                        counters[0]++;
                        if (failure != null) {
                            // per-driver loop: asserts are the substance —
                            // executed stays 0, verified carries the count
                            return new Outcome.Ran(counters[0], counters[1], 0,
                                    List.of(failure));
                        }
                        continue;
                    }
                    return new Outcome.Unsupported("unrecognized"
                            + " statement in a per-driver golden loop");
                }
            }
        return null;
    }

    /** STATEMENT-position map over a literal collection of VARIABLES
     * with a (possibly multi-statement) lambda — the per-result
     * assert-block idiom ({@code [$r1,$r2]->map(r|let o=$r.values;
     * assertEquals(..);)}): HOST-side unroll, sibling of the per-driver
     * enum loop (enum literals bind the loop var, body statements splice
     * back into the work queue). Variables only — a map over computed
     * elements is a QUERY and must not unroll. */
    private static @com.legend.Nullable List<ValueSpecification> resultVarLoop(ValueSpecification stmt) {
        if (!(stmt instanceof AppliedFunction m
                && resolvesTo(m, null, MAP_FQNS)
                && m.parameters().size() == 2
                && m.parameters().get(0) instanceof PureCollection pc
                && !pc.values().isEmpty()
                && pc.values().stream().allMatch(v -> v instanceof Variable)
                && m.parameters().get(1) instanceof LambdaFunction lf
                && lf.parameters().size() == 1)) {
            return null;
        }
        List<ValueSpecification> out = new ArrayList<>();
        for (ValueSpecification el : pc.values()) {
            for (ValueSpecification b : lf.body()) {
                out.add(substitute(b, Map.of(lf.parameters().get(0).name(), el)));
            }
        }
        return out;
    }

    private static @com.legend.Nullable List<ValueSpecification> enumDriverLoop(
            ValueSpecification stmt) {
        ValueSpecification enumLoop = stmt;
        if (stmt instanceof AppliedFunction eqw
                && simpleName(eqw.function()).equals("equal")
                && eqw.parameters().size() == 2
                && eqw.parameters().get(0) instanceof AppliedFunction dw
                && simpleName(dw.function()).equals("distinct")
                && dw.parameters().size() == 1) {
            // the asserts INSIDE the body carry the verification
            enumLoop = dw.parameters().get(0);
        }
        if (enumLoop instanceof AppliedFunction emap
                && simpleName(emap.function()).equals("map")
                && emap.parameters().size() == 2
                && emap.parameters().get(1) instanceof LambdaFunction dl
                && dl.parameters().size() == 1) {
            ValueSpecification esrc = emap.parameters().get(0);
            List<ValueSpecification> evs = esrc instanceof PureCollection pc0
                    ? pc0.values() : List.of(esrc);
            // STRICT literal-enum elements only (DatabaseType.H2 — an
            // EnumValue or a dotted read off an element POINTER): a map
            // over an arbitrary property chain is a QUERY, and enumTail's
            // loose property match must never unroll it (the
            // testComplexOrExistsToManyProperty misfire)
            if (!evs.isEmpty() && evs.stream().allMatch(
                    x -> x instanceof com.legend.protocol.spec.EnumValue
                            || (x instanceof AppliedProperty ap0
                                && ap0.receiver() instanceof com.legend
                                    .protocol.spec.PackageableElementPtr))) {
                List<ValueSpecification> unrolled = new ArrayList<>();
                for (ValueSpecification ev : evs) {
                    for (ValueSpecification b : dl.body()) {
                        unrolled.add(substitute(b, Map.of(
                                dl.parameters().get(0).name(), ev)));
                    }
                }
                return unrolled;
            }
        }
        return null;
    }

    private static @com.legend.Nullable LambdaFunction driverPairLoop(ValueSpecification v,
            Map<String, ValueSpecification> lets,
            List<AppliedFunction> pairsOut) {
        if (!(v instanceof AppliedFunction d
                && simpleName(d.function()).equals("distinct")
                && d.parameters().size() == 1
                && d.parameters().get(0) instanceof AppliedFunction m
                && simpleName(m.function()).equals("map")
                && m.parameters().size() == 2
                && m.parameters().get(1) instanceof LambdaFunction lam
                && lam.parameters().size() == 1)) {
            return null;
        }
        ValueSpecification src = m.parameters().get(0);
        if (src instanceof Variable var) {
            src = lets.get(var.name());
        }
        List<ValueSpecification> elems = src instanceof PureCollection pc
                ? pc.values() : src == null ? List.of() : List.of(src);
        if (elems.isEmpty()) {
            return null;
        }
        for (ValueSpecification e : elems) {
            if (e instanceof AppliedFunction p
                    && simpleName(p.function()).equals("pair")
                    && p.parameters().size() == 2) {
                pairsOut.add(p);
            } else {
                return null;
            }
        }
        return lam;
    }

    /** Rewrite {@code $p.first}/{@code $p.second} reads to the pair's
     * concrete values (shadowing lambdas stop the walk). */
    private static @com.legend.Nullable ValueSpecification substPairReads(
            @com.legend.Nullable ValueSpecification v,
            String pVar, ValueSpecification first, ValueSpecification second) {
        return switch (v) {
            case null -> null;
            case AppliedProperty ap when ap.receiver() instanceof Variable pv
                    && pv.name().equals(pVar)
                    && ap.property().equals("first") -> first;
            case AppliedProperty ap when ap.receiver() instanceof Variable pv
                    && pv.name().equals(pVar)
                    && ap.property().equals("second") -> second;
            case AppliedProperty ap -> new AppliedProperty(
                    java.util.Objects.requireNonNull(substPairReads(
                            ap.receiver(), pVar, first, second)),
                    ap.property());
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream()
                            .map(x -> substPairReads(x, pVar, first, second))
                            .toList());
            case LambdaFunction lf when lf.parameters().stream()
                    .noneMatch(pv2 -> pv2.name().equals(pVar)) ->
                    new LambdaFunction(lf.parameters(), lf.body().stream()
                            .map(x -> substPairReads(x, pVar, first, second))
                            .toList());
            case PureCollection pc -> new PureCollection(pc.values().stream()
                    .map(x -> substPairReads(x, pVar, first, second))
                    .toList());
            default -> v.mapChildren(x -> requireNonNull(substPairReads(x, pVar, first, second)));
        };
    }

    /** The trailing member name of an enum-shaped read ({@code DatabaseType.H2}
     * as an EnumValue or a property read); null when neither shape. */
    private static @com.legend.Nullable String enumTail(ValueSpecification v) {
        if (v instanceof com.legend.protocol.spec.EnumValue ev) {
            return ev.value();
        }
        if (v instanceof AppliedProperty ap) {
            return ap.property();
        }
        return null;
    }

    static String simpleName(String fn) {
        int cut = fn.lastIndexOf("::");
        return cut < 0 ? fn : fn.substring(cut + 2);
    }

    /** Per-assert SQL-TEXT verification OUTCOME (user-ratified buckets
     * 2026-08-28): set at each verify exit, consumed by the v7 census —
     * the census reads what actually HAPPENED, never what was
     * classified. One assert, one outcome; null = no sql-text machinery
     * touched this assert. */
    static final ThreadLocal<@com.legend.Nullable String> SQL_TEXT_OUTCOME =
            new ThreadLocal<>();

    static void sqlTextOutcome(String o) {
        SQL_TEXT_OUTCOME.set(o);
    }

    /** The sql-producer register, by EXACT FQN (task #13 slice 2): the
     * activity-log SQL reads (helperFunctions.pure:38-60, the splice's
     * own register) and the generate-without-executing renders. */
    static final java.util.Set<String> SQL_PRODUCER_FQNS = java.util.Set.of(
            com.legend.compiler.spec.ResultEnvelopeSplice.SQL_FQN,
            com.legend.compiler.spec.ResultEnvelopeSplice
                    .SQL_REMOVE_FORMATTING_FQN,
            com.legend.compiler.element.type.PlatformTypes.TO_SQL_STRING,
            com.legend.compiler.element.type.PlatformTypes
                    .TO_SQL_STRING_PRETTY,
            // TDG 49er: .sqls reads ARE produced SQL — outcome-bucketed
            // classification (a replay RESCUE must not dual-eval into a
            // text-equality disagreement)
            com.legend.compiler.element.type.PlatformTypes
                    .GENERATE_TEST_DATA);

    /** SQL-text ASSERT FORMS by exact FQN (testAssert.pure:18,
     * sqlQueryToString/h2, testDataGeneration/tests). The old
     * simple-name set also carried 'assertSameSQLs' — defined NOWHERE
     * in the engine (vestigial, audit R8's smaller sibling): dropped. */
    static final java.util.Set<String> SQL_ASSERT_FORM_FQNS = java.util.Set.of(
            "meta::relational::functions::asserts::assertSameSQL",
            "meta::relational::functions::sqlQueryToString::h2"
                    + "::assertEqualsH2Compatible",
            "meta::relational::testDataGeneration::tests::assertSqlEquals");

    /** Whether a call RESOLVES to one of {@code fqns} — the platform's
     * own resolution layers: an explicit FQN spelling, the resolver's
     * recorded import candidates, the model lookup (when a context is
     * available), and — for BARE spellings — an exact simple-name
     * lookup against the set's OWN FQNs (a register lookup, never a
     * suffix scan; audit 23 D3's hijack exposure for a bare user
     * function shadowing a register simple name is unchanged from the
     * old vocab gate and dies at the typed-tree cutover). */
    static boolean resolvesTo(AppliedFunction af,
            @com.legend.Nullable ModelContext ctx,
            java.util.Set<String> fqns) {
        if (fqns.contains(af.function())) {
            return true;
        }
        for (String c : af.candidateFqns()) {
            if (fqns.contains(c)) {
                return true;
            }
        }
        if (af.function().contains("::")) {
            return false;
        }
        if (ctx != null) {
            for (var f : ctx.findFunction(af.function())) {
                if (fqns.contains(f.qualifiedName())) {
                    return true;
                }
            }
        }
        for (String p : fqns) {
            int cut = p.lastIndexOf("::");
            if (cut >= 0 && p.substring(cut + 2).equals(af.function())) {
                return true;
            }
        }
        return false;
    }

    /** A Result VALUES read anywhere in the expression — the assert also
     * verifies row data, so it must not be swallowed as advisory. */
    private static boolean containsValuesRead(ValueSpecification v) {
        if (v instanceof AppliedProperty ap && ap.property().equals("values")) {
            return true;
        }
        if (v instanceof AppliedFunction af) {
            for (ValueSpecification p2 : af.parameters()) {
                if (containsValuesRead(p2)) {
                    return true;
                }
            }
        }
        if (v instanceof AppliedProperty ap2) {
            return containsValuesRead(ap2.receiver());
        }
        return false;
    }

    /** A golden-SQL read ANYWHERE in the expression (nested spellings:
     * {@code $r->sqlRemoveFormatting()->toLower()->contains(...)}) — the
     * whole assertion is about SQL text, advisory by policy. */
    /** Whether the expression tree contains a call resolving to the
     * sql-producer register — the CONTENT half of the sql-text
     * partition, by resolution, never by name shape. */
    static boolean containsSqlProducer(ValueSpecification v,
            @com.legend.Nullable ModelContext ctx) {
        if (v instanceof AppliedFunction af) {
            if (resolvesTo(af, ctx, SQL_PRODUCER_FQNS)) {
                return true;
            }
            for (ValueSpecification p : af.parameters()) {
                if (containsSqlProducer(p, ctx)) {
                    return true;
                }
            }
        }
        if (v instanceof AppliedProperty ap) {
            return containsSqlProducer(ap.receiver(), ctx);
        }
        return false;
    }

    // ===== evaluation: compile one side through the pipeline =====

    /** One evaluated side: the execution result + how it compares. */
    record Eval(com.legend.exec.ExecutionResult result, boolean sortedChain,
            boolean csvTail, @com.legend.Nullable String joinSep, boolean flatCells) {

        Eval(com.legend.exec.ExecutionResult result, boolean sortedChain,
                boolean csvTail) {
            this(result, sortedChain, csvTail, null, false);
        }

        Eval(com.legend.exec.ExecutionResult result, boolean sortedChain,
                boolean csvTail, String joinSep) {
            this(result, sortedChain, csvTail, joinSep, false);
        }

        long size() {
            return switch (result) {
                case com.legend.exec.ExecutionResult.Scalar sc ->
                        sc.value() == null ? 0
                                : flatten(sc.value(), sc.returnType()).size();
                case com.legend.exec.ExecutionResult.Collection c -> c.values().size();
                case com.legend.exec.ExecutionResult.Tabular t -> t.rows().size();
                case com.legend.exec.ExecutionResult.Graph g -> {
                    Object p = com.legend.sql.Json.parse(g.json());
                    yield p instanceof List<?> l ? l.size() : 1;
                }
            };
        }

        List<Object> values() {
            return switch (result) {
                case com.legend.exec.ExecutionResult.Scalar sc ->
                        sc.value() == null ? List.of()
                                : flatten(sc.value(), sc.returnType());
                case com.legend.exec.ExecutionResult.Collection c ->
                        c.values();
                case com.legend.exec.ExecutionResult.Tabular t -> {
                    List<Object> out = new ArrayList<>();
                    t.rows().forEach(r -> out.addAll(r.values()));
                    yield out;
                }
                case com.legend.exec.ExecutionResult.Graph g -> {
                    Object p = com.legend.sql.Json.parse(g.json());
                    yield p instanceof List<?> l ? new ArrayList<>(l) : List.of(p);
                }
            };
        }


        String render() {
            List<Object> v = values();
            return v.size() == 1 ? String.valueOf(v.get(0)) : String.valueOf(v);
        }

        /** A collection-literal root arrives as an ARRAY-valued scalar.
         * F6.3: the temporal decode fires ONLY on the byte[] JSON-carrier
         * branch — JSON is the one arrival with no temporal types, so the
         * DECLARED type drives the decode back exactly there. A String
         * where a Date is declared on any OTHER path stays a String and
         * reaches wireEquals's typing-bug refusal. */
        private static List<Object> flatten(Object v,
                com.legend.compiler.element.type.Type declared) {
            if (v == null) {
                return new ArrayList<>();   // SQL NULL = pure empty
            }
            if (v instanceof List<?> l) {
                return new ArrayList<>(l);
            }
            // native java.sql.Array and byte[] JSON-carrier arrivals —
            // one decoder, hoisted (H2Verify.carrierList)
            List<Object> carried = H2Verify.carrierList(v);
            if (carried == null) {
                return List.of(v);
            }
            return v instanceof byte[]
                    ? H2Verify.coerceTemporal(carried, declared) : carried;
        }
    }

    static Eval eval(ValueSpecification expr,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts, java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        ValueSpecification spliced = subst(expr, lets);
        // A TOP-LEVEL LET ALIAS (let res = rows->map(..)->makeString(','))
        // lives in the exec-statement frame, not in lets — the shape
        // sniffs below (joinSep/toCSV/replace) must see the real chain,
        // not the Variable. Same last-binding-wins chase as
        // ExecCallFinder, cycle-guarded.
        java.util.Set<String> seenLets = new java.util.HashSet<>();
        while (spliced instanceof Variable av && seenLets.add(av.name())) {
            ValueSpecification bound =
                    ExecCallFinder.lastLetBinding(av.name(), execStmts);
            if (bound == null) {
                break;
            }
            spliced = subst(bound, lets);
        }
        // SERIALIZATION TAILS (toCSV/toString over a TDS) strip: the grid
        // compares STRUCTURALLY (or renders for a string-literal peer) —
        // rendering is a wire concern, not a query. A tail whose receiver
        // turns out non-relational falls back to the original expression.
        boolean csv = false;
        // toCSV(tds)->replace(a, b): render the grid to CSV text, apply the
        // replace LITERALLY, compare as a string (the calendar family's
        // one-line assert spelling)
        // F4.3: the RENDER tails are NO LONGER STRIPPED — the whole
        // expression evaluates through the platform (the toCSV/toString
        // lowerings: the DATABASE's text). The recognition below carries
        // only the COMPARISON POLICY (order view + text form) — keeping a
        // structural comparison does not require keeping a renderer.
        String renderForm = null;
        boolean renderSorted = false;
        if (spliced instanceof AppliedFunction rep
                && simpleName(rep.function()).equals("replace")
                && rep.parameters().size() == 3
                && rep.parameters().get(0) instanceof AppliedFunction innerCsv
                && simpleName(innerCsv.function()).equals("toCSV")
                && innerCsv.parameters().size() == 1
                && rep.parameters().get(1) instanceof CString from
                && "\n".equals(from.value())
                && rep.parameters().get(2) instanceof CString to) {
            renderForm = "CSVJOIN:" + to.value();
            renderSorted = endsInSort(orderView(
                    innerCsv.parameters().get(0), execChains));
        } else if (spliced instanceof AppliedFunction tail
                && (simpleName(tail.function()).equals("toCSV")
                        || simpleName(tail.function()).equals("toString"))
                && tail.parameters().size() == 1) {
            renderForm = simpleName(tail.function()).equals("toCSV")
                    ? "CSVTEXT" : "TDSTEXT";
            renderSorted = endsInSort(orderView(
                    tail.parameters().get(0), execChains));
        }
        com.legend.exec.ExecutionResult r = evalSpliced(spliced, execStmts,
                execVars, ctx, imports, runtimeFqn, conn);
        if (renderForm != null
                && r instanceof com.legend.exec.ExecutionResult.Scalar rsc
                && rsc.value() instanceof String) {
            return new Eval(r, renderSorted, false, "RENDERED:" + renderForm);
        }
        // A makeString/joinStrings tail over an UNSORTED chain: the joined
        // string's element order is the DB's incidental row order — record
        // the separator so the compare can fall back to split-multiset
        // (the ORDER POLICY at string granularity).
        String joinSep = null;
        if (spliced instanceof AppliedFunction jf
                && (simpleName(jf.function()).equals("makeString")
                        || simpleName(jf.function()).equals("joinStrings"))
                && jf.parameters().size() == 2
                && jf.parameters().get(1) instanceof CString sep
                && !endsInSort(orderView(jf.parameters().get(0),
                        execChains))) {
            joinSep = sep.value();
        }
        return new Eval(java.util.Objects.requireNonNull(r, "spliced eval without a result"),
                endsInSort(orderView(spliced, execChains)),
                csv, joinSep, isFlatCellsRead(spliced));
    }

    /** {@code ...rows.values} — the flat-CELLS spelling. Engine semantics
     * is {@code TDSRow.values} ({@code Any[*]}): column names are OUT of
     * the comparison. Our platform erases the {@code .rows} marker and
     * returns the TDS for both spellings, so the compare must know the
     * read shape (audit 21 follow-up: testQualifierFunctionConsistency*
     * compares two TDSes with DIFFERENT column names via rows.values —
     * the grid arm's column-name pin is wrong there, engine-verified). */
    private static boolean isFlatCellsRead(ValueSpecification v) {
        return v instanceof AppliedProperty ap && "values".equals(ap.property())
                && ap.receiver() instanceof AppliedProperty rp
                && "rows".equals(rp.property());
    }

    static Object evalScalar(ValueSpecification expr,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts, java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        Eval e = eval(expr, lets, execStmts, execVars, execChains, ctx, imports, runtimeFqn, conn);
        List<Object> v = e.values();
        return v.size() == 1 ? v.get(0) : v;
    }

    /** V7 batch 1 (docs/V7_ASSERT_VERDICT_CHARTER.md §4.1) — the DUAL
     * CHANNEL: every dispatched assert ALSO routes through the
     * production verdict path, via exactly the setup-statement pattern
     * ({@link #evalSpliced}: the {@code LambdaFunction(execStmts +
     * spliced)} wrap → {@code executeResolved} → StatementExecutor's
     * statement-root assert dispatch → {@code AssertVerdicts}) — never
     * hand-plumbed adjudication. The HOST verdict stays the verdict of
     * record; this channel only populates the per-form census
     * (agree/disagree/declined) whose disagree rows are batch 2's work
     * list. Host-partition results (§2: plan/sql-text advisory, TDG)
     * and host-unsupported forms are NAMED declines, not routed — that
     * partition is by design, not migration debt. */
    private static void v7DualChannel(AppliedFunction af,
            @com.legend.Nullable String hostFailure,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts,
            java.util.Set<String> execVars, ModelContext ctx,
            ImportScope imports, String runtimeFqn, Connection conn,
            java.util.Set<String> tdgVars,
            java.util.Set<String> planTextVars) {
        String form = simpleName(af.function()) + "/"
                + af.parameters().size();
        // §2: TDG generator reads are the CSV test-data bucket; plan-text
        // LETS are renders (the plan string is the contract) — separated
        // (user catch 2026-08-28: the old shared reason conflated them)
        if (!tdgVars.isEmpty() && referencesAny(af, tdgVars)) {
            // S4: every TDG binding FLOWS (no consuming arms remain) —
            // the asserts classify by their own shapes below; the
            // ZERO-frozen csv pin is the regression guard
        }
        if (!planTextVars.isEmpty() && referencesAny(af, planTextVars)) {
            com.legend.exec.CanonicalDivergence.v7Declined(form,
                    "assert-sql-text-only :: plan-let");
            return;
        }
        // §2: the SQL-TEXT partition — the user-ratified OUTCOME buckets
        // (2026-08-28). The verify machinery recorded what actually
        // HAPPENED for this assert (SQL_TEXT_OUTCOME, set per exit);
        // classification here is shape + outcome, never a guess:
        //   assert-sql-text-with-exec-passing — the golden EXECUTED on
        //     H2 and rows compared EQUAL (the only comfort bucket);
        //   assert-sql-text-only — nothing executed anywhere in the
        //     assert's sides: the text IS the contract (render/plan/
        //     both-ours), hard pass/fail;
        //   assert-sql-text-unable-to-exec :: <sub> — transparent
        //     residue by named reason (match-noreplay, diff-noreplay,
        //     predicate, mixed, no-generator-noreplay, exec-diverged
        //     stays a REAL failure and never lands here).
        String outcome = SQL_TEXT_OUTCOME.get();
        SQL_TEXT_OUTCOME.remove();
        java.util.function.Supplier<String> sqltextReason = () -> {
            // S3 correction (user catch): a TDG sqls read means OUR side
            // EXECUTED — the .sqls text IS the fetch SQL the generator
            // ran, and its output data is row-verified by the same
            // test's agreeing assertTestData. These rows are
            // unable-to-exec (golden replay declined), NEVER text-only
            // ("nothing executed anywhere" would be a false claim).
            boolean run = af.parameters().stream().anyMatch(p -> {
                if (referencesAny(p, execVars) || containsExecute(p)) {
                    return true;
                }
                TestDataGenForm.Read r = TestDataGenForm.read(p);
                return r != null && tdgVars.contains(r.var())
                        && "sqls".equals(r.kind());
            });
            if ("exec-pass".equals(outcome)) {
                return "assert-sql-text-with-exec-passing";
            }
            if (!run) {
                return "assert-sql-text-only"
                        + (outcome != null ? " :: " + outcome : "");
            }
            return "assert-sql-text-unable-to-exec :: "
                    + (outcome != null ? outcome : "unclassified");
        };
        // sql-text ASSERT FORMS — resolved exact FQNs, never names
        if (resolvesTo(af, ctx, SQL_ASSERT_FORM_FQNS)) {
            com.legend.exec.CanonicalDivergence.v7Declined(form,
                    sqltextReason.get());
            return;
        }
        // §8 leg 4 (census-first split): an assert whose ARGUMENTS pull
        // a sql-producer call — §2 partition when its verdict came from
        // the TEXT machinery (outcome recorded). SLICE 3: an sql-content
        // assert that REALLY EVALUATED (predicate over the folded string;
        // no outcome recorded, hostFailure is a real verdict) flows to
        // the ordinary dual channel below — both channels judged it.
        if (af.parameters().stream()
                .anyMatch(p -> containsSqlProducer(subst(p, lets), ctx))) {
            if (outcome != null || hostFailure == UNSUPPORTED_MARKER
                    || hostFailure == ADVISORY_MARKER) {
                com.legend.exec.CanonicalDivergence.v7Declined(form,
                        sqltextReason.get());
                return;
            }
        }
        if (hostFailure == UNSUPPORTED_MARKER) {
            com.legend.exec.CanonicalDivergence.v7Declined(form,
                    "host-unsupported");
            return;
        }
        if (hostFailure == ADVISORY_MARKER || hostFailure != null
                && hostFailure.startsWith("sql-text: ")) {
            com.legend.exec.CanonicalDivergence.v7Declined(form,
                    sqltextReason.get());
            return;
        }
        boolean hostPass = hostFailure == null;
        // probe isolation: the duplicate executions must not double-feed
        // the primary lane's pinned censuses (SqlTypeCensus ceilings,
        // the R1 [canon] disagree pin); the sql-verdict channel and the
        // V7 table stay live — they are the probe's own instruments
        com.legend.exec.SqlTypeCensus.probeSuspend(true);
        com.legend.exec.CanonicalDivergence.r1Suspend(true);
        com.legend.exec.SqlTextEmission.probeSuspend(true);
        com.legend.exec.SqlTextEmission.consumeArmFired();
        try {
            evalSpliced(subst(af, lets), execStmts, execVars, ctx,
                    imports, runtimeFqn, conn);
            // SQLTEXT slice 3a: the arm's ROW verdict vs the walk's
            // TEXT verdict is DESIGNED divergence — its own counted
            // census, never the pinned disagree channel
            if (com.legend.exec.SqlTextEmission.consumeArmFired()) {
                com.legend.exec.CanonicalDivergence.v7Declined(form,
                        "sqltext-arm :: host="
                                + (hostPass ? "pass" : "fail")
                                + " rows=pass");
                return;
            }
            com.legend.exec.CanonicalDivergence.v7Verdict(form, hostPass,
                    true, "");
        } catch (java.sql.SQLException
                | com.legend.error.DataError
                | com.legend.error.AssertFailed prodFail) {
            // the seam: the platform's fail channel is AssertFailed/
            // DataError now — identical prod-side classification
            if (System.getenv("LL_TMP_DEBUG") != null) {
                System.err.println("[v7-prod-fail] " + prodFail.getMessage());
            }
            if (com.legend.exec.SqlTextEmission.consumeArmFired()) {
                com.legend.exec.CanonicalDivergence.v7Declined(form,
                        "sqltext-arm :: host="
                                + (hostPass ? "pass" : "fail")
                                + " rows=fail: "
                                + firstLine(prodFail.getMessage()));
                return;
            }
            com.legend.exec.CanonicalDivergence.v7Verdict(form, hostPass,
                    false, firstLine(prodFail.getMessage()));
        } catch (com.legend.error.NotImplementedException wall) {
            com.legend.exec.CanonicalDivergence.v7Declined(form,
                    "wall: " + firstLine(wall.getMessage()));
        } catch (RuntimeException other) {
            // V7 decline tunnel (the V2/V6 idiom, ErrorShapeGuardrail
            // register): a probe failure becomes a COUNTED per-form
            // decline — never a swallow, never a verdict (the host
            // verdict of record was already computed above)
            com.legend.exec.CanonicalDivergence.v7Declined(form,
                    other.getClass().getSimpleName() + ": "
                            + firstLine(other.getMessage()));
        } finally {
            com.legend.exec.SqlTypeCensus.probeSuspend(false);
            com.legend.exec.CanonicalDivergence.r1Suspend(false);
            com.legend.exec.SqlTextEmission.probeSuspend(false);
        }
    }

    private static String firstLine(@com.legend.Nullable String msg) {
        for (String ln : String.valueOf(msg).split("\\n")) {
            if (!ln.isBlank()) {
                return ln;
            }
        }
        return String.valueOf(msg);
    }

    // (v7Spell DELETED, tenet correction 2026-08-28: the assert family
    // is registry natives — bare names resolve via the catalog like
    // every platform function; no splice-time FQN spelling needed.)

    /** Compile + execute ONE expression through THE one back-half sequence
     * ({@link Compiler#executeResolved}); an expression that reads an
     * execute() binding rides behind the forwarded statement PREFIX — the
     * platform's result frame owns the envelope splice (audit 19d B2). */
    private static com.legend.exec.@com.legend.Nullable ExecutionResult evalSpliced(ValueSpecification expr,
            List<ValueSpecification> execStmts, java.util.Set<String> execVars,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        List<ValueSpecification> stmts = new ArrayList<>();
        if (referencesAny(expr, execVars) || containsExecute(expr)) {
            stmts.addAll(execStmts);
        }
        stmts.add(expr);
        LambdaFunction wrapped = new LambdaFunction(List.of(), stmts);
        ValueSpecification resolved = NameResolver.resolveQuery(wrapped, imports,
                ctx.elementFqns());
        // the harness IS the test context: every evaluation env carries
        // the replay oracle (SQLTEXT charter §2 registration)
        return Compiler.executeResolved(resolved, ctx, runtimeFqn, conn,
                null, ReplayOracle.INSTANCE);
    }

    /** Evaluate the forwarded statement list AS-IS (a trailing let IS its
     * value) — the EAGER run at an execute() binding. */
    private static void evalStatements(List<ValueSpecification> stmts,
            ModelContext ctx, ImportScope imports, String runtimeFqn,
            Connection conn) throws java.sql.SQLException {
        for (ValueSpecification s : stmts) {
            List<ValueSpecification[]> csvs = new ArrayList<>();
            collectInlineCsv(s, csvs);
            for (ValueSpecification[] csvExpr : csvs) {
                seedInlineCsv(csvExpr, imports, ctx, conn);
            }
        }
        LambdaFunction wrapped = new LambdaFunction(List.of(),
                new ArrayList<>(stmts));
        ValueSpecification resolved = NameResolver.resolveQuery(wrapped, imports,
                ctx.elementFqns());
        Compiler.executeResolved(resolved, ctx, runtimeFqn, conn);
    }

    /** A runtime COPY carrying an inline {@code testDataSetupCsv} override
     * (^$connection(testDataSetupCsv=...)) declares the test's OWN seed
     * data — engine semantics: the test connection seeds from this
     * property before the query runs. The harness runs the SAME CsvSeed
     * synthesis the corpus's setUpDataSQLsV2 path uses; each test has a
     * FRESH DuckDB connection (Runner opens jdbc:duckdb: per test), so
     * DELETE+INSERT over the family-DDL tables is exactly the override. */
    private static void collectInlineCsv(ValueSpecification v,
            List<ValueSpecification[]> sink) {
        collectInlineCsv(v, sink, null);
    }

    /** Collects {csvExpr, enclosing ConnectionStore's element ref} pairs
     * — the CSV's DATABASE rides the SIBLING property of the very node
     * that carries it (^ConnectionStore(element=DB, connection=^...(
     * testDataSetupCsv=...))). FULL_RESIDUE_CENSUS §9a: without the db,
     * CsvSeed cannot find the declared table shapes and degrades every
     * block to a bare DELETE — the lane's creation half existed but was
     * unwired. */
    private static void collectInlineCsv(ValueSpecification v,
            List<ValueSpecification[]> sink,
            @com.legend.Nullable ValueSpecification dbRef) {
        switch (v) {
            case NewInstance ni -> {
                ValueSpecification db = dbRef;
                KeyExpression el = ni.first("element");
                if (el != null && el.value() instanceof
                        com.legend.protocol.spec.PackageableElementPtr) {
                    db = el.value();
                }
                KeyExpression k = ni.first("testDataSetupCsv");
                if (k != null) {
                    sink.add(new ValueSpecification[]{k.value(), db});
                }
                ValueSpecification fdb = db;
                ni.properties().stream().map(com.legend.protocol.spec.NewInstance.KeyBinding::expression).toList().forEach(x ->
                        collectInlineCsv(x.value(), sink, fdb));
            }
            case AppliedFunction af ->
                    af.parameters().forEach(x -> collectInlineCsv(x, sink, dbRef));
            case AppliedProperty ap -> collectInlineCsv(ap.receiver(), sink, dbRef);
            case PureCollection pc ->
                    pc.values().forEach(x -> collectInlineCsv(x, sink, dbRef));
            case LambdaFunction lf ->
                    lf.body().forEach(x -> collectInlineCsv(x, sink, dbRef));
            default -> v.children().forEach(x -> collectInlineCsv(x, sink, dbRef));
        }
    }

    private static void seedInlineCsv(ValueSpecification[] csvAndDb,
            ImportScope imports, ModelContext ctx, Connection conn)
            throws java.sql.SQLException {
        String csv = foldStringLiteral(csvAndDb[0]);
        // resolve the paired store ref by exact candidates (the raw
        // spelling, then each import wildcard's qualification) — never
        // a suffix scan
        String dbFqn = null;
        if (csvAndDb[1] instanceof
                com.legend.protocol.spec.PackageableElementPtr ptr) {
            java.util.List<String> cands = new ArrayList<>();
            cands.add(ptr.fullPath());
            for (String w : imports.wildcards()) {
                cands.add(w + "::" + ptr.fullPath());
            }
            for (String c : cands) {
                if (ctx.findDatabase(c).isPresent()) {
                    dbFqn = c;
                    break;
                }
            }
        }
        for (String sql : com.legend.exec.CsvSeed.sqls(csv, dbFqn, ctx)) {
            try (var st = conn.createStatement()) {
                st.execute(sql);
                // transcript fidelity (the mirror-executed-reality
                // invariant, §9a): the mirror replays what the session
                // ran — these statements are H2-valid as spelled
                var rec = com.legend.sql.dialect.RawSqlBoundary.recording();
                if (rec != null) {
                    rec.add(sql);
                }
            }
        }
    }

    /** Fold a '+'-concatenated string literal tree to its value — the
     * corpus spells inline CSVs as 'a\n'+'b\n'+... Loud on anything
     * non-literal (a computed CSV cannot be seeded honestly). */
    private static String foldStringLiteral(ValueSpecification v) {
        return switch (v) {
            case CString cs -> cs.value();
            case AppliedFunction af when af.function().equals("plus") -> {
                StringBuilder sb = new StringBuilder();
                for (ValueSpecification p : af.parameters()) {
                    sb.append(foldStringLiteral(p));
                }
                yield sb.toString();
            }
            case PureCollection pc -> {
                StringBuilder sb = new StringBuilder();
                for (ValueSpecification p : pc.values()) {
                    sb.append(foldStringLiteral(p));
                }
                yield sb.toString();
            }
            default -> throw new com.legend.error.NotImplementedException(
                    "inline testDataSetupCsv is not a foldable string literal ("
                    + v.getClass().getSimpleName() + ") — computed CSVs are"
                    + " not seeded yet");
        };
    }

    private static List<ValueSpecification> append(
            List<ValueSpecification> prefix, ValueSpecification last) {
        List<ValueSpecification> out = new ArrayList<>(prefix);
        out.add(last);
        return out;
    }

    // ===== comparison (both sides share ONE wire convention — strict) =====

    static boolean compare(Eval expected, Eval actual, boolean ordered) {
        // F4.3: RENDERED text (the platform's toCSV/toString/joined form)
        // vs a string-literal peer — TEXT policy: frame/header lines
        // pinned, data lines ordered or multiset, cells string-equal or
        // bounded-float-tolerant (the kept leniencies; header pinning by
        // the harness and the cross-kind numeric collapse are DELETED —
        // the platform emits the header now, and '007'=='7' was a
        // Double.parseDouble side effect nothing justified)
        if (actual.joinSep() != null
                && actual.joinSep().startsWith("RENDERED:")
                && actual.result()
                        instanceof com.legend.exec.ExecutionResult.Scalar rsc
                && rsc.value() instanceof String atext
                && expected.values().size() == 1
                && expected.values().get(0) instanceof String etext) {
            return com.legend.exec.TdsCompare.renderedText(etext, atext,
                    actual.joinSep().substring("RENDERED:".length()),
                    ordered && actual.sortedChain());
        }
        if (expected.joinSep() != null
                && expected.joinSep().startsWith("RENDERED:")
                && expected.result()
                        instanceof com.legend.exec.ExecutionResult.Scalar esc
                && esc.value() instanceof String etext2
                && actual.values().size() == 1
                && actual.values().get(0) instanceof String atext2) {
            // the mirrored spelling (rendered expected vs literal actual)
            return com.legend.exec.TdsCompare.renderedText(atext2, etext2,
                    expected.joinSep().substring("RENDERED:".length()),
                    true);
        }
        // TDS grids compare STRUCTURALLY: column names ordered, rows under
        // the order policy — both sides evaluated by the same pipeline.
        // NOT for a flat-cells side (rows.values): that spelling compares
        // raw cell values only — column names are out (engine TDSRow
        // semantics; see isFlatCellsRead).
        if (expected.result() instanceof com.legend.exec.ExecutionResult.Tabular te
                && actual.result() instanceof com.legend.exec.ExecutionResult.Tabular ta
                && !expected.flatCells() && !actual.flatCells()) {
            return com.legend.exec.TdsCompare.grids(te, ta, ordered && actual.sortedChain());
        }

        // MIXED flat-cells vs whole-TDS VALUE (audit 22b F2): pure equality
        // of a raw-cell list against a TabularDataSet instance is FALSE —
        // flattening both sides would drop the TDS side's column-name pin.
        // (A flat-cells side vs a plain literal list stays the values path.)
        if (expected.flatCells() != actual.flatCells()
                && (expected.flatCells() ? actual : expected).result()
                        instanceof com.legend.exec.ExecutionResult.Tabular) {
            return false;
        }
        List<Object> e = expected.values();
        List<Object> a = actual.values();
        if (e.size() != a.size()) {
            return false;
        }
        // ORDER POLICY (the single deliberate leniency, documented): pure
        // assertEquals is ordered, but an actual side with NO sort in its
        // chain has no defined SQL row order — the engine's expectation
        // encodes H2's incidental order, ours is DuckDB's. Multiset-compare
        // exactly then; a sorted chain compares exactly ordered.
        if (ordered && actual.sortedChain()) {
            for (int i = 0; i < e.size(); i++) {
                if (!goldenEqualScalar(e.get(i), a.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (ordered) {
            // try ordered first — identical orders stay strongest evidence
            boolean ok = true;
            for (int i = 0; i < e.size() && ok; i++) {
                ok = goldenEqualScalar(e.get(i), a.get(i));
            }
            if (ok) {
                return true;
            }
        }
        // ORDER POLICY at STRING granularity: a makeString over an
        // unsorted chain joined the DB's incidental row order — compare
        // the split parts as a multiset.
        if (actual.joinSep() != null && !actual.joinSep().isEmpty()
                && e.size() == 1 && a.size() == 1
                && e.get(0) instanceof String es2 && a.get(0) instanceof String as2
                && !es2.equals(as2)) {
            List<String> ep = new ArrayList<>(List.of(
                    es2.split(java.util.regex.Pattern.quote(actual.joinSep()), -1)));
            List<String> ap = new ArrayList<>(List.of(
                    as2.split(java.util.regex.Pattern.quote(actual.joinSep()), -1)));
            if (ep.size() == ap.size()) {
                java.util.Collections.sort(ep);
                java.util.Collections.sort(ap);
                return ep.equals(ap);
            }
            return false;
        }
        // ROW COHESION (audit 9): an ORDERED compare's multiset fallback
        // (the order policy) must match ROW TUPLES, not loose cells —
        // cross-row cell shuffles must not compare equal. assertSameElements
        // stays a loose pool: the corpus itself writes its flat expected
        // sets column-grouped (testGreaterThanWithOptionalProperty), so
        // loose multiset IS that assert's reference semantics.
        if (ordered
                && actual.result() instanceof com.legend.exec.ExecutionResult.Tabular tab
                && tab.columns().size() > 1
                && (!(expected.result()
                        instanceof com.legend.exec.ExecutionResult.Tabular)
                        // audit 22b F3: BOTH-flat-cells exec-vs-exec compares
                        // must keep row cohesion too — a loose cell multiset
                        // let cross-row shuffles pass
                        || (expected.flatCells() && actual.flatCells()))
                && e.size() == a.size() && a.size() % tab.columns().size() == 0) {
            // row cohesion + F2.4 instrument live with the policy owner
            return com.legend.exec.TdsCompare.rowTupleMultiset(
                    e, a, tab.columns().size());
        }
        List<Object> pool = new ArrayList<>(a);
        for (Object x : e) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (goldenEqualScalar(x, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                if (System.getenv("LEGEND_LITE_CMP_DEBUG") != null) {
                    System.err.println("[cmp] pool miss: expected " + x + " ("
                            + (x == null ? "null" : x.getClass().getSimpleName())
                            + ") pool types=" + pool.stream().map(o ->
                            o == null ? "null" : o.getClass().getSimpleName())
                            .toList());
                }
                return false;
            }
            pool.remove(hit);
        }
        // F2.4: loose-pool cell multiset — previously uninstrumented.
        // The tag (audit #10) separates THIS site — assertSameElements'
        // pure-spec order-insensitivity — from genuine row leniency.
        com.legend.exec.TdsCompare.ordLeniency("sameElements-values", () -> {
            for (int i = 0; i < e.size(); i++) {
                if (!goldenEqualScalar(e.get(i), a.get(i))) {
                    return false;
                }
            }
            return true;
        });
        return true;
    }

    /** Column-name + row-grid equality (rows ordered iff the chain sorts). */
    // ===== substitution: lets inline, handles splice =====

    private static boolean isExecuteCall(AppliedFunction af) {
        // by RESOLUTION to the one real execute FQN (router, post-R8) —
        // a user function my::execute never matches (audit 23 D3 intact,
        // now by identity instead of the vocab gate + suffix pair)
        return resolvesTo(af, null, ExecCallFinder.EXECUTE_FQNS)
                && af.parameters().size() >= 2;
    }

    /**
     * Replace let-bound variables with their expressions (shadowing lambda
     * params stop substitution). Reads over execute() bindings are NOT
     * substituted here — those statements forward to the platform's result
     * frame, which owns the envelope splice (audit 19d B2).
     */
    private static boolean endsInSort(@com.legend.Nullable ValueSpecification v) {
        // names compare by SIMPLE name uniformly — an FQN-spelled sort must
        // still count as sorted (audit 9: raw-name matching left FQN
        // spellings silently lenient)
        if (!(v instanceof AppliedFunction af)) {
            return false;
        }
        String fn = simpleName(af.function());
        if (fn.equals("sort") || fn.equals("sortBy")) {
            return true;
        }
        // order survives through order-preserving tails only. filter/
        // select/rename/restrict/concatenate-free projections preserve
        // pure's order too (audit 23 D1: their absence granted multiset
        // leniency where order was contractual — sweep-classified strict)
        return switch (fn) {
            case "map", "limit", "take", "drop", "slice", "rows", "toOne", "at",
                    "makeString", "toCSV", "toString", "from",
                    "filter", "select", "rename", "renameColumns", "restrict",
                    "project", "distinct" ->
                    !af.parameters().isEmpty() && endsInSort(af.parameters().get(0));
            default -> false;
        };
    }

    /** The EFFECTIVE sort keys of an ordered chain — the column/property
     * names of the sort nearest the tail (the engine's own semantics:
     * {@code sortBy(a)->sortBy(b)} emits {@code order by b} ALONE —
     * testSortByLambdaMultiple's golden is the receipt), reached through
     * the same order-preserving tails {@link #endsInSort} walks. Null =
     * underivable (a computed sort expression) — the ordered compare
     * then DECLINES, counted, never guessed. Ties are the point: rows
     * equal on these keys have no defined relative order on either
     * backend, so the §7 in-order compare groups them (key sequence
     * positional, full rows multiset WITHIN a tie run). */
    static java.util.@com.legend.Nullable List<String> sortKeyCols(
            @com.legend.Nullable ValueSpecification v) {
        if (!(v instanceof AppliedFunction af)) {
            return null;
        }
        String fn = simpleName(af.function());
        if (fn.equals("sort") || fn.equals("sortBy")) {
            if (af.parameters().size() < 2) {
                return null;
            }
            java.util.List<String> keys = new java.util.ArrayList<>();
            if (!collectSortKeys(af.parameters().get(1), keys)) {
                return null;
            }
            return keys.isEmpty() ? null : keys;
        }
        return switch (fn) {
            case "map", "limit", "take", "drop", "slice", "rows", "toOne",
                    "at", "makeString", "toCSV", "toString", "from",
                    "filter", "select", "rename", "renameColumns",
                    "restrict", "project", "distinct" ->
                    af.parameters().isEmpty() ? null
                            : sortKeyCols(af.parameters().get(0));
            default -> null;
        };
    }

    /** One sort-key argument → its column/property names; false =
     * a shape this walk cannot name (computed key). */
    private static boolean collectSortKeys(ValueSpecification a,
            java.util.List<String> out) {
        switch (a) {
            case CString s -> out.add(s.value());
            case com.legend.protocol.spec.ColSpec cs -> out.add(cs.name());
            case com.legend.protocol.spec.ColSpecArray ca -> {
                for (var c : ca.colSpecs()) {
                    out.add(c.name());
                }
            }
            case com.legend.protocol.spec.PureCollection pc -> {
                for (ValueSpecification e : pc.values()) {
                    if (!collectSortKeys(e, out)) {
                        return false;
                    }
                }
            }
            case LambdaFunction lf -> {
                // sortBy(p | $p.prop): a DIRECT property read names its
                // key; anything computed is underivable
                if (lf.body().size() == 1
                        && lf.body().get(0) instanceof AppliedProperty ap
                        && ap.receiver() instanceof Variable) {
                    out.add(ap.property());
                } else {
                    return false;
                }
            }
            case com.legend.protocol.spec.PathLiteral pl -> {
                // sortBy(#/Person/lastName#): a single-segment property
                // path names its key (testSortSimple)
                if (pl.segments().size() == 1) {
                    out.add(pl.segments().get(0).name());
                } else {
                    return false;
                }
            }
            case AppliedFunction sf -> {
                // ascending(~col)/descending(~col) wrappers, and the
                // func-spec spelling sortBy(func) — direction is
                // irrelevant to TIE detection, only the key name matters
                String sfn = simpleName(sf.function());
                if ((sfn.equals("ascending") || sfn.equals("descending")
                        || sfn.equals("asc") || sfn.equals("desc"))
                        && sf.parameters().size() == 1) {
                    return collectSortKeys(sf.parameters().get(0), out);
                }
                return false;
            }
            default -> {
                return false;
            }
        }
        return true;
    }

    /** TRUE when an exec-bound query chain is a SUB-COLLECTION of a
     * class extent: a {@code getAll} root reached through
     * subset-preserving operations only. Pure's {@code filter} keeps a
     * subset of its source (filter.pure: "filters out the ones where
     * the applied function returns false") and {@code Class.all()}
     * yields each instance once, so such a chain CANNOT contain the
     * same instance twice — a golden-side full-row duplicate is then
     * the engine re-manufacturing one object per joined row
     * (RelationalResult builds one instance per row, zero dedup
     * sites), the receipt {@link H2Verify#EXTENT_SUBSET}'s collapse
     * rule requires. Anything else (map, navigation reads, unions) may
     * duplicate legitimately in pure and gets NO collapse. The static
     * decision mirrors {@link #endsInSort} (the order-policy twin —
     * SQLTEXT charter §7 doctrine: decided from OUR query, never from
     * SQL text). */
    private static boolean extentSubset(@com.legend.Nullable ValueSpecification v) {
        if (!(v instanceof AppliedFunction af)) {
            return false;
        }
        String fn = simpleName(af.function());
        if (fn.equals("getAll") || fn.equals("getAllVersions")) {
            return true;
        }
        return switch (fn) {
            case "filter", "sort", "sortBy", "limit", "take", "drop",
                    "slice", "first", "last", "toOne", "from" ->
                    !af.parameters().isEmpty()
                            && extentSubset(af.parameters().get(0));
            default -> false;
        };
    }
}
