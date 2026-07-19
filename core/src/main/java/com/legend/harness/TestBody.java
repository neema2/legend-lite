// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.harness;

import com.legend.Compiler;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.model.ImportScope;
import com.legend.parser.SpecParser;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CBoolean;
import com.legend.model.spec.CString;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.NewInstance;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * NATIVE test-body execution &mdash; runs a real pure {@code <<test.Test>>}
 * function body (a STATEMENT SEQUENCE of lets, {@code execute(...)} calls
 * and {@code assert*} calls) through the ordinary compile-to-SQL pipeline.
 *
 * <p><strong>No interpreter.</strong> Tenet #1 applies to tests too:
 * <ul>
 *   <li>{@code let r = execute(|Q, mapping, runtime, ext)} binds {@code r}
 *       to <em>the query expression itself</em> (a lazy handle) plus its
 *       execution context. Nothing runs yet.</li>
 *   <li>Every downstream read ({@code $r.values.rows->map(...)->sort()})
 *       SPLICES the query into the surrounding chain; the whole chain
 *       compiles and executes as ONE SQL statement.</li>
 *   <li>{@code assert*} natives are the orchestration boundary: BOTH sides
 *       compile and execute through the pipeline; Java compares the two
 *       wire values &mdash; strictly, since both sides share one wire
 *       convention.</li>
 * </ul>
 *
 * <p><strong>The one driver-level form.</strong> {@code execute(...)}'s
 * runtime/extensions arguments are the engine harness's plumbing
 * ({@code testRuntime()}, {@code relationalExtensions()} &mdash; functions
 * whose bodies construct engine-runtime objects legend-lite deliberately
 * does not model). The driver consumes the QUERY (arg 0, fully compiled)
 * and the MAPPING (arg 1, an element ref resolved under the caller's
 * imports); the trailing config arguments are accepted un-typed and the
 * CALLER supplies the physical connection + runtime. This is the same
 * boundary the engine's own {@code execute} crosses into Java.
 *
 * <p><strong>Failure polarity.</strong> Anything this driver does not
 * recognize is {@link Outcome.Unsupported} (named, loud) &mdash; never a
 * silent skip; a compile error in any chain propagates as an exception.
 * Assertion evaluation STOPS at the first failing assert (real pure
 * {@code assert} raises).
 */
public final class TestBody {

    private TestBody() {
    }

    /** The result of driving one test body. */
    public sealed interface Outcome {

        /**
         * The body ran to completion or first assert failure.
         *
         * @param verified  row/value-comparing asserts that ran
         * @param advisory  golden-SQL asserts recognized but not compared
         *                  (legend-lite's SQL is its dialect's, by design)
         * @param failures  first assert failure (empty = all held)
         */
        record Ran(int verified, int advisory, List<String> failures) implements Outcome {
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
    private static boolean containsExecute(ValueSpecification v) {
        if (v instanceof AppliedFunction af && isExecuteCall(af)) {
            return true;
        }
        return switch (v) {
            case AppliedFunction af -> af.parameters().stream()
                    .anyMatch(TestBody::containsExecute);
            case AppliedProperty ap -> containsExecute(ap.receiver());
            case PureCollection pc -> pc.values().stream()
                    .anyMatch(TestBody::containsExecute);
            case LambdaFunction lf -> lf.body().stream()
                    .anyMatch(TestBody::containsExecute);
            default -> false;
        };
    }

    /** Does the expression read any of the given variables? (No shadow
     * tracking — execute bindings are never usefully shadowed, and
     * over-forwarding a statement prefix is safe.) */
    private static boolean referencesAny(ValueSpecification v,
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
                && ap.receiver() instanceof Variable var
                && execChains.containsKey(var.name())) {
            return execChains.get(var.name());
        }
        if (v instanceof Variable var && execChains.containsKey(var.name())) {
            return execChains.get(var.name());
        }
        return switch (v) {
            case AppliedFunction af -> new AppliedFunction(af.function(),
                    af.parameters().stream()
                            .map(p -> orderView(p, execChains)).toList());
            case AppliedProperty ap -> new AppliedProperty(
                    orderView(ap.receiver(), execChains), ap.property());
            default -> v;
        };
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
    public static Outcome run(ModelContext ctx, String body, ImportScope imports,
            String runtimeFqn, Connection conn) throws java.sql.SQLException {
        return run(ctx, body, imports, runtimeFqn, conn, false);
    }

    /**
     * {@code emptinessUnverifiable}: the caller knows the database may be
     * missing rows for environmental reasons (failed seed replay) — an
     * emptiness-shaped assertion (assertEmpty, assertSize 0, an empty
     * expected grid) proves nothing then and the body reports Unsupported
     * instead of a hollow pass.
     */
    public static Outcome run(ModelContext ctx, String body, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable)
            throws java.sql.SQLException {
        return run(ctx, SpecParser.parseCodeBlock(body), imports, runtimeFqn,
                conn, emptinessUnverifiable);
    }

    /**
     * AST entry (Phase C): the test body arrives ALREADY PARSED — the
     * harness discovers test functions from the parsed model, so their
     * statement lists come straight off the FunctionDefinition, no
     * re-parse of extracted text.
     */
    public static Outcome run(ModelContext ctx,
            java.util.List<ValueSpecification> statements, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable)
            throws java.sql.SQLException {
        return run(ctx, statements, imports, runtimeFqn, conn,
                emptinessUnverifiable, null);
    }

    /**
     * The ORDER CONTRACT is the CALLER's semantics, not a harness guess:
     *
     * <ul>
     *   <li>{@link #SQL_ORDER_POLICY} — relational-corpus semantics: the
     *       corpus expectations encode H2's INCIDENTAL row order and we
     *       execute on DuckDB, so an unsorted chain compares as a multiset
     *       and only a sort-carrying chain (the {@code endsInSort} shape
     *       walk) is an order contract.</li>
     *   <li>{@link #PURE_ORDERED} — pure list semantics: expectations and
     *       execution share ONE semantics (the PCT corpus — payloads are
     *       pure expressions over literals; pure lists are ordered, full
     *       stop). EVERY compare is ordered; an order divergence is an
     *       honest FAIL naming a platform gap, and the shape walk is
     *       irrelevant on this path.</li>
     * </ul>
     */
    public enum OrderPolicy { SQL_ORDER_POLICY, PURE_ORDERED }

    public static Outcome run(ModelContext ctx,
            java.util.List<ValueSpecification> statements, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable,
            java.util.List<String> seedFailures)
            throws java.sql.SQLException {
        return run(ctx, statements, imports, runtimeFqn, conn,
                emptinessUnverifiable, seedFailures, OrderPolicy.SQL_ORDER_POLICY);
    }

    /**
     * {@code seedFailures}: the caller's failed-seed LEDGER — setup calls
     * the body makes report per-statement raw-SQL failures here instead of
     * aborting (engine-harness tolerance), and a non-empty ledger makes
     * emptiness-shaped assertions unverifiable from that point on.
     */
    public static Outcome run(ModelContext ctx,
            java.util.List<ValueSpecification> statements, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable,
            java.util.List<String> seedFailures, OrderPolicy order)
            throws java.sql.SQLException {
        java.util.ArrayDeque<ValueSpecification> work =
                new java.util.ArrayDeque<>(statements);
        Map<String, ValueSpecification> lets = new LinkedHashMap<>();
        // the PLATFORM-forwarded statements (execute bindings + reads over
        // them, in order) and their bound names; execChains is the
        // order-policy view of each binding's query chain
        List<ValueSpecification> execStmts = new ArrayList<>();
        java.util.Set<String> execVars = new java.util.HashSet<>();
        Map<String, ValueSpecification> execChains = new LinkedHashMap<>();
        // ONE eval context per run: it holds REFERENCES to the live
        // structures above, so every later binding is visible through it
        EvalCtx cx = new EvalCtx(order, lets, execStmts, execVars, execChains,
                ctx, imports, runtimeFqn, conn, emptinessUnverifiable,
                seedFailures);
        int verified = 0;
        int advisory = 0;
        while (!work.isEmpty()) {
            ValueSpecification stmt = work.poll();
            // side-effect-free harness noise
            if (stmt instanceof AppliedFunction pln
                    && harnessVocabName(pln.function())
                    && ("println".equals(simpleName(pln.function()))
                            || "print".equals(simpleName(pln.function())))) {
                continue;
            }
            // engine test-harness WRAPPERS: the lambda argument's body IS
            // the test — inline its statements at the front of the worklist
            if (stmt instanceof AppliedFunction wrap
                    && harnessVocabName(wrap.function())
                    && java.util.Set.of("runLegendTest", "runTest",
                            "runGraphFetchTest", "mayExecuteAlloyTest",
                            "mayExecuteLegendTest")
                            .contains(simpleName(wrap.function()))) {
                LambdaFunction inner = null;
                for (ValueSpecification arg : wrap.parameters()) {
                    ValueSpecification a2 = arg instanceof Variable av
                            && lets.get(av.name()) != null
                            ? lets.get(av.name()) : arg;
                    if (a2 instanceof LambdaFunction lf0
                            && lf0.parameters().isEmpty()) {
                        inner = lf0;
                        break;
                    }
                }
                if (inner != null) {
                    List<ValueSpecification> bodyStmts =
                            new ArrayList<>(inner.body());
                    for (int i = bodyStmts.size() - 1; i >= 0; i--) {
                        work.addFirst(bodyStmts.get(i));
                    }
                    continue;
                }
                return new Outcome.Unsupported("harness wrapper '"
                        + simpleName(wrap.function())
                        + "' carries no zero-arg lambda body");
            }
            // let name = rhs
            if (stmt instanceof AppliedFunction af && af.function().equals("letFunction")
                    && af.parameters().size() == 2
                    && af.parameters().get(0) instanceof CString name) {
                ValueSpecification rhs =
                        substitute(af.parameters().get(1), lets);
                // an execute() binding — or any read over one — forwards to
                // the PLATFORM's result frame (audit 19d B2). Forwarding is
                // EAGER (audit 16 F1, engine parity): the statement executor
                // runs the query AT the let, so a broken pipeline surfaces
                // even when no assert ever reads the binding.
                if (containsExecute(rhs) || referencesAny(rhs, execVars)) {
                    execStmts.add(new AppliedFunction("letFunction",
                            List.of(name, rhs)));
                    execVars.add(name.value());
                    recordExecChain(name.value(), rhs, execChains);
                    evalStatements(execStmts, ctx, imports, runtimeFqn, conn);
                    continue;
                }
                lets.put(name.value(), rhs);
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
                    Outcome o = runPerDriverLoop(pairs, perDriver, cx, counters);
                    verified = counters[0];
                    advisory = counters[1];
                    if (o != null) {
                        return o;
                    }
                    continue;
                }
            }
            if (stmt instanceof AppliedFunction af
                    && harnessVocabName(af.function())
                    && simpleName(af.function()).startsWith("assert")) {
                String failure = checkAssert(af, cx);
                if (failure == UNSUPPORTED_MARKER) {
                    return new Outcome.Unsupported("assert form '" + af.function()
                            + "/" + af.parameters().size() + "' is not supported yet");
                }
                if (failure == ADVISORY_MARKER) {
                    advisory++;
                    continue;
                }
                verified++;
                if (failure != null) {
                    return new Outcome.Ran(verified, advisory, List.of(failure));
                }
                continue;
            }
            // the conventional trailing `true`
            if (stmt instanceof CBoolean) {
                continue;
            }
            // K-natives arc (S4): any other EXPRESSION STATEMENT executes
            // through the platform — the engine's setup calls
            // (createTablesAndFillDb(), setUp($m), executeInDb(...)) are
            // ordinary pure code, and the pipeline is the only executor
            // (a statement-position execute() runs its frame there too).
            // SQLExceptions propagate (an honest ERROR); compile/type
            // failures report Unsupported — the body's data cannot be
            // trusted after a failed setup statement.
            if (stmt instanceof AppliedFunction af3) {
                try {
                    ValueSpecification sub = substitute(stmt, lets);
                    ValueSpecification wrapped =
                            referencesAny(sub, execVars)
                                    ? new LambdaFunction(List.of(),
                                            append(execStmts, sub))
                                    : sub;
                    Compiler.executeResolved(
                            NameResolver.resolveQuery(wrapped,
                                    imports, ctx.elementFqns()),
                            ctx, runtimeFqn, conn,
                            seedFailures == null ? null : seedFailures::add);
                    continue;
                } catch (java.sql.SQLException sql) {
                    throw sql;
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
        return new Outcome.Ran(verified, advisory, List.of());
    }

    /** One side of a JSON assert as a PARSED structure: a GRAPH result's
     * envelope, or a String value holding JSON text. Null = not JSON-shaped
     * (the caller reports Unsupported, never a false verdict). */
    private static Object jsonValueOf(Eval e) {
        if (e.result instanceof com.legend.exec.ExecutionResult.Graph g) {
            return ExecJson.parse(g.json());
        }
        List<Object> vals = e.values();
        if (vals.size() == 1 && vals.get(0) instanceof String str) {
            try {
                return ExecJson.parse(str);
            } catch (RuntimeException notJson) {
                return null;
            }
        }
        return null;
    }

    private static String abbreviate(String s) {
        return s.length() <= 160 ? s : s.substring(0, 157) + "...";
    }

    /** The elements of a CONSTANT string collection ({@code ['a'+'b', $x]}
     * with let-resolved, concat-folded elements), or null if any element
     * is not a compile-time string. */
    private static List<String> constantStrings(ValueSpecification v) {
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

    private static String constantString(ValueSpecification v) {
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


    // ===== the eval context =====

    /**
     * The ONE eval context threaded from {@link #run} through
     * {@code checkAssert}/{@code eval}/{@code evalScalar}/
     * {@code runPerDriverLoop} — the statement loop's shared state as a
     * single parameter instead of the former 9&ndash;13 positional ones.
     *
     * <p>{@code lets}/{@code execStmts}/{@code execVars}/{@code execChains}
     * are the run's LIVE mutable structures — the record holds references
     * and is constructed ONCE per run, so bindings made after construction
     * are visible through it.
     */
    record EvalCtx(OrderPolicy order,
            Map<String, ValueSpecification> lets,
            List<ValueSpecification> execStmts,
            java.util.Set<String> execVars,
            Map<String, ValueSpecification> execChains,
            ModelContext ctx, ImportScope imports, String runtimeFqn,
            Connection conn, boolean baseUnverifiable,
            List<String> seedFailures) {

        /** Emptiness-shaped asserts are unverifiable when the caller said
         * so up front OR the failed-seed ledger is non-empty — the ledger
         * is LIVE (statements append to it), so this derives at each use,
         * never at construction. */
        boolean unverifiable() {
            return baseUnverifiable
                    || seedFailures != null && !seedFailures.isEmpty();
        }

        /** The same context over an OVERRIDING let frame (the per-driver
         * golden loop's loop-local lets). */
        EvalCtx withLets(Map<String, ValueSpecification> newLets) {
            return new EvalCtx(order, newLets, execStmts, execVars,
                    execChains, ctx, imports, runtimeFqn, conn,
                    baseUnverifiable, seedFailures);
        }
    }

    // ===== assert dispatch =====

    private static final String UNSUPPORTED_MARKER = new String("unsupported");
    private static final String ADVISORY_MARKER = new String("advisory");

    /** null = held; ADVISORY_MARKER = golden-SQL; UNSUPPORTED_MARKER; else the failure text.
     *
     * <p>THIN DISPATCHER — the arm bodies live in the per-family methods
     * below (verbatim splits of the former single switch); every
     * marker/return contract is theirs. */
    private static String checkAssert(AppliedFunction af, EvalCtx cx)
            throws java.sql.SQLException {
        return switch (simpleName(af.function())) {
            case "assert", "assertFalse", "assertEquals", "assertEq",
                    "assertEqualsH2Compatible", "assertNotEquals",
                    "assertSameElements" -> assertEqualityFamily(af, cx);
            case "assertEqWithinTolerance", "assertSize", "assertEmpty",
                    "assertNotEmpty" -> assertSizeFamily(af, cx);
            case "assertTdsEquivalent", "assertError" ->
                    assertTdsErrorFamily(af, cx);
            case "assertSameSQL", "assertJsonStringsEqual" ->
                    assertWireFamily(af, cx);
            default -> UNSUPPORTED_MARKER;
        };
    }

    /** The EQUALITY family: assert/assertFalse, the equals spellings and
     * assertSameElements. Same return contract as {@link #checkAssert}. */
    private static String assertEqualityFamily(AppliedFunction af, EvalCtx cx)
            throws java.sql.SQLException {
        List<ValueSpecification> args = af.parameters();
        switch (simpleName(af.function())) {
            case "assert", "assertFalse" -> {
                if (args.isEmpty()) {
                    return UNSUPPORTED_MARKER;
                }
                if (containsSqlText(args.get(0))) {
                    // predicate PURELY over golden SQL text is advisory; a
                    // MIXED assert (sql text AND value reads) must not have
                    // its value conjuncts silently skipped (audit 9)
                    return containsValuesRead(args.get(0))
                            ? UNSUPPORTED_MARKER : ADVISORY_MARKER;
                }
                if (cx.unverifiable()) {
                    // seeds failed: a predicate like isEmpty(...) would
                    // hollow-PASS over the tables the failed seeds left
                    // empty — same guard as the equals/size-0 spellings
                    // (audit 16 F4); assert over verifiable state is rare
                    // enough that blanket-unsupported stays honest
                    return UNSUPPORTED_MARKER;
                }
                Object v = evalScalar(args.get(0), cx);
                boolean expect = af.function().equals("assert");
                return Boolean.valueOf(expect).equals(v) ? null
                        : "assert" + (expect ? "" : "False") + " did not hold ("
                                + v + ")";
            }
            case "assertEquals", "assertEq", "assertEqualsH2Compatible", "assertNotEquals" -> {
                if (args.size() < 2) {
                    return UNSUPPORTED_MARKER;
                }
                // golden-SQL spellings are advisory: our SQL is DuckDB's.
                // A MIXED side (sql text AND value reads) is loud instead —
                // skipping its value conjuncts would be silent (audit 9).
                if (containsSqlText(args.get(args.size() - 1))
                        || containsSqlText(args.get(0))) {
                    return containsValuesRead(args.get(0))
                            || containsValuesRead(args.get(args.size() - 1))
                            ? UNSUPPORTED_MARKER : ADVISORY_MARKER;
                }
                // legacy 3-arg H2-compat: (legacySql, h2Sql, actualSql) —
                // all SQL text, advisory
                if (args.size() == 3 && af.function().equals("assertEqualsH2Compatible")) {
                    return ADVISORY_MARKER;
                }
                Eval e = eval(args.get(0), cx);
                if (cx.unverifiable() && e.size() == 0) {
                    // seeds failed: an EMPTY expectation would hollow-PASS
                    // against the empty tables (audit 9 — the assertSize-0/
                    // assertEmpty guard alone missed the equals spellings)
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(1), cx);
                boolean equal = compare(e, a, /* ordered */ true);
                if (af.function().equals("assertNotEquals")) {
                    return equal ? "assertNotEquals: both sides are " + e.render() : null;
                }
                return equal ? null : "assertEquals: expected " + e.render()
                        + ", got " + a.render();
            }
            case "assertSameElements" -> {
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Eval e = eval(args.get(0), cx);
                if (cx.unverifiable() && e.size() == 0) {
                    return UNSUPPORTED_MARKER;   // see the assertEquals guard
                }
                Eval a = eval(args.get(1), cx);
                return compare(e, a, /* ordered */ false) ? null
                        : "assertSameElements: expected " + e.render() + ", got " + a.render();
            }
            default -> throw new IllegalStateException(
                    "not an equality-family assert: " + af.function());
        }
    }

    /** The SIZE family: assertSize/assertEmpty/assertNotEmpty and the
     * numeric-tolerance spelling. Same return contract as
     * {@link #checkAssert}. */
    private static String assertSizeFamily(AppliedFunction af, EvalCtx cx)
            throws java.sql.SQLException {
        List<ValueSpecification> args = af.parameters();
        switch (simpleName(af.function())) {
            case "assertEqWithinTolerance" -> {
                if (args.size() != 3) {
                    return UNSUPPORTED_MARKER;
                }
                Object e = evalScalar(args.get(0), cx);
                Object a = evalScalar(args.get(1), cx);
                Object tol = evalScalar(args.get(2), cx);
                if (!(e instanceof Number en && a instanceof Number an
                        && tol instanceof Number tn)) {
                    return "assertEqWithinTolerance: non-numeric operand ("
                            + e + ", " + a + ", " + tol + ")";
                }
                return Math.abs(en.doubleValue() - an.doubleValue())
                        <= tn.doubleValue() ? null
                        : "assertEqWithinTolerance: expected " + e + " ± "
                                + tol + ", got " + a;
            }
            case "assertSize" -> {
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Object n = evalScalar(args.get(1), cx);
                if (cx.unverifiable() && n instanceof Number zn && zn.longValue() == 0) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(0), cx);
                long actual = a.size();
                return (n instanceof Number num && num.longValue() == actual) ? null
                        : "assertSize: expected " + n + ", got " + actual;
            }
            case "assertEmpty" -> {
                if (args.size() != 1) {
                    return UNSUPPORTED_MARKER;
                }
                if (cx.unverifiable()) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(0), cx);
                return a.size() == 0 ? null : "assertEmpty: got " + a.size() + " values";
            }
            case "assertNotEmpty" -> {
                if (args.size() != 1) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(0), cx);
                return a.size() > 0 ? null : "assertNotEmpty: got 0 values";
            }
            default -> throw new IllegalStateException(
                    "not a size-family assert: " + af.function());
        }
    }

    /** The TDS + ERROR family: assertTdsEquivalent and assertError. Same
     * return contract as {@link #checkAssert}. */
    private static String assertTdsErrorFamily(AppliedFunction af, EvalCtx cx)
            throws java.sql.SQLException {
        List<ValueSpecification> args = af.parameters();
        switch (simpleName(af.function())) {
            case "assertTdsEquivalent" -> {
                // real semantics (engine tdsEquivalent.pure): same column
                // NAMES in order, same row count, ordered row-wise cells —
                // Numbers within |delta|, Dates within |timeDeltaInSeconds|,
                // everything else exact.
                if (args.size() != 3 && args.size() != 4) {
                    return UNSUPPORTED_MARKER;
                }
                Double delta = literalNumber(args.get(2));
                Double timeDelta = args.size() == 4 ? literalNumber(args.get(3)) : 0.0;
                if (delta == null || timeDelta == null) {
                    return UNSUPPORTED_MARKER;
                }
                Eval one = eval(args.get(0), cx);
                if (cx.unverifiable() && one.size() == 0) {
                    return UNSUPPORTED_MARKER;   // see the assertEquals guard
                }
                Eval two = eval(args.get(1), cx);
                if (!(one.result() instanceof com.legend.exec.ExecutionResult.Tabular t1)
                        || !(two.result() instanceof com.legend.exec.ExecutionResult.Tabular t2)) {
                    return UNSUPPORTED_MARKER;
                }
                return tdsEquivalent(t1, t2, delta, timeDelta);
            }
            case "assertError" -> {
                // real semantics (assertError.pure): run the thunk, expect a
                // raise, message EQUALS; line/column pins are checked by the
                // interpreted harness against SourceInformation — core has
                // no source spans yet (P3, docs/PCT_NATIVE_PLAN.md), so
                // positional args are ADVISORY here, message stays exact.
                if (args.size() != 2 && args.size() != 4) {
                    return UNSUPPORTED_MARKER;
                }
                if (!(args.get(0) instanceof LambdaFunction thunk)
                        || thunk.body().isEmpty()
                        || !(args.get(1) instanceof CString expected)) {
                    return UNSUPPORTED_MARKER;
                }
                // POSITIONAL pins (4-arg with non-empty line/column) are
                // unverifiable until P3 source spans — SHAPE, never a
                // silently-dropped conjunct scoring verified (audit 9
                // principle; audit pct-b/c). Empty-collection positional
                // args ([],[]) are the 2-arg form.
                if (args.size() == 4
                        && (!isEmptyCollection(args.get(2))
                                || !isEmptyCollection(args.get(3)))) {
                    return UNSUPPORTED_MARKER;
                }
                try {
                    for (ValueSpecification st : thunk.body()) {
                        evalScalar(st, cx);
                    }
                    return "assertError: no error was raised (expected '"
                            + expected.value() + "')";
                } catch (com.legend.error.NotImplementedException nie) {
                    // platform GAP, not the asserted error — rethrown; the
                    // runner books it as a loud ERROR
                    throw nie;
                } catch (com.legend.error.LegendCompileException ce) {
                    // COMPILE-stage failure (parse/type): real pure would
                    // fail the test function's compile, never satisfy
                    // assertError (audit pct-b M: 'unknown function'
                    // wrong-PASSed) — rethrown
                    throw ce;
                } catch (RuntimeException | java.sql.SQLException e) {
                    String actual = e.getMessage() == null
                            ? e.getClass().getSimpleName() : e.getMessage();
                    return actual.equals(expected.value()) ? null
                            : "assertError: expected '" + expected.value()
                                    + "', got '" + actual + "'";
                }
            }
            default -> throw new IllegalStateException(
                    "not a tds/error-family assert: " + af.function());
        }
    }

    /** The WIRE family: assertSameSQL (advisory by policy) and
     * assertJsonStringsEqual. Same return contract as
     * {@link #checkAssert}. */
    private static String assertWireFamily(AppliedFunction af, EvalCtx cx)
            throws java.sql.SQLException {
        List<ValueSpecification> args = af.parameters();
        switch (simpleName(af.function())) {
            case "assertSameSQL" -> {
                return ADVISORY_MARKER;
            }
            case "assertJsonStringsEqual" -> {
                // graph-fetch JSON equality (engine semantics): object keys
                // order-INSENSITIVE, arrays order-SENSITIVE — exactly deep
                // equality over the PARSED structures (both sides through
                // the same parser, so number spelling is symmetric)
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Eval e = eval(args.get(0), cx);
                if (cx.unverifiable()) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(1), cx);
                Object expected = jsonValueOf(e);
                Object actual = jsonValueOf(a);
                if (expected == null || actual == null) {
                    return UNSUPPORTED_MARKER;
                }
                // pure's [x] ≡ x value semantics at the ROOT: the engine
                // serializes a one-element result as the bare object; our
                // envelope always arrays. Bridge exactly that case — an
                // object-shaped expectation against a singleton array.
                if (!(expected instanceof List) && actual instanceof List<?> al
                        && al.size() == 1) {
                    actual = al.get(0);
                }
                return jsonDeepEquals(expected, actual) ? null
                        : "assertJsonStringsEqual: expected "
                                + abbreviate(String.valueOf(expected))
                                + ", got " + abbreviate(String.valueOf(actual));
            }
            default -> throw new IllegalStateException(
                    "not a wire-family assert: " + af.function());
        }
    }

    /** Harness vocabulary matches by SIMPLE name only for BARE or
     * meta::-qualified spellings — a user function my::pkg::assertFoo
     * must route to the platform, never be hijacked (audit 17). */
    private static boolean harnessVocabName(String fn) {
        return !fn.contains("::") || fn.startsWith("meta::");
    }


    /** The per-driver golden loop body — null when every pair verified
     * clean; counters = {verified, advisory} accumulate in place. */
    private static Outcome runPerDriverLoop(List<AppliedFunction> pairs,
            LambdaFunction perDriver, EvalCtx cx, int[] counters)
            throws java.sql.SQLException {
            for (AppliedFunction pair : pairs) {
                String db = enumTail(pair.parameters().get(0));
                if (!"H2".equals(db)) {
                    return new Outcome.Unsupported(
                            "per-driver golden loop declares"
                            + " DatabaseType." + db
                            + " — only the H2 renderer is built");
                }
            }
            for (AppliedFunction pair : pairs) {
                Map<String, ValueSpecification> loopLets =
                        new LinkedHashMap<>(cx.lets());
                EvalCtx loopCx = cx.withLets(loopLets);
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
                            && harnessVocabName(af2.function())
                            && simpleName(af2.function())
                                    .startsWith("assert")) {
                        String failure = checkAssert(af2, loopCx);
                        if (failure == UNSUPPORTED_MARKER) {
                            return new Outcome.Unsupported(
                                    "assert form '" + af2.function()
                                    + "' in a per-driver golden loop");
                        }
                        if (failure == ADVISORY_MARKER) {
                            counters[1]++;
                            continue;
                        }
                        counters[0]++;
                        if (failure != null) {
                            return new Outcome.Ran(counters[0], counters[1],
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

    /**
     * {@code distinct(map(src, p|...))} where {@code src} (a let or an
     * inline collection) is a list of {@code pair(DatabaseType.X, sql)} —
     * the per-driver golden idiom's pieces; null when the shape differs.
     */
    private static LambdaFunction driverPairLoop(ValueSpecification v,
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
    private static ValueSpecification substPairReads(ValueSpecification v,
            String pVar, ValueSpecification first, ValueSpecification second) {
        return switch (v) {
            case AppliedProperty ap when ap.receiver() instanceof Variable pv
                    && pv.name().equals(pVar)
                    && ap.property().equals("first") -> first;
            case AppliedProperty ap when ap.receiver() instanceof Variable pv
                    && pv.name().equals(pVar)
                    && ap.property().equals("second") -> second;
            case AppliedProperty ap -> new AppliedProperty(
                    substPairReads(ap.receiver(), pVar, first, second),
                    ap.property());
            case AppliedFunction af -> new AppliedFunction(af.function(),
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
            default -> v;
        };
    }

    /** The trailing member name of an enum-shaped read ({@code DatabaseType.H2}
     * as an EnumValue or a property read); null when neither shape. */
    private static String enumTail(ValueSpecification v) {
        if (v instanceof com.legend.model.spec.EnumValue ev) {
            return ev.value();
        }
        if (v instanceof AppliedProperty ap) {
            return ap.property();
        }
        return null;
    }

    /** Deep JSON equality with NUMERIC BigDecimal compare (scale drops:
     * the engine prints 5.0 where our envelope prints 5.000000000 for the
     * same DECIMAL(38,9) value). Long-vs-BigDecimal stays UNEQUAL on
     * purpose — an integer-typed expectation against a decimal wire value
     * is a typing bug this compare must catch, same stance as wireEquals'
     * int/fp split. */
    private static boolean jsonDeepEquals(Object e, Object a) {
        if (e instanceof java.math.BigDecimal be
                && a instanceof java.math.BigDecimal ba) {
            return be.compareTo(ba) == 0;
        }
        if (e instanceof Map<?, ?> em && a instanceof Map<?, ?> am) {
            if (!em.keySet().equals(am.keySet())) {
                return false;
            }
            for (Object k : em.keySet()) {
                if (!jsonDeepEquals(em.get(k), am.get(k))) {
                    return false;
                }
            }
            return true;
        }
        if (e instanceof List<?> el && a instanceof List<?> al) {
            if (el.size() != al.size()) {
                return false;
            }
            for (int i = 0; i < el.size(); i++) {
                if (!jsonDeepEquals(el.get(i), al.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return java.util.Objects.equals(e, a);
    }

    private static String simpleName(String fn) {
        int cut = fn.lastIndexOf("::");
        return cut < 0 ? fn : fn.substring(cut + 2);
    }

    /** A golden-SQL spelling: any chain ending in sqlRemoveFormatting()/sql(). */
    private static boolean isSqlText(ValueSpecification v) {
        return v instanceof AppliedFunction af
                && (af.function().equals("sqlRemoveFormatting") || af.function().equals("sql"));
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
    private static boolean containsSqlText(ValueSpecification v) {
        if (isSqlText(v)) {
            return true;
        }
        if (v instanceof AppliedFunction af) {
            for (ValueSpecification p : af.parameters()) {
                if (containsSqlText(p)) {
                    return true;
                }
            }
        }
        if (v instanceof AppliedProperty ap) {
            return containsSqlText(ap.receiver());
        }
        return false;
    }

    // ===== evaluation: compile one side through the pipeline =====

    /** One evaluated side: the execution result + how it compares.
     * NOTE: {@code sortedChain} is the ORDER-POLICY RESULT ({@code ordered()}),
     * not the raw chain walk — under PURE_ORDERED it is unconditionally
     * true regardless of whether the chain ends in a sort. */
    private record Eval(com.legend.exec.ExecutionResult result, boolean sortedChain,
            boolean csvTail, String joinSep) {

        Eval(com.legend.exec.ExecutionResult result, boolean sortedChain,
                boolean csvTail) {
            this(result, sortedChain, csvTail, null);
        }

        long size() {
            return switch (result) {
                case com.legend.exec.ExecutionResult.Scalar sc ->
                        sc.value() == null ? 0 : flatten(sc.value()).size();
                case com.legend.exec.ExecutionResult.Collection c -> c.values().size();
                case com.legend.exec.ExecutionResult.Tabular t -> t.rows().size();
                case com.legend.exec.ExecutionResult.Graph g -> {
                    Object p = ExecJson.parse(g.json());
                    yield p instanceof List<?> l ? l.size() : 1;
                }
            };
        }

        List<Object> values() {
            return switch (result) {
                case com.legend.exec.ExecutionResult.Scalar sc ->
                        sc.value() == null ? List.of() : flatten(sc.value());
                case com.legend.exec.ExecutionResult.Collection c -> c.values();
                case com.legend.exec.ExecutionResult.Tabular t -> {
                    List<Object> out = new ArrayList<>();
                    t.rows().forEach(r -> out.addAll(r.values()));
                    yield out;
                }
                case com.legend.exec.ExecutionResult.Graph g -> {
                    Object p = ExecJson.parse(g.json());
                    yield p instanceof List<?> l ? new ArrayList<>(l) : List.of(p);
                }
            };
        }


        String render() {
            List<Object> v = values();
            return v.size() == 1 ? String.valueOf(v.get(0)) : String.valueOf(v);
        }

        /** A collection-literal root arrives as an ARRAY-valued scalar. */
        private static List<Object> flatten(Object v) {
            if (v instanceof List<?> l) {
                return new ArrayList<>(l);
            }
            if (v instanceof java.sql.Array arr) {
                try {
                    return new ArrayList<>(List.of((Object[]) arr.getArray()));
                } catch (java.sql.SQLException e) {
                    throw new IllegalStateException(e);
                }
            }
            return List.of(v);
        }
    }

    private static Eval eval(ValueSpecification expr, EvalCtx cx)
            throws java.sql.SQLException {
        ValueSpecification spliced = substitute(expr, cx.lets());
        // SERIALIZATION TAILS (toCSV/toString over a TDS) strip: the grid
        // compares STRUCTURALLY (or renders for a string-literal peer) —
        // rendering is a wire concern, not a query. A tail whose receiver
        // turns out non-relational falls back to the original expression.
        boolean csv = false;
        // toCSV(tds)->replace(a, b): render the grid to CSV text, apply the
        // replace LITERALLY, compare as a string (the calendar family's
        // one-line assert spelling)
        if (spliced instanceof AppliedFunction rep
                && simpleName(rep.function()).equals("replace")
                && rep.parameters().size() == 3
                && rep.parameters().get(0) instanceof AppliedFunction innerCsv
                && simpleName(innerCsv.function()).equals("toCSV")
                && innerCsv.parameters().size() == 1
                && rep.parameters().get(1) instanceof CString from
                && "\n".equals(from.value())
                && rep.parameters().get(2) instanceof CString to) {
            com.legend.exec.ExecutionResult stripped2 = evalSpliced(
                    innerCsv.parameters().get(0), cx.execStmts(), cx.execVars(),
                    cx.ctx(), cx.imports(), cx.runtimeFqn(), cx.conn());
            if (stripped2 instanceof com.legend.exec.ExecutionResult.Tabular tab2) {
                // structured compare: keep the TABULAR and the joined-line
                // separator — string-exact comparison broke on ROW ORDER
                // (unordered groupBy) and float ULPs (5.72 vs 5.7199...);
                // csvJoinedEquals below applies the header/multiset/
                // tolerant-cell policy instead
                return new Eval(stripped2,
                        ordered(cx.order(), orderView(innerCsv.parameters().get(0),
                                cx.execChains())), false,
                        "CSVJOIN:" + to.value());
            }
        }
        // toCSV tails still strip (grid compares structurally under the
        // order policy — corpus semantics). toString tails DO NOT strip
        // anymore: the platform owns the '#TDS' rendering now
        // (StatementExecutor.relationToString, the K presentation native —
        // PCT pins the exact text), so the whole expression forwards and
        // the compare is string-exact.
        if (spliced instanceof AppliedFunction tail
                && simpleName(tail.function()).equals("toCSV")
                && tail.parameters().size() == 1) {
            com.legend.exec.ExecutionResult stripped = evalSpliced(
                    tail.parameters().get(0), cx.execStmts(), cx.execVars(),
                    cx.ctx(), cx.imports(), cx.runtimeFqn(), cx.conn());
            if (stripped instanceof com.legend.exec.ExecutionResult.Tabular) {
                return new Eval(stripped,
                        ordered(cx.order(), orderView(tail.parameters().get(0),
                                cx.execChains())),
                        true);
            }
        }
        com.legend.exec.ExecutionResult r = evalSpliced(spliced, cx.execStmts(),
                cx.execVars(), cx.ctx(), cx.imports(), cx.runtimeFqn(), cx.conn());
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
                && !ordered(cx.order(), orderView(jf.parameters().get(0),
                        cx.execChains()))) {
            joinSep = sep.value();
        }
        return new Eval(r, ordered(cx.order(), orderView(spliced, cx.execChains())),
                csv, joinSep);
    }

    private static Object evalScalar(ValueSpecification expr, EvalCtx cx)
            throws java.sql.SQLException {
        Eval e = eval(expr, cx);
        List<Object> v = e.values();
        return v.size() == 1 ? v.get(0) : v;
    }

    /** Compile + execute ONE expression through THE one back-half sequence
     * ({@link Compiler#executeResolved}); an expression that reads an
     * execute() binding rides behind the forwarded statement PREFIX — the
     * platform's result frame owns the envelope splice (audit 19d B2). */
    private static com.legend.exec.ExecutionResult evalSpliced(ValueSpecification expr,
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
        return Compiler.executeResolved(resolved, ctx, runtimeFqn, conn);
    }

    /** Evaluate the forwarded statement list AS-IS (a trailing let IS its
     * value) — the EAGER run at an execute() binding. */
    private static void evalStatements(List<ValueSpecification> stmts,
            ModelContext ctx, ImportScope imports, String runtimeFqn,
            Connection conn) throws java.sql.SQLException {
        LambdaFunction wrapped = new LambdaFunction(List.of(),
                new ArrayList<>(stmts));
        ValueSpecification resolved = NameResolver.resolveQuery(wrapped, imports,
                ctx.elementFqns());
        Compiler.executeResolved(resolved, ctx, runtimeFqn, conn);
    }

    private static List<ValueSpecification> append(
            List<ValueSpecification> prefix, ValueSpecification last) {
        List<ValueSpecification> out = new ArrayList<>(prefix);
        out.add(last);
        return out;
    }

    // ===== comparison (both sides share ONE wire convention — strict) =====

    private static boolean compare(Eval expected, Eval actual, boolean ordered) {
        // toCSV(..)->replace('\n', SEP) actual vs a string-literal expected:
        // header EXACT, rows as an (un)ordered multiset, CELLS via the
        // tolerant wire comparison (numeric ULP policy included)
        if (actual.joinSep() != null && actual.joinSep().startsWith("CSVJOIN:")
                && actual.result()
                        instanceof com.legend.exec.ExecutionResult.Tabular tj
                && expected.values().size() == 1
                && expected.values().get(0) instanceof String es) {
            return csvJoinedEquals(es,
                    actual.joinSep().substring("CSVJOIN:".length()), tj,
                    ordered && actual.sortedChain());
        }
        // TDS grids compare STRUCTURALLY: column names ordered, rows under
        // the order policy — both sides evaluated by the same pipeline
        if (expected.result() instanceof com.legend.exec.ExecutionResult.Tabular te
                && actual.result() instanceof com.legend.exec.ExecutionResult.Tabular ta) {
            return gridEquals(te, ta, ordered && actual.sortedChain());
        }
        // toCSV() against a STRING literal: render the grid (wire concern);
        // header pinned, data lines under the order policy
        if (actual.csvTail()
                && actual.result() instanceof com.legend.exec.ExecutionResult.Tabular tt
                && expected.values().size() == 1
                && expected.values().get(0) instanceof String es) {
            return csvEquals(es, tt, actual.sortedChain());
        }
        if (expected.csvTail()
                && expected.result() instanceof com.legend.exec.ExecutionResult.Tabular tt2
                && actual.values().size() == 1
                && actual.values().get(0) instanceof String as) {
            return csvEquals(as, tt2, true);
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
                if (!wireEquals(e.get(i), a.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (ordered) {
            // try ordered first — identical orders stay strongest evidence
            boolean ok = true;
            for (int i = 0; i < e.size() && ok; i++) {
                ok = wireEquals(e.get(i), a.get(i));
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
                && !(expected.result()
                        instanceof com.legend.exec.ExecutionResult.Tabular)
                && e.size() == a.size() && a.size() % tab.columns().size() == 0) {
            int w = tab.columns().size();
            List<List<Object>> ep = chunk(e, w);
            List<List<Object>> ap = chunk(a, w);
            for (List<Object> row : ep) {
                int hit = -1;
                for (int i = 0; i < ap.size(); i++) {
                    boolean all = true;
                    for (int j = 0; j < w && all; j++) {
                        all = wireEquals(row.get(j), ap.get(i).get(j));
                    }
                    if (all) {
                        hit = i;
                        break;
                    }
                }
                if (hit < 0) {
                    return false;
                }
                ap.remove(hit);
            }
            return true;
        }
        List<Object> pool = new ArrayList<>(a);
        for (Object x : e) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (wireEquals(x, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static List<List<Object>> chunk(List<Object> flat, int w) {
        List<List<Object>> out = new ArrayList<>(flat.size() / w);
        for (int i = 0; i + w <= flat.size(); i += w) {
            out.add(new ArrayList<>(flat.subList(i, i + w)));
        }
        return out;
    }

    /** Column-name + row-grid equality (rows ordered iff the chain sorts). */
    private static boolean gridEquals(com.legend.exec.ExecutionResult.Tabular expected,
            com.legend.exec.ExecutionResult.Tabular actual, boolean ordered) {
        if (expected.columns().size() != actual.columns().size()) {
            return false;
        }
        for (int i = 0; i < expected.columns().size(); i++) {
            if (!expected.columns().get(i).name().equals(actual.columns().get(i).name())) {
                return false;
            }
        }
        List<List<Object>> e = new ArrayList<>();
        expected.rows().forEach(r -> e.add(r.values()));
        List<List<Object>> a = new ArrayList<>();
        actual.rows().forEach(r -> a.add(r.values()));
        if (e.size() != a.size()) {
            return false;
        }
        if (ordered) {
            for (int i = 0; i < e.size(); i++) {
                if (!rowEquals(e.get(i), a.get(i))) {
                    return false;
                }
            }
            return true;
        }
        List<List<Object>> pool = new ArrayList<>(a);
        for (List<Object> row : e) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (rowEquals(row, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static boolean rowEquals(List<Object> e, List<Object> a) {
        if (e.size() != a.size()) {
            return false;
        }
        for (int i = 0; i < e.size(); i++) {
            if (!wireEquals(e.get(i), a.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** toCSV wire rendering vs an expected CSV string (header pinned). */
    /** {@code toCSV->replace('\n', sep)} against a string literal: tokens
     * split on {@code sep}; the first nCols are the HEADER (exact); the
     * rest group into rows of nCols compared as a multiset (ordered when
     * the chain sorts) with wireEquals cells (numeric tolerance). */
    private static boolean csvJoinedEquals(String expected, String sep,
            com.legend.exec.ExecutionResult.Tabular t, boolean ordered) {
        int n = t.columns().size();
        if (n == 0 || sep.isEmpty()) {
            return false;
        }
        List<String> tokens = new ArrayList<>(
                List.of(expected.split(java.util.regex.Pattern.quote(sep), -1)));
        // a trailing separator leaves one empty tail token
        if (!tokens.isEmpty() && tokens.get(tokens.size() - 1).isEmpty()) {
            tokens.remove(tokens.size() - 1);
        }
        if (tokens.size() < n || tokens.size() % n != 0) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (!t.columns().get(i).name().equals(tokens.get(i))) {
                return false;
            }
        }
        List<List<String>> expRows = new ArrayList<>();
        for (int i = n; i < tokens.size(); i += n) {
            expRows.add(tokens.subList(i, i + n));
        }
        List<List<Object>> actRows = new ArrayList<>();
        t.rows().forEach(r -> actRows.add(r.values()));
        if (expRows.size() != actRows.size()) {
            return false;
        }
        if (ordered) {
            for (int i = 0; i < expRows.size(); i++) {
                if (!csvRowEquals(expRows.get(i), actRows.get(i))) {
                    return false;
                }
            }
            return true;
        }
        List<List<Object>> pool = new ArrayList<>(actRows);
        for (List<String> er : expRows) {
            int hit = -1;
            for (int i = 0; i < pool.size(); i++) {
                if (csvRowEquals(er, pool.get(i))) {
                    hit = i;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        return true;
    }

    private static boolean csvRowEquals(List<String> expected, List<Object> actual) {
        for (int i = 0; i < expected.size(); i++) {
            String e = expected.get(i);
            Object a = actual.get(i);
            String aCell = csvCell(a);
            if (e.equals(aCell)) {
                continue;
            }
            // numeric cells: the expected CSV prints ROUNDED (the engine
            // truncates to ~12 significant digits — 0.383333333333 vs our
            // 0.38333333333333336) — equal iff the actual rounds to the
            // expected at the EXPECTED's own printed precision
            try {
                double ev = Double.parseDouble(e);
                double av = a instanceof Number num ? num.doubleValue()
                        : Double.parseDouble(aCell);
                int dp = e.contains(".")
                        ? e.length() - e.indexOf('.') - 1 : 0;
                // Two leniencies, BOTH bounded by the same rationale (the
                // engine prints ~12 significant digits; H2 and DuckDB sum
                // doubles in different orders and addition is not
                // associative — the engine's own 12th digit moves,
                // testPwaValue):
                //   1. relative 1e-11 accumulation epsilon — always;
                //   2. half-ulp at the expected's PRINTED precision — ONLY
                //      when the printed token actually carries >= 10
                //      significant digits (i.e. it IS a ~12-sig-digit
                //      truncation artifact; a TRIMMED TRAILING ZERO can
                //      shave the visible count to 10: 0.05657370518 in
                //      testPwaValueOnStartYear). A coarse golden like
                //      '100' (audit 16: dp=0 granted +-0.5) is exact
                //      decimal output, not a truncation — it gets the
                //      relative floor only.
                int sig = 0;
                boolean seenNonZero = false;
                for (int ci = 0; ci < e.length(); ci++) {
                    char ch = e.charAt(ci);
                    if (ch >= '1' && ch <= '9') {
                        seenNonZero = true;
                    }
                    if (Character.isDigit(ch) && (seenNonZero || ch != '0')) {
                        sig++;
                    }
                }
                double tol = sig >= 10
                        ? Math.max(0.5 * Math.pow(10, -dp), Math.abs(ev) * 1e-11)
                        : Math.abs(ev) * 1e-11;
                if (Math.abs(av - ev) > tol) {
                    return false;
                }
            } catch (NumberFormatException nfe) {
                return false;
            }
        }
        return true;
    }

    /** The toCSV wire text: header line + one line per row, every line
     * newline-terminated (the engine's Result->toCSV convention). */
    private static String csvText(com.legend.exec.ExecutionResult.Tabular t) {
        StringBuilder header = new StringBuilder();
        for (var c : t.columns()) {
            if (header.length() > 0) {
                header.append(',');
            }
            header.append(c.name());
        }
        StringBuilder out = new StringBuilder(header).append('\n');
        for (var r : t.rows()) {
            StringBuilder line = new StringBuilder();
            for (Object v : r.values()) {
                if (line.length() > 0) {
                    line.append(',');
                }
                line.append(csvCell(v));
            }
            out.append(line).append('\n');
        }
        return out.toString();
    }

    private static boolean csvEquals(String expected,
            com.legend.exec.ExecutionResult.Tabular actual, boolean sorted) {
        StringBuilder header = new StringBuilder();
        for (var c : actual.columns()) {
            if (header.length() > 0) {
                header.append(',');
            }
            header.append(c.name());
        }
        List<String> lines = new ArrayList<>();
        for (var r : actual.rows()) {
            StringBuilder line = new StringBuilder();
            for (Object v : r.values()) {
                if (line.length() > 0) {
                    line.append(',');
                }
                line.append(csvCell(v));
            }
            lines.add(line.toString());
        }
        String rendered = header + "\n"
                + lines.stream().map(l -> l + "\n").reduce("", String::concat);
        if (expected.equals(rendered)) {
            return true;
        }
        if (sorted) {
            return false;
        }
        // order policy: header line pinned, data lines as a multiset
        String[] el = expected.split("\n", -1);
        if (el.length == 0 || !el[0].equals(header.toString())) {
            return false;
        }
        List<String> pool = new ArrayList<>(lines);
        int dataLines = 0;
        for (int i = 1; i < el.length; i++) {
            if (el[i].isEmpty() && i == el.length - 1) {
                continue;   // trailing newline
            }
            dataLines++;
            if (!pool.remove(el[i])) {
                return false;
            }
        }
        return pool.isEmpty() && dataLines == lines.size();
    }

    /** STRICT wire equality: integral kinds normalize; decimal by compareTo; no cross-kind. */
    private static boolean isTemporal(Object v) {
        return v instanceof java.sql.Timestamp || v instanceof java.sql.Date
                || v instanceof java.time.LocalDate
                || v instanceof java.time.LocalDateTime
                || v instanceof java.time.OffsetDateTime;
    }

    private static boolean temporalEquals(String s, Object t) {
        // wire temporals are NAIVE (UTC-normalized); strip a UTC-zero
        // offset/zone suffix from the string form
        String v = s.trim().replaceFirst("(Z|\\+00(:?00)?|\\+0000)$", "")
                .replace('T', ' ').trim();
        try {
            if (t instanceof java.sql.Date d) {
                return java.time.LocalDate.parse(v).equals(d.toLocalDate());
            }
            if (t instanceof java.time.LocalDate ld) {
                return java.time.LocalDate.parse(v).equals(ld);
            }
            java.time.LocalDateTime other = t instanceof java.sql.Timestamp ts
                    ? ts.toLocalDateTime()
                    : t instanceof java.time.LocalDateTime ldt ? ldt
                    : t instanceof java.time.OffsetDateTime odt
                            ? odt.toLocalDateTime() : null;
            if (other == null) {
                return false;
            }
            String norm = v.contains(" ") ? v.replace(' ', 'T') : v + "T00:00";
            return java.time.LocalDateTime.parse(norm).equals(other);
        } catch (java.time.format.DateTimeParseException ex) {
            return false;
        }
    }

    /** RFC4180 cell rendering (the engine's toCSV): a cell containing a
     * comma, quote or newline wraps in quotes, inner quotes double. */
    private static String csvCell(Object v) {
        if (v == null) {
            return "";
        }
        String s = String.valueOf(v);
        if (s.indexOf(',') < 0 && s.indexOf('"') < 0 && s.indexOf('\n') < 0) {
            return s;
        }
        return '"' + s.replace("\"", "\"\"") + '"';
    }

    /** The empty-collection literal {@code []} (assertError's absent
     * line/column positional args). */
    private static boolean isEmptyCollection(ValueSpecification vs) {
        return vs instanceof com.legend.model.spec.PureCollection pc
                && pc.values().isEmpty();
    }

    /** A plain numeric LITERAL argument (assertTdsEquivalent's delta /
     * timeDelta); anything computed returns null and the assert stays a
     * loud SHAPE rather than guessing. */
    private static Double literalNumber(ValueSpecification vs) {
        return switch (vs) {
            case com.legend.model.spec.CInteger ci -> ci.value().doubleValue();
            case com.legend.model.spec.CFloat cf -> cf.value();
            case com.legend.model.spec.CDecimal cd -> cd.value().doubleValue();
            default -> null;
        };
    }

    /** Ordered TDS equivalence per the engine's tdsEquivalent.pure: column
     * names ordered-exact, row counts equal, cells row-wise — Numbers
     * within |delta|, temporals within |timeDeltaInSeconds|, both-null
     * equal, everything else {@link #wireEquals}. */
    private static String tdsEquivalent(com.legend.exec.ExecutionResult.Tabular one,
            com.legend.exec.ExecutionResult.Tabular two, double delta, double timeDelta) {
        List<String> names1 = one.columns().stream().map(com.legend.exec.Column::name).toList();
        List<String> names2 = two.columns().stream().map(com.legend.exec.Column::name).toList();
        if (!names1.equals(names2)) {
            return "assertTdsEquivalent: columns " + names1 + " vs " + names2;
        }
        if (one.rows().size() != two.rows().size()) {
            return "assertTdsEquivalent: " + one.rows().size() + " rows vs "
                    + two.rows().size();
        }
        for (int r = 0; r < one.rows().size(); r++) {
            for (int c = 0; c < names1.size(); c++) {
                Object v1 = one.rows().get(r).get(c);
                Object v2 = two.rows().get(r).get(c);
                if (v1 == null && v2 == null) {
                    continue;
                }
                if (v1 == null || v2 == null) {
                    return cellMismatch(names1.get(c), r, v1, v2);
                }
                // arm keying by DECLARED column types on BOTH sides (real
                // tdsEquivalent.pure keys on classifierGenericType, never
                // runtime value classes — value-keying opened a false-PASS
                // channel on Any-typed columns; audit pct-a F5)
                if (isNumberColumn(one.columns().get(c))
                        && isNumberColumn(two.columns().get(c))
                        && v1 instanceof Number n1 && v2 instanceof Number n2) {
                    if (Math.abs(n1.doubleValue() - n2.doubleValue()) > Math.abs(delta)) {
                        return cellMismatch(names1.get(c), r, v1, v2);
                    }
                    continue;
                }
                if (isDateColumn(one.columns().get(c))
                        && isDateColumn(two.columns().get(c))) {
                    Double s1 = epochSeconds(v1);
                    Double s2 = epochSeconds(v2);
                    if (s1 != null && s2 != null) {
                        // real: abs(dateDiff(a, b, SECONDS)) <= timeDelta —
                        // dateDiff TRUNCATES whole seconds (a 1ms diff passes
                        // timeDelta 0; fractional compare failed it)
                        if ((long) Math.abs(s1 - s2) > Math.abs(timeDelta)) {
                            return cellMismatch(names1.get(c), r, v1, v2);
                        }
                        continue;
                    }
                }
                if (!wireEquals(v1, v2)) {
                    return cellMismatch(names1.get(c), r, v1, v2);
                }
            }
        }
        return null;
    }

    private static boolean isNumberColumn(com.legend.exec.Column c) {
        return c.pureType() instanceof com.legend.compiler.element.type.Type.Primitive p
                && switch (p) {
                    case INTEGER, FLOAT, DECIMAL, NUMBER -> true;
                    default -> false;
                };
    }

    private static boolean isDateColumn(com.legend.exec.Column c) {
        return c.pureType() instanceof com.legend.compiler.element.type.Type.Primitive p
                && switch (p) {
                    case DATE, DATE_TIME, STRICT_DATE -> true;
                    default -> false;
                };
    }

    private static String cellMismatch(String col, int row, Object v1, Object v2) {
        return "assertTdsEquivalent: cell " + col + "[" + row + "]: "
                + v1 + " vs " + v2;
    }

    private static Double epochSeconds(Object v) {
        // WALL-CLOCK-AS-UTC uniformly: JDBC Timestamp/Date getTime() maps
        // the wall time through the JVM-DEFAULT zone (a TZ-dependent
        // result on non-UTC machines — audit pct-b M4); the connection
        // runs SET TimeZone='UTC', so the wall reading IS the value.
        return switch (v) {
            case java.sql.Timestamp ts ->
                    (double) ts.toLocalDateTime().toEpochSecond(java.time.ZoneOffset.UTC)
                            + ts.getNanos() / 1_000_000_000.0;
            case java.sql.Date d ->
                    (double) d.toLocalDate().atStartOfDay(java.time.ZoneOffset.UTC)
                            .toEpochSecond();
            case java.time.LocalDate ld ->
                    (double) ld.atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond();
            case java.time.LocalDateTime ldt ->
                    (double) ldt.toEpochSecond(java.time.ZoneOffset.UTC)
                            + ldt.getNano() / 1_000_000_000.0;
            case java.time.OffsetDateTime odt ->
                    (double) odt.toEpochSecond() + odt.getNano() / 1_000_000_000.0;
            default -> null;
        };
    }

    private static boolean wireEquals(Object e, Object a) {
        // the null-cell wire sentinel: an expected ^TDSNull() (or a TDS-grid
        // 'TDSNull' cell) equals an actual NULL cell — 'TDSNull' is never a
        // genuine string payload (established: real pure parses it as the
        // instance, our literals decode it to SQL NULL)
        if ("TDSNull".equals(e) && a == null) {
            return true;
        }
        // NO actual-side bridge (audit 16 F5): if a bug ever put the
        // literal string 'TDSNull' on OUR wire where a NULL belongs, the
        // symmetric grant would mask it — same refusal as the temporal
        // bridge below (audit 9)
        if (e == null || a == null) {
            return e == a;
        }
        boolean eInt = e instanceof Long || e instanceof Integer
                || e instanceof Short || e instanceof Byte
                || e instanceof java.math.BigInteger;
        boolean aInt = a instanceof Long || a instanceof Integer
                || a instanceof Short || a instanceof Byte
                || a instanceof java.math.BigInteger;
        if (eInt || aInt) {
            if (!(eInt && aInt)) {
                return false;
            }
            return ((Number) e).longValue() == ((Number) a).longValue();
        }
        if (e instanceof java.math.BigDecimal be && a instanceof java.math.BigDecimal ba) {
            return be.compareTo(ba) == 0;
        }
        boolean eFp = e instanceof Double || e instanceof Float || e instanceof java.math.BigDecimal;
        boolean aFp = a instanceof Double || a instanceof Float || a instanceof java.math.BigDecimal;
        if (eFp || aFp) {
            if (!(eFp && aFp)) {
                return false;
            }
            if (new java.math.BigDecimal(String.valueOf(e))
                    .compareTo(new java.math.BigDecimal(String.valueOf(a))) == 0) {
                return true;
            }
            // DIALECT-ARITHMETIC leniency (documented, the only float one):
            // corpus expectations encode H2's libm (ln/asin/acos/atan...);
            // DuckDB's differs in the LAST ULP on the same real number.
            // Two DOUBLE wire values within 2 ULP compare equal. Exact-zero,
            // kind and BigDecimal (pure Decimal) compares stay strict.
            if (e instanceof Double de && a instanceof Double da
                    && !de.isNaN() && !da.isNaN()) {
                double ulp = Math.ulp(Math.max(Math.abs(de), Math.abs(da)));
                return Math.abs(de - da) <= 2 * ulp;
            }
            return false;
        }
        // TEMPORAL through the Any-carrier: a mixed-collection LITERAL's
        // date decodes as its JSON STRING (the variant carrier is untyped
        // for temporals) — bridge by PARSING, value-exact, and ONLY in the
        // expected-string vs actual-temporal direction: an ACTUAL that
        // comes back as a string where the engine returns a Date is a
        // TYPING BUG this compare must catch (audit 9), never bridge.
        if (e instanceof String es && isTemporal(a)) {
            return temporalEquals(es, a);
        }
        if (e instanceof Map<?, ?> em && a instanceof Map<?, ?> am) {
            if (!em.keySet().equals(am.keySet())) {
                return false;
            }
            for (Object k : em.keySet()) {
                if (!wireEquals(em.get(k), am.get(k))) {
                    return false;
                }
            }
            return true;
        }
        return e.equals(a);
    }

    // ===== substitution: lets inline, handles splice =====

    private static boolean isExecuteCall(AppliedFunction af) {
        return af.function().equals("execute") && af.parameters().size() >= 2;
    }

    /**
     * Replace let-bound variables with their expressions (shadowing lambda
     * params stop substitution). Reads over execute() bindings are NOT
     * substituted here — those statements forward to the platform's result
     * frame, which owns the envelope splice (audit 19d B2).
     */
    private static ValueSpecification substitute(ValueSpecification v,
            Map<String, ValueSpecification> lets) {
        // ^TDSNull() in a TEST literal is the engine's null-cell INSTANCE
        // (a real value, not a pure empty — an empty would VANISH from
        // [^TDSNull(), 5.0] and break the comparison): it travels as the
        // wire sentinel, which wireEquals equates with an actual null cell
        if (v instanceof NewInstance tn
                && (tn.className().equals("TDSNull")
                        || tn.className().equals("meta::pure::tds::TDSNull"))) {
            return new CString("TDSNull");
        }
        if (v instanceof AppliedFunction nf && nf.function().equals("new")
                && nf.parameters().size() == 2
                && nf.parameters().get(1) instanceof NewInstance tn2
                && (tn2.className().equals("TDSNull")
                        || tn2.className().equals("meta::pure::tds::TDSNull"))) {
            return new CString("TDSNull");
        }
        return switch (v) {
            // RECURSIVE: the pulled RHS may itself read lets bound earlier
            // (the per-driver loop's toSQLString($driver) — audit 19d B3
            // exposed the shallow pull when the K-native began TYPING what
            // the old harness arm resolved by hand)
            case Variable var when lets.containsKey(var.name()) ->
                    substitute(lets.get(var.name()), lets);
            case AppliedFunction af -> new AppliedFunction(af.function(),
                    substituteAll(af.parameters(), lets));
            case AppliedProperty ap3 -> new AppliedProperty(
                    substitute(ap3.receiver(), lets), ap3.property());
            case PureCollection pc -> new PureCollection(
                    substituteAll(pc.values(), lets));
            case LambdaFunction lf -> {
                // shadowing params stop LET substitution
                Map<String, ValueSpecification> visible = new LinkedHashMap<>(lets);
                lf.parameters().forEach(p2 -> visible.remove(p2.name()));
                yield new LambdaFunction(lf.parameters(),
                        substituteAll(lf.body(), visible));
            }
            default -> v;
        };
    }

    private static List<ValueSpecification> substituteAll(List<ValueSpecification> vs,
            Map<String, ValueSpecification> lets) {
        List<ValueSpecification> out = new ArrayList<>(vs.size());
        for (ValueSpecification v : vs) {
            out.add(substitute(v, lets));
        }
        return out;
    }

    /** The caller's order contract for one compared side: PURE_ORDERED is
     * unconditional (pure list semantics — the shape walk is irrelevant);
     * SQL_ORDER_POLICY consults the {@link #endsInSort} chain walk. */
    private static boolean ordered(OrderPolicy order, ValueSpecification chainView) {
        return order == OrderPolicy.PURE_ORDERED || endsInSort(chainView);
    }

    /** The chain's OUTER tail carries a sort — its order is a contract. */
    private static boolean endsInSort(ValueSpecification v) {
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
        // order survives through order-preserving tails only
        return switch (fn) {
            case "map", "limit", "take", "drop", "slice", "rows", "toOne", "at",
                    "makeString", "toCSV", "toString", "from" ->
                    !af.parameters().isEmpty() && endsInSort(af.parameters().get(0));
            // eval of a lambda IS its payload (the PCT adapter shape
            // $f->eval(|...->sort()): the platform β-reduces before
            // lowering, so the ORDER CONTRACT rides the payload's last
            // statement. Without this arm every PCT sort assert silently
            // fell to multiset compare — audit pct-b H1.
            case "eval" -> {
                for (int i = af.parameters().size() - 1; i >= 0; i--) {
                    if (af.parameters().get(i) instanceof LambdaFunction lf
                            && !lf.body().isEmpty()) {
                        yield endsInSort(lf.body().get(lf.body().size() - 1));
                    }
                }
                yield false;
            }
            default -> false;
        };
    }

    /** Minimal JSON reader for the GRAPH result envelope (core's own wire). */
    static final class ExecJson {
        private final String s;
        private int i;

        private ExecJson(String s) {
            this.s = s;
        }

        static Object parse(String json) {
            ExecJson p = new ExecJson(json);
            p.ws();
            Object v = p.value();
            p.ws();
            if (p.i < p.s.length()) {
                throw new IllegalStateException("trailing JSON at " + p.i);
            }
            return v;
        }

        private Object value() {
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> obj();
                case '[' -> arr();
                case '"' -> str();
                case 't' -> { i += 4; yield Boolean.TRUE; }
                case 'f' -> { i += 5; yield Boolean.FALSE; }
                case 'n' -> { i += 4; yield null; }
                default -> num();
            };
        }

        private Map<String, Object> obj() {
            Map<String, Object> out = new LinkedHashMap<>();
            i++;
            ws();
            if (s.charAt(i) == '}') {
                i++;
                return out;
            }
            while (true) {
                ws();
                String k = str();
                ws();
                i++;    // ':'
                ws();
                out.put(k, value());
                ws();
                if (s.charAt(i) == ',') {
                    i++;
                    continue;
                }
                i++;    // '}'
                return out;
            }
        }

        private List<Object> arr() {
            List<Object> out = new ArrayList<>();
            i++;
            ws();
            if (s.charAt(i) == ']') {
                i++;
                return out;
            }
            while (true) {
                ws();
                out.add(value());
                ws();
                if (s.charAt(i) == ',') {
                    i++;
                    continue;
                }
                i++;    // ']'
                return out;
            }
        }

        private String str() {
            StringBuilder b = new StringBuilder();
            i++;
            while (s.charAt(i) != '"') {
                char c = s.charAt(i);
                if (c == '\\') {
                    i++;
                    char e = s.charAt(i);
                    b.append(switch (e) {
                        case 'n' -> '\n';
                        case 't' -> '\t';
                        case 'r' -> '\r';
                        case 'b' -> '\b';
                        case 'f' -> '\f';
                        case 'u' -> {
                            char u = (char) Integer.parseInt(
                                    s.substring(i + 1, i + 5), 16);
                            i += 4;
                            yield u;
                        }
                        default -> e;
                    });
                } else {
                    b.append(c);
                }
                i++;
            }
            i++;
            return b.toString();
        }

        private Object num() {
            int start = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
                i++;
            }
            String t = s.substring(start, i);
            // Decimal tokens parse as BigDecimal, NOT double (audit 18):
            // two distinct Decimals beyond 17 significant digits round to
            // the SAME double, so a wrong Decimal wire value would compare
            // equal — the JSON bridge must stay as strict as wireEquals.
            return t.contains(".") || t.contains("e") || t.contains("E")
                    ? (Object) new java.math.BigDecimal(t)
                    : (Object) Long.parseLong(t);
        }

        private void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }
    }
}
