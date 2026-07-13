// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.ImportScope;
import com.legend.parser.SpecParser;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.AppliedProperty;
import com.legend.parser.spec.CBoolean;
import com.legend.parser.spec.CString;
import com.legend.parser.spec.LambdaFunction;
import com.legend.parser.spec.PackageableElementPtr;
import com.legend.parser.spec.PureCollection;
import com.legend.parser.spec.ValueSpecification;
import com.legend.parser.spec.Variable;

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

    /** A bound {@code execute(...)}: the lazy query handle + its context. */
    private record ExecHandle(LambdaFunction query, ValueSpecification mappingRef) {
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
        return run(ctx, body, imports, runtimeFqn, conn, emptinessUnverifiable,
                java.util.Set.of());
    }

    /**
     * {@code harnessSetupCalls}: names (simple + qualified) of engine-harness
     * SETUP functions whose effects the caller already replayed (seed
     * machinery) — a bare statement calling one is the harness contract,
     * not test semantics, and skips.
     */
    public static Outcome run(ModelContext ctx, String body, ImportScope imports,
            String runtimeFqn, Connection conn, boolean emptinessUnverifiable,
            java.util.Set<String> harnessSetupCalls)
            throws java.sql.SQLException {
        List<ValueSpecification> stmts = SpecParser.parseCodeBlock(body);
        Map<String, ValueSpecification> lets = new LinkedHashMap<>();
        Map<String, ExecHandle> handles = new LinkedHashMap<>();
        int verified = 0;
        int advisory = 0;
        for (ValueSpecification stmt : stmts) {
            // let name = rhs
            if (stmt instanceof AppliedFunction af && af.function().equals("letFunction")
                    && af.parameters().size() == 2
                    && af.parameters().get(0) instanceof CString name) {
                ValueSpecification raw = af.parameters().get(1);
                // let r = execute(...).values / .values->at(0) / ->toOne():
                // the wrappers are the Result envelope — peel to the handle
                while (true) {
                    if (raw instanceof AppliedProperty pw
                            && pw.property().equals("values")) {
                        raw = pw.receiver();
                        continue;
                    }
                    if (raw instanceof AppliedFunction fw
                            && (fw.function().equals("at") || fw.function().equals("toOne"))
                            && !fw.parameters().isEmpty()
                            && (fw.parameters().get(0) instanceof AppliedProperty
                                    || fw.parameters().get(0) instanceof AppliedFunction ifn
                                            && isExecuteCall(ifn))) {
                        raw = fw.parameters().get(0);
                        continue;
                    }
                    break;
                }
                ValueSpecification rhs = raw instanceof AppliedFunction rex
                        && isExecuteCall(rex)
                        ? new AppliedFunction(rex.function(), substituteAll(
                                rex.parameters(), lets, handles, runtimeFqn))
                        : substitute(af.parameters().get(1), lets, handles, runtimeFqn);
                if (rhs instanceof AppliedFunction ex && isExecuteCall(ex)) {
                    ExecHandle h = toHandle(ex);
                    if (h == null) {
                        return new Outcome.Unsupported(
                                "execute() whose query argument is not a lambda");
                    }
                    handles.put(name.value(), h);
                } else {
                    lets.put(name.value(), rhs);
                }
                continue;
            }
            if (stmt instanceof AppliedFunction af && af.function().startsWith("assert")) {
                String failure = checkAssert(af, lets, handles, ctx, imports,
                        runtimeFqn, conn, emptinessUnverifiable);
                if (failure == UNSUPPORTED_MARKER) {
                    return new Outcome.Unsupported("assert form '" + af.function()
                            + "/" + af.parameters().size() + "' is not supported yet");
                }
                if (ADVISORY_MARKER.equals(failure)) {
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
            if (stmt instanceof AppliedFunction af && isExecuteCall(af)) {
                return new Outcome.Unsupported("execute() not bound to a let");
            }
            if (stmt instanceof AppliedFunction af
                    && (harnessSetupCalls.contains(af.function())
                            || harnessSetupCalls.contains(simpleName(af.function())))) {
                continue;   // seed-replayed harness setup — already applied
            }
            return new Outcome.Unsupported("unsupported statement: "
                    + (stmt instanceof AppliedFunction af2 ? af2.function()
                            : stmt.getClass().getSimpleName()));
        }
        return new Outcome.Ran(verified, advisory, List.of());
    }

    // ===== assert dispatch =====

    private static final String UNSUPPORTED_MARKER = new String("unsupported");
    private static final String ADVISORY_MARKER = new String("advisory");

    /** null = held; ADVISORY_MARKER = golden-SQL; UNSUPPORTED_MARKER; else the failure text. */
    private static String checkAssert(AppliedFunction af,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn,
            boolean emptinessUnverifiable) throws java.sql.SQLException {
        List<ValueSpecification> args = af.parameters();
        switch (af.function()) {
            case "assert", "assertFalse" -> {
                if (args.isEmpty()) {
                    return UNSUPPORTED_MARKER;
                }
                Object v = evalScalar(args.get(0), lets, handles, ctx, imports,
                        runtimeFqn, conn);
                boolean expect = af.function().equals("assert");
                return Boolean.valueOf(expect).equals(v) ? null
                        : "assert" + (expect ? "" : "False") + " did not hold ("
                                + v + ")";
            }
            case "assertEquals", "assertEqualsH2Compatible", "assertNotEquals" -> {
                if (args.size() < 2) {
                    return UNSUPPORTED_MARKER;
                }
                // golden-SQL spellings are advisory: our SQL is DuckDB's
                if (isSqlText(args.get(args.size() - 1)) || isSqlText(args.get(0))) {
                    return ADVISORY_MARKER;
                }
                // legacy 3-arg H2-compat: (legacySql, h2Sql, actualSql) —
                // all SQL text, advisory
                if (args.size() == 3 && af.function().equals("assertEqualsH2Compatible")) {
                    return ADVISORY_MARKER;
                }
                Eval e = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                Eval a = eval(args.get(1), lets, handles, ctx, imports, runtimeFqn, conn);
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
                Eval e = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                Eval a = eval(args.get(1), lets, handles, ctx, imports, runtimeFqn, conn);
                return compare(e, a, /* ordered */ false) ? null
                        : "assertSameElements: expected " + e.render() + ", got " + a.render();
            }
            case "assertSize" -> {
                if (args.size() != 2) {
                    return UNSUPPORTED_MARKER;
                }
                Object n = evalScalar(args.get(1), lets, handles, ctx, imports,
                        runtimeFqn, conn);
                if (emptinessUnverifiable && n instanceof Number zn && zn.longValue() == 0) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                long actual = a.size();
                return (n instanceof Number num && num.longValue() == actual) ? null
                        : "assertSize: expected " + n + ", got " + actual;
            }
            case "assertEmpty" -> {
                if (args.size() != 1) {
                    return UNSUPPORTED_MARKER;
                }
                if (emptinessUnverifiable) {
                    return UNSUPPORTED_MARKER;
                }
                Eval a = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                return a.size() == 0 ? null : "assertEmpty: got " + a.size() + " values";
            }
            case "assertSameSQL" -> {
                return ADVISORY_MARKER;
            }
            default -> {
                return UNSUPPORTED_MARKER;
            }
        }
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

    // ===== evaluation: compile one side through the pipeline =====

    /** One evaluated side: the execution result + how it compares. */
    private record Eval(com.legend.exec.ExecutionResult result, boolean sortedChain,
            boolean csvTail) {

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

    private static Eval eval(ValueSpecification expr,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        ValueSpecification spliced = substitute(expr, lets, handles, runtimeFqn);
        // SERIALIZATION TAILS (toCSV/toString over a TDS) strip: the grid
        // compares STRUCTURALLY (or renders for a string-literal peer) —
        // rendering is a wire concern, not a query. A tail whose receiver
        // turns out non-relational falls back to the original expression.
        boolean csv = false;
        if (spliced instanceof AppliedFunction tail
                && (simpleName(tail.function()).equals("toCSV")
                        || simpleName(tail.function()).equals("toString"))
                && tail.parameters().size() == 1) {
            com.legend.exec.ExecutionResult stripped = evalSpliced(
                    tail.parameters().get(0), ctx, imports, runtimeFqn, conn);
            if (stripped instanceof com.legend.exec.ExecutionResult.Tabular) {
                return new Eval(stripped, endsInSort(tail.parameters().get(0)),
                        simpleName(tail.function()).equals("toCSV"));
            }
        }
        com.legend.exec.ExecutionResult r = evalSpliced(spliced, ctx, imports,
                runtimeFqn, conn);
        return new Eval(r, endsInSort(spliced), csv);
    }

    private static Object evalScalar(ValueSpecification expr,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        Eval e = eval(expr, lets, handles, ctx, imports, runtimeFqn, conn);
        List<Object> v = e.values();
        return v.size() == 1 ? v.get(0) : v;
    }

    /** Compile + execute ONE expression (handles already spliced in). */
    private static com.legend.exec.ExecutionResult evalSpliced(ValueSpecification expr,
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
        LambdaFunction wrapped = new LambdaFunction(List.of(), List.of(expr));
        ValueSpecification resolved = NameResolver.resolveQuery(wrapped, imports,
                ctx.elementFqns());
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(resolved);
        body = new com.legend.compiler.spec.UserCallInliner(specs).inlineBody(body);
        body = new com.legend.resolver.StoreResolver(ctx, specs).resolve(body, runtimeFqn);
        TypedSpec root = body.get(body.size() - 1);
        com.legend.sql.SqlQuery plan = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent()).lower(body);
        com.legend.sql.dialect.SqlDialect dialect = Compiler.dialectOf(ctx, runtimeFqn);
        return com.legend.exec.Executor.execute(dialect.render(plan), plan,
                root.info(), com.legend.exec.ResultShape.of(root), conn, dialect);
    }

    // ===== comparison (both sides share ONE wire convention — strict) =====

    private static boolean compare(Eval expected, Eval actual, boolean ordered) {
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
                line.append(v == null ? "" : String.valueOf(v));
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
    private static boolean wireEquals(Object e, Object a) {
        if (e == null || a == null) {
            return e == a;
        }
        boolean eInt = e instanceof Long || e instanceof Integer || e instanceof java.math.BigInteger;
        boolean aInt = a instanceof Long || a instanceof Integer || a instanceof java.math.BigInteger;
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
            return new java.math.BigDecimal(String.valueOf(e))
                    .compareTo(new java.math.BigDecimal(String.valueOf(a))) == 0;
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

    private static ExecHandle toHandle(AppliedFunction ex) {
        ValueSpecification q = ex.parameters().get(0);
        if (!(q instanceof LambdaFunction lf) || !lf.parameters().isEmpty()) {
            return null;
        }
        return new ExecHandle(lf, ex.parameters().get(1));
    }

    /**
     * Replace let-bound variables with their expressions and handle reads
     * with the SPLICED query: {@code $r.values} &rarr; the query chain
     * wrapped in {@code ->from(mapping, runtime? no — context rides the
     * driver runtime)}; head spellings {@code $r.values->at(0)} /
     * {@code ->toOne()} collapse onto the chain (the engine's Result.values
     * for a TDS query IS the single TDS). Shadowing lambda params stop
     * let-substitution.
     */
    private static ValueSpecification substitute(ValueSpecification v,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            String runtimeFqn) {
        // $r.values → spliced query (with mapping context attached)
        if (v instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable var
                && handles.containsKey(var.name())
                && ap.property().equals("values")) {
            return splice(handles.get(var.name()), runtimeFqn);
        }
        // $r.values->at(0) / ->toOne(): the values envelope of a TDS query
        // holds ONE TDS — the wrapper collapses
        if (v instanceof AppliedFunction af
                && (af.function().equals("at") || af.function().equals("toOne"))
                && !af.parameters().isEmpty()
                && af.parameters().get(0) instanceof AppliedProperty ap2
                && ap2.receiver() instanceof Variable var2
                && handles.containsKey(var2.name())
                && ap2.property().equals("values")) {
            return splice(handles.get(var2.name()), runtimeFqn);
        }
        return switch (v) {
            case Variable var when lets.containsKey(var.name()) -> lets.get(var.name());
            case Variable var when handles.containsKey(var.name()) ->
                    splice(handles.get(var.name()), runtimeFqn);
            // an INLINE execute (chained without a let: execute(...).values
            // or nested in an assert arg) splices in place
            case AppliedFunction af when isExecuteCall(af) && toHandle(af) != null ->
                    splice(toHandle(new AppliedFunction(af.function(),
                            substituteAll(af.parameters(), lets, handles, runtimeFqn))),
                            runtimeFqn);
            case AppliedFunction af -> new AppliedFunction(af.function(),
                    substituteAll(af.parameters(), lets, handles, runtimeFqn));
            case AppliedProperty ap3 -> new AppliedProperty(
                    substitute(ap3.receiver(), lets, handles, runtimeFqn), ap3.property());
            case PureCollection pc -> new PureCollection(
                    substituteAll(pc.values(), lets, handles, runtimeFqn));
            case LambdaFunction lf -> {
                // shadowing params stop LET substitution (handles keep —
                // a lambda param can't shadow an execute binding usefully)
                Map<String, ValueSpecification> visible = new LinkedHashMap<>(lets);
                lf.parameters().forEach(p2 -> visible.remove(p2.name()));
                yield new LambdaFunction(lf.parameters(),
                        substituteAll(lf.body(), visible, handles, runtimeFqn));
            }
            default -> v;
        };
    }

    private static List<ValueSpecification> substituteAll(List<ValueSpecification> vs,
            Map<String, ValueSpecification> lets, Map<String, ExecHandle> handles,
            String runtimeFqn) {
        List<ValueSpecification> out = new ArrayList<>(vs.size());
        for (ValueSpecification v : vs) {
            out.add(substitute(v, lets, handles, runtimeFqn));
        }
        return out;
    }

    /** The handle's query chain with its execution context attached. */
    private static ValueSpecification splice(ExecHandle h, String runtimeFqn) {
        ValueSpecification chain = h.query().body().get(h.query().body().size() - 1);
        // ->from(mapping, runtime): the 3-ARG form — the 2-arg overload is
        // (source, RUNTIME) and would read the mapping as a runtime. An
        // in-chain from() the corpus wrote itself already carries its own.
        if (containsFrom(chain)) {
            return chain;
        }
        return new AppliedFunction("from", List.of(chain, h.mappingRef(),
                new PackageableElementPtr(runtimeFqn)));
    }

    private static boolean containsFrom(ValueSpecification v) {
        if (v instanceof AppliedFunction af) {
            if (af.function().equals("from")) {
                return true;
            }
            for (ValueSpecification p : af.parameters()) {
                if (containsFrom(p)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The chain's OUTER tail carries a sort — its order is a contract. */
    private static boolean endsInSort(ValueSpecification v) {
        if (!(v instanceof AppliedFunction af)) {
            return false;
        }
        if (af.function().equals("sort") || af.function().equals("sortBy")) {
            return true;
        }
        // order survives through order-preserving tails only
        return switch (af.function()) {
            case "map", "limit", "take", "drop", "slice", "rows", "toOne", "at",
                    "makeString", "toCSV", "toString", "from" ->
                    !af.parameters().isEmpty() && endsInSort(af.parameters().get(0));
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
            return t.contains(".") || t.contains("e") || t.contains("E")
                    ? (Object) Double.parseDouble(t) : (Object) Long.parseLong(t);
        }

        private void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }
    }
}
