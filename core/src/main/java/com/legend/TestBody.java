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
                ValueSpecification rhs = substitute(af.parameters().get(1), lets, handles, runtimeFqn);
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
                        runtimeFqn, conn);
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
            ModelContext ctx, ImportScope imports, String runtimeFqn, Connection conn)
            throws java.sql.SQLException {
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
                Eval a = eval(args.get(0), lets, handles, ctx, imports, runtimeFqn, conn);
                Object n = evalScalar(args.get(1), lets, handles, ctx, imports,
                        runtimeFqn, conn);
                long actual = a.size();
                return (n instanceof Number num && num.longValue() == actual) ? null
                        : "assertSize: expected " + n + ", got " + actual;
            }
            case "assertEmpty" -> {
                if (args.size() != 1) {
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

    /** A golden-SQL spelling: any chain ending in sqlRemoveFormatting()/sql(). */
    private static boolean isSqlText(ValueSpecification v) {
        return v instanceof AppliedFunction af
                && (af.function().equals("sqlRemoveFormatting") || af.function().equals("sql"));
    }

    // ===== evaluation: compile one side through the pipeline =====

    /** One evaluated side: the execution result + how it compares. */
    private record Eval(com.legend.exec.ExecutionResult result, boolean sortedChain) {

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
        com.legend.exec.ExecutionResult r = evalSpliced(spliced, ctx, imports,
                runtimeFqn, conn);
        return new Eval(r, endsInSort(spliced));
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

    /** Minimal JSON reader for graph envelopes (mirrors the exec wire). */
    static final class ExecJson {
        static Object parse(String json) {
            // graph results compare rarely in M1; a real reader arrives with
            // M3 (class-query results). Loud, never wrong:
            throw new com.legend.error.NotImplementedException(
                    "graph-result comparison in TestBody is not supported yet (M3)");
        }
    }
}
