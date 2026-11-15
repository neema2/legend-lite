// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * THE PRODUCT'S RAISE TEXT, MEASURED ON EACH EXTERNAL DATABASE (the orchestrator's request on PR #104's fix round,
 * 2026-11-15): the PCT lanes run the compatibility edge's style, so the PRODUCT road's raise text -- U+001C, the kind,
 * U+001C, its where, a string literal inside every guarded statement whether or not it raises (register S308) -- never
 * reaches an external database there. Here three product-style queries run on the database {@code
 * LEGENDLITE_PROBE_DATABASE} names, each statement's SQL printed:
 * <ol>
 *   <li>a guarded division that does NOT raise must answer its rows (the literal is parsed and planned by the
 *       database even where its branch is never taken);</li>
 *   <li>a division by zero must surface as {@code DIVIDE_BY_ZERO}, as {@code RaisedErrors} reads it;</li>
 *   <li>{@code toOne()} over two rows must surface as {@code SIZE_NOT_MULTIPLICITY} with its where, {@code [1]} (a
 *       collection's count carried none until this probe found it on the five embedded servers, 2026-11-15; the
 *       orchestrator, under the user's ruling of 2026-11-10: placed by its bound, register S308).</li>
 * </ol>
 *
 * <p>A manual probe in {@code //gates:heavy}, beside {@code :identifier_escaping_cloud_probe}: {@code bazel test
 * //core:product_raise_probe} with {@code LEGENDLITE_PROBE_DATABASE} one of {@code mariadb}, {@code mysql},
 * {@code clickhouse}, {@code trino}, {@code mongodb}, {@code postgres} (the embedded servers; Linux x86_64; Postgres
 * as an unprivileged user, tools/postgres/as_unprivileged.sh) or {@code snowflake},
 * {@code databricks}, {@code bigquery} (with {@code LEGENDLITE_PCT_CLOUD_PROPERTIES}, the cloud lanes' account file).
 * One database a run; without it the probe fails by name, it never skips (the skip census).
 */
@Tag("probe")
class ProductRaiseProbeTest {

    /** The backend of the database {@code LEGENDLITE_PROBE_DATABASE} names, by its lane name
     *  ({@code TestBackends.named}). */
    static com.legend.testbackend.TestBackend backend() {
        String named = System.getenv("LEGENDLITE_PROBE_DATABASE");
        if (named == null) {
            throw new IllegalStateException("LEGENDLITE_PROBE_DATABASE names the backend this probe runs on (mariadb,"
                    + " mysql, clickhouse, trino, mongodb, snowflake, databricks or bigquery): it is not defaulted");
        }
        return TestBackends.named(named);
    }

    private static final String ROWS = "#TDS\n  x:Integer, y:Integer\n  4, 2\n  9, 3\n#";

    /** The query planned in the product's style (the door's default) on {@code backend}'s storeless runtime, its every
     *  statement printed, then run: its JSON rows, or the failure it raised. */
    private static Object run(com.legend.testbackend.TestBackend backend, String label, String query)
            throws java.io.IOException, java.sql.SQLException {
        ExecutionPlan plan = Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")), query)
                .executionPlan(StorelessRuntime.RUNTIME);
        System.out.println("[product raise probe] " + backend.name() + " " + label + ": "
                + plan.answerSql().statement().replace(String.valueOf(com.legend.sql.RaiseKind.MARK), "<U+001C>")
                        .replace("\n", " "));
        StringWriter out = new StringWriter();
        try {
            if (backend.opensJdbc()) {
                try (java.sql.Connection c = backend.open()) {
                    PlanRunner.stream(plan, Map.of(), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                            PlanCases.Written.JSON.writer(out, plan.answerShape()), java.util.UUID.randomUUID());
                }
            } else {
                // the session its connector opens, from the plan's own connection
                PlanRunner.stream(plan, Map.of(), PlanSessions.shared(), PlanCases.Written.JSON.writer(out,
                        plan.answerShape()), java.util.UUID.randomUUID());
            }
        } catch (RuntimeException failed) {
            return failed;
        }
        return out.toString();
    }

    @Test
    void theProductsGuardedStatementsRunAndRaiseTheirKinds() throws java.io.IOException, java.sql.SQLException {
        var backend = backend();
        List<String> wrong = new ArrayList<>();
        // 1. a guarded division that does not raise: the literal is in the statement, its branch never taken
        Object rows = run(backend, "guarded, no raise", "|" + ROWS + "->extend(~r: r|$r.x / $r.y)->select(~r)->from("
                + StorelessRuntime.RUNTIME + ")");
        if (!(rows instanceof String s) || !s.contains("2") || !s.contains("3")) {
            wrong.add("guarded, no raise: " + describe(rows));
        }
        // 2. a division by zero: the kind, as RaisedErrors reads it
        Object zero = run(backend, "divide by zero", "|" + ROWS.replace("4, 2", "4, 0")
                + "->extend(~r: r|$r.x / $r.y)->select(~r)->from(" + StorelessRuntime.RUNTIME + ")");
        expectKind(wrong, "divide by zero", zero, com.legend.error.ErrorKind.DIVIDE_BY_ZERO, null);
        // 3. toOne() over two rows: the kind and its where, the bound
        Object two = run(backend, "toOne over two rows", "|" + ROWS + "->map(r|$r.x)->toOne()->from("
                + StorelessRuntime.RUNTIME + ")");
        expectKind(wrong, "toOne over two rows", two, com.legend.error.ErrorKind.SIZE_NOT_MULTIPLICITY, "[1]");
        // 4. a print site: a Float's text (D1.2, register S311): six decimals, trimmed, the same on every database
        Object text = run(backend, "a Float's text", "|#TDS\n  x:Float\n  1.5\n  2.0\n  -1.25\n  100000000000000000000.0\n"
                + "  0.0000001\n#->extend(~t: r|"
                + "$r.x->toOne()->toString())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
        if (!(text instanceof String t4) || !t4.replace(" ", "").equals("[{\"t\":\"1.5\"},{\"t\":\"2\"},{\"t\":\"-1.25\"},"
                + "{\"t\":\"100000000000000000000\"},{\"t\":\"0\"}]")) {
            wrong.add("a Float's text: " + describe(text));
        }
        // 5. the edges of a Float's text (the orchestrator's round 54, 2026-11-15): whole numbers (a trim must keep their
        // zeros), zero and its sign, the halfway region, noise above 4.5e9, NUMBER(38,6)'s 32 integer digits, the
        // double's extremes. Exact where register S311 says every database agrees; -0 and below 5e-7 either sign of
        // zero (S311's rows); from 1e21 up the plain digits with no exponent and no failure (the noise is S311's)
        String[] values = {"100.0", "10.0", "0.0", "-0.0", "123456789.0", "0.30000000000000004", "-0.0000001",
                "1.0E21", "1.0E32", "1.0E300", "-1.0E300", "1.7976931348623157E308", "2.2250738585072014E-308"};
        String[][] expected = {{"100"}, {"10"}, {"0"}, {"0", "-0"}, {"123456789"}, {"0.3"}, {"0", "-0"},
                null, null, null, null, null, {"0"}};
        Object edges = run(backend, "a Float's text, its edges", "|#TDS\n  i:Integer, x:Float\n"
                + java.util.stream.IntStream.range(0, values.length).mapToObj(i -> "  " + i + ", " + values[i] + "\n")
                        .collect(java.util.stream.Collectors.joining())
                + "#->sort(~i->ascending())->extend(~t: r|$r.x->toOne()->toString())->select(~t)->from("
                + StorelessRuntime.RUNTIME + ")");
        if (!(edges instanceof String e5)) {
            wrong.add("a Float's text, its edges: " + describe(edges));
        } else {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"t\":\"([^\"]*)\"").matcher(e5);
            List<String> got = new ArrayList<>();
            while (m.find()) {
                got.add(m.group(1));
            }
            for (int i = 0; i < values.length; i++) {
                String g = i < got.size() ? got.get(i) : null;
                boolean ok = g != null && (expected[i] != null ? List.of(expected[i]).contains(g)
                        : g.matches("-?[0-9]+(\\.[0-9]{1,6})?") && g.replace("-", "").length() >= 22);
                if (!ok) {
                    wrong.add("a Float's text of " + values[i] + ": " + g);
                }
            }
        }
        // 6. a DateTime's text (D1.2, register S312): ISO 8601 in UTC, six fraction digits and Z
        Object dt = run(backend, "a DateTime's text", "|#TDS\n  x:DateTime\n  2014-01-01T10:20:30.5\n#->extend(~t: r|"
                + "$r.x->toOne()->toString())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
        if (!(dt instanceof String t6) || !t6.replace(" ", "").equals("[{\"t\":\"2014-01-01T10:20:30.500000Z\"}]")) {
            wrong.add("a DateTime's text: " + describe(dt));
        }
        // what came back, each case's (the orchestrator, 2026-11-15: every lane's log shows it, not only the count)
        System.out.println("[product raise probe] " + backend.name() + " guarded, no raise, answered: " + describe(rows));
        System.out.println("[product raise probe] " + backend.name() + " divide by zero, raised: " + describe(zero));
        System.out.println("[product raise probe] " + backend.name() + " toOne over two rows, raised: " + describe(two));
        System.out.println("[product raise probe] " + backend.name() + " a Float's text, answered: " + describe(text));
        System.out.println("[product raise probe] " + backend.name() + " a Float's text, its edges "
                + List.of(values) + ", answered: " + describe(edges));
        System.out.println("[product raise probe] " + backend.name() + " a DateTime's text, answered: " + describe(dt));
        System.out.println("[product raise probe] " + backend.name() + ": " + wrong.size() + " wrong " + wrong);
        assertEquals(List.of(), wrong, backend.name());
    }

    private static void expectKind(List<String> wrong, String label, Object answered,
            com.legend.error.ErrorKind kind, @com.legend.base.Nullable String where) {
        com.legend.error.DataError error = null;
        for (Throwable t = answered instanceof Throwable th ? th : null; t != null; t = t.getCause()) {
            if (t instanceof com.legend.error.DataError d) {
                error = d;
                break;
            }
        }
        if (error == null || error.kind() != kind
                || where != null && !String.valueOf(error.getMessage()).endsWith(": " + where)) {
            wrong.add(label + ": expected " + kind + (where == null ? "" : " at " + where) + ", got " + describe(answered));
        }
    }

    private static String describe(Object answered) {
        return answered instanceof Throwable t ? t.getClass().getSimpleName() + ": " + t.getMessage() : String.valueOf(answered);
    }
}
