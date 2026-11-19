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
    private static Object run(com.legend.testbackend.TestBackend backend, String label, String query) {
        StringWriter out = new StringWriter();
        // each case on its own (W2 of docs/records/d-plan-2026-11-16.md, round 57's H2 finding): a refusal at planning
        // or at run is this case's answer, recorded by name, never an abort of the cases after it
        try {
            ExecutionPlan plan = Compiler.query(Compiler.compileModel(backend.withStorelessRuntime("")), query)
                    .executionPlan(StorelessRuntime.RUNTIME);
            System.out.println("[product raise probe] " + backend.name() + " " + label + ": "
                    + plan.answerSql().statement().replace(String.valueOf(com.legend.sql.RaiseKind.MARK), "<U+001C>")
                            .replace("\n", " "));
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
        } catch (RuntimeException | java.io.IOException | java.sql.SQLException failed) {
            return failed;
        }
        return out.toString();
    }

    @Test
    void theProductsGuardedStatementsRunAndRaiseTheirKinds() {
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
        if (two instanceof com.legend.sql.DialectCapability dc && backend.name().equalsIgnoreCase("h2")) {
            // a database with no list encoding (H2's) refuses the list toOne() reads by name at planning, never
            // answering (the orchestrator's W3 item 5, 2026-11-16: the size check, or a refusal by name the probe asserts;
            // H2's alone, the P2 checkpoint audit's A-S4: on any other database the refusal is wrong)
            if (!String.valueOf(dc.getMessage()).contains("LIST_GET reached a dialect without a list encoding")) {
                wrong.add("toOne over two rows: expected the list's refusal by name, got " + describe(two));
            }
        } else {
            expectKind(wrong, "toOne over two rows", two, com.legend.error.ErrorKind.SIZE_NOT_MULTIPLICITY, "[1]");
        }
        // 4. a print site: a Float's text (D1.2, register S311): six decimals, trimmed, the same on every database
        Object text = run(backend, "a Float's text", "|#TDS\n  x:Float\n  1.5\n  2.0\n  -1.25\n#->extend(~t: r|"
                + "$r.x->toOne()->toString())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
        if (!(text instanceof String t4) || !t4.replace(" ", "").equals("[{\"t\":\"1.5\"},{\"t\":\"2\"},"
                + "{\"t\":\"-1.25\"}]")) {
            wrong.add("a Float's text: " + describe(text));
        }
        // 5. a Float's text on ORDINARY values (the orchestrator's revert of 2026-11-16, the user's question: S311 is
        // ONE simple format call per database, six decimals trimmed): whole numbers, a fraction, a third, 0.1 + 0.2, a
        // nine-digit integer, zero. S311's recorded divergences -- a huge value's digits, a six-place tie, a value
        // below 5e-7, -0 -- are the register's, by database, and not judged here
        // -0.0 prints 0 on every database (S311's ruled line, the P2 checkpoint audit's case, 2026-11-16)
        String[] values = {"100.0", "10.0", "0.0", "123456789.0", "0.30000000000000004", "1234567.89",
                "-98765.4321", "0.3333333333333333", "42.125", "-0.0"};
        Object edges = run(backend, "a Float's text, ordinary values", "|#TDS\n  i:Integer, x:Float\n"
                + java.util.stream.IntStream.range(0, values.length).mapToObj(i -> "  " + i + ", " + values[i] + "\n")
                        .collect(java.util.stream.Collectors.joining())
                + "#->sort(~i->ascending())->extend(~t: r|$r.x->toOne()->toString())->select(~t)->from("
                + StorelessRuntime.RUNTIME + ")");
        if (!(edges instanceof String e5)) {
            wrong.add("a Float's text, ordinary values: " + describe(edges));
        } else {
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"t\":\"([^\"]*)\"").matcher(e5);
            List<String> got = new ArrayList<>();
            while (m.find()) {
                got.add(m.group(1));
            }
            for (int i = 0; i < values.length; i++) {
                String g = i < got.size() ? got.get(i) : null;
                String want = floatText(Double.parseDouble(values[i]));
                if (!want.equals(g)) {
                    wrong.add("a Float's text of " + values[i] + ": " + g + " (want " + want + ")");
                }
            }
        }
        // 5b. NaN's text (S311, ruled 2026-11-10: NaN): "NaN" where the database holds it, else the kind by name
        // (S138's VALUE_NOT_HELD: a database that holds no NaN), never another text (the P2 checkpoint audit's case)
        Object nan = run(backend, "NaN's text", "|#TDS\n  x:String\n  NaN\n#->extend(~t: r|"
                + "$r.x->toOne()->parseFloat()->toString())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
        if (!(nan instanceof String n5 && n5.contains("NaN"))) {
            if (nan instanceof Throwable) {
                expectKind(wrong, "NaN's text", nan, com.legend.error.ErrorKind.VALUE_NOT_HELD, null);
            } else {
                wrong.add("NaN's text: " + describe(nan));
            }
        }
        // 5c. a user's text holding '?' (round 73: BigQuery's driver counts a '?' inside a literal as a placeholder,
        // so a dialect writes none raw): the text itself
        Object q = run(backend, "a text holding ?", "|#TDS\n  k:Integer\n  1\n#->extend(~t: r|'what?')->select(~t)"
                + "->from(" + StorelessRuntime.RUNTIME + ")");
        if (!(q instanceof String q5 && q5.replace(" ", "").equals("[{\"t\":\"what?\"}]"))) {
            wrong.add("a text holding ?: " + describe(q));
        }
        System.out.println("[product raise probe] " + backend.name() + " a text holding ?, answered: " + describe(q));
        // 5d. a parsed Integer's text (round 73: on Snowflake a raise typed VARCHAR beside the BIGINT conversion made
        // the guard a scaled NUMBER, whose text is 17.00000): the Integer's own digits
        Object pi = run(backend, "a parsed Integer's text", "|#TDS\n  x:String\n  17\n#->extend(~t: r|"
                + "$r.x->toOne()->parseInteger()->toString())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
        if (!(pi instanceof String p5 && p5.replace(" ", "").equals("[{\"t\":\"17\"}]"))) {
            wrong.add("a parsed Integer's text: " + describe(pi));
        }
        System.out.println("[product raise probe] " + backend.name() + " a parsed Integer's text, answered: "
                + describe(pi));
        // 6. a DateTime's text (D1.2, register S312): ISO 8601 in UTC, six fraction digits and Z
        Object dt = run(backend, "a DateTime's text", "|#TDS\n  x:DateTime\n  2014-01-01T10:20:30.5\n#->extend(~t: r|"
                + "$r.x->toOne()->toString())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
        if (!(dt instanceof String t6) || !t6.replace(" ", "").equals("[{\"t\":\"2014-01-01T10:20:30.500000Z\"}]")) {
            wrong.add("a DateTime's text: " + describe(dt));
        }
        // 7. the parse family (Phase 8, 2026-11-16; round 71's gap: the external lanes run this probe alone): each
        // input's parseFloat and parseInteger, legend-pure's value or a refusal by name, never a silent wrong value
        parses(backend, wrong);
        // 8. the Float carrier (Phase 8, 2026-11-16): a computed Float carried in a list and read back bit-exact, or
        // the list refused by name where the database has no list encoding (round 73 measures every database's)
        carriers(backend, wrong);
        // 9. a row's cells joined (Phase 3 step 3, CONCAT_WS where the database skips a NULL part, its own form where it
        // does not; the user's ruling of 2026-11-10, "makeString skips empty elements"): an empty cell skipped with its
        // separator, an all-empty row and an empty single cell the empty text (never a NULL), and a cell's own text
        // never escaped (a separator inside a cell stays, as legend-pure's makeString)
        String[][] joins = {
                {"three cells, the middle empty; then all empty", "#TDS\n  a:String, b:String, c:String\n"
                        + "  a, null, c\n  null, null, null\n#", "a, c", ""},
                {"one cell, empty; then 'x'", "#TDS\n  a:String\n  null\n  x\n#", "", "x"},
                {"a cell holding the separator", "#TDS\n  c:String\n  c\n#->extend(~s: r|'a, b')->select(~[s, c])",
                        "a, b, c"}};
        List<Object> joined = new ArrayList<>();
        for (String[] j : joins) {
            Object answered = run(backend, "a row's cells joined: " + j[0], "|" + j[1] + "->extend(~t: r|$r.values"
                    + "->makeString(', '))->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
            joined.add(answered);
            List<String> want = List.of(j).subList(2, j.length);
            if (!(answered instanceof String a7)) {
                wrong.add("a row's cells joined, " + j[0] + ": " + describe(answered));
                continue;
            }
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"t\":(\"([^\"]*)\"|null)").matcher(a7);
            List<String> got = new ArrayList<>();
            while (m.find()) {
                got.add(m.group(2) == null ? "<NULL>" : m.group(2));
            }
            if (!got.equals(want)) {
                wrong.add("a row's cells joined, " + j[0] + ": " + got + " (want " + want + ")");
            }
        }
        // 10. a Float literal is the double (register S20, built 2026-11-17, Phase 5b): IEEE arithmetic on every
        // database -- 0.1 + 0.2 is the double 0.30000000000000004, and eighteen written digits are the double's
        // (DuckDB's DECIMAL(18) raised "Overflow in multiplication" on 123456789.123456789 * 3.0 until then); each
        // case's SQL is printed above, so a lane shows which DOUBLE form ran
        Object sum = run(backend, "0.1 + 0.2", "|0.1 + 0.2");
        Object product = run(backend, "123456789.123456789 * 3.0", "|123456789.123456789 * 3.0");
        expectDouble(wrong, "0.1 + 0.2", sum, 0.1 + 0.2);
        expectDouble(wrong, "123456789.123456789 * 3.0", product, 123456789.123456789 * 3.0);
        // 11. the four ways (the user's request, 2026-11-17; FloatAndDecimalTest): a Float literal and a Float column
        // answer the double's, a Decimal literal and a Decimal column the exact value -- or, on a database that holds no
        // exact decimal, its named refusal (D2a). Every row judged but round's (F47, parked: legend-pure rounds half-up
        // over the shortest decimal, lite half-even until it lands), and each case's SQL printed above
        String row = "|#TDS\n  a:%1$s, b:%1$s\n  %2$s, %3$s\n#->extend(~v: r|%4$s)->select(~v)->from("
                + StorelessRuntime.RUNTIME + ")";
        String[][] fourWays = {{"0.1", "0.2", "$a + $b"}, {"0.1", "0.2", "($a + $b) == 0.3"},
                {"0.3", "0.1", "rem($a, $b)"}, {"123456789.123456789", "3.0", "$a * $b"}, {"0.25", "0", "round($a, 1)"}};
        for (String[] w : fourWays) {
            String lit = w[2].replace("$a", w[0]).replace("$b", w[1]);
            String col = w[2].replace("$a", "$r.a->toOne()").replace("$b", "$r.b->toOne()");
            String decLit = "|" + w[2].replace("$a", w[0] + "D").replace("$b", w[1] + "D").replace("== 0.3", "== 0.3D");
            Object fl = run(backend, "four ways: Float literal " + lit, "|" + lit);
            Object fc = run(backend, "four ways: Float column " + col, String.format(row, "Float", w[0], w[1], col));
            Object dl = run(backend, "four ways: Decimal literal " + lit, decLit);
            Object dc = run(backend, "four ways: Decimal column " + col, String.format(row, "Decimal", w[0], w[1],
                    col.replace("== 0.3", "== 0.3D")));
            System.out.println("[product raise probe] " + backend.name() + " four ways " + lit + ": Float literal "
                    + describe(fl) + ", Float column " + describe(fc) + ", Decimal literal " + describe(dl)
                    + ", Decimal column " + describe(dc));
            double a = Double.parseDouble(w[0]);
            double b = Double.parseDouble(w[1]);
            java.math.BigDecimal x = new java.math.BigDecimal(w[0]);
            java.math.BigDecimal y = new java.math.BigDecimal(w[1]);
            switch (w[2]) {
                case "$a + $b" -> {
                    expectDouble(wrong, "four ways: Float literal " + lit, fl, a + b);
                    expectDouble(wrong, "four ways: Float column " + col, fc, a + b);
                    expectExact(wrong, "four ways: Decimal literal " + lit, dl, x.add(y));
                    expectExact(wrong, "four ways: Decimal column " + col, dc, x.add(y));
                }
                case "($a + $b) == 0.3" -> {
                    expectText(wrong, "four ways: Float literal " + lit, fl, "false");
                    expectText(wrong, "four ways: Float column " + col, fc, "false");
                    expectDecimalText(wrong, "four ways: Decimal literal " + lit, dl, "true");
                    expectDecimalText(wrong, "four ways: Decimal column " + col, dc, "true");
                }
                case "rem($a, $b)" -> {
                    expectDouble(wrong, "four ways: Float literal " + lit, fl, a % b);
                    expectDouble(wrong, "four ways: Float column " + col, fc, a % b);
                    expectExact(wrong, "four ways: Decimal literal " + lit, dl, x.remainder(y));
                    expectExact(wrong, "four ways: Decimal column " + col, dc, x.remainder(y));
                }
                case "$a * $b" -> {
                    expectDouble(wrong, "four ways: Float literal " + lit, fl, a * b);
                    expectDouble(wrong, "four ways: Float column " + col, fc, a * b);
                    expectExact(wrong, "four ways: Decimal literal " + lit, dl, x.multiply(y));
                    expectExact(wrong, "four ways: Decimal column " + col, dc, x.multiply(y));
                }
                default -> {
                    // round: printed only (F47, parked)
                }
            }
        }
        // 12. lite's canonical text of a Float (FLOAT_SHORTEST_TEXT, a message's text of a Float; Q4 and the 2^60 case of
        // the PR #118 follow-ups, 2026-11-19): the host writer's text, CellText.carriedFloat, the double's shortest
        // digits laid out -- SQLite printed 9223372036854775807.0 for every whole value past 2^63, ClickHouse's Int128
        // 10000000000000000000482.0 for 1e22, Postgres's numeric 15 digits; each statement sent as rendered, on a
        // database the probe reaches over JDBC
        if (backend.opensJdbc()) {
            canonicalTexts(backend, wrong);
        }
        // what came back, each case's (the orchestrator, 2026-11-15: every lane's log shows it, not only the count)
        System.out.println("[product raise probe] " + backend.name() + " guarded, no raise, answered: " + describe(rows));
        System.out.println("[product raise probe] " + backend.name() + " divide by zero, raised: " + describe(zero));
        System.out.println("[product raise probe] " + backend.name() + " toOne over two rows, raised: " + describe(two));
        System.out.println("[product raise probe] " + backend.name() + " a Float's text, answered: " + describe(text));
        System.out.println("[product raise probe] " + backend.name() + " a Float's text, ordinary values "
                + List.of(values) + ", answered: " + describe(edges));
        System.out.println("[product raise probe] " + backend.name() + " NaN's text, answered: " + describe(nan));
        System.out.println("[product raise probe] " + backend.name() + " a DateTime's text, answered: " + describe(dt));
        for (int i = 0; i < joins.length; i++) {
            System.out.println("[product raise probe] " + backend.name() + " a row's cells joined, " + joins[i][0]
                    + ", answered: " + describe(joined.get(i)));
        }
        System.out.println("[product raise probe] " + backend.name() + " 0.1 + 0.2, answered: " + describe(sum));
        System.out.println("[product raise probe] " + backend.name() + " 123456789.123456789 * 3.0, answered: "
                + describe(product));
        System.out.println("[product raise probe] " + backend.name() + ": " + wrong.size() + " wrong " + wrong);
        assertEquals(List.of(), wrong, backend.name());
    }

    /** A parse's expected answer: a number (legend-pure's), a raise, NaN or Infinity (the value, or VALUE_NOT_HELD
     *  where the database holds none), or either (a follow-up's open case, printed). */
    private record Parse(String fn, String text, String expect) {
    }

    private static final List<Parse> PARSES = List.of(
            new Parse("parseFloat", "abc", "raise"), new Parse("parseFloat", "", "raise"),
            new Parse("parseFloat", "+-1", "raise"), new Parse("parseFloat", "0x10", "raise"),
            new Parse("parseFloat", " 1 ", "1.0"), new Parse("parseFloat", "1f", "1.0"),
            new Parse("parseFloat", "1e5", "100000.0"), new Parse("parseFloat", "NaN", "NaN"),
            new Parse("parseFloat", "Infinity", "Infinity"), new Parse("parseFloat", "12", "12.0"),
            // the grammar's optional parts, which a database's own conversion may not read (round 73: MongoDB's)
            new Parse("parseFloat", "+1", "1.0"), new Parse("parseFloat", ".5", "0.5"),
            new Parse("parseFloat", "5.", "5.0"),
            new Parse("parseInteger", "abc", "raise"), new Parse("parseInteger", "", "raise"),
            new Parse("parseInteger", "+-1", "raise"), new Parse("parseInteger", "0x10", "raise"),
            new Parse("parseInteger", "1f", "raise"), new Parse("parseInteger", "1e5", "raise"),
            new Parse("parseInteger", "NaN", "raise"), new Parse("parseInteger", "Infinity", "raise"),
            new Parse("parseInteger", "12", "12"),
            // legend-pure raises on whitespace in both modes (Long.valueOf, Long.parseLong; the orchestrator's
            // decision, 2026-11-16: exact)
            new Parse("parseInteger", " 1 ", "raise"));

    private static void carriers(com.legend.testbackend.TestBackend backend, List<String> wrong) {
        Map<String, Double> cases = new java.util.LinkedHashMap<>();
        cases.put("0.1 + 0.2", 0.1 + 0.2);
        cases.put("1.0E-150 * 1.0E-150", 1.0E-150 * 1.0E-150);
        cases.put("123456789.123456789 * 3.0", 123456789.123456789 * 3.0);
        for (Map.Entry<String, Double> c : cases.entrySet()) {
            String label = "the carrier of " + c.getKey();
            // the double itself, a Float column's cell: a literal's arithmetic is the decimal of its digits on a
            // database that types the literal (the numeric charter), which is not the carrier under test
            Object got = run(backend, label, "|#TDS\n  x:Float\n  " + c.getValue()
                    + "\n#->extend(~t: r|[$r.x, -1.0]->max())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
            System.out.println("[product raise probe] " + backend.name() + " " + label + ", answered: " + describe(got));
            if (got instanceof com.legend.sql.DialectCapability dc
                    && String.valueOf(dc.getMessage()).contains("list encoding")) {
                continue;
            }
            String cell = got instanceof String rows && rows.contains("\"t\":")
                    ? rows.substring(rows.indexOf("\"t\":") + 4).replaceAll("[}\\]\\s\"]", "") : null;
            if (cell == null || !cell.matches("-?[0-9.eE+-]+") || Double.parseDouble(cell) != c.getValue()) {
                wrong.add(label + ": expected " + c.getValue() + " exactly, answered " + describe(got));
            }
        }
    }

    private static void canonicalTexts(com.legend.testbackend.TestBackend backend, List<String> wrong) {
        var dialect = com.legend.database.Databases.dialect(backend.type());
        try (java.sql.Connection c = backend.open(); java.sql.Statement st = c.createStatement()) {
            for (String setup : dialect.sessionSetup()) {
                st.execute(setup);
            }
            for (double d : List.of(9007199254740992.0, 1152921504606846976.0, 1e19, 9223372036854775808.0,
                    1.2345678901234567e19, 1e22, 1e23, 1e37, -1152921504606846976.0, 123456789012345678.0, 1e16, 1.5,
                    0.1, 1e-5, 2.5e-7)) {
                // the double as the product writes a Float literal (S20), so its reading is held too (the same
                // statement //core:numbers_probe_test sends on DuckDB, H2, SQLite and Postgres)
                com.legend.sql.SqlExpr x = com.legend.sql.SqlExpr.FloatLit.of(d, com.legend.sql.SqlExpr.FloatLit.Kind.DOUBLE);
                String sql = dialect.render(new com.legend.sql.SqlSelect(List.of(new com.legend.sql.SqlSelect.Projection(
                        com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.FLOAT_SHORTEST_TEXT, x), "t",
                        new com.legend.sql.OutputCol("t", com.legend.sql.SqlType.Scalar.VARCHAR, true))), false,
                        new com.legend.sql.SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null,
                        List.of()));
                String got;
                try (java.sql.ResultSet rs = st.executeQuery(sql)) {
                    rs.next();
                    got = rs.getString(1);
                } catch (java.sql.SQLException failed) {
                    got = "SQLException: " + failed.getMessage();
                }
                // 1e23 is a rounding boundary Postgres's own digits leave out (//core:numbers_probe_test's
                // BOUNDARY_TEXTS): the host's text is expected there too
                String want = com.legend.result.CellText.carriedFloat(d);
                System.out.println("[product raise probe] " + backend.name() + " the canonical text of " + d
                        + ", answered: " + got);
                if (!want.equals(got)) {
                    wrong.add("the canonical text of " + d + ": " + got + " (want " + want + ")");
                }
            }
        } catch (java.sql.SQLException failed) {
            wrong.add("the canonical texts: " + describe(failed));
        }
    }

    private static void parses(com.legend.testbackend.TestBackend backend, List<String> wrong) {
        for (Parse p : PARSES) {
            String label = p.fn() + "('" + p.text() + "')";
            Object got = run(backend, label, "|#TDS\n  k:Integer\n  1\n#->extend(~t: r|'" + p.text() + "'->" + p.fn()
                    + "())->select(~t)->from(" + StorelessRuntime.RUNTIME + ")");
            System.out.println("[product raise probe] " + backend.name() + " " + label + ", answered: " + describe(got));
            String cell = got instanceof String rows && rows.contains("\"t\":")
                    ? rows.substring(rows.indexOf("\"t\":") + 4).replaceAll("[}\\]\\s\"]", "") : null;
            switch (p.expect()) {
                case "either" -> { }
                // lite's own kind by name on every database, never the database's own error (the user's error design,
                // 2026-11-16 21:20: lite's guard runs on every database for every Pure-defined runtime error)
                case "raise" -> expectKind(wrong, label, got, p.fn().equals("parseFloat")
                        ? com.legend.error.ErrorKind.TEXT_NOT_FLOAT : com.legend.error.ErrorKind.TEXT_NOT_INTEGER, null);
                case "NaN", "Infinity" -> {
                    if (got instanceof Throwable) {
                        expectKind(wrong, label, got, com.legend.error.ErrorKind.VALUE_NOT_HELD, null);
                    } else if (cell == null || !cell.contains(p.expect())) {
                        wrong.add(label + ": expected " + p.expect() + " or VALUE_NOT_HELD, answered " + describe(got));
                    }
                }
                default -> {
                    // a NULL cell is an answer too: a wrong one (ClickHouse's CAST reads bad text as NULL, measured)
                    if (cell == null || !cell.matches("-?[0-9.eE+]+")
                            || Double.parseDouble(cell) != Double.parseDouble(p.expect())) {
                        wrong.add(label + ": expected " + p.expect() + ", answered " + describe(got));
                    }
                }
            }
        }
    }

    /** The product's text of {@code x} (register S311): its shortest round-trip digits, rounded half up to six places,
     *  trailing zeros trimmed, a zero unsigned. */
    static String floatText(double x) {
        String t = new java.math.BigDecimal(Double.toString(x)).setScale(6, java.math.RoundingMode.HALF_UP)
                .stripTrailingZeros().toPlainString();
        return t.equals("-0") || new java.math.BigDecimal(t).signum() == 0 ? "0" : t;
    }

    /** A Decimal row: its one value is {@code want} exactly (by {@code compareTo}), or the database's named refusal of a
     *  Decimal's arithmetic where it holds no exact decimal (D2a) -- never another value. */
    private static void expectExact(List<String> wrong, String label, Object answered, java.math.BigDecimal want) {
        if (answered instanceof Throwable t) {
            for (Throwable c = t; c != null; c = c.getCause()) {
                if (String.valueOf(c.getMessage()).contains("holds no exact decimal")) {
                    return;
                }
            }
        }
        java.util.regex.Matcher m = answered instanceof String s
                ? java.util.regex.Pattern.compile(":\\s*(-?[0-9][0-9.eE+-]*)").matcher(s) : null;
        if (m == null || !m.find() || new java.math.BigDecimal(m.group(1)).compareTo(want) != 0) {
            wrong.add(label + ": expected exactly " + want.toPlainString() + ", got " + describe(answered));
        }
    }

    /** A Decimal row's token: {@code want}, or the database's named refusal of a Decimal's arithmetic (D2a). */
    private static void expectDecimalText(List<String> wrong, String label, Object answered, String want) {
        for (Throwable c = answered instanceof Throwable t ? t : null; c != null; c = c.getCause()) {
            if (String.valueOf(c.getMessage()).contains("holds no exact decimal")) {
                return;
            }
        }
        expectText(wrong, label, answered, want);
    }

    /** {@code answered}'s one value is the token {@code want} ({@code true}, {@code false}). */
    private static void expectText(List<String> wrong, String label, Object answered, String want) {
        if (!(answered instanceof String s && s.matches("(?s).*:\\s*" + want + "\\s*}.*"))) {
            wrong.add(label + ": expected " + want + ", got " + describe(answered));
        }
    }

    /** {@code answered}'s one value (a JSON row's number) is the double {@code want}, exactly. */
    private static void expectDouble(List<String> wrong, String label, Object answered, double want) {
        java.util.regex.Matcher m = answered instanceof String s
                ? java.util.regex.Pattern.compile("-?[0-9][0-9.eE+-]*").matcher(s) : null;
        if (m == null || !m.find() || Double.parseDouble(m.group()) != want) {
            wrong.add(label + ": expected the double " + want + ", got " + describe(answered));
        }
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
