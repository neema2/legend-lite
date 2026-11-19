// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A FLOAT IS THE DOUBLE AND A DECIMAL IS EXACT, ON EVERY DATABASE (Q1 of the PR #118 follow-ups,
 * docs/records/pr118-audit-2026-11-18/FOLLOW_UPS.md, fixed 2026-11-19): the product probe's four ways
 * ({@code //core:product_raise_probe}, run by hand per database) found two defects present at main --
 * <ul>
 *   <li>a Float literal read as the exact decimal of its digits on BigQuery, Snowflake and ClickHouse (register S20,
 *       "IEEE everywhere", not yet built there: the edge program's F39), so {@code 0.1 + 0.2} answered 0.3;</li>
 *   <li>a Decimal's scale lost or overflowing on every database that casts its root column to its type
 *       ({@code dialect.RootNumericTypes}: Trino, Databricks, Snowflake, ClickHouse and Postgres): a #TDS
 *       {@code a:Decimal} column was a Decimal(38, 0) (F51), so {@code $a + $b} over 0.1 and 0.2 answered 0; and a
 *       product's type carried DuckDB's 18-digit cap, so {@code 123456789.123456789D * 3.0D} was cast to a
 *       DECIMAL(18, 10) no database holds the value in.</li>
 * </ul>
 * The live half runs here on DuckDB, H2, SQLite and the embedded Postgres (a CI database, so B's Postgres half is held
 * per push); the planned half reads every product dialect's statement, the cloud dialects' among them (no account is
 * reached: a placeholder connection, planning only), for the spellings the lanes measured or the databases document.
 * A third case holds lite's canonical text of a Float on the four databases (Q4: a whole value past 2^63 on SQLite).
 */
class NumbersProbeTest {

    /** A calculation over {@code a} and {@code b}: a literal substitutes them, a column reads them from one row. */
    private record Calc(String name, String expr, String a, String b) {
    }

    private static final List<Calc> CALCS = List.of(
            new Calc("sum", "$a + $b", "0.1", "0.2"),
            new Calc("equality", "($a + $b) == 0.3", "0.1", "0.2"),
            new Calc("remainder", "rem($a, $b)", "0.3", "0.1"),
            new Calc("product", "$a * $b", "123456789.123456789", "3.0"));

    private static String literal(Calc c, String suffix) {
        return "|" + c.expr().replace("$a", c.a() + suffix).replace("$b", c.b() + suffix)
                .replace("== 0.3", "== 0.3" + suffix);
    }

    private static String column(Calc c, String type, String suffix) {
        return "|#TDS\n  a:" + type + ", b:" + type + "\n  " + c.a() + ", " + c.b() + "\n#->extend(~v: r|"
                + c.expr().replace("$a", "$r.a->toOne()").replace("$b", "$r.b->toOne()")
                        .replace("== 0.3", "== 0.3" + suffix)
                + ")->select(~v)->from(" + StorelessRuntime.RUNTIME + ")";
    }

    /** The double's answer, Java's arithmetic on the doubles (legend-pure's compiled runtime). */
    private static Object floatAnswer(Calc c) {
        double a = Double.parseDouble(c.a());
        double b = Double.parseDouble(c.b());
        return switch (c.name()) {
            case "sum" -> a + b;
            case "equality" -> a + b == 0.3;
            case "remainder" -> a % b;
            case "product" -> a * b;
            default -> throw new IllegalStateException(c.name());
        };
    }

    /** The exact answer, BigDecimal arithmetic on the written digits. */
    private static Object decimalAnswer(Calc c) {
        BigDecimal a = new BigDecimal(c.a());
        BigDecimal b = new BigDecimal(c.b());
        return switch (c.name()) {
            case "sum" -> a.add(b);
            case "equality" -> a.add(b).compareTo(new BigDecimal("0.3")) == 0;
            case "remainder" -> a.remainder(b);
            case "product" -> a.multiply(b);
            default -> throw new IllegalStateException(c.name());
        };
    }

    @Test
    void floatIsTheDouble_decimalIsExact_onFourDatabases() throws Exception {
        List<String> wrong = new ArrayList<>();
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite,
                DatabaseType.Postgres)) {
            TestBackend backend = TestBackends.of(type);
            String model = backend.withStorelessRuntime("");
            try (Connection conn = backend.open()) {
                for (Calc c : CALCS) {
                    check(wrong, type + " " + c.name() + " Float literal", floatAnswer(c),
                            run(model, conn, literal(c, "")));
                    check(wrong, type + " " + c.name() + " Float column", floatAnswer(c),
                            run(model, conn, column(c, "Float", "")));
                    for (String[] decimal : new String[][] {{"literal", literal(c, "D")},
                            {"column", column(c, "Decimal", "D")}}) {
                        Object got = run(model, conn, decimal[1]);
                        // a database that holds no exact decimal refuses a Decimal's arithmetic by name (D2a: SQLite)
                        if (!(type == DatabaseType.SQLite && refusedAsInexact(got))) {
                            check(wrong, type + " " + c.name() + " Decimal " + decimal[0], decimalAnswer(c), got);
                        }
                    }
                }
            }
        }
        assertEquals(List.of(), wrong);
    }

    /** The whole doubles lite's canonical text of a Float ({@code FLOAT_SHORTEST_TEXT}, a message's text of a Float)
     *  lays out from the database's exponent text, and a few inside the other windows. */
    static final List<Double> WHOLE_AND_OTHER_FLOATS = List.of(9007199254740992.0, 1152921504606846976.0, 1e19,
            9223372036854775808.0, 1.2345678901234567e19, 1e22, 1e23, 1e37, -1152921504606846976.0, 123456789012345678.0,
            1e16, 1.5, 0.1, 1e-5, 2.5e-7);

    /** The rounding boundaries a database's own shortest digits leave out: Postgres's float8 output (Ryu, the bounds
     *  excluded) writes 1e23's double (99999999999999991611392, which 1e23 reaches only by round-half-even) as
     *  9.999999999999999e+22, where Java writes 1.0E23 (measured 2026-11-19); lite's canonical text there takes the
     *  shortest rounding that reads back (FloatShortestText.shortestOf, the orchestrator's ruling of 2026-11-19), so the
     *  host's text is expected, named here so the case stays in view. */
    static final Map<String, String> BOUNDARY_TEXTS = Map.of("Postgres 1.0E23", "100000000000000000000000.0");

    /** Lite's canonical text of each double is the host writer's ({@code CellText.carriedFloat}: its shortest digits,
     *  laid out): the database judge's message text and the host judge's alike (Q4 and the 2^60 case of the PR #118
     *  follow-ups, 2026-11-19). Until then a whole value with an exponent text was cast through a 128-bit integer:
     *  SQLite's is 64-bit, so 1e19 printed 9223372036854775807.0; DuckDB wrote the exact binary digits (2^60
     *  1152921504606846976.0); Postgres's numeric kept 15 digits (2^53 9007199254740990.0, no longer the value). */
    @Test
    void aFloatsCanonicalTextIsTheHostWritersText_onFourDatabases() throws Exception {
        List<String> wrong = new ArrayList<>();
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite,
                DatabaseType.Postgres)) {
            var dialect = com.legend.database.Databases.dialect(type);
            try (Connection conn = TestBackends.of(type).open(); java.sql.Statement st = conn.createStatement()) {
                for (String setup : dialect.sessionSetup()) {
                    st.execute(setup);
                }
                for (double d : WHOLE_AND_OTHER_FLOATS) {
                    String got;
                    try (java.sql.ResultSet rs = st.executeQuery(dialect.render(canonicalTextOf(d)))) {
                        rs.next();
                        got = rs.getString(1);
                    }
                    String want = BOUNDARY_TEXTS.getOrDefault(type + " " + Double.toString(d),
                            com.legend.result.CellText.carriedFloat(d));
                    if (!want.equals(got) || Double.parseDouble(got) != d) {
                        wrong.add(type + " " + Double.toString(d) + ": " + got + " (want " + want + ")");
                    }
                }
            }
        }
        assertEquals(List.of(), wrong);
    }

    /** {@code SELECT FLOAT_SHORTEST_TEXT(<d>)}, {@code d} written as the product writes a Float literal (S20, the
     *  double), so the database's reading of it is held too. */
    static com.legend.sql.SqlSelect canonicalTextOf(double d) {
        com.legend.sql.SqlExpr x = com.legend.sql.SqlExpr.FloatLit.of(d, com.legend.sql.SqlExpr.FloatLit.Kind.DOUBLE);
        return new com.legend.sql.SqlSelect(List.of(new com.legend.sql.SqlSelect.Projection(
                com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.FLOAT_SHORTEST_TEXT, x), "t",
                new com.legend.sql.OutputCol("t", com.legend.sql.SqlType.Scalar.VARCHAR, true))), false,
                new com.legend.sql.SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of());
    }

    /** Every product dialect's statement for the four ways' cases, planned (no database reached). */
    @Test
    void everyDialectPlansTheDoubleAndAnExactDecimal() {
        // each dialect's spelling of the Float literal 0.1, a double on its database: measured on the embedded lanes
        // (DuckDB, H2, SQLite, Postgres, MariaDB, MySQL, Trino, ClickHouse, 2026-11-19); BigQuery's by its lexical
        // reference ("numeric literals that contain either a decimal point or an exponent marker are presumed to be
        // type double"), Snowflake's by its SYSTEM$TYPEOF reference (a numeric constant is a NUMBER, 1E10 included, so
        // the double is cast from its text), Databricks' as Spark reads an exponent literal (a DOUBLE)
        Map<DatabaseType, String> doubleOf01 = Map.ofEntries(Map.entry(DatabaseType.DuckDB, "0.1E0"),
                Map.entry(DatabaseType.H2, "CAST(0.1 AS DOUBLE PRECISION)"),
                Map.entry(DatabaseType.SQLite, "0.1"),
                Map.entry(DatabaseType.Postgres, "0.1::float8"),
                Map.entry(DatabaseType.MariaDB, "0.1E0"), Map.entry(DatabaseType.MySQL, "0.1E0"),
                Map.entry(DatabaseType.Trino, "0.1E0"), Map.entry(DatabaseType.ClickHouse, "0.1E0"),
                Map.entry(DatabaseType.Databricks, "0.1E0"), Map.entry(DatabaseType.BigQuery, "0.1E0"),
                Map.entry(DatabaseType.Snowflake, "CAST('0.1' AS DOUBLE)"));
        List<String> wrong = new ArrayList<>();
        for (Map.Entry<DatabaseType, String> e : doubleOf01.entrySet()) {
            DatabaseType type = e.getKey();
            String model = model(type);
            String sum = sql(model, "|0.1 + 0.2");
            if (!sum.contains(e.getValue()) || sum.contains("BIGNUMERIC") || sum.contains("Decimal(")) {
                wrong.add(type + ": 0.1 + 0.2 is not over the doubles " + e.getValue() + ": " + sum);
            }
            // BigQuery coerces a float literal cast to BIGNUMERIC as its own decimal digits (rem(0.3, 0.1) answered 0.0,
            // its lane at 4d2f383c): the remainder's literal is a FLOAT64 cast from its text, whose value is the double
            if (type == DatabaseType.BigQuery && !sql(model, "|rem(0.3, 0.1)").contains("CAST(CAST('0.3' AS FLOAT64)"
                    + " AS BIGNUMERIC)")) {
                wrong.add(type + ": rem(0.3, 0.1) casts the literal itself to BIGNUMERIC: " + sql(model, "|rem(0.3, 0.1)"));
            }
            if (type == DatabaseType.SQLite) {
                continue;   // a Decimal's arithmetic is refused by name there (D2a): the live half holds it
            }
            // the Decimal column's sum and product state no scale nobody wrote (F51: DECIMAL(38, 0)) and none the
            // value cannot hold (the unbounded carrier's 18 + 18); the literals' product its exact type, (20, 10)
            Calc sumCalc = CALCS.get(0);
            Calc product = CALCS.get(3);
            String columnSum = sql(model, column(sumCalc, "Decimal", "D"));
            String columnProduct = sql(model, column(product, "Decimal", "D"));
            String literalProduct = sql(model, literal(product, "D"));
            if (columnSum.contains("(38, 0)") || columnSum.contains("(38,0)")) {
                wrong.add(type + ": a Decimal column's sum is cast to scale 0: " + columnSum);
            }
            if (columnProduct.contains("38, 36")) {
                wrong.add(type + ": a Decimal column's product is cast to DECIMAL(38, 36): " + columnProduct);
            }
            if (literalProduct.contains("18, 10")) {
                wrong.add(type + ": the literals' product is cast to DECIMAL(18, 10): " + literalProduct);
            }
        }
        assertEquals(List.of(), wrong);
    }

    /** {@code type}'s storeless runtime, planning only: a server's or a cloud's at placeholder coordinates (no account
     *  and no credential is read: the plan is never run). */
    private static String model(DatabaseType type) {
        return switch (type) {
            case Snowflake -> StorelessRuntime.declaring("", type, "specification: Snowflake { name: 'd'; account: 'a';"
                    + " warehouse: 'w'; region: 'r'; };\n  auth: SnowflakePublic { publicUserName: 'snowflake.user';"
                    + " privateKeyVaultReference: 'snowflake.privateKey'; passPhraseVaultReference:"
                    + " 'snowflake.passPhrase'; }");
            case BigQuery -> StorelessRuntime.declaring("", type, "specification: BigQuery { projectId: 'p';"
                    + " defaultDataset: 'd'; };\n  auth: GCPApplicationDefaultCredentials");
            case Databricks -> StorelessRuntime.declaring("", type, "specification: Databricks { hostname: 'h'; port:"
                    + " '443'; protocol: 'https'; httpPath: 'p'; };\n  auth: ApiToken { apiToken: 'databricks.token'; }");
            case ClickHouse, Trino, MariaDB, MySQL, Postgres -> StorelessRuntime.onServer("", type, "localhost", 1, "d");
            default -> StorelessRuntime.with("", type);
        };
    }

    private static String sql(String model, String query) {
        return Compiler.query(Compiler.compileModel(model), query).executionPlan(StorelessRuntime.RUNTIME).answerSql()
                .statement();
    }

    private static Object run(String model, Connection conn, String query) {
        try {
            return Lean.run(model, query, StorelessRuntime.RUNTIME, conn).rows().get(0).get(0);
        } catch (RuntimeException refused) {
            return refused;
        }
    }

    private static boolean refusedAsInexact(Object got) {
        for (Throwable t = got instanceof Throwable th ? th : null; t != null; t = t.getCause()) {
            if (String.valueOf(t.getMessage()).contains("holds no exact decimal")) {
                return true;
            }
        }
        return false;
    }

    /** {@code got} is {@code want} by value: a double exactly, a decimal by {@code compareTo}, a Boolean as itself. */
    private static void check(List<String> wrong, String label, Object want, Object got) {
        boolean same;
        if (got instanceof Throwable || got == null) {
            same = false;
        } else if (want instanceof Double d) {
            same = got instanceof Double g && g.doubleValue() == d;
        } else if (want instanceof BigDecimal d) {
            same = got instanceof BigDecimal g && g.compareTo(d) == 0;
        } else {
            same = want.equals(got);
        }
        if (!same) {
            wrong.add(label + ": want " + want + ", got " + (got instanceof Throwable t
                    ? t.getClass().getSimpleName() + ": " + t.getMessage() : got));
        }
    }
}
