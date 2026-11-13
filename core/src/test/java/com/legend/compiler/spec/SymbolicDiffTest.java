package com.legend.compiler.spec;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase G&frac12; &mdash; symbolic differentiation ({@code meta::legend::lite::diff})
 * lowered to pure SQL scalar expressions and executed on the database engine.
 */
class SymbolicDiffTest {

    private static final String MODEL = """
            Class m::OptionTrade {
                id: String[1];
                spot: Float[1];
                strike: Float[1];
                vol: Float[1];
                rate: Float[1];
                expiry: Float[1];
            }
            ###Relational
            Database s::DB (
                Table OPT (
                    ID VARCHAR(50),
                    SPOT DOUBLE,
                    STRIKE DOUBLE,
                    VOL DOUBLE,
                    RATE DOUBLE,
                    EXPIRY DOUBLE
                )
            )
            ###Mapping
            Mapping m::M (
                *m::OptionTrade: Relational {
                    ~mainTable [s::DB] OPT
                    id: OPT.ID,
                    spot: OPT.SPOT,
                    strike: OPT.STRIKE,
                    vol: OPT.VOL,
                    rate: OPT.RATE,
                    expiry: OPT.EXPIRY
                }
            )
            ###Connection
            RelationalDatabaseConnection s::DBDuckDB { store: s::DB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            RelationalDatabaseConnection s::DBPostgres { store: s::DB; type: Postgres; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: [m::M]; connections: [ s::DB: [ c0: s::DBDuckDB ] ]; }
            Runtime m::RTPg { mappings: [m::M]; connections: [ s::DB: [ c0: s::DBPostgres ] ]; }

            ###Pure
            function m::poly(x: Float[1]): Float[1] {
                3.0 * $x * $x * $x - 2.0 * $x * $x + 5.0 * $x - 7.0
            }

            function m::normalPdf(x: Float[1]): Float[1] {
                exp(-0.5 * $x * $x) / sqrt(2.0 * pi())
            }

            function m::cnd(x: Float[1]): Float[1] {
                0.5 * (1.0 + erf($x / sqrt(2.0)))
            }

            function m::bsCall(s: Float[1], k: Float[1], v: Float[1], r: Float[1], t: Float[1]): Float[1] {
                let volSqrtT = $v * sqrt($t);
                let d1 = (log($s / $k) + ($r + 0.5 * $v * $v) * $t) / $volSqrtT;
                let d2 = $d1 - $volSqrtT;
                $s * m::cnd($d1) - $k * exp(-$r * $t) * m::cnd($d2);
            }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE OPT (ID VARCHAR, SPOT DOUBLE, STRIKE DOUBLE, VOL DOUBLE, RATE DOUBLE, EXPIRY DOUBLE)");
            st.execute("INSERT INTO OPT VALUES ('T1', 100.0, 100.0, 0.20, 0.05, 1.0), ('T2', 110.0, 100.0, 0.25, 0.05, 0.5)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private double evalDouble(String expr) throws SQLException {
        Object v = Lean.run(MODEL, "|" + expr, "m::RT", conn).one();
        return ((Number) v).doubleValue();
    }

    @Test
    @DisplayName("polynomial and rational derivatives")
    void polynomialAndRational() throws SQLException {
        // d/dx (x^2) at x = 3 => 6
        assertEquals(6.0, evalDouble("diff({x | $x * $x}, 3.0)"), 1e-12);
        // d/dx (42.0) at x = 3 => 0
        assertEquals(0.0, evalDouble("diff({x | 42.0}, 3.0)"), 1e-12);
        // d/dx (3x^3 - 2x^2 + 5x - 7) at x = 2 => 9(4) - 4(2) + 5 = 33
        assertEquals(33.0, evalDouble("diff({x | m::poly($x)}, 2.0)"), 1e-12);
        // d/dx ((x + 1) / (x - 1)) = -2 / (x - 1)^2; at x = 3 => -0.5
        assertEquals(-0.5, evalDouble("diff({x | ($x + 1.0) / ($x - 1.0)}, 3.0)"), 1e-12);
    }

    @Test
    @DisplayName("transcendental functions: exp, log, sqrt, cbrt, pow, trig, hyperbolic")
    void transcendentalFunctions() throws SQLException {
        // d/dx exp(2x) at x = 0.5 => 2 * e
        assertEquals(2.0 * Math.E, evalDouble("diff({x | exp(2.0 * $x)}, 0.5)"), 1e-11);
        // d/dx log(x^2) at x = 4 => 2/x = 0.5
        assertEquals(0.5, evalDouble("diff({x | log($x * $x)}, 4.0)"), 1e-12);
        // d/dx sqrt(x) at x = 4 => 1 / (2 * 2) = 0.25
        assertEquals(0.25, evalDouble("diff({x | sqrt($x)}, 4.0)"), 1e-12);
        // d/dx cbrt(x) at x = 8 => 1 / (3 * 4) = 1/12
        assertEquals(1.0 / 12.0, evalDouble("diff({x | cbrt($x)}, 8.0)"), 1e-12);
        // d/dx pow(x, 3) at x = 2 => 12
        assertEquals(12.0, evalDouble("diff({x | $x->pow(3.0)}, 2.0)"), 1e-12);
        // d/dx pow(2, x) at x = 3 => 8 * ln(2)
        assertEquals(8.0 * Math.log(2.0), evalDouble("diff({x | (2.0)->pow($x)}, 3.0)"), 1e-11);
        // d/dx pow(x, x) at x = 2 => 4 * (ln(2) + 1)
        assertEquals(4.0 * (Math.log(2.0) + 1.0), evalDouble("diff({x | $x->pow($x)}, 2.0)"), 1e-11);
        // d/dx sin(x) at x = 0 => 1, d/dx cos(x) at x = pi/2 => -1
        assertEquals(1.0, evalDouble("diff({x | sin($x)}, 0.0)"), 1e-12);
        assertEquals(-1.0, evalDouble("diff({x | cos($x)}, pi() / 2.0)"), 1e-12);
        // d/dx tan(x) at x = 0 => 1
        assertEquals(1.0, evalDouble("diff({x | tan($x)}, 0.0)"), 1e-12);
        // d/dx atan(x) at x = 1 => 0.5
        assertEquals(0.5, evalDouble("diff({x | atan($x)}, 1.0)"), 1e-12);
        // d/dx sinh(x) at x = 0 => 1, cosh(0) => 0, tanh(0) => 1
        assertEquals(1.0, evalDouble("diff({x | sinh($x)}, 0.0)"), 1e-12);
        assertEquals(0.0, evalDouble("diff({x | cosh($x)}, 0.0)"), 1e-12);
        assertEquals(1.0, evalDouble("diff({x | tanh($x)}, 0.0)"), 1e-12);
        // log10 and decimal literal precision hygiene
        assertEquals(1.0 / (10.0 * Math.log(10.0)), evalDouble("diff({x | log10($x)}, 10.0)"), 1e-15);
    }

    @Test
    @DisplayName("erf IEEE-754 accuracy and exact analytic Gaussian derivative")
    void erfAndNormalCdf() throws SQLException {
        assertEquals(0.0, evalDouble("erf(0.0)"), 1e-15);
        assertEquals(0.27632639016823693, evalDouble("erf(0.25)"), 1e-15);
        assertEquals(0.8427007929497149, evalDouble("erf(1.0)"), 1e-15);
        assertEquals(0.999593047982555, evalDouble("erf(2.5)"), 1e-15);
        assertEquals(-0.9661051464753108, evalDouble("erf(-1.5)"), 1e-15);
        assertEquals(1.0, evalDouble("erf(10.0)"), 1e-15);

        // d/dx erf(x) = (2/sqrt(pi)) * exp(-x^2)
        double x = 0.7;
        double expectedErfDeriv = (2.0 / Math.sqrt(Math.PI)) * Math.exp(-x * x);
        assertEquals(expectedErfDeriv, evalDouble("diff({x | erf($x)}, 0.7)"), 1e-14);

        // d/dx cnd(x) = normalPdf(x)
        double expectedPdf = Math.exp(-0.5 * x * x) / Math.sqrt(2.0 * Math.PI);
        assertEquals(expectedPdf, evalDouble("diff({x | m::cnd($x)}, 0.7)"), 1e-14);
    }

    @Test
    @DisplayName("piecewise and non-smooth functions: abs, max, min, if")
    void piecewiseFunctions() throws SQLException {
        assertEquals(2.0, evalDouble("diff({x | ($x * 2.0)->abs()}, 3.0)"), 1e-12);
        assertEquals(-2.0, evalDouble("diff({x | ($x * 2.0)->abs()}, -3.0)"), 1e-12);
        assertEquals(2.0, evalDouble("diff({x | max($x * 2.0, $x + 10.0)}, 20.0)"), 1e-12);
        assertEquals(1.0, evalDouble("diff({x | max($x * 2.0, $x + 10.0)}, 2.0)"), 1e-12);
        assertEquals(1.0, evalDouble("diff({x | min($x * 2.0, $x + 10.0)}, 20.0)"), 1e-12);
        assertEquals(6.0, evalDouble("diff({x | if($x > 0.0, |$x * $x, |-$x)}, 3.0)"), 1e-12);
        assertEquals(-1.0, evalDouble("diff({x | if($x > 0.0, |$x * $x, |-$x)}, -3.0)"), 1e-12);
    }

    @Test
    @DisplayName("nested higher-order differentiation (2nd derivative / Gamma)")
    void higherOrderDiff() throws SQLException {
        // d^2/dx^2 (x^3) = 6x; at x = 2 => 12
        assertEquals(12.0, evalDouble("diff({x | diff({y | $y * $y * $y}, $x)}, 2.0)"), 1e-12);
        // Black-Scholes Gamma: d^2 V / dS^2 at S=100, K=100, v=0.2, r=0.05, T=1.0
        // Analytical Gamma = N'(d1) / (S * v * sqrt(T)), where d1 = 0.35
        double d1 = (Math.log(100.0 / 100.0) + (0.05 + 0.5 * 0.04) * 1.0) / 0.2;
        double expectedGamma = (Math.exp(-0.5 * d1 * d1) / Math.sqrt(2.0 * Math.PI)) / (100.0 * 0.2 * 1.0);
        double actualGamma = evalDouble(
                "diff({s | diff({s2 | m::bsCall($s2, 100.0, 0.20, 0.05, 1.0)}, $s)}, 100.0)");
        assertEquals(expectedGamma, actualGamma, 1e-12);
    }

    @Test
    @DisplayName("Black-Scholes Greeks via relational project over DuckDB table")
    void relationalBlackScholesGreeks() throws SQLException {
        String query = "m::OptionTrade.all()"
                + "->project(~["
                + "id: t|$t.id, "
                + "price: t|m::bsCall($t.spot, $t.strike, $t.vol, $t.rate, $t.expiry), "
                + "delta: t|diff({s | m::bsCall($s, $t.strike, $t.vol, $t.rate, $t.expiry)}, $t.spot), "
                + "vega:  t|diff({v | m::bsCall($t.spot, $t.strike, $v, $t.rate, $t.expiry)}, $t.vol), "
                + "rho:   t|diff({r | m::bsCall($t.spot, $t.strike, $t.vol, $r, $t.expiry)}, $t.rate)"
                + "])->sort(~id->ascending())";
        var ctx = com.legend.Compiler.compileModel(MODEL);
        String sql = com.legend.Compiler.query(ctx, query).plan("m::RT").sql();
        assertTrue(sql.toUpperCase(Locale.ROOT).contains("SELECT"));

        var r = Lean.run(MODEL, query, "m::RT", conn);
        List<List<Object>> rows = r.rows();
        assertEquals(2, rows.size());

        // Row 0: T1 (S=100, K=100, v=0.20, r=0.05, T=1.0) -> d1 = 0.35, d2 = 0.15
        List<Object> t1 = rows.get(0);
        assertEquals("T1", t1.get(0));
        assertEquals(10.450583572185565, ((Number) t1.get(1)).doubleValue(), 1e-12);
        assertEquals(0.6368306511756191, ((Number) t1.get(2)).doubleValue(), 1e-12);
        assertEquals(37.52403469169379, ((Number) t1.get(3)).doubleValue(), 1e-12);
        assertEquals(53.232481545376345, ((Number) t1.get(4)).doubleValue(), 1e-12);
    }

    @Test
    @DisplayName("automatic projection CSE deduplicates exp across pillars and respects conditional guards")
    void automaticProjectionCseAndGuardSafety() throws SQLException {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        // 1. Extend query: shared exp(-rate * expiry) across df, pv, and dr is lifted once, and no _cs* column leaks
        String extendQuery = "#>{s::DB.OPT}#->extend(~["
                + "df: r|exp(-$r.RATE->toOne() * $r.EXPIRY->toOne()), "
                + "pv: r|$r.SPOT->toOne() - $r.STRIKE->toOne() * exp(-$r.RATE->toOne() * $r.EXPIRY->toOne()), "
                + "dr: r|diff({rate | $r.SPOT->toOne() - $r.STRIKE->toOne() * exp(-$rate * $r.EXPIRY->toOne())}, $r.RATE->toOne())"
                + "])->sort(~ID->ascending())";
        String extendSql = com.legend.Compiler.query(ctx, extendQuery).plan("m::RT").sql();
        assertTrue(extendSql.toUpperCase(Locale.ROOT).contains("SELECT"));

        var extRes = Lean.run(MODEL, extendQuery, "m::RT", conn);
        for (Lean.Column col : extRes.columns()) {
            assertTrue(!col.name().startsWith("__c"), "Synthetic CSE column leaked into output: " + col.name());
        }
        assertEquals(2, extRes.rowCount());

        // 2. Guard safety: log(x) repeated inside a guarded branch MUST NOT hoist above CASE WHEN on rows where x == 0
        String guardedQuery = "#TDS\nid, kind, x\n1, LOG, 2.0\n2, LIN, 0.0\n#"
                + "->project(~["
                + "id: r|$r.id, "
                + "val: r|if($r.kind == 'LOG', |log($r.x) + log($r.x), |$r.x + 1.0), "
                + "dval: r|if($r.kind == 'LOG', |diff({z | log($z) * log($z)}, $r.x), |1.0)"
                + "])->sort(~id->ascending())";
        var guardRes = Lean.run(MODEL, guardedQuery, "m::RT", conn);
        assertEquals(2, guardRes.rowCount());
        assertEquals(2.0 * Math.log(2.0), ((Number) guardRes.rows().get(0).get(1)).doubleValue(), 1e-12);
        assertEquals(1.0, ((Number) guardRes.rows().get(1).get(1)).doubleValue(), 1e-12);

        // 3. Guarded + grouped erf(...): DuckDB binds once via list_transform;
        //    non-spellsErf dialects (e.g. Postgres) expand ErfMath inline.
        String groupedGuardedErf = "#TDS\ngrp, kind, spot, strike, vol, expiry\n"
                + "G1, OPT, 105.0, 100.0, 0.20, 2.0\n"
                + "G1, LIN, 0.0, 0.0, 0.0, 0.0\n#"
                + "->groupBy(~[grp], ~[total: r|if($r.kind == 'OPT', "
                + "|erf(log($r.spot / $r.strike) / ($r.vol * sqrt($r.expiry))), "
                + "|$r.spot + 2.5) : y|$y->plus()])";
        String duckErfSql = com.legend.Compiler.query(ctx, groupedGuardedErf).plan("m::RT").sql();
        assertTrue(duckErfSql.contains("list_transform"), "Expected DuckDB list_transform erf binding: " + duckErfSql);
        assertTrue(duckErfSql.length() < 12_000,
                "Expected compact DuckDB ERF SQL (< 12 KB), got " + duckErfSql.length() + " chars");
        String pgErfSql = com.legend.Compiler.query(ctx, groupedGuardedErf).plan("m::RTPg").sql();
        assertTrue(pgErfSql.toUpperCase(Locale.ROOT).contains("SELECT"));
        var groupedRes = Lean.run(MODEL, groupedGuardedErf, "m::RT", conn);
        assertEquals(1, groupedRes.rowCount());
        double expectedArg = Math.log(105.0 / 100.0) / (0.20 * Math.sqrt(2.0));
        // erf(expectedArg) + 2.5
        double expectedErf = evalDouble("erf(" + expectedArg + ")");
        assertEquals(expectedErf + 2.5, ((Number) groupedRes.rows().get(0).get(1)).doubleValue(), 1e-12);
    }

    @Test
    @DisplayName("lambda bound via let reduces and differentiates cleanly")
    void letBoundLambdaDifferentiates() throws SQLException {
        assertEquals(6.0, evalDouble("let f = {x: Float[1] | $x * $x}; diff($f, 3.0);"), 1e-12);
    }

    @Test
    @DisplayName("non-differentiable expression inside diff is rejected with a clear diagnostic")
    void nonDifferentiableExprRejected() {
        var ex = assertThrows(Exception.class,
                () -> Lean.run(MODEL,
                        "|diff({x | $x->toString()->length()}, 3.0)",
                        "m::RT", conn));
        assertTrue(String.valueOf(ex.getMessage()).contains("diff: cannot differentiate"), ex.getMessage());
    }
}
