// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.TestBackend;
import com.legend.testing.Lean;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end execution test for {@code projects/pricing} on the active test lane (DuckDB, H2, SQLite):
 * <ul>
 *   <li>Reads {@code //projects:pricing_files} strictly from Bazel runfiles (no filesystem fallback).</li>
 *   <li>Verifies distinct {@code LINEAR_ZERO}, {@code LOG_LINEAR_DF}, and {@code LINEAR_DF} curves, exact knot
 *       discount factors {@code exp(-r * t)}, dual-curve + spread pricing, and seasoned past-cashflow exclusion.</li>
 *   <li>Verifies runtime refusal on an unknown {@code INTERP_METHOD} code and on a scenario missing a curve.</li>
 * </ul>
 */
public class PricingIntegrationTest extends AbstractDatabaseTest {

    private static String loadProjectPure(TestBackend backend) throws Exception {
        List<Path> files = Runfile.envList("PRICING_PROJECT_FILES").stream()
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .toList();
        StringBuilder sb = new StringBuilder();
        for (Path p : files) {
            sb.append(Files.readString(p)).append('\n');
        }
        sb.append("""
                ###Connection
                RelationalDatabaseConnection pricing::TestConn
                {
                   %s
                }
                ###Runtime
                Runtime pricing::TestRuntime
                {
                   mappings: [ pricing::Mapping ];
                   connections: [ pricing::Store: [ environment: pricing::TestConn ] ];
                }
                """.formatted(backend.connectionBody()));
        return sb.toString();
    }

    private static void createTables(Statement st) throws Exception {
        st.execute("""
                CREATE TABLE PR_CURVE_KNOT (
                    SCENARIO_ID VARCHAR(40),
                    CURVE_ID VARCHAR(40),
                    KNOT_IDX SMALLINT,
                    KNOT_T DOUBLE PRECISION,
                    ZERO_RATE DOUBLE PRECISION,
                    INTERP_METHOD VARCHAR(20)
                )
                """);
        st.execute("""
                CREATE TABLE PR_CASHFLOW (
                    TRADE_ID VARCHAR(40),
                    LEG_ID VARCHAR(20),
                    CF_IDX SMALLINT,
                    INSTRUMENT_TYPE VARCHAR(20),
                    CCY CHAR(3),
                    CSA VARCHAR(40),
                    DISCOUNT_CURVE_ID VARCHAR(40),
                    PROJ_CURVE_ID VARCHAR(40),
                    ACCRUAL_START_T DOUBLE PRECISION,
                    ACCRUAL_END_T DOUBLE PRECISION,
                    PAY_T DOUBLE PRECISION,
                    DCF DOUBLE PRECISION,
                    NOTIONAL DOUBLE PRECISION,
                    FIXED_RATE DOUBLE PRECISION,
                    SPREAD DOUBLE PRECISION,
                    IS_FLOAT BOOLEAN,
                    PAY_RECEIVE DOUBLE PRECISION
                )
                """);
    }

    @Test
    void testSwapFraDualCurveAndSeasonedAcrossInterpolations() throws Exception {
        TestBackend backend = backend();
        String fullModel = loadProjectPure(backend);

        // Pin rendered SQL byte size and table-scan counts of pricing::cashflowPvs() across DuckDB, H2, and SQLite
        var ctx = com.legend.Compiler.compileModel(fullModel);
        String cfSql = com.legend.Compiler.query(ctx, "|pricing::cashflowPvs()")
                .executionPlan("pricing::TestRuntime").answerSql().statement();
        assertTrue(cfSql.length() < 25_000, "Expected compact cashflowPvs SQL (<25 KB), got " + cfSql.length() + " bytes");
        assertEquals(4, countOccurrences(cfSql, "PR_CASHFLOW"), "Expected 4 PR_CASHFLOW scans (vs 10 with concatenated targets)");
        assertEquals(7, countOccurrences(cfSql, "PR_CURVE_KNOT"), "Expected 7 PR_CURVE_KNOT scans (vs 13 with concatenated targets)");

        try (Connection conn = backend.open();
             Statement st = conn.createStatement()) {
            createTables(st);

            // 3 scenarios for USD_OIS + USD_3M (dual-curve), each using a distinct interpolation method
            for (String[] scen : new String[][]{
                    {"BASE_LIN", "LINEAR_ZERO"},
                    {"BASE_LOGDF", "LOG_LINEAR_DF"},
                    {"BASE_LINDF", "LINEAR_DF"}
            }) {
                String sid = scen[0];
                String method = scen[1];
                st.execute(String.format(
                        "INSERT INTO PR_CURVE_KNOT VALUES " +
                                "('%s', 'USD_OIS', 0, 0.5, 0.040, '%s')," +
                                "('%s', 'USD_OIS', 1, 1.0, 0.045, '%s')," +
                                "('%s', 'USD_OIS', 2, 2.0, 0.050, '%s')," +
                                "('%s', 'USD_3M',  0, 0.5, 0.042, '%s')," +
                                "('%s', 'USD_3M',  1, 1.0, 0.047, '%s')," +
                                "('%s', 'USD_3M',  2, 2.0, 0.052, '%s')",
                        sid, method, sid, method, sid, method,
                        sid, method, sid, method, sid, method));
            }

            // 1) Exact knot discount factor check: P(0, t_k) == exp(-r_k * t_k)
            String knotDfQuery = """
                    pricing::PrCurveKnot.all()
                      ->filter(k | ($k.scenarioId == 'BASE_LOGDF') && ($k.curveId == 'USD_OIS'))
                      ->project(~[scenarioId: k | $k.scenarioId, curveId: k | $k.curveId, t: k | $k.knotT])
                      ->pricing::curveDiscountFactors()
                      ->sort(~t->ascending())
                    """;
            var dfRows = Lean.run(fullModel, knotDfQuery, "pricing::TestRuntime", conn).rows();
            assertEquals(3, dfRows.size());
            double[][] oisKnots = {{0.5, 0.040}, {1.0, 0.045}, {2.0, 0.050}};
            double[][] projKnots = {{0.5, 0.042}, {1.0, 0.047}, {2.0, 0.052}};
            for (int i = 0; i < oisKnots.length; i++) {
                double t = ((Number) dfRows.get(i).get(2)).doubleValue();
                double df = ((Number) dfRows.get(i).get(3)).doubleValue();
                assertEquals(Math.exp(-oisKnots[i][1] * t), df, 1e-14, "Exact knot DF at t=" + t);
            }

            // 2) Insert:
            //    - IRS_DUAL: dual-curve payer IRS (USD_OIS discount, USD_3M projection + 25bp spread), plus a past paid coupon (pay_t = -0.5)
            //    - FRA_1: 0.75x1.25 single-curve FRA
            st.execute("""
                    INSERT INTO PR_CASHFLOW VALUES
                    ('IRS_DUAL', 'FIXED', 0, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_3M', -1.25, -0.50, -0.50, 0.75, 1000000.0, 0.045, 0.0, false, -1.0),
                    ('IRS_DUAL', 'FLOAT', 0, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_3M', -1.25, -0.50, -0.50, 0.75, 1000000.0, 0.0, 0.0025, true, 1.0),
                    ('IRS_DUAL', 'FIXED', 1, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_3M', -0.25, 0.75, 0.75, 1.00, 1000000.0, 0.045, 0.0, false, -1.0),
                    ('IRS_DUAL', 'FIXED', 2, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_3M', 0.75, 1.50, 1.50, 0.75, 1000000.0, 0.045, 0.0, false, -1.0),
                    ('IRS_DUAL', 'FLOAT', 1, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_3M', -0.25, 0.75, 0.75, 1.00, 1000000.0, 0.0, 0.0025, true, 1.0),
                    ('IRS_DUAL', 'FLOAT', 2, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_3M', 0.75, 1.50, 1.50, 0.75, 1000000.0, 0.0, 0.0025, true, 1.0),
                    ('FRA_1', 'SINGLE', 0, 'FRA', 'USD', 'USD', 'USD_OIS', 'USD_OIS', 0.75, 1.25, 1.25, 0.50, 2000000.0, 0.0, 0.0, true, 1.0)
                    """);

            String query = """
                    pricing::cashflowPvs()
                      ->groupBy(~[scenarioId, tradeId, currency, csa], ~[npv: x | $x.pv: y | $y->sum()])
                      ->sort([~scenarioId->ascending(), ~tradeId->ascending()])
                    """;

            var result = Lean.run(fullModel, query, "pricing::TestRuntime", conn);
            assertEquals(6, result.rows().size(), "Expected 3 scenarios x 2 trades = 6 rows");

            double npvLin = 0.0, npvLogDf = 0.0, npvLinDf = 0.0;
            for (var row : result.rows()) {
                String scen = (String) row.get(0);
                String trade = (String) row.get(1);
                assertEquals("USD", row.get(2));
                assertEquals("USD", row.get(3));
                double actual = ((Number) row.get(4)).doubleValue();
                String method = switch (scen) {
                    case "BASE_LIN" -> "LINEAR_ZERO";
                    case "BASE_LOGDF" -> "LOG_LINEAR_DF";
                    default -> "LINEAR_DF";
                };
                double expected = referenceTradeNpv(oisKnots, projKnots, method, trade);
                assertEquals(expected, actual, 1e-6, scen + " / " + trade);
                if ("FRA_1".equals(trade)) {
                    if ("BASE_LIN".equals(scen)) npvLin = actual;
                    if ("BASE_LOGDF".equals(scen)) npvLogDf = actual;
                    if ("BASE_LINDF".equals(scen)) npvLinDf = actual;
                }
            }
            // All three interpolation methods produce genuinely distinct FRA values
            assertNotEquals(npvLin, npvLogDf, 1e-3);
            assertNotEquals(npvLogDf, npvLinDf, 1e-3);
            assertNotEquals(npvLin, npvLinDf, 1e-3);
        }
    }

    @Test
    void testUnknownInterpMethodAndMissingCurveFailLoudly() throws Exception {
        TestBackend backend = backend();
        String fullModel = loadProjectPure(backend);

        // 1) Unknown interpolation method ('LOGLINEAR' typo) must fail
        try (Connection conn = backend.open();
             Statement st = conn.createStatement()) {
            createTables(st);
            st.execute("INSERT INTO PR_CURVE_KNOT VALUES ('BASE', 'USD_OIS', 0, 1.0, 0.04, 'LOGLINEAR')");
            st.execute("INSERT INTO PR_CASHFLOW VALUES ('T1', 'FIXED', 0, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_OIS', 0.0, 1.0, 1.0, 1.0, 1000.0, 0.04, 0.0, false, 1.0)");
            assertThrows(Exception.class, () -> Lean.run(fullModel, "pricing::cashflowPvs()", "pricing::TestRuntime", conn));
        }

        // 2) Scenario missing a trade's discount curve must fail instead of silently dropping the cashflow
        try (Connection conn = backend.open();
             Statement st = conn.createStatement()) {
            createTables(st);
            st.execute("INSERT INTO PR_CURVE_KNOT VALUES ('SCEN_INCOMPLETE', 'OTHER_CURVE', 0, 1.0, 0.04, 'LOG_LINEAR_DF')");
            st.execute("INSERT INTO PR_CASHFLOW VALUES ('T1', 'FIXED', 0, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_OIS', 0.0, 1.0, 1.0, 1.0, 1000.0, 0.04, 0.0, false, 1.0)");
            assertThrows(Exception.class, () -> Lean.run(fullModel, "pricing::cashflowPvs()", "pricing::TestRuntime", conn));
        }

        // 3) Duplicate knotT or mixed interpMethod within the same (scenarioId, curveId) curve must fail loudly
        for (String badCurveSql : new String[]{
                "('BASE', 'USD_OIS', 0, 1.0, 0.04, 'LOG_LINEAR_DF'), ('BASE', 'USD_OIS', 1, 1.0, 0.05, 'LOG_LINEAR_DF')",
                "('BASE', 'USD_OIS', 0, 1.0, 0.04, 'LOG_LINEAR_DF'), ('BASE', 'USD_OIS', 1, 2.0, 0.05, 'LINEAR_ZERO')"
        }) {
            try (Connection conn = backend.open();
                 Statement st = conn.createStatement()) {
                createTables(st);
                st.execute("INSERT INTO PR_CURVE_KNOT VALUES " + badCurveSql);
                st.execute("INSERT INTO PR_CASHFLOW VALUES ('T1', 'FIXED', 0, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_OIS', 0.0, 1.5, 1.5, 1.5, 1000.0, 0.04, 0.0, false, 1.0)");
                assertThrows(Exception.class, () -> Lean.run(fullModel, "pricing::cashflowPvs()", "pricing::TestRuntime", conn));
            }
        }

        // 4) Non-positive dcf <= 0 on a cashflow must fail loudly instead of dividing by zero
        try (Connection conn = backend.open();
             Statement st = conn.createStatement()) {
            createTables(st);
            st.execute("INSERT INTO PR_CURVE_KNOT VALUES ('BASE', 'USD_OIS', 0, 1.0, 0.04, 'LOG_LINEAR_DF')");
            st.execute("INSERT INTO PR_CASHFLOW VALUES ('T1', 'FLOAT', 0, 'IRS', 'USD', 'USD', 'USD_OIS', 'USD_OIS', 0.0, 1.0, 1.0, 0.0, 1000.0, 0.0, 0.0, true, 1.0)");
            assertThrows(Exception.class, () -> Lean.run(fullModel, "pricing::cashflowPvs()", "pricing::TestRuntime", conn));
        }

        // 5) Counterparty CSA collateralized discounting: cleared USD (CSA='USD', USD_OIS) vs EUR-CSA OTC USD (CSA='EUR', USD_DISC_EUR)
        try (Connection conn = backend.open();
             Statement st = conn.createStatement()) {
            createTables(st);
            st.execute("""
                    INSERT INTO PR_CURVE_KNOT VALUES
                    ('BASE', 'USD_OIS',      0, 2.0, 0.040, 'LOG_LINEAR_DF'),
                    ('BASE', 'USD_3M',       0, 2.0, 0.043, 'LOG_LINEAR_DF'),
                    ('BASE', 'USD_DISC_EUR', 0, 2.0, 0.036, 'LOG_LINEAR_DF')
                    """);
            st.execute("""
                    INSERT INTO PR_CASHFLOW VALUES
                    ('IRS_USD_CLEARED', 'FIXED', 0, 'IRS', 'USD', 'USD', 'USD_OIS',      'USD_3M', 0.0, 2.0, 2.0, 2.0, 1000000.0, 0.040, 0.0, false, -1.0),
                    ('IRS_USD_CLEARED', 'FLOAT', 0, 'IRS', 'USD', 'USD', 'USD_OIS',      'USD_3M', 0.0, 2.0, 2.0, 2.0, 1000000.0, 0.0,   0.0, true,   1.0),
                    ('IRS_USD_EUR_CSA', 'FIXED', 0, 'IRS', 'USD', 'EUR', 'USD_DISC_EUR', 'USD_3M', 0.0, 2.0, 2.0, 2.0, 1000000.0, 0.040, 0.0, false, -1.0),
                    ('IRS_USD_EUR_CSA', 'FLOAT', 0, 'IRS', 'USD', 'EUR', 'USD_DISC_EUR', 'USD_3M', 0.0, 2.0, 2.0, 2.0, 1000000.0, 0.0,   0.0, true,   1.0)
                    """);
            var csaRows = Lean.run(fullModel, """
                    pricing::cashflowPvs()
                      ->groupBy(~[currency, csa], ~[npv: x | $x.pv: y | $y->sum()])
                      ->sort(~csa->ascending())
                    """, "pricing::TestRuntime", conn).rows();
            assertEquals(2, csaRows.size(), "USD cashflows under EUR vs USD CSA must stay segregated in groupBy(~[currency, csa])");
            assertEquals("EUR", csaRows.get(0).get(1));
            assertEquals("USD", csaRows.get(1).get(1));
            double fwd = (Math.exp(0.043 * 2.0) - 1.0) / 2.0;
            double undiscounted = 1_000_000.0 * 2.0 * (fwd - 0.040);
            assertEquals(undiscounted * Math.exp(-0.036 * 2.0), ((Number) csaRows.get(0).get(2)).doubleValue(), 1e-8);
            assertEquals(undiscounted * Math.exp(-0.040 * 2.0), ((Number) csaRows.get(1).get(2)).doubleValue(), 1e-8);
        }
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    private static double refDf(double[][] knots, String method, double t) {
        if (t <= 0.0) return 1.0;
        if (t <= knots[0][0]) return Math.exp(-knots[0][1] * t);
        if (t >= knots[knots.length - 1][0]) return Math.exp(-knots[knots.length - 1][1] * t);
        for (int i = 0; i < knots.length - 1; i++) {
            double t0 = knots[i][0], r0 = knots[i][1];
            double t1 = knots[i + 1][0], r1 = knots[i + 1][1];
            if (t >= t0 && t <= t1) {
                double w = (t - t0) / (t1 - t0);
                return switch (method) {
                    case "LOG_LINEAR_DF" -> Math.exp(-((1.0 - w) * (r0 * t0) + w * (r1 * t1)));
                    case "LINEAR_DF" -> (1.0 - w) * Math.exp(-r0 * t0) + w * Math.exp(-r1 * t1);
                    case "LINEAR_ZERO" -> Math.exp(-((1.0 - w) * r0 + w * r1) * t);
                    default -> throw new IllegalArgumentException("Unknown method: " + method);
                };
            }
        }
        throw new IllegalStateException();
    }

    private static double referenceTradeNpv(double[][] oisKnots, double[][] projKnots, String method, String trade) {
        if ("IRS_DUAL".equals(trade)) {
            // cf_idx=0 has pay_t = -0.50 (paid in past -> excluded); cf_idx=1 has accrual_start_t clamped to 0.0
            double fwd1 = ((refDf(projKnots, method, 0.0) / refDf(projKnots, method, 0.75)) - 1.0) / 1.00 + 0.0025;
            double pv1 = 1_000_000.0 * 1.00 * (fwd1 - 0.045) * refDf(oisKnots, method, 0.75);
            double fwd2 = ((refDf(projKnots, method, 0.75) / refDf(projKnots, method, 1.50)) - 1.0) / 0.75 + 0.0025;
            double pv2 = 1_000_000.0 * 0.75 * (fwd2 - 0.045) * refDf(oisKnots, method, 1.50);
            return pv1 + pv2;
        }
        double fwd = ((refDf(oisKnots, method, 0.75) / refDf(oisKnots, method, 1.25)) - 1.0) / 0.50;
        return 2_000_000.0 * 0.50 * fwd * refDf(oisKnots, method, 1.25);
    }
}
