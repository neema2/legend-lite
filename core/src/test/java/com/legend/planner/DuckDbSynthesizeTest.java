// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.executionplan.DatabaseType;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TEST-DATA GENERATOR'S SYNTHESIZE, its simple slice (the user's ruling of 2026-11-12; register S303;
 * {@link Synthesize}): rows the database generates into a declared table, as one planned HOST_SESSION load -- the table
 * made as declared, then ONE DuckDB statement over {@code range(N)}, each column a formula of the row's number. The
 * statement for each type and knob; DataCube's demo table filled with 200,000 rows, its aggregates exactly those the
 * formulas give; refusals by name; no row in the plan.
 */
class DuckDbSynthesizeTest {

    /** Every type the slice makes, and one it does not. */
    private static final String MODEL = """
            ###Relational
            Database x::DB
            (
              Table ALL_TYPES (ID INTEGER, NAME VARCHAR(20), SMALL TINYINT, AMOUNT DECIMAL(6, 2), RATE DOUBLE,
                DAY DATE, AT TIMESTAMP, FLAG BIT)
              Table BLOB (ID INTEGER, DATA VARBINARY(10))
            )
            """;

    /** DataCube's demo table (datacube/demo/trades.pure). */
    private static final String TRADES = """
            ###Relational
            Database trades::DB
            (
                Table TRADES
                (
                    region VARCHAR(32), desk VARCHAR(32), book VARCHAR(32), year INTEGER, qtr VARCHAR(8),
                    notional DOUBLE, pnl DOUBLE, qty INTEGER
                )
            )
            """;

    /** Today's demo sample's domains (datacube/src/sample-trades.ts) as knobs. */
    private static Map<String, Synthesize.Knob> tradeKnobs() {
        Map<String, Synthesize.Knob> k = new LinkedHashMap<>();
        k.put("region", new Synthesize.Knob.Values(List.of("EMEA", "AMER", "APAC")));
        k.put("desk", new Synthesize.Knob.Values(List.of("Rates", "Credit", "FX", "Equity", "Commodities")));
        k.put("book", new Synthesize.Knob.Values(List.of("Book 1", "Book 2", "Book 3", "Book 4")));
        k.put("qtr", new Synthesize.Knob.Values(List.of("Q1", "Q2", "Q3", "Q4")));
        k.put("year", new Synthesize.Knob.Range("2021", "2025", null));
        k.put("notional", new Synthesize.Knob.Range("0", "9999.99", 2));
        k.put("pnl", new Synthesize.Knob.Range("-1000", "999.99", 2));
        k.put("qty", new Synthesize.Knob.Range("1", "97", null));
        return k;
    }

    private static ExecutionPlan.Load load(ExecutionPlan plan) {
        assertEquals(plan, PlanJson.read(PlanJson.write(plan)), "the plan survives its JSON");
        ExecutionPlan.Load load = (ExecutionPlan.Load) plan.root();
        assertEquals(ExecutionPlan.Load.Cadence.HOST_SESSION, load.cadence());
        return load;
    }

    private static ExecutionPlan plan(String model, String db, String table, long count,
            Map<String, Synthesize.Knob> knobs) {
        return Synthesize.plan(new DuckDb(), DatabaseType.DuckDB, new LoadPlanner.DeclaredTable(
                com.legend.Compiler.compileModel(model), db, "default", table), count, knobs);
    }

    /** The load's last step: the one statement that fills the table. */
    private static String fill(ExecutionPlan plan) {
        List<ExecutionPlan.LoadStep> steps = load(plan).steps();
        return ((ExecutionPlan.LoadStep.Statement) steps.get(steps.size() - 1)).sql();
    }

    @Test
    void eachTypeHasItsFormula() {
        // AT is a reserved word: DuckDB's own spelling of the declared name quotes it
        assertEquals("INSERT INTO ALL_TYPES (ID, NAME, SMALL, AMOUNT, RATE, DAY, \"AT\", FLAG) SELECT"
                + " (0 + ((i * 2147483647) % (CAST(round((9999 - 0) / 1) AS BIGINT) + 1)) * 1),"
                + " 'NAME_' || CAST((i % 5) + 1 AS VARCHAR),"
                + " (0 + ((i * 2147483647) % (CAST(round((99 - 0) / 1) AS BIGINT) + 1)) * 1),"
                + " (0 + ((i * 2147483647) % (CAST(round((9999.99 - 0) / 0.01) AS BIGINT) + 1)) * 0.01),"
                + " (0 + ((i * 2147483647) % (CAST(round((9999.99 - 0) / 0.01) AS BIGINT) + 1)) * 0.01),"
                + " (DATE '2021-01-01' + CAST((i * 2147483647) % (DATE '2025-12-31' - DATE '2021-01-01' + 1) AS INTEGER)),"
                + " (CAST(DATE '2021-01-01' AS TIMESTAMP) + to_seconds((i * 2147483647) % ((DATE '2025-12-31'"
                + " - DATE '2021-01-01' + 1) * 86400))),"
                + " ((i % 2) = 0)"
                + " FROM range(10) AS t(i)", fill(plan(MODEL, "x::DB", "ALL_TYPES", 10, Map.of())));
    }

    @Test
    void aKnobGivesValuesInTurnOrARange() {
        Map<String, Synthesize.Knob> knobs = new LinkedHashMap<>();
        knobs.put("NAME", new Synthesize.Knob.Values(List.of("a", "o'b")));
        knobs.put("ID", new Synthesize.Knob.Values(List.of("7", "-1")));
        knobs.put("AMOUNT", new Synthesize.Knob.Range("-5", "5", 1));
        knobs.put("DAY", new Synthesize.Knob.Range("2024-02-01", "2024-02-29", null));
        String sql = fill(plan(MODEL, "x::DB", "ALL_TYPES", 3, knobs));
        assertTrue(sql.contains("([7, -1])[(i % 2) + 1], (['a', 'o''b'])[(i % 2) + 1]"), sql);
        assertTrue(sql.contains("(-5 + ((i * 2147483647) % (CAST(round((5 - -5) / 0.1) AS BIGINT) + 1)) * 0.1)"), sql);
        assertTrue(sql.contains("(DATE '2024-02-01' + CAST((i * 2147483647) % (DATE '2024-02-29' - DATE '2024-02-01'"
                + " + 1) AS INTEGER))"), sql);
    }

    /** DataCube's demo table, 200,000 rows on DuckDB, its knobs today's domains: every aggregate exactly the formulas'
     *  (computed here from the same formulas, row by row). */
    @Test
    void theDemoTableIsFilledWithTwoHundredThousandRows() throws Exception {
        int n = 200_000;
        ExecutionPlan plan = plan(TRADES, "trades::DB", "TRADES", n, tradeKnobs());
        try (Connection c = com.legend.exec.Connectors.of(com.legend.planner.ConnectionPlans.type(
                com.legend.model.ConnectionDefinition.DatabaseType.DuckDB)).openPrivate()) {
            try (Statement s = c.createStatement()) {
                for (ExecutionPlan.LoadStep step : load(plan).steps()) {
                    s.execute(((ExecutionPlan.LoadStep.Statement) step).sql());
                }
            }
            long p = 2_147_483_647L;
            long years = 0;
            long qty = 0;
            long notional = 0;
            long pnl = 0;
            for (long i = 0; i < n; i++) {
                years += 2021 + i * p % 5;
                qty += 1 + i * p % 97;
                notional += i * p % 1_000_000;
                pnl += -100_000 + i * p % 200_000;
            }
            assertEquals(List.of(List.of(String.valueOf(n), "3", "5", "4", "4", String.valueOf(years), "2021", "2025",
                    String.valueOf(qty), "1", "97", String.valueOf(notional), String.valueOf(pnl), "-1000.0", "999.99")),
                    rows(c, "SELECT count(*), count(DISTINCT region), count(DISTINCT desk), count(DISTINCT book),"
                            + " count(DISTINCT qtr), sum(year), min(year), max(year), sum(qty), min(qty), max(qty),"
                            + " sum(CAST(round(notional * 100) AS BIGINT)), sum(CAST(round(pnl * 100) AS BIGINT)),"
                            + " min(pnl), max(pnl) FROM TRADES"));
            // the value lists in turn: row i's region is the (i mod 3)-th
            assertEquals(List.of(List.of("AMER", "66667"), List.of("APAC", "66666"), List.of("EMEA", "66667")),
                    rows(c, "SELECT region, count(*) FROM TRADES GROUP BY region ORDER BY region"));
        }
    }

    /** No row is in the plan: a plan of 200,000 rows is a plan of 10 but for the count's digits (and its identity). */
    @Test
    void thePlanHoldsNoRow() {
        String ten = PlanJson.write(plan(TRADES, "trades::DB", "TRADES", 10, tradeKnobs()));
        String many = PlanJson.write(plan(TRADES, "trades::DB", "TRADES", 200_000, tradeKnobs()));
        assertEquals(ten.length() + 4, many.length(), "only the count's four more digits differ");
        assertTrue(load(plan(TRADES, "trades::DB", "TRADES", 200_000, tradeKnobs())).steps().stream()
                .allMatch(s -> s instanceof ExecutionPlan.LoadStep.Statement), "every step a statement: no rows, no file");
    }

    @Test
    void whatTheSliceDoesNotMakeIsRefusedByName() {
        // every database but DuckDB: its host operations refuse the operation by name
        var ctx = com.legend.Compiler.compileModel(TRADES);
        for (com.legend.model.ConnectionDefinition.DatabaseType t
                : com.legend.model.ConnectionDefinition.DatabaseType.values()) {
            if (t == com.legend.model.ConnectionDefinition.DatabaseType.DuckDB) {
                continue;
            }
            com.legend.sql.dialect.SqlDialect dialect;
            try {
                dialect = com.legend.database.Databases.dialect(t);
            } catch (com.legend.error.NotImplementedException noDialect) {
                continue; // a database with no dialect at all plans nothing, this least of all
            }
            com.legend.sql.DialectCapability refused = assertThrows(com.legend.sql.DialectCapability.class,
                    () -> Synthesize.plan(dialect, ConnectionPlans.type(t),
                            new LoadPlanner.DeclaredTable(ctx, "trades::DB", "default", "TRADES"), 10, Map.of()),
                    t.name());
            assertTrue(refused.getMessage().contains("Synthesize on"), refused.getMessage());
        }
        IllegalArgumentException blob = assertThrows(IllegalArgumentException.class,
                () -> plan(MODEL, "x::DB", "BLOB", 10, Map.of()));
        assertTrue(blob.getMessage().contains("BLOB.DATA") && blob.getMessage().contains("is not made by this slice"),
                blob.getMessage());
        IllegalArgumentException undeclared = assertThrows(IllegalArgumentException.class,
                () -> plan(MODEL, "x::DB", "ALL_TYPES", 10, Map.of("NOPE", new Synthesize.Knob.Values(List.of("x")))));
        assertTrue(undeclared.getMessage().contains("the column NOPE"), undeclared.getMessage());
        IllegalArgumentException textRange = assertThrows(IllegalArgumentException.class,
                () -> plan(MODEL, "x::DB", "ALL_TYPES", 10, Map.of("NAME", new Synthesize.Knob.Range("a", "z", null))));
        assertTrue(textRange.getMessage().contains("a range for the text column NAME"), textRange.getMessage());
        IllegalArgumentException notANumber = assertThrows(IllegalArgumentException.class,
                () -> plan(MODEL, "x::DB", "ALL_TYPES", 10, Map.of("ID", new Synthesize.Knob.Values(List.of("one")))));
        assertTrue(notANumber.getMessage().contains("takes no value 'one'"), notANumber.getMessage());
    }

    private static List<List<String>> rows(Connection c, String sql) throws Exception {
        List<List<String>> out = new java.util.ArrayList<>();
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            int width = r.getMetaData().getColumnCount();
            while (r.next()) {
                List<String> row = new java.util.ArrayList<>(width);
                for (int i = 1; i <= width; i++) {
                    row.add(String.valueOf(r.getObject(i)));
                }
                out.add(row);
            }
        }
        return out;
    }
}
