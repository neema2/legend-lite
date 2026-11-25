// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testbackend.TestBackends;

import com.legend.testing.Lean;
import com.legend.error.LegendCompileException;
import com.legend.testbackend.TestBackend;
import com.legend.testcases.SqlIslandCases;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code #SQL{...}#} end to end on DuckDB (docs/LEGEND_SQL_STUDY_2026_10_11.md §3): every case of
 * {@link SqlIslandCases} returns the rows Postgres returns for it; the island composes with Pure; names resolve by
 * the study's rule; and a refusal is the compile error its kind says, placed in the source.
 */
class SqlIslandIntegrationTest {

    /** The platform's own engine: the table, the island's session and the answer it is held to. */
    private static final TestBackend PLATFORM = TestBackends.platform();

    private static final String MODEL =
            SqlIslandCases.model(PLATFORM.connectionBody());

    private static Connection session;

    @BeforeAll
    static void seed() throws SQLException {
        session = PLATFORM.open();
        try (Statement s = session.createStatement()) {
            s.execute(SqlIslandCases.table(PLATFORM));
            s.execute("INSERT INTO EMP VALUES " + SqlIslandCases.ROWS);
        }
    }

    @AfterAll
    static void close() throws SQLException {
        session.close();
    }

    private static Lean.Result run(String query) throws SQLException {
        return Lean.run(MODEL, query, SqlIslandCases.RUNTIME, session);
    }

    private static List<List<String>> rows(Lean.Result result) {
        List<List<String>> rows = new ArrayList<>();
        for (var row : result.rows()) {
            List<String> cells = new ArrayList<>();
            for (Object value : row) {
                cells.add(SqlIslandCases.cell(value));
            }
            rows.add(cells);
        }
        return rows;
    }

    static Stream<Arguments> cases() {
        return SqlIslandCases.CASES.stream().map(c -> Arguments.of(c.sql(), c));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void returnsTheRecordedAnswer(String sql, SqlIslandCases.Case c) throws SQLException {
        Lean.Result result = run(c.island());
        assertEquals(c.columns(), result.columns().stream().map(col -> col.name()).toList(), sql);
        List<List<String>> actual = rows(result);
        if (c.ordered()) {
            assertEquals(c.rows(), actual, sql);
        } else {
            assertEquals(sorted(c.rows()), sorted(actual), sql);
        }
    }

    private static List<String> sorted(List<List<String>> rows) {
        return rows.stream().map(Object::toString).sorted().toList();
    }

    @Test
    void anUnquotedNameMatchesTheModelsNameIgnoringCase() throws SQLException {
        // Postgres would fold FIRSTNAME to firstname and find nothing; the study's rule then matches firstName
        Lean.Result result =
                run("#SQL{select FIRSTNAME, last_name from tb('store::HR.EMP') where id = 1}#");
        assertEquals(List.of("firstName", "LAST_NAME"), result.columns().stream().map(col -> col.name()).toList());
        assertEquals(List.of(List.of("Ann", "Lee")), rows(result));
    }

    @Test
    void theIslandComposesWithPure() throws SQLException {
        Lean.Result result = run("#SQL{select dept, count(*) as n from tb('store::HR.EMP')"
                + " where dept is not null group by dept}#->filter(x|$x.n > 2)->select(~[dept])");
        assertEquals(List.of(List.of("eng")), rows(result));
    }

    @Test
    void theIslandsOwnWindowsAreComputedBeforeAPureFilterAfterIt() throws SQLException {
        // 2026-10-16 (the IQ landing's review): the island's result is an inline view; the filter after it once folded
        // under its window, which then numbered the filtered rows (1, 2, 3)
        Lean.Result result = run("#SQL{select id, row_number() over (order by id) as rn"
                + " from tb('store::HR.EMP')}#->filter(x|$x.ID > 3)->sort(~ID->ascending())");
        assertEquals(List.of(List.of("4", "4"), List.of("5", "5"), List.of("6", "6")), rows(result));
    }

    @Test
    void aNameApartFromAColumnOnlyInCaseIsReadAsItself() throws SQLException {
        // 2026-10-16 (the IQ landing's review): "Id" beside ID; DuckDB reads names without regard to case, even quoted,
        // so a select over a subselect showing both read the first. An extend after the island's limit keeps the
        // names apart (it folds where they are not)
        Lean.Result result = run("#SQL{select id, age as \"Id\" from tb('store::HR.EMP')"
                + " order by \"ID\" limit 3}#->extend(~z: x|$x.Id->toOne() + 1)");
        assertEquals(List.of(List.of("1", "30", "31"), List.of("2", "40", "41"), List.of("3", "25", "26")),
                rows(result));
    }

    @Test
    void aViewsWindowsShareOneSubqueryLevel() throws SQLException {
        // 2026-10-16 (the IQ landing's review): only the view's outermost window is a boundary; a level each window
        // passed DuckDB's expression depth at 1,100 windows
        String query = "#SQL{select * from (select id, row_number() over (order by id) as a, rank() over (order by age)"
                + " as b, count(*) over () as c from tb('store::HR.EMP')) d where d.id > 4}#->sort(~ID->ascending())";
        assertEquals(List.of(List.of("5", "5", "4", "6"), List.of("6", "6", "2", "6")), rows(run(query)));
        String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(MODEL), query)
                .plan(SqlIslandCases.RUNTIME).sql();
        // the windows' select and the one over it
        assertEquals(2, sql.split("SELECT", -1).length - 1, sql);
    }

    @Test
    void aDoublePrecisionConstantIsTheDoubleNearestItsDigits() throws SQLException {
        // 2026-11-24: as a Decimal literal the database cast, a constant was DuckDB's double of it, which is not
        // always the nearest (0.9927456070959739 answered 0.992745607095974). Every constant here is written as a
        // numeric constant cast and as a string constant read, and must come back bit for bit the double nearest its
        // digits: doubles of every magnitude by their shortest digits, doubles in [0, 1) with seventeen digits (the
        // kind that failed), and decimals and whole numbers on each side of the line PostgresCasts.floatConstant
        // draws (to 15 digits and 22 decimal places a Decimal literal the database casts; past either, digits it
        // parses)
        java.util.Random random = new java.util.Random(20261124L);
        List<String> constants = new ArrayList<>();
        for (double named : new double[] {0.9927456070959739, -0.9927456070959739, 6.62607015e-34, 1e-40, 0.1, 1e22,
                1e23, 123456789012345680.0, Double.MAX_VALUE, Double.MIN_NORMAL, Double.MIN_VALUE, 0.0}) {
            constants.add(Double.toString(named));
        }
        constants.addAll(List.of("0.1000000000000000055511151231257827", "0.00000000000000000000001", "1e-23",
                "999999999999999e-22", "9007199254740993", "35031194039299790746", "-35031194039299790746", "5e35",
                "3.4637169762754e19", "7e280", "999999999999999", "1e15", "0e5", "0.000"));
        while (constants.size() < 4000) {
            double any = Double.longBitsToDouble(random.nextLong());
            if (Double.isFinite(any)) {
                constants.add(Double.toString(any));
            }
            constants.add(Double.toString(random.nextDouble()));
            constants.add(Double.toString(-random.nextDouble() * 1000));
            constants.add(Double.toString(random.nextInt(2_000_000) / 1000.0));
            // up to 15 digits with up to 22 decimal places, then up to 18 digits with up to 38
            constants.add(java.math.BigDecimal.valueOf(random.nextLong(1_000_000_000_000_000L), random.nextInt(23))
                    .toPlainString());
            constants.add(java.math.BigDecimal.valueOf(random.nextLong(Long.MAX_VALUE), random.nextInt(39))
                    .negate().toPlainString());
            // a whole number of 15 digits and up to 25 zeros, as its exponent form writes it
            constants.add(java.math.BigDecimal.valueOf(random.nextLong(1_000_000_000_000_000L), -random.nextInt(26))
                    .toString());
        }
        for (int from = 0; from < constants.size(); from += 100) {
            List<String> batch = constants.subList(from, Math.min(from + 100, constants.size()));
            StringBuilder items = new StringBuilder();
            for (int i = 0; i < batch.size(); i++) {
                items.append(i == 0 ? "" : ", ").append("cast(").append(batch.get(i))
                        .append(" as double precision) as n").append(i).append(", '").append(batch.get(i))
                        .append("'::float8 as s").append(i);
            }
            List<Object> row = run("#SQL{select " + items + " from tb('store::HR.EMP') where id = 1}#").rows().get(0);
            for (int i = 0; i < batch.size(); i++) {
                // -0 as a numeric constant is 0 (a numeric has no negative zero); as a string constant it is 0 here
                // too, as it was (Postgres's '-0'::float8 is -0)
                long want = Double.doubleToLongBits(Double.parseDouble(batch.get(i)) + 0.0);
                assertEquals(want, bits(row.get(2 * i)), "cast(" + batch.get(i) + " as double precision)");
                assertEquals(want, bits(row.get(2 * i + 1)), "'" + batch.get(i) + "'::float8");
            }
        }
    }

    @Test
    void aDoublePrecisionConstantBesideANumericIsStillADouble() throws SQLException {
        // 2026-11-24: Pure's Float literal beside a Decimal is a Decimal in the lowering (Scalars.undoubled), so a
        // double precision constant is never written as one: the sum is the double, as Postgres's
        List<Object> row = run("#SQL{select cast(0.1 as float8) + 0.2 as a, 0.2 * cast(0.1 as float8) as b,"
                + " cast(0.9927456070959739 as float8) - 0.5 as c from tb('store::HR.EMP') where id = 1}#").rows().get(0);
        assertEquals(List.of(0.1 + 0.2, 0.2 * 0.1, 0.9927456070959739 - 0.5), row);
    }

    /** A double precision cell's bits: the answer's own double, never a value near it. */
    private static long bits(Object cell) {
        assertEquals(Double.class, cell.getClass(), "a double precision cell is a double");
        return Double.doubleToLongBits((Double) cell);
    }

    @Test
    void aNumericConstantNoDoubleHoldsIsTheSpecsErrorAsADoublePrecision() {
        // Postgres converts a numeric by its text (numeric_float8 is float8in of numeric_out), so its error names the
        // plain digits; SqlIslandPostgresTest holds these sentences to Postgres's own
        LegendCompileException over = assertThrows(LegendCompileException.class,
                () -> run("#SQL{select cast(1e400 as float8) from tb('store::HR.EMP')}#"));
        assertEquals("#SQL{...}# at line 1, column 13: \"1" + "0".repeat(400) + "\" is out of range for type double"
                + " precision", over.getMessage());
        LegendCompileException under = assertThrows(LegendCompileException.class,
                () -> run("#SQL{select cast(-1.5e-400 as double precision) from tb('store::HR.EMP')}#"));
        assertEquals("#SQL{...}# at line 1, column 13: \"-0." + "0".repeat(399) + "15\" is out of range for type"
                + " double precision", under.getMessage());
        // past Postgres's numeric itself (16,383 decimal places): its error for the literal, before any digits are
        // written out
        for (String past : List.of("1e-16384", "1e-2147483647", "1e131073")) {
            LegendCompileException overflows = assertThrows(LegendCompileException.class,
                    () -> run("#SQL{select cast(" + past + " as float8) from tb('store::HR.EMP')}#"), past);
            assertEquals("#SQL{...}# at line 1, column 13: value overflows numeric format", overflows.getMessage(),
                    past);
        }
    }

    @Test
    void aSyntaxErrorIsAParseErrorAtItsPlaceInTheSource() {
        LegendCompileException e = assertThrows(LegendCompileException.class,
                () -> run("#SQL{select ID from tb('store::HR.EMP') where}#"));
        assertEquals(LegendCompileException.Phase.PARSE, e.phase());
        assertEquals("#SQL{...}# at line 1, column 46: syntax error at end of input", e.getMessage());
    }

    @Test
    void aQueryTheSpecRejectsIsATypeError() {
        LegendCompileException e = assertThrows(LegendCompileException.class,
                () -> run("#SQL{select dept, age from tb('store::HR.EMP') group by dept}#"));
        assertEquals(LegendCompileException.Phase.TYPE, e.phase());
        assertTrue(e.getMessage().endsWith("column \"age\" must appear in the GROUP BY clause or be used in an"
                + " aggregate function"), e.getMessage());
    }

    @Test
    void sqlNotTranslatedYetIsNotImplemented() {
        com.legend.error.NotImplementedException e = assertThrows(com.legend.error.NotImplementedException.class,
                () -> run("#SQL{select dept, mode() within group (order by age) from tb('store::HR.EMP')"
                        + " group by dept}#"));
        assertEquals("#SQL{...}# at line 1, column 19: WITHIN GROUP is not supported yet", e.getMessage());
    }

    @Test
    void anAverageIsAFloatWhereTheSpecGivesANumeric() throws SQLException {
        // A KNOWN DIFFERENCE (docs/LEGEND_SQL_STUDY_2026_10_11.md §3.4): Pure's average is a Float, so avg over
        // integers is 33.333333333333336 here where Postgres's numeric gives 33.3333333333333333
        Lean.Result result = run("#SQL{select avg(age) as a from tb('store::HR.EMP')"
                + " where dept = 'eng'}#");
        assertEquals(List.of(List.of("33.333333333333336")), rows(result));
    }
}
