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
