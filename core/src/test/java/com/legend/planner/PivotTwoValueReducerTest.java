// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.Compiler;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.PlanCases;

import java.sql.Connection;

import org.junit.jupiter.api.Test;

/**
 * A PIVOT'S REDUCER OF TWO ROW VALUES (maxBy, minBy: the value at its key's greatest or least row) over a group that
 * holds several keys' rows (the S374 probe's follow-up, 2026-11-19): each key's cell ranks that key's rows alone. The
 * pivot's per-key form restricted the reducer's first argument to the key's rows and left the second every row's, so
 * ARG_MAX(CASE WHEN key THEN name END, w) ranked the other keys' rows by w and answered their null -- in the dynamic
 * pivot's probe plan (KeyedPivot) and in the static emulation of a database with no native pivot (PivotAsGroupBy, its
 * carriers' copy, Snowflake's). On every database the platform runs in-process, H2 among them since its ARG_MAX and
 * ARG_MIN are written (H2 has neither; the orchestrator's (a), 2026-11-19). A tie answers the earliest of its rows, as
 * Pure's maxBy and minBy do (a stable sort of the zipped pairs, its head: maxBy.pure, minBy.pure): measured that day,
 * H2, DuckDB and SQLite each answered it so, grouped and pivoted, and H2 and DuckDB windowed.
 */
class PivotTwoValueReducerTest {

    /** LON 2011's three rows tie on w (m first); PAR has no 2011 row, its cell null. */
    private static final String DATA = "|#TDS\n city, year, name, w\n NYC, 2011, a, 1\n NYC, 2011, x, 5\n"
            + " NYC, 2000, b, 2\n NYC, 2000, y, 9\n LON, 2000, c, 3\n LON, 2011, m, 4\n LON, 2011, a, 4\n"
            + " LON, 2011, z, 4\n PAR, 2000, d, 6\n#->select(~[city, year, name, w])";

    private static final String ROW_MAPPER = "meta::pure::functions::math::mathUtility::rowMapper($x.name, $x.w)";

    private static String answer(TestBackend backend, String query) throws Exception {
        ExecutionPlan typed = Compiler.query(Compiler.compileModel(PlanMakerTest.model(backend.type().name())),
                DATA + query + "->sort(~city->ascending())").executionPlan("s::RT");
        try (Connection c = backend.open()) {
            return PlanCases.write(typed, c, java.util.Map.of(), PlanCases.Written.JSON::writer);
        }
    }

    @Test
    void eachKeysCellRanksThatKeysRowsAlone() throws Exception {
        for (TestBackend backend : TestBackends.inProcess()) {
            // the dynamic pivot (a probe plan on every database)
            assertEquals("[{\"city\":\"LON\",\"2000__|__mb\":\"c\",\"2011__|__mb\":\"m\"},"
                    + "{\"city\":\"NYC\",\"2000__|__mb\":\"y\",\"2011__|__mb\":\"x\"},"
                    + "{\"city\":\"PAR\",\"2000__|__mb\":\"d\",\"2011__|__mb\":null}]",
                    answer(backend, "->pivot(~[year], ~['mb' : x | " + ROW_MAPPER + " : y | $y->maxBy()])"),
                    backend.name() + " maxBy");
            assertEquals("[{\"city\":\"LON\",\"2000__|__mn\":\"c\",\"2011__|__mn\":\"m\"},"
                    + "{\"city\":\"NYC\",\"2000__|__mn\":\"b\",\"2011__|__mn\":\"a\"},"
                    + "{\"city\":\"PAR\",\"2000__|__mn\":\"d\",\"2011__|__mn\":null}]",
                    answer(backend, "->pivot(~[year], ~['mn' : x | " + ROW_MAPPER + " : y | $y->minBy()])"),
                    backend.name() + " minBy");
            // the static pivot (its values named: a native PIVOT, or the emulation of a database with none)
            assertEquals("[{\"city\":\"LON\",\"2000__|__mb\":\"c\",\"2011__|__mb\":\"m\"},"
                    + "{\"city\":\"NYC\",\"2000__|__mb\":\"y\",\"2011__|__mb\":\"x\"},"
                    + "{\"city\":\"PAR\",\"2000__|__mb\":\"d\",\"2011__|__mb\":null}]",
                    answer(backend, "->pivot(~year, [2000, 2011], ~mb : x | " + ROW_MAPPER + " : y | $y->maxBy())"),
                    backend.name() + " static maxBy");
        }
    }

    /** The tie grouped, as Pure answers it: the earliest of the tied rows, for maxBy and minBy alike. */
    @Test
    void aTieAnswersTheEarliestOfItsRows() throws Exception {
        for (TestBackend backend : TestBackends.inProcess()) {
            assertEquals("[{\"city\":\"LON\",\"mb\":\"m\",\"mn\":\"m\"}]",
                    answer(backend, "->filter(x | $x.year == 2011 && $x.city == 'LON')->groupBy(~[city], ~['mb' : x | "
                            + ROW_MAPPER + " : y | $y->maxBy(), 'mn' : x | " + ROW_MAPPER + " : y | $y->minBy()])"),
                    backend.name() + " tie");
        }
    }
}
