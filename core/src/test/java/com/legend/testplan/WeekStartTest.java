// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testplan;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.ExecuteOptions;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testing.Program;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * A WEEK STARTS ON MONDAY ON EVERY DATABASE (track A5, 2026-11-19): {@code firstDayOfWeek} is legend-engine's
 * {@code $date->mostRecentDayOfWeek(DayOfWeek.Monday)} (core/pure/corefunctions/dateExtension.pure), the most recent
 * Monday no later than the date -- a Monday its own, a Sunday six days back. H2's {@code date_trunc('week', ...)} starts
 * the week on the session locale's first day, Sunday under en_US, so lite answered 2014-11-30 on H2 where DuckDB and
 * SQLite answer 2014-12-01. One answer on each in-process database (the server and cloud dialects
 * spell a Monday start already: ClickHouse's toMonday, BigQuery's WEEK(MONDAY), MariaDB's WEEKDAY, MongoDB's
 * startOfWeek, Snowflake's WEEK_START, Postgres's, Trino's and Databricks' week).
 */
class WeekStartTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";

    /** A date and the Monday that starts its week, Pure's. */
    private static final Map<String, String> MONDAYS = Map.of(
            "%2014-12-04", "2014-12-01",
            "%2014-12-01", "2014-12-01",
            "%2014-11-30", "2014-11-24",
            "%2015-01-01", "2014-12-29",
            "%2014-12-04T10:30:00", "2014-12-01");

    /** The query's answer, its one cell's text, on each database. (An assert over it is not the probe: the host judge
     *  reads a Date-declared side as a DateTime, F5.4's carrier, and fails an equality with a StrictDate literal the
     *  database judge holds -- reported apart, 2026-11-19.) */
    @Test
    void firstDayOfWeekIsTheMondayOnEveryDatabase() throws Exception {
        List<String> wrong = new ArrayList<>();
        for (var backend : TestBackends.inProcess()) {
            for (var e : MONDAYS.entrySet()) {
                String query = "{|" + e.getKey() + "->meta::pure::functions::date::firstDayOfWeek()}";
                String got;
                try (Connection c = backend.open()) {
                    got = String.valueOf(Program.last(StorelessRuntime.with(MODEL, backend.type()), query,
                            StorelessRuntime.RUNTIME, c, ExecuteOptions.NONE).rows().get(0).get(0));
                } catch (Throwable t) {
                    got = t.getClass().getSimpleName() + ": " + t.getMessage();
                }
                if (!got.equals(e.getValue())) {
                    wrong.add(backend.type() + " " + query + " -> " + got + " (Pure's: " + e.getValue() + ")");
                }
            }
        }
        assertTrue(wrong.isEmpty(), "a week starts on Monday:\n  " + String.join("\n  ", wrong));
    }
}
