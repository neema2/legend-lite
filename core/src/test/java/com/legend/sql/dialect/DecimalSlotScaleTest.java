// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testplan.PlanningOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * A DECIMAL SIDE'S TYPE THROUGH THE HOST JUDGE'S SIDE BATCH ON MARIADB AND MYSQL (the PR #118 follow-ups' Q3b, track
 * A5, 2026-11-18): the batch reads consecutive asserts' sides by one {@code UNION ALL}, each branch a typed slot for the
 * other sides' columns ({@code lowering.HostSideBatch}), so each column keeps the type its side answers. This dialect
 * wrote every {@code CAST(NULL AS T)} as a bare {@code NULL}, and MySQL 8.4 types a CTE's bare NULL column with 31
 * decimals: a DECIMAL(2,1) side's union answered DECIMAL(31,30), so {@code toDecimal(3.8D)} reached the host judge as
 * {@code 3.800000000000000000000000000000}. Compiled legend-pure compares two Decimals scale and all, so under PCT's
 * compatibility rules the host failed what the database judge held: channel B's essential, grammar and standard
 * ledgers on MySQL under BOTH (toDecimal, abs, rem, round, parseDecimal). Held here at planning, which needs no
 * server, under every judge: the MySQL and MariaDB lanes are manual (pct/BUILD.bazel), and they were measured green
 * with this rule (2026-11-18).
 */
class DecimalSlotScaleTest {

    private static final String MODEL = "###Pure\nClass t::Marker {}\n";

    /** Channel B's failing bodies on MySQL, their sides each a Decimal. */
    private static final List<String> BODIES = List.of(
            "|assertEq(3.8D, meta::pure::functions::math::toDecimal(3.8))",
            "|assertEq(3.8D, meta::pure::functions::math::toDecimal(3.8D))",
            "|assertEq(8D, meta::pure::functions::math::toDecimal(8))",
            "|assertEq(3.0D, abs(-3.0D))",
            "|assertEq(3.14D, round(3.14159D, 2))",
            "|assertEq(3.14159D, parseDecimal('3.14159'))");

    /** A side batch's column ({@code HostSideBatching}'s names). */
    private static final Pattern BATCH_COLUMN = Pattern.compile("\"__h\\d+_\\d+\"");

    /** A batch branch's slot for another side's column, written with no type. */
    private static final Pattern BARE_SLOT = Pattern.compile("(?<![A-Za-z(])NULL AS \"__h\\d+_\\d+\"");

    /** A Decimal slot, written with its type. */
    private static final Pattern TYPED_SLOT = Pattern.compile("CAST\\(NULL AS DECIMAL\\(\\d+,\\s?\\d+\\)\\) AS \"__h");

    @Test
    void aDecimalSidesSlotKeepsItsTypeUnderEveryJudge() {
        List<String> wrong = new ArrayList<>();
        int batches = 0;
        for (DatabaseType type : List.of(DatabaseType.MariaDB, DatabaseType.MySQL)) {
            // planned only: the server's coordinates are declared, never opened
            var ctx = Compiler.compileModel(StorelessRuntime.onServer(MODEL, type, "localhost", 3306, "legend"));
            for (PlanningOptions.JudgeMode mode : List.of(PlanningOptions.JudgeMode.HOST,
                    PlanningOptions.JudgeMode.DATABASE, PlanningOptions.JudgeMode.BOTH)) {
                for (String body : BODIES) {
                    String json = com.legend.testexecutionplan.TestPlanJson.write(com.legend.TestExecution.planBody(
                            Compiler.query(ctx, body), StorelessRuntime.RUNTIME,
                            ExecuteOptions.NONE.withJudgeMode(mode)));
                    for (String statement : statements(json)) {
                        if (!BATCH_COLUMN.matcher(statement).find()) {
                            continue;   // no host side batch in it (the database judge's own statements)
                        }
                        batches++;
                        Matcher bare = BARE_SLOT.matcher(statement);
                        if (bare.find()) {
                            wrong.add(type + " " + mode + " " + body + ": a slot with no type, " + bare.group());
                        } else if (!TYPED_SLOT.matcher(statement).find()) {
                            wrong.add(type + " " + mode + " " + body + ": no typed Decimal slot in\n" + statement);
                        }
                    }
                }
            }
        }
        // the host judge batches these bodies' two sides under HOST and BOTH, on each database
        assertTrue(batches >= 2 * 2 * BODIES.size(), "too few side batches planned to prove anything: " + batches);
        assertFalse(!wrong.isEmpty(), "each Decimal slot keeps its type:\n  " + String.join("\n  ", wrong));
    }

    /** Every statement text a test plan's JSON holds. */
    private static List<String> statements(String json) {
        List<String> out = new ArrayList<>();
        collect(com.legend.json.Json.parse(json), out);
        return out;
    }

    private static void collect(com.legend.json.Json.Node node, List<String> out) {
        switch (node) {
            case com.legend.json.Json.Str s -> {
                if (s.value().startsWith("{\"format\"")) {
                    collect(com.legend.json.Json.parse(s.value()), out);   // an isolated BOTH's judge plan, as its text
                } else if (s.value().contains("SELECT")) {
                    out.add(s.value());
                }
            }
            case com.legend.json.Json.Obj o -> o.fields().values().forEach(v -> collect(v, out));
            case com.legend.json.Json.Arr a -> a.items().forEach(v -> collect(v, out));
            default -> {
            }
        }
    }
}
