// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE MAY-SKIP SET'S CENSUS (the PR1 re-review's A6 and B6, 2026-11-16): {@link SqlFn#MAY_SKIP_AN_ARGUMENT} is a hand
 * list of the functions some dialect renders with a branch, which may leave an argument unevaluated, so
 * {@code LetBinding.alwaysEvaluated} never descends into them (R6). A function outside the set whose rendering on any
 * dialect branches would let a raising repeat be bound out of an untaken arm. This renders every function on every
 * dialect, the dialects' helpers included (the whole call is rendered, not one arm), and fails where a rendering
 * branches ({@code CASE}, {@code IF(}, {@code IIF(}, {@code DECODE(}, {@code multiIf(}) for a function outside the set.
 */
class MaySkipCensusTest {

    private static final Pattern BRANCH = Pattern.compile("(?i)\\bCASE\\b|\\bIIF\\s*\\(|\\bIF\\s*\\(|\\bDECODE\\s*\\("
            + "|\\bmultiIf\\s*\\(");

    /** Argument types tried, each at arities 1 to 4: the first that renders is read. */
    private static final List<SqlType.Scalar> TYPES = List.of(SqlType.Scalar.INTEGER, SqlType.Scalar.VARCHAR,
            SqlType.Scalar.DOUBLE, SqlType.Scalar.BOOLEAN, SqlType.Scalar.DATE, SqlType.Scalar.TIMESTAMP);

    private static List<String> renderings(SqlDialect d, SqlFn fn) {
        List<String> out = new ArrayList<>();
        for (SqlType.Scalar t : TYPES) {
            for (int arity = 1; arity <= 4; arity++) {
                List<SqlExpr> args = new ArrayList<>();
                List<OutputCol> cols = new ArrayList<>();
                for (int i = 0; i < arity; i++) {
                    args.add(SqlExpr.Column.of("t", "c" + i, t, false, OutputCol.Origin.PHYSICAL));
                    cols.add(new OutputCol("c" + i, t, false));
                }
                try {
                    out.add(d.render(new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Call.of(fn,
                            args.toArray(SqlExpr[]::new)), "x", null)), false,
                            new SqlSource.Table("t", "t", cols, false, Map.of(), com.legend.sql.NameQuoting.BARE),
                            null, List.of(), null, null, List.of(), null, null, List.of())));
                } catch (RuntimeException notThisShape) {
                    // an arity or type this function does not take: the next is tried
                }
            }
        }
        return out;
    }

    @Test
    void everyBranchingRenderingIsInTheMaySkipSet() {
        Map<String, TreeSet<String>> outside = new TreeMap<>();
        int rendered = 0;
        for (DatabaseType type : DatabaseType.values()) {
            SqlDialect d;
            try {
                d = Databases.dialect(type);
            } catch (RuntimeException noDialect) {
                continue;
            }
            for (SqlFn fn : SqlFn.values()) {
                for (String sql : renderings(d, fn)) {
                    rendered++;
                    if (!SqlFn.MAY_SKIP_AN_ARGUMENT.contains(fn) && BRANCH.matcher(sql).find()) {
                        outside.computeIfAbsent(fn.name(), k -> new TreeSet<>()).add(type.name());
                    }
                }
            }
        }
        assertTrue(rendered > 1000, "the census rendered " + rendered + " calls");
        assertEquals(Map.of(), outside, "a function whose rendering branches, outside SqlFn.MAY_SKIP_AN_ARGUMENT"
                + " (function -> the dialects that branch): add it to the set -- a strict function read as may-skip"
                + " costs a missed binding, the reverse a wrong raise (the PR1 re-review's A6, 2026-11-16)");
    }

    @Test
    void theCensusBites() {
        // a function the set does not hold whose rendering is a CASE: found
        assertTrue(BRANCH.matcher("SELECT CASE WHEN t.c0 IS NULL THEN NULL ELSE t.c0 END AS x").find());
        assertTrue(BRANCH.matcher("SELECT multiIf(a, b, c) AS x").find());
        assertTrue(!BRANCH.matcher("SELECT lower(t.c0) AS x").find());
    }
}
