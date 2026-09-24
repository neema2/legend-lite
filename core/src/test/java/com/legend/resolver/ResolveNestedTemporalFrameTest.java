// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.SpecParser;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Remediation T1.3 — a temporal root whose chain contains an INNER
 * temporal query ({@code ->in(Other.all(date)...)}): resolving the inner
 * query re-roots the resolver's temporal frame; without save/restore the
 * OUTER chain's specs then attach to the INNER fetch's frame and the
 * outer window derives from the wrong context.
 */
class ResolveNestedTemporalFrameTest {

    private static final String MODEL = """
            Class <<temporal.businesstemporal>> q::A { id: Integer[1]; p: q::P[*]; }
            Class <<temporal.businesstemporal>> q::B { id: Integer[1]; }
            Class <<temporal.businesstemporal>> q::P { name: String[1]; }
            ###Relational
            Database q::DB (
              Table AT (
                milestoning( business(BUS_FROM=a_from, BUS_THRU=a_thru) )
                ID INTEGER PRIMARY KEY, a_from DATE, a_thru DATE )
              Table BT (
                milestoning( business(BUS_FROM=b_from, BUS_THRU=b_thru) )
                ID INTEGER PRIMARY KEY, b_from DATE, b_thru DATE )
              Table PT (
                milestoning( business(BUS_FROM=p_from, BUS_THRU=p_thru) )
                ID INTEGER PRIMARY KEY, name VARCHAR(64),
                p_from DATE, p_thru DATE )
              Join AP (AT.ID = PT.ID)
            )
            ###Mapping
            Mapping q::M (
              *q::A : Relational { ~mainTable [q::DB] AT id: AT.ID,
                p: [q::DB]@AP }
              *q::B : Relational { ~mainTable [q::DB] BT id: BT.ID }
              *q::P : Relational { ~mainTable [q::DB] PT name: PT.name }
            )
            ###Runtime
            Runtime q::RT { mappings: [q::M]; }
            """;

    private static String sqlOf(String query) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(com.legend.testing.Own.spec(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs)
                .resolve(body, "q::RT");
        SqlQuery plan = new Lowerer(com.legend.lowering.PlatformRegistrations.catalogTable()).lower(resolved);
        return new DuckDb().render(plan);
    }

    @Test
    @DisplayName("outer temporal window survives an inner temporal query")
    void outerWindowSurvivesInnerQuery() {
        // the PROJECTED milestoned hop $a.p PROPAGATES the outer root
        // date through the chain specs — the exact channel the clobber
        // re-pointed at the inner fetch's frame
        String sql = sqlOf("|q::A.all(%2020-01-01)"
                + "->filter(a|$a.id->in(q::B.all(%2021-06-01)->map(b|$b.id)))"
                + "->project(~[n: a|$a.p.name])");
        // the OUTER extent filters on A's columns with A's date …
        assertTrue(sql.contains("2020-01-01"),
                "outer window date missing: " + sql);
        int outer = sql.indexOf("2020-01-01");
        assertTrue(sql.lastIndexOf("a_from", outer + 40) >= 0
                        && sql.substring(0, outer).contains("a_from")
                        || sql.substring(outer).contains("a_from")
                        || sql.contains("a_thru"),
                "A's window must ride A's milestoning columns: " + sql);
        // … and the INNER extent filters with B's date; the two never swap
        assertTrue(sql.contains("2021-06-01"),
                "inner window date missing: " + sql);
        assertTrue(windowUses(sql, "a_thru", "2020-01-01")
                        && windowUses(sql, "b_thru", "2021-06-01"),
                "window/date pairing swapped or lost: " + sql);
        // the propagated HOP window carries the OUTER date, never the
        // inner query's
        assertTrue(windowUses(sql, "p_thru", "2020-01-01"),
                "hop window must inherit the OUTER root date: " + sql);
        assertTrue(!windowUses(sql, "p_thru", "2021-06-01"),
                "hop window bled the INNER query's date: " + sql);
    }

    /** The comparison {@code <col> > DATE '<date>'}-ish adjacency: the
     * date literal appears within one predicate's reach of the column. */
    private static boolean windowUses(String sql, String col, String date) {
        for (int i = sql.indexOf(col); i >= 0; i = sql.indexOf(col, i + 1)) {
            int window = Math.min(sql.length(), i + 60);
            if (sql.substring(i, window).contains(date)) {
                return true;
            }
        }
        return false;
    }
}
