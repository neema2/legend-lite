// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EVERY EDGE RAISE IS A SITE, ITS COMPUTED OPERANDS COUNTED (stage B's B1, errors as records, 2026-11-20; before it,
 * phase D's Phase 2 interim: a raise whose upstream sentence needed a computed operand kept legend-pure's sentence in
 * SQL, with no site). Over a fixed query set planned in the edge's style, every raise a statement writes is one of its
 * sites -- no sentence in SQL, on any door -- and legend-pure's sentence of each, worded after the run from the site's
 * record ({@code edge.PureMessages#worded}), is counted where it prints {@code <computed>} (a value the database
 * computed, register S389, the user's D1 of 2026-11-20 ~17:00), so that count cannot grow silently: it moves only with
 * a dated reason here.
 */
class InterimEdgeSitesTest {

    /** The fixed query set: each raising family a query reaches with a column's value or a literal. */
    private static final List<String> QUERIES = List.of(
            "|#>{s::DB.T}#->extend(~r: r|$r.ID / $r.D)->select(~r)",
            "|#>{s::DB.T}#->extend(~r: r|$r.ID->rem($r.D))->select(~r)",
            "|#>{s::DB.T}#->extend(~r: r|$r.ID->rem(0))->select(~r)",
            "|#>{s::DB.T}#->map(r|$r.ID)->toOne()",
            "|#>{s::DB.T}#->extend(~r: r|$r.ID->toString()->parseInteger())->select(~r)",
            "|#>{s::DB.T}#->extend(~r: r|$r.ID->bitShiftLeft(70))->select(~r)",
            "|#>{s::DB.T}#->extend(~r: r|sqrt(-1.0))->select(~r)",
            "|#>{s::DB.T}#->extend(~r: r|[1, 2, 3]->at($r.D))->select(~r)");

    /** {@code query} planned in the edge's style on DuckDB (planning alone, no database). */
    private static ExecutionPlan planned(String query) {
        String model = """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, D INTEGER NOT NULL ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: DuckDB;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,D\\n1,0\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """;
        return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query)
                .executionPlan("s::RT", java.util.Set.of(), com.legend.edge.PureText.UPSTREAM);
    }

    @Test
    void everyEdgeRaiseIsASite_andItsComputedOperandsDoNotGrow() {
        List<String> withheld = new ArrayList<>();
        int sites = 0;
        for (String query : QUERIES) {
            ExecutionPlan plan;
            try {
                plan = planned(query);
            } catch (com.legend.sql.DialectCapability | com.legend.error.NotImplementedException
                     | com.legend.error.LegendCompileException refused) {
                withheld.add("REFUSED " + query + ": " + refused.getMessage());
                continue;
            }
            for (ExecutionPlan.Sql sql : statements(plan.root())) {
                sites += sql.sites().size();
                if (sql.tree() != null) {
                    int raises = 0;
                    for (SqlExpr.RaiseText r : raises(sql.tree())) {
                        if (r.asData() == null && r.kind() != RaiseKind.USER_FAIL) {
                            raises++;
                        }
                    }
                    assertTrue(raises <= sql.sites().size(), "every raise is a site (" + raises + " raises, "
                            + sql.sites().size() + " sites): " + sql.statement());
                }
                for (ExecutionPlan.Site site : sql.sites()) {
                    String words = com.legend.edge.PureMessages.worded(site);
                    if (words.contains(com.legend.result.RaiseWords.COMPUTED)) {
                        withheld.add(site.kind() + " in " + query + ": " + words);
                    }
                }
            }
        }
        // MEASURED 2026-11-20 (stage B's B1): the sites whose legend-pure sentence prints a value the database computed
        // over this set. The count moves only with a dated reason.
        assertEquals(WITHHELD, withheld.size(), String.join("\n", withheld));
        assertTrue(sites > 0, "sites " + sites);
    }

    /** The sites over {@link #QUERIES} whose words print {@code <computed>} (D1, register S389), as measured 2026-11-20
     *  (stage B's B1): rem's dividend twice (a column), a relation's row count under toOne, at's offset (a column) --
     *  each a value only the database holds. A ceiling: it moves only down, or up with the user's word. */
    private static final int WITHHELD = 4;

    private static List<SqlExpr.RaiseText> raises(com.legend.sql.SqlQuery q) {
        List<SqlExpr.RaiseText> out = new ArrayList<>();
        new com.legend.sql.SqlRewriter() {
            @Override
            protected SqlExpr expr(SqlExpr e) {
                if (e instanceof SqlExpr.RaiseText r) {
                    out.add(r);
                }
                return e;
            }
        }.rewriteRoot(q);
        return out;
    }

    private static List<ExecutionPlan.Sql> statements(ExecutionPlan.Node node) {
        List<ExecutionPlan.Sql> out = new ArrayList<>();
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> out.addAll(statements(step)));
            case ExecutionPlan.TdsResult t -> out.add(t.sql());
            case ExecutionPlan.Keyed k -> {
                out.add(k.keys());
                out.add(k.body());
            }
            case ExecutionPlan.Update u -> out.add(u.sql());
            case ExecutionPlan.Check c -> out.add(c.sql());
            case ExecutionPlan.Effect f -> out.add(f.sql());
            case ExecutionPlan.Load l -> {
            }
            case ExecutionPlan.Constant c -> {
            }
        }
        return out;
    }
}
