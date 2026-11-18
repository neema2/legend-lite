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
 * THE INTERIM'S SITES, COUNTED (phase D's Phase 2, raises as SITES, 2026-11-16: docs/records/d-phase2-raise-marker-2026-11-16.md,
 * "Interim, stated"): a raise whose upstream sentence needs a COMPUTED operand keeps legend-pure's sentence in SQL in an
 * edge plan ({@code edge.PureMessages#wordable} false), with no token and no site, until the user rules. Over a fixed
 * query set planned in the edge's style, this counts those raises and the ones the edge words at planning, so the
 * interim cannot grow silently: a new computed-operand raise moves the count, and the count moves only with a dated
 * reason here.
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

    /** {@code query} planned in a fresh recorder of the edge's style on DuckDB (planning alone, no database), and the
     *  words of its raise sites the recorder kept (K10b, 2026-11-17: the edge's records, read once after the planning). */
    private record Planned(ExecutionPlan plan, java.util.Map<String, java.util.Map<Integer, String>> words) {
    }

    private static Planned planned(String query) {
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
        com.legend.edge.SiteWordsRecorder recording = com.legend.edge.PureText.recording(
                com.legend.edge.PureText.UPSTREAM);
        ExecutionPlan plan = com.legend.Compiler.query(com.legend.Compiler.compileModel(model), query)
                .executionPlan("s::RT", java.util.Set.of(), recording);
        return new Planned(plan, recording.read());
    }

    @Test
    void theInterimsComputedOperandSitesDoNotGrow() {
        List<String> interim = new ArrayList<>();
        int worded = 0;
        int sites = 0;
        for (String query : QUERIES) {
            Planned planned;
            try {
                planned = planned(query);
            } catch (com.legend.sql.DialectCapability | com.legend.error.NotImplementedException
                     | com.legend.error.LegendCompileException refused) {
                interim.add("REFUSED " + query + ": " + refused.getMessage());
                continue;
            }
            for (ExecutionPlan.Sql sql : statements(planned.plan().root())) {
                sites += sql.sites().size();
                java.util.Set<Integer> worded_ = planned.words().getOrDefault(sql.statement(), java.util.Map.of())
                        .keySet();
                for (ExecutionPlan.Site site : sql.sites()) {
                    assertTrue(worded_.contains(site.n()), "the edge's recorder words every edge site: " + site);
                }
                if (sql.tree() == null) {
                    continue;
                }
                for (SqlExpr.RaiseText r : raises(sql.tree())) {
                    if (r.asData() != null || r.kind() == RaiseKind.USER_FAIL) {
                        continue;
                    }
                    if (com.legend.edge.PureMessages.wordable(r)) {
                        worded++;
                    } else {
                        interim.add(r.kind() + " in " + query);
                    }
                }
            }
        }
        // MEASURED 2026-11-16 (phase D's Phase 2): the computed-operand raises the edge still words in SQL over this
        // set. The count moves only with a dated reason; a ruling on the interim (the design note's §5.3) takes it to 0.
        assertEquals(INTERIM, interim.size(), String.join("\n", interim));
        assertTrue(worded > 0 && sites >= worded, "worded " + worded + ", sites " + sites);
    }

    /** The computed-operand raises over {@link #QUERIES}, as measured 2026-11-16. */
    private static final int INTERIM = 4;   // rem by a column and by 0 (its dividend a column), sqrt of a
                                            // literal (its operand cast), at by a column

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
