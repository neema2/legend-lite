// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.dialect.RaiseSite;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AN UPSTREAM MODE'S WORDS OF A RAISE SITE, THE SITE'S OWN RECORD (K1 of the presentation program, S1, 2026-11-19;
 * until then the edge's recorder, one per planning call, kept them beside the plan): planned with the site by the
 * style's two-form {@code planner.SiteFacts}, statelessly; none in the product's plan; carried through the plan's JSON
 * by the edge's codec, the registered one or the one it hands the writer; one record a site.
 */
class SiteWordsTest {

    private static RaiseSite site(int n, RaiseKind kind, SqlExpr... operands) {
        return new RaiseSite(n, kind, null, List.of(operands));
    }

    @Test
    void theEdgesStylesWordASiteAtPlanning_theProductsNone() {
        List<RaiseSite> written = List.of(site(1, RaiseKind.DIVIDE_BY_ZERO));
        for (com.legend.database.PlanStyle edge : List.of(PureText.UPSTREAM, PureText.PURE_V1)) {
            ExecutionPlan.Site planned = com.legend.planner.PlanMaker.sites(written, edge).get(0);
            assertEquals(List.of(new SiteWords("Division by zero")), planned.extensions());
            assertEquals("Division by zero", SiteWords.of(planned));
            // the product's fields are lite's, whatever the style (K10)
            assertEquals(ExecutionPlan.Site.sentence(RaiseKind.DIVIDE_BY_ZERO, null), planned.message());
        }
        ExecutionPlan.Site product = com.legend.planner.PlanMaker.sites(written,
                com.legend.database.PlanStyle.PRODUCT).get(0);
        assertEquals(List.of(), product.extensions(), "lite's plan carries no upstream words");
        assertEquals(null, SiteWords.of(product));
    }

    @Test
    void aSiteCarriesOneRecordOfItsWords() {
        ExecutionPlan.Site twice = new ExecutionPlan.Site(1, RaiseKind.DIVIDE_BY_ZERO, null,
                ExecutionPlan.Site.sentence(RaiseKind.DIVIDE_BY_ZERO, null),
                List.of(new SiteWords("Division by zero"), new SiteWords("Division by zero")));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> SiteWords.of(twice));
        assertTrue(e.getMessage().contains("two records"), e.getMessage());
    }

    private static final String MODEL = """
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

    private static final String RAISING = "|#>{s::DB.T}#->extend(~r: r|$r.ID / $r.D)->select(~r)";

    /** A plan made in the edge's style: its site's words survive its JSON, written by the registered codec or the one
     *  the edge hands the writer (WebAssembly's loader finds none), each the same text; lite's plan of the same query
     *  carries none, and needs no codec (K2). */
    @Test
    void theWordsSurviveThePlansJson() {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        ExecutionPlan edge = com.legend.Compiler.query(ctx, RAISING).executionPlan("s::RT", Set.of(),
                PureText.UPSTREAM);
        List<ExecutionPlan.Site> sites = sites(edge.root());
        assertFalse(sites.isEmpty(), "the edge's plan raises a site");
        sites.forEach(s -> assertTrue(SiteWords.of(s) != null, "worded at planning: " + s));
        String written = PlanJson.write(edge);
        assertEquals(written, PlanJson.write(edge, EdgeCodec.INSTANCE), "the registered codec and the edge's agree");
        assertTrue(written.contains("\"extensions\":[{\"_type\":\"siteWords\",\"words\":"), written);
        assertEquals(edge, PlanJson.read(written), "the edge's plan read back whole");
        ExecutionPlan lite = com.legend.Compiler.query(ctx, RAISING).executionPlan("s::RT", Set.of());
        assertFalse(sites(lite.root()).isEmpty(), "lite's plan raises the same site");
        assertFalse(PlanJson.write(lite).contains("\"extensions\""), "lite's plan carries no edge data");
        // the hosts' site view: each site as the plan writes it, its words too (a host declares a statement's sites
        // from the view and from the plan carried beside it, and refuses two that differ), by either codec
        assertTrue(edge.root() instanceof ExecutionPlan.TdsResult, "one statement answers: " + edge.root());
        String statement = ((ExecutionPlan.TdsResult) edge.root()).sql().statement();
        String view = com.legend.json.Json.toCompact(PlanJson.sitesOf(edge, statement, EdgeCodec.INSTANCE));
        assertTrue(view.contains("\"extensions\""), view);
        assertTrue(written.contains("\"sites\":" + view), "the view is the plan's own sites: " + view);
    }

    private static List<ExecutionPlan.Site> sites(ExecutionPlan.Node node) {
        List<ExecutionPlan.Site> out = new ArrayList<>();
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> out.addAll(sites(step)));
            case ExecutionPlan.TdsResult t -> out.addAll(t.sql().sites());
            case ExecutionPlan.Keyed k -> {
                out.addAll(k.keys().sites());
                out.addAll(k.body().sites());
            }
            case ExecutionPlan.Update u -> out.addAll(u.sql().sites());
            case ExecutionPlan.Check c -> out.addAll(c.sql().sites());
            case ExecutionPlan.Effect f -> out.addAll(f.sql().sites());
            case ExecutionPlan.Load l -> {
            }
            case ExecutionPlan.Constant c -> {
            }
        }
        return out;
    }
}
