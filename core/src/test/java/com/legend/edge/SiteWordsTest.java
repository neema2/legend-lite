// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.Operand;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * H13 SPIKE (slice 1.2 of the compatibility program; scratch, never proposed for merge): A RAISE SITE'S OPERANDS ARE
 * TYPED DATA OF THE PLAN, AND ITS UPSTREAM WORDS ARE WORDED AFTER THE RUN. The record is one under every style; the
 * plan's JSON carries it natively (no codec); legend-pure's sentence comes from {@link PureMessages#worded} over the
 * raised site, exact over literals, the value withheld over a computed operand (A3). The file keeps S1's name so the
 * spike moves no build list.
 */
class SiteWordsTest {

    private static RaiseSite site(int n, RaiseKind kind, SqlExpr... operands) {
        return new RaiseSite(n, kind, null, List.of(operands));
    }

    @Test
    void aSitesRecordIsTheSameUnderEveryStyle_andCarriesNoWords() {
        SqlExpr column = new SqlExpr.Column("t", "c");
        List<RaiseSite> written = List.of(site(1, RaiseKind.DIVIDE_BY_ZERO, new SqlExpr.IntLit(5)),
                site(2, RaiseKind.DIVIDE_BY_ZERO, column));
        List<ExecutionPlan.Site> product = com.legend.planner.PlanMaker.sites(written,
                com.legend.database.PlanStyle.PRODUCT);
        assertEquals(List.of(new Operand.Int(5)), product.get(0).operands());
        assertEquals(List.of(Operand.COMPUTED), product.get(1).operands(), "a computed operand keeps its place alone");
        for (com.legend.database.PlanStyle edge : List.of(PureText.UPSTREAM, PureText.PURE_V1)) {
            assertEquals(product, com.legend.planner.PlanMaker.sites(written, edge));
        }
        // the formatter, after the run: exact over the literal, the value withheld over the computed one
        assertEquals(new PureMessages.Worded("Cannot divide 5 by zero", 0, 0),
                PureMessages.worded(RaiseKind.DIVIDE_BY_ZERO, product.get(0).operands()));
        assertEquals(new PureMessages.Worded("Cannot divide " + PureMessages.WITHHELD + " by zero", 1, 0),
                PureMessages.worded(RaiseKind.DIVIDE_BY_ZERO, product.get(1).operands()));
    }

    /** Every literal kind, and the computed marker, through the plan's JSON and back. */
    @Test
    void everyOperandKindSurvivesThePlansJson() {
        List<SqlExpr> operands = List.of(new SqlExpr.StringLit("a 'quoted' text"), new SqlExpr.IntLit(-7),
                new SqlExpr.FloatLit(-1.0), new SqlExpr.FloatLit(1e-7), new SqlExpr.FloatLit(0.1 + 0.2),
                new SqlExpr.DecimalLit(new java.math.BigDecimal("1.500")), new SqlExpr.BoolLit(true),
                new SqlExpr.DateLit("2014-02-29"), new SqlExpr.TimestampLit("2014-01-01 10:10:10.123"),
                new SqlExpr.Column("t", "c"), new SqlExpr.NullLit(), new SqlExpr.TimeLit("10:10:10"));
        List<ExecutionPlan.Site> planned = com.legend.planner.PlanMaker.sites(List.of(new RaiseSite(1,
                RaiseKind.VALUE_NOT_HELD, "somewhere", operands)), com.legend.database.PlanStyle.PRODUCT);
        assertEquals(List.of(new Operand.Text("a 'quoted' text"), new Operand.Int(-7), new Operand.Flt(-1.0),
                new Operand.Flt(1e-7), new Operand.Flt(0.1 + 0.2), new Operand.Dec(new java.math.BigDecimal("1.500")),
                new Operand.Bool(true), new Operand.Day("2014-02-29"), new Operand.Instant("2014-01-01 10:10:10.123"),
                Operand.COMPUTED, Operand.COMPUTED, Operand.COMPUTED), planned.get(0).operands());
        var ctx = com.legend.Compiler.compileModel(MODEL);
        ExecutionPlan plan = com.legend.Compiler.query(ctx, RAISING).executionPlan("s::RT", Set.of());
        ExecutionPlan.TdsResult root = (ExecutionPlan.TdsResult) plan.root();
        ExecutionPlan carrying = new ExecutionPlan(plan.parameters(), new ExecutionPlan.TdsResult(root.shape(),
                root.columns(), root.sql().raising(planned)), plan.writes());
        String written = PlanJson.write(carrying);
        assertTrue(written.contains("\"operands\":[{\"string\":\"a 'quoted' text\"},{\"integer\":-7},{\"float\":\"-1.0\"},"
                + "{\"float\":\"1.0E-7\"},{\"float\":\"0.30000000000000004\"},{\"decimal\":\"1.500\"},{\"boolean\":true},"
                + "{\"date\":\"2014-02-29\"},{\"dateTime\":\"2014-01-01 10:10:10.123\"},{\"computed\":true},"
                + "{\"computed\":true},{\"computed\":true}]"), written);
        assertEquals(carrying, PlanJson.read(written), "the plan read back whole, each operand of its kind");
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

    private static final String RAISING = "|#>{s::DB.T}#->extend(~r: r|$r.ID->rem($r.D))->select(~r)";

    /** A plan made in the edge's style and lite's plan of the same query declare the SAME sites, operands included;
     *  each survives its JSON with no codec; the hosts' site view is the plan's own sites. MEASURED here: what the
     *  operands add to lite's plan's JSON (the plan said "byte-identical but its version"; it is not). */
    @Test
    void theEdgesPlanAndLitesDeclareTheSameSites() {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        ExecutionPlan edge = com.legend.Compiler.query(ctx, RAISING).executionPlan("s::RT", Set.of(),
                PureText.UPSTREAM);
        ExecutionPlan lite = com.legend.Compiler.query(ctx, RAISING).executionPlan("s::RT", Set.of());
        List<ExecutionPlan.Site> sites = sites(edge.root());
        assertFalse(sites.isEmpty(), "the edge's plan raises a site");
        assertEquals(sites(lite.root()).stream().map(ExecutionPlan.Site::kind).toList(),
                sites.stream().map(ExecutionPlan.Site::kind).toList(), "the same raises, each a site, in both plans");
        assertEquals(sites(lite.root()).stream().map(ExecutionPlan.Site::operands).toList(),
                sites.stream().map(ExecutionPlan.Site::operands).toList(), "and the same operands");
        String written = PlanJson.write(edge);
        assertEquals(edge, PlanJson.read(written), "the edge's plan read back whole");
        assertFalse(written.contains("Cannot divide"), "no upstream sentence in the edge's plan: " + written);
        String litePlan = PlanJson.write(lite);
        assertEquals(lite, PlanJson.read(litePlan));
        assertTrue(litePlan.contains("\"operands\":[{\"computed\":true}]"), "lite's plan says its sites' operands: "
                + litePlan);
        assertTrue(edge.root() instanceof ExecutionPlan.TdsResult, "one statement answers: " + edge.root());
        String statement = ((ExecutionPlan.TdsResult) edge.root()).sql().statement();
        String view = com.legend.json.Json.toCompact(PlanJson.sitesOf(edge, statement, EdgeCodec.INSTANCE));
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
