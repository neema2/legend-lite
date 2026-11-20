// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A MANY-VALUED PLAN PARAMETER IN AN {@code in(...)} on the plan-text route ({@code executionPlan(...)->planToString})
 * is refused by name and never printed. Legend-engine writes it as a FreeMarker conditional that splices the collection
 * into its own SQL text ({@code ${inFilterClause_names}}), and lite's SQL has no such splice (CP-4: legend-engine's SQL
 * spelling is not lite's). A2 of the compatibility program (stage A ratified by the user 2026-11-19) deleted the splice
 * and kept a refusal in {@code InProtocol.allNodeTexts}. Nothing pinned it (A2's reviewer's should, 2026-11-19), so
 * this test does, on the stage-A batch branch, at two levels:
 * <ul>
 *   <li>the route: what a request answers today. The plan-text lowering renders its statement as soon as it lowers it
 *       ({@code EnginePlans.engineSql}), and a declared parameter there is a template with no type, so the dialect
 *       refuses it as no whole list before the IN protocol runs (measured on the merged batch, 2026-11-19: the corpus's
 *       four such plan tests answer the same refusal on both lanes, and none reaches InProtocol's).</li>
 *   <li>the protocol: a conditional site handed to {@code allNodeTexts} is refused by its message. Taking the refusal
 *       out turns this case red (shown at the plant, 2026-11-19).</li>
 * </ul>
 */
class InCollectionSpliceRefusalTest {

    private static final String MODEL = """
            ###Pure
            Class q::Item { id: Integer[1]; label: String[1]; }
            ###Relational
            Database q::DB ( Table items (id INTEGER PRIMARY KEY, label VARCHAR(20)) )
            ###Mapping
            Mapping q::M ( q::Item: Relational { ~mainTable [q::DB]items id: [q::DB]items.id, label: [q::DB]items.label } )
            ###Connection
            RelationalDatabaseConnection q::Conn { store: q::DB; type: H2; specification: LocalH2 { }; auth: DefaultH2; }
            ###Runtime
            Runtime q::RT { mappings: [q::M]; connections: [ q::DB: [ c1: q::Conn ] ]; }
            """;

    /** A test connection (threshold 50, the engine's {@code getCollectionThresholdLimitForDatabaseType}), so the
     *  protocol runs; a String[*] parameter of unknown size is its conditional site. The shape is upstream's
     *  ({@code executionPlanTest.pure}'s {@code testFilterInWithResultSorcedFromAnExpression}: a class's
     *  {@code all()}, filtered by {@code in} over the parameter, projected, the plan's text asserted). */
    private static final String BODY = "|let plan = meta::pure::executionPlan::executionPlan({names: String[*] |"
            + " q::Item.all()->filter(i | $i.label->in($names))->project([i | $i.id], ['id'])}, q::M,"
            + " ^meta::core::runtime::Runtime(connectionStores = ^meta::core::runtime::ConnectionStore("
            + "element = q::DB, connection = ^meta::external::store::relational::runtime::TestDatabaseConnection("
            + "type = meta::relational::runtime::DatabaseType.H2))), []);\n"
            + "assertEquals('a plan', $plan->meta::pure::executionPlan::toString::planToString([]));";

    private static final String SPLICE = "a collection parameter spliced into plan text (names): lite's SQL has no"
            + " FreeMarker collection splice (legend-engine's SQL spelling is not lite's, CP-4)";

    @Test
    void theRouteRefusesACollectionParametersInByName() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            // the corpus's road for upstream's plan tests: the compatibility rules, the host judge
            Exception refused = assertThrows(Exception.class, () -> com.legend.testing.EdgeProgram.run(
                    TestBackends.platform().withStorelessRuntime(MODEL), BODY,
                    com.legend.testbackend.StorelessRuntime.RUNTIME, c));
            String chain = chain(refused);
            assertTrue(chain.contains("DialectCapability: plan parameter 'names' is not a list parameter (an array of a"
                    + " named element type): it is not bound as a whole list"), chain);
            assertFalse(chain.contains("inFilterClause"), "no splice's text printed: " + chain);
        }
    }

    @Test
    void theProtocolRefusesAConditionalSiteByName() {
        OutputCol label = new OutputCol("label", SqlType.Scalar.VARCHAR, true, OutputCol.Origin.PHYSICAL);
        SqlSource.Table items = new SqlSource.Table("items", "t0", List.of(label), false, java.util.Map.of(),
                com.legend.sql.NameQuoting.BARE);
        SqlExpr.PlanParam names = EngineParams.template("names", SqlExpr.PlanParam.ValueType.STRING, false);
        SqlSelect plan = new SqlSelect(List.of(new SqlSelect.Projection(SqlExpr.Column.of("t0", label), "label", null)),
                false, items, new SqlExpr.Call(SqlFn.IN, List.of(SqlExpr.Column.of("t0", label), names)), List.of(),
                null, null, List.of(), null, null, List.of(label));
        String conn = "TestDatabaseConnection(type = \"H2\")";
        InProtocol.Result sites = InProtocol.apply(plan, InProtocol.thresholdFor(conn, null), "");
        assertNotNull(sites, "a String[*] parameter as the whole in-list is a site under a test connection");
        assertEquals(1, sites.sites().size());
        assertTrue(sites.sites().get(0).conditional(), "a parameter of unknown size is the conditional form");
        Exception refused = assertThrows(com.legend.error.NotImplementedException.class,
                () -> InProtocol.allNodeTexts(sites, conn, null));
        assertEquals(SPLICE, refused.getMessage());
    }

    private static String chain(Throwable t) {
        StringBuilder out = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            out.append(c.getClass().getSimpleName()).append(": ").append(c.getMessage()).append(" | ");
        }
        return out.toString();
    }
}
