// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.graphql;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.exec.ExecutionResult;
import com.legend.server.QueryService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

/**
 * Aggregates (docs/GRAPHQL_STUDY_2026_10_18.md G6, §5.5; PR4; the relation node since 2026-10-21, §13.8, register
 * S118) over {@link GqlExecutionTest}'s rows: the schema's aggregate fields without a model member behind them, the
 * nested aggregate with and without a clause, the aggregate root as a relation. Acme: Alice 45/120k, Bob 32/90k, Carol 51/150k, Dan 28/60k; Globex: Eve
 * 39/110k, Frank 61/null; Initech: nobody; Grace 24/70k has no firm.
 */
class GqlAggregateTest {

    static final QueryService qs = new QueryService();

    static String gql(String document) throws SQLException {
        GqlExecutionTest.open();
        try {
            return GqlExecutionTest.gql(document);
        } finally {
            GqlExecutionTest.close();
        }
    }

    /** The schema offers an aggregate beside every to-many property (an association end's included); the model
     *  carries no member or class for it: the translator spells the relation node's table. */
    @Test
    void theAggregateFieldsHaveNoModelMembers() {
        ModelContext ctx = Compiler.compileModel(GqlExecutionTest.MODEL);
        assertTrue(ctx.findProperty("demo::Firm", "employeesAggregate").isEmpty(), "no generated member");
        String sdl = com.legend.compiler.graphql.SdlPrinter.print(com.legend.compiler.graphql.GqlIslands.schemaOf(ctx));
        assertTrue(sdl.contains("employeesAggregate(where: PersonWhere): PersonAggregate!"), sdl);
        assertTrue(sdl.contains("residentsAggregate(where: PersonWhere): PersonAggregate!"), sdl);
        assertTrue(sdl.contains("type PersonSum {"), sdl);
        assertTrue(!sdl.contains("companion"), sdl.substring(0, 400));
    }

    @Test
    void nestedAggregateWithoutAClause() throws SQLException {
        String json = gql("{ Firm(orderBy: [{ legalName: ASC }]) { legalName"
                + " employeesAggregate { count sum { salary } avg { age } max { salary } } } }");
        GqlExecutionTest.inOrder(json, "Acme", "Globex", "Initech");
        assertTrue(json.contains("\"count\":4") && json.contains("\"count\":2") && json.contains("\"count\":0"), json);
        assertTrue(json.contains("420000") && json.contains("110000"), json);
        assertTrue(json.contains("\"age\":39.0") || json.contains("\"age\":39"), "Acme's average age: " + json);
    }

    @Test
    void nestedAggregateWithAClause() throws SQLException {
        String json = gql("{ Firm(orderBy: [{ legalName: ASC }]) { legalName"
                + " seniors: employeesAggregate(where: { age: { gt: 40 } }) { count } } }");
        GqlExecutionTest.inOrder(json, "Acme", "\"seniors\"", "\"count\":2", "Globex", "\"count\":1", "Initech", "\"count\":0");
    }

    @Test
    void theAggregateRootIsARelation() throws SQLException {
        GqlExecutionTest.open();
        try {
            ExecutionResult r = qs.execute(GqlExecutionTest.MODEL,
                    "#GQL{ { PersonAggregate(where: { age: { gt: 30 } }) { count sum { salary } } } }#",
                    "demo::Runtime", GqlExecutionTest.conn);
            assertInstanceOf(ExecutionResult.Tabular.class, r, r.toString());
            assertEquals(1, r.rowCount(), r.toString());
            assertEquals(5L, ((Number) r.rows().get(0).get(0)).longValue(), r.toString());
            assertEquals(470000.0, ((Number) r.rows().get(0).get(1)).doubleValue(), 0.01, r.toString());
        } finally {
            GqlExecutionTest.close();
        }
    }
}
