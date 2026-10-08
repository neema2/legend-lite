// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.SqlExpr;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The one parameter list: what a query's declared parameters are, as the lite plan and the legacy plan read them. */
class QueryParametersTest {

    private static final String MODEL = """
            Enum test::Status { ACTIVE, CLOSED }
            """;

    private static final ModelContext CTX = Compiler.compileModel(MODEL);

    /** The plan path's reading: the query lambda's own declarations. */
    private static List<QueryParameters.Declared> declared(String query) {
        return Compiler.query(CTX, query).parameters();
    }

    /** The legacy printer's reading: a typed lambda (the query here returns the lambda as its value). */
    private static List<QueryParameters.Declared> ofTypedLambda(String lambda) {
        return QueryParameters.of((TypedLambda) Compiler.query(CTX, "|" + lambda).expression());
    }

    @Test
    void bothReadingsAgree() {
        String lambda = "{s:test::Status[1], n:Integer[0..1], names:String[*]|$s}";
        assertEquals(declared(lambda), ofTypedLambda(lambda));
    }

    @Test
    void theLambdasParametersInOrder_withTheirTypesAndMultiplicities() {
        List<QueryParameters.Declared> ps = declared("{s:test::Status[1], n:Integer[0..1], names:String[*]|$s}");
        assertEquals(List.of("s", "n", "names"), ps.stream().map(QueryParameters.Declared::name).toList());
        assertEquals(List.of("test::Status[1]", "Integer[0..1]", "String[*]"),
                ps.stream().map(QueryParameters.Declared::signature).toList());
        assertFalse(ps.get(0).optional());
        assertTrue(ps.get(1).optional());
        assertFalse(ps.get(2).optional());
    }

    @Test
    void theLitePlansDeclaration_anEnumCarriesItsNames() {
        List<QueryParameters.Declared> ps = declared("{s:test::Status[1], n:Integer[0..1], names:String[*]|$s}");
        assertEquals(new ExecutionPlan.Parameter("s", "test::Status", new ExecutionPlan.Multiplicity(1, 1),
                List.of("ACTIVE", "CLOSED")), ps.get(0).declaration(CTX));
        assertEquals(new ExecutionPlan.Parameter("n", "Integer", new ExecutionPlan.Multiplicity(0, 1), List.of()),
                ps.get(1).declaration(CTX));
        assertEquals(new ExecutionPlan.Parameter("names", "String", new ExecutionPlan.Multiplicity(0, null), List.of()),
                ps.get(2).declaration(CTX));
    }

    @Test
    void theLegacyPlansTemplateParameter() {
        List<QueryParameters.Declared> ps = declared("{s:test::Status[1], n:Integer[0..1]|$s}");
        assertEquals(new SqlExpr.PlanParam("s", SqlExpr.PlanParam.Kind.ENUM, false, "enumMap_m_x"),
                ps.get(0).planParam("enumMap_m_x"));
        assertEquals(new SqlExpr.PlanParam("n", SqlExpr.PlanParam.Kind.OTHER, true, null), ps.get(1).planParam(null));
    }

    @Test
    void anUntypedParameterIsRefusedByName() {
        var refused = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> Compiler.query(CTX, "{x|$x}").parameters());
        assertTrue(refused.getMessage().contains("query parameter 'x' declares no type"), refused.getMessage());
    }

    @Test
    void aQueryWithNoParametersHasNone() {
        assertEquals(List.of(), declared("|1"));
    }
}
