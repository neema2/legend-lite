// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend;

import com.legend.executionplan.ExecutionPlan;
import com.legend.testcases.PlanCases;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A value root that is no primitive answers TYPED cells (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, phase 4: the text
 * carrier the database spelled for the PCT went with the TDS text): a class instance as its structure (its identity
 * fields kept, not shown), a Pair, a List, a Map, a Type's path, an {@code Any} value's own kind, an enum value's name
 * -- read by the plan's column facts and written here by lite's JSON writer on DuckDB.
 */
class StructuredValuesTest {
    private static final String MODEL = """
            Class my::P { name: String[1]; age: Integer[1]; when: StrictDate[0..1]; tags: String[*];
                          boss: my::P[0..1]; kind: my::E[0..1]; score: Decimal[0..1]; }
            Enum my::E { A, B }
            ###Relational
            Database s::DB ( Table T ( ID INTEGER PRIMARY KEY ) )
            ###Connection
            RelationalDatabaseConnection s::Conn
            {
                store: s::DB;
                type: DuckDB;
                specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID\\n2\\n'; };
                auth: DefaultH2;
            }
            ###Runtime
            Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
            """;

    @Test
    void everyFormAnswersTypedCells() throws Exception {
        Map<String, String> cases = new LinkedHashMap<>();
        // an instance: its declared properties, an absent one null (its identity fields dropped)
        cases.put("|^my::P(name='a', age=1)", "[{\"value\":{\"name\":\"a\",\"age\":1,\"when\":null,\"tags\":null,\"boss\":null,\"kind\":null,\"score\":null}}]");
        cases.put("|^my::P(name='a\\'b\\nc', age=1, when=%2024-01-02, tags=['x', 'y'], boss=^my::P(name='b', age=2),"
                + " kind=my::E.B, score=1.50D)", "[{\"value\":{\"name\":\"a'b\\nc\",\"age\":1,\"when\":\"2024-01-02\","
                + "\"tags\":[\"x\",\"y\"],\"boss\":{\"name\":\"b\",\"age\":2,\"when\":null,\"tags\":null,\"boss\":null,\"kind\":null,\"score\":null},\"kind\":\"B\",\"score\":1.50}}]");
        cases.put("|[^my::P(name='a', age=1), ^my::P(name='b', age=2)]", "[{\"value\":{\"name\":\"a\",\"age\":1,\"when\":null,\"tags\":null,\"boss\":null,\"kind\":null,\"score\":null}},{\"value\":{\"name\":\"b\",\"age\":2,\"when\":null,\"tags\":null,\"boss\":null,\"kind\":null,\"score\":null}}]");
        cases.put("|list([1, 2])", "[{\"value\":[1,2]}]");
        cases.put("|pair(1, 'a')", "[{\"value\":{\"first\":1,\"second\":\"a\"}}]");
        cases.put("|newMap([pair('k', 1)])", "[{\"value\":{\"k\":1}}]");
        // an Any collection: each element its own kind
        cases.put("|[1, 'a', %2015-01-01, 1.5, true]",
                "[{\"value\":1},{\"value\":\"a\"},{\"value\":\"2015-01-01\"},{\"value\":1.5},{\"value\":true}]");
        cases.put("|my::P", "[{\"value\":\"my::P\"}]");
        cases.put("|my::E.A", "[{\"value\":\"A\"}]");
        var ctx = Compiler.compileModel(MODEL);
        List<String> wrong = new java.util.ArrayList<>();
        for (Map.Entry<String, String> c : cases.entrySet()) {
            String cell;
            try (Connection conn = com.legend.testbackend.TestBackends.platform().open()) {
                ExecutionPlan plan = Compiler.query(ctx, c.getKey()).executionPlan("s::RT");
                com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, c.getKey());
                cell = PlanCases.write(plan, conn, Map.of(), PlanCases.Written.JSON::writer);
            } catch (RuntimeException e) {
                cell = "ERROR " + e.getMessage();
            }
            if (!cell.equals(c.getValue())) {
                wrong.add(c.getKey() + " writes " + cell.replace("\n", "\\n") + ", not " + c.getValue().replace("\n", "\\n"));
            }
        }
        assertEquals(List.of(), wrong);
    }
}
