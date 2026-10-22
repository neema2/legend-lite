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
 * The text carrier (docs/PLAN_PATH_PROGRAM_2026_10_17.md §4, step 3, 2026-10-18): on the TDS text output a value
 * root that is no primitive crosses as Pure's own literal grammar, spelled by the database -- a class instance
 * {@code ^my::P(...)} over its canonical layout (an absent property omitted, a to-many one bracketed, a nested
 * instance the same), a Pair, a List, a Map, a Type's path, an {@code Any} value's own literal -- which the reader
 * ({@code compileValueSpecification(text).result->reactivate()}, the PCT adapter) reads back as the value. A String
 * inside is a quoted literal with its escapes; a date its {@code %} form; a Decimal its {@code D}; an enum value
 * its path. The cells are judged by their text on DuckDB.
 */
class TextCarrierTest {
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
    void everyFormCrossesAsPuresOwnLiteralGrammar() throws Exception {
        Map<String, String> cases = new LinkedHashMap<>();
        cases.put("|^my::P(name='a', age=1)", "^my::P(name='a',age=1)");
        cases.put("|^my::P(name='a\\'b\\nc', age=1, when=%2024-01-02, tags=['x', 'y'], boss=^my::P(name='b', age=2),"
                + " kind=my::E.B, score=1.50D)",
                "^my::P(name='a\\'b\\nc',age=1,when=%2024-01-02,tags=['x', 'y'],boss=^my::P(name='b',age=2),"
                        + "kind=my::E.B,score=1.50D)");
        cases.put("|[^my::P(name='a', age=1), ^my::P(name='b', age=2)]", "^my::P(name='a',age=1)\n^my::P(name='b',age=2)");
        cases.put("|list([1, 2])", "list([1, 2])");
        cases.put("|pair(1, 'a')", "pair(1, 'a')");
        cases.put("|newMap([pair('k', 1)])", "newMap([pair('k', 1)])");
        cases.put("|[1, 'a', %2015-01-01, 1.5, true]", "1\n'a'\n%2015-01-01\n1.5\ntrue");
        cases.put("|my::P", "my::P");
        cases.put("|my::E.A", "A");   // an enumeration's value: its name, read by the enumeration the return type names
        var ctx = Compiler.compileModel(MODEL);
        List<String> wrong = new java.util.ArrayList<>();
        for (Map.Entry<String, String> c : cases.entrySet()) {
            String cell;
            try (Connection conn = com.legend.testbackend.TestBackends.platform().open()) {
                ExecutionPlan plan = Compiler.query(ctx, c.getKey()).executionPlan("s::RT", TypedQuery.Output.TDS_TEXT);
                com.legend.testcases.PlanRoundTrip.assertRoundTrips(plan, c.getKey());
                String text = PlanCases.run(plan, conn);
                cell = text.substring(text.indexOf('\n') + 1);
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
