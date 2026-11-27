// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * A QUERY WHOSE ANSWER IS A COMPARISON OF TWO WHOLE SIDES (the edition program's step 6, the one equality,
 * 2026-11-27): each side is planned once as rows and the product's equality over two sides compares them
 * ({@code planner.RootComparison}, {@code lowering.SideEqualities}), the code a test's {@code assertEquals} is judged
 * by. So the answers below are the same on every in-process database: no list is compared as a value where a database
 * has no list equality, a computed empty list is empty, a Decimal is compared by its number, and a value whose type is
 * known only when the query runs is compared by the type it holds.
 */
class RootComparisonTest {

    private static final String MODEL = """
            ###Pure
            Class p::Party { id: Integer[1]; name: String[1]; }
            ###Relational
            Database p::DB ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(64)) )
            ###Mapping
            Mapping p::M ( p::Party: Relational { ~mainTable [p::DB] PARTY id: PARTY.ID, name: PARTY.NAME } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 {
                    testDataSetupCSV: 'default\\nPARTY\\nID,NAME\\n1,Meridian\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    private static Map<String, Boolean> cases() {
        Map<String, Boolean> out = new LinkedHashMap<>();
        // a computed list against a written one, in order
        out.put("|[1, 2]->filter(x | $x > 0) == [1, 2]", true);
        out.put("|[1, 2]->filter(x | $x > 1) == [1, 2]", false);
        out.put("|[2, 1]->filter(x | $x > 0) == [1, 2]", false);
        out.put("|!([1, 2]->filter(x | $x > 1) == [1, 2])", true);
        // a computed empty list is empty
        out.put("|[] == [1, 2]->filter(x | $x > 5)", true);
        out.put("|[1, 2]->filter(x | $x > 5) == [1, 2]->filter(x | $x > 7)", true);
        out.put("|[1] == [1, 2]->filter(x | $x > 5)", false);
        // one value against a list of one
        out.put("|1 == [1, 2]->filter(x | $x < 2)", true);
        out.put("|2 == [1, 2]->filter(x | $x < 2)", false);
        // a Decimal by its number, whatever its written scale
        out.put("|[1.0D, 10.125D]->filter(x | $x > 0D) == [1.000D, 10.125D]->filter(x | $x > 0D)", true);
        // strings and booleans
        out.put("|['a', 'b']->filter(x | $x != 'c') == ['a', 'b']", true);
        out.put("|[true, false]->filter(x | $x || true) == [true]", false);
        // a value typed only when the query runs is compared by the type it holds
        out.put("|[1, 2]->cast(@Number) == [1, 2]->cast(@Number)", true);
        out.put("|[1, 2]->cast(@Number) == [1.0, 2.0]->cast(@Number)", false);
        // two values of one known type stay the plain comparison
        out.put("|1 == 1", true);
        out.put("|'a' == 'b'", false);
        return out;
    }

    @Test
    void aComparisonOfTwoWholeSidesAnswersTheSameOnEveryDatabase() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            for (Map.Entry<String, Boolean> c : cases().entrySet()) {
                try (Connection connection = backend.open()) {
                    Lean.Result r = Lean.established(model, c.getKey(), "p::RT", connection);
                    assertEquals(List.of(List.of(c.getValue())), r.rows(), backend.type() + ": " + c.getKey() + "\n"
                            + r.text());
                }
            }
        }
    }
}
