// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.Compiler;
import com.legend.testcases.PlanCases;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A relation the statement reads more than once is one common table expression at its head ({@code
 * lowering.SharedRelations}, 2026-11-04) on every database that holds one, H2 too: H2 2.1 and 2.2 lost a bound value
 * inside a common table expression another one reads, and such a relation was written back at each read there
 * ({@link SharedRelationsInPlace}) until main's H2 2.5.252, which keeps it (2026-11-06: the statement below runs on it,
 * its value bound, and answers its rows). MongoDB's pipeline has none, and writes every shared relation in place.
 */
class SharedRelationsInPlaceTest {

    /** A let read twice: its relation joined to itself. */
    private static String model(String type) {
        return """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, AGE INTEGER ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: []; connections: [ s::DB: [ c1: s::Conn ] ]; }
                ###Pure
                function s::selfJoined(t: Relation<(ID:Integer, AGE:Integer)>[1]): Relation<(ID:Integer, AGE:Integer)>[1]
                {
                  let base = $t->filter(r|$r.AGE > 0);
                  $base->join($base->rename(~ID, ~ID2)->rename(~AGE, ~AGE2), meta::pure::functions::relation::JoinKind.INNER,
                              {a, b|$a.ID == $b.ID2})->select(~[ID, AGE]);
                }
                """.formatted(type);
    }

    private static String statement(String type, String query) {
        return Compiler.query(Compiler.compileModel(model(type)), query).executionPlan("s::RT").answerSql()
                .statement();
    }

    private static final String WITH_PARAMETER =
            "{min: Integer[1] | s::selfJoined(#>{s::DB.T}#->select(~[ID, AGE])->filter(r|$r.AGE > $min))}";

    @Test
    void aSharedRelationThatBindsAValueIsWrittenOnceOnH2AndAnswersItsRows() throws Exception {
        String h2 = statement("H2", WITH_PARAMETER);
        assertTrue(h2.startsWith("WITH __let0 AS ("), h2);
        assertEquals(1, h2.split("\\?", -1).length - 1, h2);
        var plan = Compiler.query(Compiler.compileModel(model("H2")), WITH_PARAMETER).executionPlan("s::RT");
        try (Connection c = DriverManager.getConnection("jdbc:h2:mem:shared_relations_in_place")) {
            try (var st = c.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, AGE INTEGER)");
                st.execute("INSERT INTO T VALUES (1, 10), (2, 20), (3, 30)");
            }
            // the rows over AGE > 15, each joined to itself: the bound value held inside the WITH its reads name
            String text = PlanCases.write(plan, c, Map.of("min", 15L), PlanCases.Written.CSV::writer);
            java.util.List<String> lines = java.util.List.of(text.split("\r\n"));
            assertEquals("ID,AGE", lines.get(0), text);
            assertEquals(java.util.List.of("2,20", "3,30"), lines.subList(1, lines.size()).stream().sorted().toList(),
                    text);
        }
    }

    @Test
    void elsewhereItIsWrittenOnce() {
        String duckDb = statement("DuckDB", WITH_PARAMETER);
        assertTrue(duckDb.startsWith("WITH __let0 AS ("), duckDb);
        assertEquals(1, duckDb.split("\\?", -1).length - 1, duckDb);
    }

    @Test
    void aSharedRelationThatBindsNothingStaysSharedOnH2() {
        String h2 = statement("H2", "|s::selfJoined(#>{s::DB.T}#->select(~[ID, AGE])->filter(r|$r.AGE > 1))");
        assertTrue(h2.contains("__let0"), h2);
    }
}
