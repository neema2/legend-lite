// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * A FAIL INSIDE AN IF, IN A PROJECTION (the PR #121 fixer's finding, 2026-11-19, FOLLOW_UPS §9): {@code fail()} is
 * Boolean[1] (legend-pure's fail.pure) and an if's type is its branches' best common type (legend-engine's Handlers:
 * {@code MostCommonType.mostCommon} over the two branches' return types), so {@code if(.., |fail(..), |$p.name)} is Any
 * on lite as upstream, and its column is Any. The row whose branch fails raises the user's message on every database;
 * until then DuckDB answered it null: DuckDB types {@code error()} NULL and {@code to_json} (the Any column's carrier)
 * binds a NULL-typed argument to the JSON null without evaluating it.
 */
class FailInIfProjectionTest {

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
                    testDataSetupCSV: 'default\\nPARTY\\nID,NAME\\n1,Meridian\\n2,Harbor\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    private static String query(int failing) {
        return "|p::Party.all()->project(~[name: p|if($p.id == " + failing + ", |fail('no such party'), |$p.name)])";
    }

    @Test
    void theFailingRowRaisesOnEveryDatabase() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            try (Connection c = backend.open()) {
                com.legend.error.DataError raised = assertThrows(com.legend.error.DataError.class,
                        () -> Lean.established(model, query(1), "p::RT", c), backend.type().toString());
                assertEquals("no such party", raised.getMessage(), backend.type().toString());
            }
        }
    }

    @Test
    void noRowFailingAnswersTheRowsTheColumnAnyAsUpstreamTypesIt() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            try (Connection c = backend.open()) {
                Lean.Result r = Lean.established(model, query(3), "p::RT", c);
                assertEquals(List.of("meta::pure::metamodel::type::Any"),
                        r.columns().stream().map(Lean.Column::type).toList(), backend.type() + ": " + r.text());
                assertEquals("[{\"name\":\"Meridian\"},{\"name\":\"Harbor\"}]", r.text().replace(" ", "")
                        .replace("\n", ""), backend.type().toString());
            }
        }
    }
}
