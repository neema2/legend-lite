// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackends;
import com.legend.testing.Lean;

import java.sql.Connection;
import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * TWO PRIMITIVES OF DISJOINT KINDS ARE NEVER EQUAL (the compatibility program's translator spike, 2026-11-19):
 * legend-pure's equal compares a primitive's type before its value (EqualityUtilities.eq; CompiledSupport.equal: a
 * Number equals a Number only), and {@code a != b} is {@code not(equal(a, b))}, so {@code 'x' != -1} is true and
 * {@code 'x' == -1} false -- legend-engine's own testPlanForDateTimeVariableESTTimeZone compares a String with -1. Until
 * then lite sent the comparison to the database, and DuckDB and H2 refused the cast.
 */
class CrossKindEqualityTest {

    private static final String MODEL = """
            ###Pure
            Class p::Party { id: Integer[1]; name: String[0..1]; active: Boolean[0..1]; }
            ###Relational
            Database p::DB ( Table PARTY (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), ACTIVE BIT) )
            ###Mapping
            Mapping p::M ( p::Party: Relational { ~mainTable [p::DB] PARTY id: PARTY.ID, name: PARTY.NAME,
                active: PARTY.ACTIVE } )
            ###Connection
            RelationalDatabaseConnection p::C { store: p::DB; type: %s;
                specification: LocalH2 {
                    testDataSetupCSV: 'default\\nPARTY\\nID,NAME,ACTIVE\\n1,Meridian,true\\n2,---null---,---null---\\n'; };
                auth: DefaultH2; }
            ###Runtime
            Runtime p::RT { mappings: [p::M]; connections: [ p::DB: [ c0: p::C ] ]; }
            """;

    @Test
    void aStringAndAnIntegerAreNeverEqual() throws Exception {
        for (var backend : TestBackends.inProcess()) {
            String model = MODEL.formatted(backend.type());
            // the corpus's own spelled comparison, and its equality
            try (Connection c = backend.open()) {
                Lean.Result r = Lean.established(model, "|p::Party.all()->project(~[ne: p|'settlementDateTime = TIMESTAMP' != -1,"
                        + " eq: p|'x' == -1])->sort(~ne->ascending())->limit(1)", "p::RT", c);
                assertEquals(List.of(List.of(true, false)), r.rows(), backend.type() + ": " + r.text());
            }
            // a column against a value of another kind: a String[0..1] and an Integer[1] never equal, an empty name
            // included; a String[0..1] and a Boolean[0..1] equal only where both are empty
            try (Connection c = backend.open()) {
                Lean.Result r = Lean.established(model, "|p::Party.all()->project(~[id: p|$p.id, ne: p|$p.name != $p.id,"
                        + " both: p|$p.name == $p.active])->sort(~id->ascending())", "p::RT", c);
                assertEquals(List.of(List.of(1L, true, false), List.of(2L, true, true)), r.rows(),
                        backend.type() + ": " + r.text());
            }
        }
    }
}
